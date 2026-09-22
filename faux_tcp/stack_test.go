package faux_tcp

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lxt1045/errors"
)

// ---------------------------------------------------------------------------
// 测试基础设施：内存网络 + 报文记录器
// ---------------------------------------------------------------------------

var (
	testServerIP = [4]byte{10, 0, 0, 2}
	testClientIP = [4]byte{10, 0, 0, 1}
)

func testEndpoint(ip [4]byte, port uint16) Endpoint {
	return Endpoint{IP: netip.AddrFrom4(ip), Port: port}
}

// recorder 记录链路上每个方向的报文（解析后）
type recorder struct {
	mu   sync.Mutex
	pkts []*Packet
}

func (r *recorder) add(p *Packet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *p
	cp.Payload = append([]byte(nil), p.Payload...)
	r.pkts = append(r.pkts, &cp)
}

func (r *recorder) snapshot() []*Packet {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*Packet(nil), r.pkts...)
}

// testPair 在内存网络上建立一对 client/server 连接。
// hook 非 nil 时装到 memNet 上（丢包/乱序注入），recorder 始终记录。
func testPair(t *testing.T, mutateCfg func(*Config), hook func(src, dst [4]byte, bs []byte) bool) (
	cli, srv net.Conn, net0 *memNet, rec *recorder) {
	t.Helper()

	cfg := Config{}
	cfg.defaults()
	if mutateCfg != nil {
		mutateCfg(&cfg)
	}

	net0 = newMemNet()
	rec = &recorder{}
	net0.hook = func(src, dst [4]byte, bs []byte) bool {
		if p, err := parsePacket(bs); err == nil {
			rec.add(p)
		}
		if hook != nil {
			return hook(src, dst, bs)
		}
		return true
	}

	srvLink := net0.link(testServerIP)
	ln := listenWithLink(cfg, srvLink, testEndpoint(testServerIP, 8080))
	t.Cleanup(func() { ln.Close() })

	acceptCh := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			acceptCh <- c
		}
	}()

	cliLink := net0.link(testClientIP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli0, err := dialWithLink(ctx, cfg, cliLink,
		testEndpoint(testClientIP, 40000), testEndpoint(testServerIP, 8080))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { cli0.Close() })

	select {
	case srv = <-acceptCh:
	case <-time.After(3 * time.Second):
		t.Fatal("accept timeout")
	}
	return cli0, srv, net0, rec
}

// waitFor 轮询条件直到超时
func waitFor(t *testing.T, d time.Duration, f func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

// ---------------------------------------------------------------------------
// 构包/解包
// ---------------------------------------------------------------------------

func TestPacketRoundTrip(t *testing.T) {
	cfg := Config{}
	cfg.defaults()
	src := testEndpoint(testClientIP, 1234)
	dst := testEndpoint(testServerIP, 8080)
	payload := []byte("hello faux tcp")

	bs := buildPacket(&cfg, src, dst, 1000, 2000, flagPSH|flagACK, 111, 222, payload, 7)
	p, err := parsePacket(bs)
	if err != nil {
		t.Fatal(err)
	}
	if p.Src != src || p.Dst != dst {
		t.Fatalf("endpoints: %v %v", p.Src, p.Dst)
	}
	if p.Seq != 1000 || p.Ack != 2000 {
		t.Fatalf("seq/ack: %d %d", p.Seq, p.Ack)
	}
	if !p.Has(flagPSH) || !p.Has(flagACK) || p.Has(flagSYN) {
		t.Fatalf("flags: %x", p.Flags)
	}
	if p.TSval != 111 || p.TSecr != 222 {
		t.Fatalf("ts: %d %d", p.TSval, p.TSecr)
	}
	if string(p.Payload) != string(payload) {
		t.Fatalf("payload: %q", p.Payload)
	}

	// SYN 选项布局（MSS/SACK/TS/WS）
	bs = buildPacket(&cfg, src, dst, 0, 0, flagSYN, 111, 0, nil, 8)
	p, err = parsePacket(bs)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Has(flagSYN) || p.TSval != 111 {
		t.Fatalf("syn: flags=%x ts=%d", p.Flags, p.TSval)
	}
}

// ---------------------------------------------------------------------------
// 握手 / 数据 / 挥手
// ---------------------------------------------------------------------------

func TestHandshakeAndEcho(t *testing.T) {
	cli, srv, _, rec := testPair(t, nil, nil)

	// 验证三次握手报文序列
	waitFor(t, time.Second, func() bool { return len(rec.snapshot()) >= 3 }, "3 handshake packets")
	pkts := rec.snapshot()
	if !pkts[0].Has(flagSYN) || pkts[0].Has(flagACK) {
		t.Fatalf("pkt0 should be SYN: %x", pkts[0].Flags)
	}
	if !pkts[1].Has(flagSYN) || !pkts[1].Has(flagACK) {
		t.Fatalf("pkt1 should be SYN+ACK: %x", pkts[1].Flags)
	}
	if pkts[1].Ack != pkts[0].Seq+1 {
		t.Fatalf("SYN+ACK ack mismatch: %d != %d", pkts[1].Ack, pkts[0].Seq+1)
	}
	if !pkts[2].Has(flagACK) || pkts[2].Has(flagSYN) || len(pkts[2].Payload) > 0 {
		t.Fatalf("pkt2 should be ACK: %x", pkts[2].Flags)
	}

	// 双向 echo
	msg := []byte("hello")
	if _, err := cli.Write(msg); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := srv.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("srv read: %q %v", buf[:n], err)
	}
	if _, err := srv.Write([]byte("world")); err != nil {
		t.Fatal(err)
	}
	n, err = cli.Read(buf)
	if err != nil || string(buf[:n]) != "world" {
		t.Fatalf("cli read: %q %v", buf[:n], err)
	}

	// 数据段特征：PSH+ACK，seq 单调
	for _, p := range rec.snapshot() {
		if len(p.Payload) > 0 {
			if !p.Has(flagPSH) || !p.Has(flagACK) {
				t.Fatalf("data segment should be PSH+ACK: %x", p.Flags)
			}
		}
	}
}

func TestCloseHandshake(t *testing.T) {
	cli, srv, _, rec := testPair(t, nil, nil)

	if _, err := cli.Write([]byte("bye")); err != nil {
		t.Fatal(err)
	}
	if err := cli.Close(); err != nil {
		t.Fatal(err)
	}

	// 服务端先读到残留数据，再读到 EOF
	buf := make([]byte, 64)
	n, err := srv.Read(buf)
	if err != nil || string(buf[:n]) != "bye" {
		t.Fatalf("srv read: %q %v", buf[:n], err)
	}
	if _, err = srv.Read(buf); err == nil {
		t.Fatal("srv should see EOF")
	}

	// 服务端也关闭 → 双端最终都进入 closed
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return cli.(*Conn).c.getState() == stClosed && srv.(*Conn).c.getState() == stClosed
	}, "both closed")

	// 挥手报文完整：应至少看到两个 FIN
	finCount := 0
	for _, p := range rec.snapshot() {
		if p.Has(flagFIN) {
			finCount++
		}
	}
	if finCount < 2 {
		t.Fatalf("want >=2 FIN packets, got %d", finCount)
	}
}

// ---------------------------------------------------------------------------
// 丢包 / 乱序 / 零重传
// ---------------------------------------------------------------------------

// TestNoRetransmitUnderLoss 注入 20% 丢包，验证：
//  1. 发送端每个数据 seq 全程只出现一次（零重传）
//  2. 接收端能收到未丢失的全部数据（空洞直投上层）
func TestNoRetransmitUnderLoss(t *testing.T) {
	var count int64
	var mu sync.Mutex
	hook := func(src, dst [4]byte, bs []byte) bool {
		mu.Lock()
		defer mu.Unlock()
		count++
		// 只丢 client→server 方向的数据段（不动握手/纯 ACK）
		if src == testClientIP && count%5 == 0 {
			if p, err := parsePacket(bs); err == nil && len(p.Payload) > 0 {
				return false
			}
		}
		return true
	}
	cli, srv, _, rec := testPair(t, nil, hook)

	const total = 50
	for i := 0; i < total; i++ {
		if _, err := cli.Write([]byte(fmt.Sprintf("msg-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	// 接收端读出未丢失的数据
	got := make(map[string]bool)
	buf := make([]byte, 64)
	for {
		srv.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := srv.Read(buf)
		if err != nil {
			break
		}
		got[string(buf[:n])] = true
	}
	if len(got) == 0 || len(got) >= total {
		t.Fatalf("got %d messages, want 0 < n < %d", len(got), total)
	}

	// 零重传：client→server 方向数据段 seq 无重复
	seen := make(map[uint32]int)
	for _, p := range rec.snapshot() {
		if p.Src.IP.As4() == testClientIP && len(p.Payload) > 0 {
			seen[p.Seq]++
		}
	}
	for seq, cnt := range seen {
		if cnt > 1 {
			t.Fatalf("seq %d retransmitted %d times", seq, cnt)
		}
	}
}

// TestAckSemanticsWithHole 精确丢一个数据段（关闭愈合定时器），验证：
//  1. 接收端 ack 停在空洞处（语义自洽）
//  2. 空洞段触发 dup ACK + SACK（仿真实 Linux 接收端），但 ack 绝不越过空洞
func TestAckSemanticsWithHole(t *testing.T) {
	dropped := false
	var dataCount int
	hook := func(src, dst [4]byte, bs []byte) bool {
		if src != testClientIP {
			return true
		}
		p, err := parsePacket(bs)
		if err != nil || len(p.Payload) == 0 {
			return true
		}
		dataCount++
		if dataCount == 3 && !dropped {
			dropped = true
			return false // 丢掉第 3 个数据段
		}
		return true
	}
	// HealDelay 调大：本用例只验证愈合前的 dup ACK/SACK 外观（愈合见 TestHoleHeal）
	cli, srv, _, rec := testPair(t, func(c *Config) { c.HealDelay = time.Hour }, hook)

	for i := 0; i < 6; i++ {
		if _, err := cli.Write([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	// 找到被丢段的 seq
	var cliSeqs []uint32
	for _, p := range rec.snapshot() {
		if p.Src.IP.As4() == testClientIP && len(p.Payload) > 0 {
			cliSeqs = append(cliSeqs, p.Seq)
		}
	}
	if len(cliSeqs) != 6 {
		t.Fatalf("want 6 data segments, got %d", len(cliSeqs))
	}
	holeSeq := cliSeqs[2] // 第 3 个段（0 起）

	// 服务端（被动侧）应发出 dup ACK + SACK（ack == 空洞起点），
	// 且 ack 绝不超过空洞起点
	dupAck, withSack := 0, 0
	for _, p := range rec.snapshot() {
		if p.Src.Port == 8080 && p.Flags == flagACK && len(p.Payload) == 0 {
			if seqAfter(p.Ack, holeSeq) {
				t.Fatalf("ack %d beyond hole %d", p.Ack, holeSeq)
			}
			if p.Ack == holeSeq {
				dupAck++
				if len(p.SACK) > 0 {
					withSack++
				}
			}
		}
	}
	if dupAck == 0 || withSack == 0 {
		t.Fatalf("want dup ACK+SACK on hole, got dupAck=%d withSack=%d", dupAck, withSack)
	}

	// 服务端仍应收到空洞后的数据（直投上层）
	got := 0
	buf := make([]byte, 64)
	srv.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	for {
		n, err := srv.Read(buf)
		if err != nil {
			break
		}
		_ = n
		got++
	}
	if got != 5 {
		t.Fatalf("got %d messages, want 5 (6 minus 1 dropped)", got)
	}
}

// TestReorder 乱序投递：先发后发先到，数据都应送达
func TestReorder(t *testing.T) {
	var mu sync.Mutex
	var held []byte // 暂存第一个数据段
	withheld := false
	hook := func(src, dst [4]byte, bs []byte) bool {
		if src != testClientIP {
			return true
		}
		mu.Lock()
		defer mu.Unlock()
		p, err := parsePacket(bs)
		if err != nil || len(p.Payload) == 0 {
			return true
		}
		if string(p.Payload) == "first" && !withheld {
			withheld = true // 只暂扣一次（补投时不再拦截）
			held = append([]byte(nil), bs...)
			return false
		}
		return true
	}
	cli, srv, net0, _ := testPair(t, nil, hook)

	for _, m := range []string{"first", "second", "third"} {
		if _, err := cli.Write([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	// 补投暂扣的包（乱序到达）——注意不能在持锁时调用 deliver（会经 hook 重入 mu）
	mu.Lock()
	bs := held
	held = nil
	mu.Unlock()
	if bs != nil {
		net0.deliver(testClientIP, testServerIP, bs)
	}

	got := make(map[string]bool)
	buf := make([]byte, 64)
	srv.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	for {
		n, err := srv.Read(buf)
		if err != nil {
			break
		}
		got[string(buf[:n])] = true
	}
	if !got["first"] || !got["second"] || !got["third"] {
		t.Fatalf("got %v", got)
	}
}

// ---------------------------------------------------------------------------
// 握手重试 / 保活 / RST
// ---------------------------------------------------------------------------

// TestHandshakeRetry SYN 丢失后客户端按真实 TCP 行为重试（同序号）
func TestHandshakeRetry(t *testing.T) {
	droppedFirstSyn := false
	cfg := Config{HandshakeTimeout: 50 * time.Millisecond, HandshakeRetries: 3}
	cfg.defaults()

	net0 := newMemNet()
	// 丢掉第一个 SYN
	var seenSyn bool
	net0.hook = func(src, dst [4]byte, bs []byte) bool {
		p, err := parsePacket(bs)
		if err != nil {
			return true
		}
		if p.Has(flagSYN) && !p.Has(flagACK) && !seenSyn {
			seenSyn = true
			droppedFirstSyn = true
			return false
		}
		return true
	}

	srvLink := net0.link(testServerIP)
	ln := listenWithLink(cfg, srvLink, testEndpoint(testServerIP, 8080))
	defer ln.Close()
	go func() { ln.Accept() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli, err := dialWithLink(ctx, cfg, net0.link(testClientIP),
		testEndpoint(testClientIP, 40000), testEndpoint(testServerIP, 8080))
	if err != nil {
		t.Fatalf("dial should succeed after SYN retry: %v", err)
	}
	defer cli.Close()
	if !droppedFirstSyn {
		t.Fatal("first SYN was not dropped")
	}
}

// TestKeepalive 空闲连接定期发裸 ACK 保活
func TestKeepalive(t *testing.T) {
	cli, _, _, rec := testPair(t, func(c *Config) { c.KeepAlive = 50 * time.Millisecond }, nil)
	defer cli.Close()

	// 空闲 300ms，应观察到多个裸 ACK（PSH=0, 无载荷）
	time.Sleep(300 * time.Millisecond)
	ka := 0
	for _, p := range rec.snapshot() {
		if p.Src.Port == 40000 && len(p.Payload) == 0 &&
			p.Has(flagACK) && !p.Has(flagSYN) && !p.Has(flagFIN) {
			ka++
		}
	}
	if ka < 2 {
		t.Fatalf("want >=2 keepalive ACKs, got %d", ka)
	}
}

// TestRstToUnknownPort 向未监听端口发数据段应收到 RST。
// RFC 793：入段带 ACK 时回 RST（seq=入段.ack）；不带 ACK 时回 RST+ACK（ack=入段.seq+载荷长）。
func TestRstToUnknownPort(t *testing.T) {
	net0 := newMemNet()
	cfg := Config{}
	cfg.defaults()

	// 服务端只监听 8080；客户端"连接"到 9999（无监听）
	srvLink := net0.link(testServerIP)
	ln := listenWithLink(cfg, srvLink, testEndpoint(testServerIP, 8080))
	defer ln.Close()

	cliLink := net0.link(testClientIP)
	cliEP := testEndpoint(testClientIP, 40000)
	srvEP := testEndpoint(testServerIP, 9999)

	readPkt := func() chan *Packet {
		ch := make(chan *Packet, 1)
		go func() {
			if bs, err := cliLink.ReadPacket(); err == nil {
				if p, err := parsePacket(bs); err == nil {
					ch <- p
				}
			}
		}()
		return ch
	}

	// 不带 ACK 的数据段 → 期望 RST+ACK，ack=1000+1
	bs := buildPacket(&cfg, cliEP, srvEP, 1000, 0, flagPSH, clockMS(), 0, []byte("x"), 1)
	if err := cliLink.WritePacket(bs); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-readPkt():
		if !p.Has(flagRST) || !p.Has(flagACK) {
			t.Fatalf("want RST+ACK, got flags=%x", p.Flags)
		}
		if p.Ack != 1001 {
			t.Fatalf("RST ack = %d, want 1001", p.Ack)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no RST received")
	}

	// 带 ACK 的数据段 → 期望纯 RST（seq=入段 ack=555）
	bs = buildPacket(&cfg, cliEP, srvEP, 2000, 555, flagPSH|flagACK, clockMS(), 0, []byte("x"), 2)
	if err := cliLink.WritePacket(bs); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-readPkt():
		if !p.Has(flagRST) || p.Has(flagACK) {
			t.Fatalf("want plain RST, got flags=%x", p.Flags)
		}
		if p.Seq != 555 {
			t.Fatalf("RST seq = %d, want 555", p.Seq)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no RST received")
	}
}

// TestDialTimeout 对端不存在时握手超时
func TestDialTimeout(t *testing.T) {
	cfg := Config{HandshakeTimeout: 50 * time.Millisecond, HandshakeRetries: 2}
	cfg.defaults()
	net0 := newMemNet()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := dialWithLink(ctx, cfg, net0.link(testClientIP),
		testEndpoint(testClientIP, 40000), testEndpoint(testServerIP, 9999))
	if c := errors.AsCode(err); c == nil || c.Code() != ErrHandshakeTimeout.Code() {
		t.Fatalf("want handshake timeout, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// delayed ACK / 空洞愈合 / 保活探测 / 会话回收
// ---------------------------------------------------------------------------

// TestHoleHeal 空洞逾 HealDelay 未愈时执行"虚拟重传愈合"：
// ack 越过空洞跳变（线上呈现快速重传恢复外观），数据照常全部投递
func TestHoleHeal(t *testing.T) {
	dropped := false
	var dataCount int
	hook := func(src, dst [4]byte, bs []byte) bool {
		if src != testClientIP {
			return true
		}
		p, err := parsePacket(bs)
		if err != nil || len(p.Payload) == 0 {
			return true
		}
		dataCount++
		if dataCount == 3 && !dropped {
			dropped = true
			return false
		}
		return true
	}
	cli, srv, _, rec := testPair(t, func(c *Config) { c.HealDelay = 50 * time.Millisecond }, hook)

	for i := 0; i < 6; i++ {
		if _, err := cli.Write([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Millisecond)
	}

	var cliSeqs []uint32
	for _, p := range rec.snapshot() {
		if p.Src.IP.As4() == testClientIP && len(p.Payload) > 0 {
			cliSeqs = append(cliSeqs, p.Seq)
		}
	}
	if len(cliSeqs) != 6 {
		t.Fatalf("want 6 data segments, got %d", len(cliSeqs))
	}
	lastEnd := cliSeqs[5] + uint32(len("msg-5"))

	// 愈合：服务端的累计确认最终越过空洞，推进到最后一段的末端
	waitFor(t, 2*time.Second, func() bool {
		for _, p := range rec.snapshot() {
			if p.Src.Port == 8080 && p.Flags == flagACK && p.Ack == lastEnd {
				return true
			}
		}
		return false
	}, "healed cumulative ack reaching last segment end")

	// 数据仍全部投递（丢的那条由上层负责，本层只见 5 条）
	got := 0
	buf := make([]byte, 64)
	srv.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	for {
		if _, err := srv.Read(buf); err != nil {
			break
		}
		got++
	}
	if got != 5 {
		t.Fatalf("got %d messages, want 5", got)
	}
}

// TestDelayedAck 验证 delayed ACK：连续两个数据包只回一个 ACK；
// 单个数据包的 ACK 在 ~40ms 冲刷前不出现
func TestDelayedAck(t *testing.T) {
	cli, _, _, rec := testPair(t, nil, nil)

	srvPureAcks := func() []*Packet {
		var out []*Packet
		for _, p := range rec.snapshot() {
			if p.Src.Port == 8080 && p.Flags == flagACK && len(p.Payload) == 0 {
				out = append(out, p)
			}
		}
		return out
	}
	cliDataEnds := func() []uint32 {
		var out []uint32
		for _, p := range rec.snapshot() {
			if p.Src.IP.As4() == testClientIP && len(p.Payload) > 0 {
				out = append(out, p.Seq+uint32(len(p.Payload)))
			}
		}
		return out
	}

	// 两个包快速连发 → 恰好一个立即 ACK（覆盖到第二个包末端）
	if _, err := cli.Write([]byte("aaaa")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := cli.Write([]byte("bbbb")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return len(srvPureAcks()) >= 1 }, "first immediate ack")
	time.Sleep(80 * time.Millisecond) // 超过 40ms 冲刷窗口：不应再有针对这两个包的 ACK
	if n := len(srvPureAcks()); n != 1 {
		t.Fatalf("2 quick packets should yield exactly 1 delayed ack, got %d", n)
	}
	ends := cliDataEnds()
	if len(ends) != 2 || srvPureAcks()[0].Ack != ends[1] {
		t.Fatalf("ack should cover 2nd packet end: acks=%v ends=%v", srvPureAcks()[0].Ack, ends)
	}

	// 单个包：短时间内无 ACK，40ms 冲刷后出现
	if _, err := cli.Write([]byte("cc")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	for _, p := range srvPureAcks() {
		if p.Ack == ends[1]+2 {
			t.Fatal("single packet acked before 40ms flush window")
		}
	}
	waitFor(t, time.Second, func() bool {
		for _, p := range srvPureAcks() {
			if p.Ack == ends[1]+2 {
				return true
			}
		}
		return false
	}, "single packet ack after 40ms flush")
}

// TestKeepaliveProbe 空闲连接发标准 TCP keepalive 探测包（seq=sndNxt-1 纯 ACK），
// 对端按规则应答（ack=rcvNxt 的纯 ACK）。
// 注意：应答也会更新本端 lastSent，两端同间隔保活会互相"顶"开探测时点
// （真实 TCP 亦然），所以断言双向合计，且窗口给足余量。
func TestKeepaliveProbe(t *testing.T) {
	cli, srv, _, rec := testPair(t, func(c *Config) { c.KeepAlive = 50 * time.Millisecond }, nil)
	defer cli.Close()

	// 无任何数据流量，空闲 300ms
	time.Sleep(300 * time.Millisecond)

	c, s := cli.(*Conn).c, srv.(*Conn).c
	c.mu.Lock()
	cSnd := c.sndNxt
	c.mu.Unlock()
	s.mu.Lock()
	sSnd := s.sndNxt
	s.mu.Unlock()

	probes, replies := 0, 0
	for _, p := range rec.snapshot() {
		if p.Flags != flagACK || len(p.Payload) > 0 {
			continue
		}
		switch p.Src.Port {
		case 40000:
			if p.Seq == cSnd-1 { // 客户端探测包
				probes++
			}
			if p.Seq == cSnd && p.Ack == sSnd { // 客户端对服务端探测的应答
				replies++
			}
		case 8080:
			if p.Seq == sSnd-1 {
				probes++
			}
			if p.Seq == sSnd && p.Ack == cSnd {
				replies++
			}
		}
	}
	if probes < 2 {
		t.Fatalf("want >=2 keepalive probes (both directions), got %d", probes)
	}
	// ≥2：排除握手 ACK（cli→srv 的第三包也匹配 replies 形状）
	if replies < 2 {
		t.Fatalf("peer never replied to keepalive probe, replies=%d", replies)
	}
}

// TestPeerDeathReap 对端静默死亡（所有回包消失）后，3 个保活周期回收连接
func TestPeerDeathReap(t *testing.T) {
	var dropServerReply atomic.Bool
	hook := func(src, dst [4]byte, bs []byte) bool {
		if dropServerReply.Load() && src == testServerIP {
			return false // 对端死亡：服务端方向全部消失
		}
		return true
	}
	cli, _, _, _ := testPair(t, func(c *Config) { c.KeepAlive = 40 * time.Millisecond }, hook)

	dropServerReply.Store(true)
	c := cli.(*Conn).c
	waitFor(t, 3*time.Second, func() bool { return c.getState() == stClosed }, "peer death reap")
	e, _ := c.err.Load().(error)
	if code := errors.AsCode(e); code == nil || code.Code() != ErrPeerTimeout.Code() {
		t.Fatalf("want ErrPeerTimeout, got %v", e)
	}
}

// TestHalfOpenReap 只发 SYN 不回 ACK 的半开连接（SYN 扫描）应被超时回收
func TestHalfOpenReap(t *testing.T) {
	cfg := Config{
		HandshakeTimeout: 30 * time.Millisecond,
		HandshakeRetries: 1,                     // 半开窗口 = 60ms
		KeepAlive:        40 * time.Millisecond, // 扫描 tick = 20ms
	}
	cfg.defaults()

	net0 := newMemNet()
	ln := listenWithLink(cfg, net0.link(testServerIP), testEndpoint(testServerIP, 8080))
	defer ln.Close()
	go func() { ln.Accept() }()

	// 手工构造一个 SYN（不回 SYNACK 的 ACK）
	cliEP := testEndpoint(testClientIP, 40000)
	syn := buildPacket(&cfg, cliEP, testEndpoint(testServerIP, 8080),
		12345, 0, flagSYN, clockMS(), 0, nil, 1)
	if err := net0.link(testClientIP).WritePacket(syn); err != nil {
		t.Fatal(err)
	}

	connCount := func() int {
		ln.d.mu.Lock()
		defer ln.d.mu.Unlock()
		return len(ln.d.conns)
	}
	waitFor(t, time.Second, func() bool { return connCount() == 1 }, "half-open session created")
	waitFor(t, 2*time.Second, func() bool { return connCount() == 0 }, "half-open session reaped")
}

// TestHalfCloseWrite 对端先 FIN：本端读到 EOF 后仍可写（半关闭），
// 本端再 Close 后完成四次挥手
func TestHalfCloseWrite(t *testing.T) {
	cli, srv, _, _ := testPair(t, nil, nil)

	// 服务端先关闭
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	// 客户端读到 EOF
	buf := make([]byte, 64)
	cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := cli.Read(buf); err != io.EOF {
		t.Fatalf("want EOF after peer FIN, got %v", err)
	}
	// 半关闭：客户端仍可写，服务端（stFinWait）仍能收
	if _, err := cli.Write([]byte("still-alive")); err != nil {
		t.Fatalf("write in half-close: %v", err)
	}
	srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := srv.Read(buf)
	if err != nil || string(buf[:n]) != "still-alive" {
		t.Fatalf("srv read in fin-wait: %q %v", buf[:n], err)
	}
	// 客户端关闭 → 四次挥手完成，双端 closed
	if err := cli.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return cli.(*Conn).c.getState() == stClosed && srv.(*Conn).c.getState() == stClosed
	}, "both closed after 4-way handshake")
}

// ---------------------------------------------------------------------------
// seq 回绕 / 出站队列背压
// ---------------------------------------------------------------------------

// TestSeqWrapAround 序号回绕（4GB/连接处）安全性：
// 正序推进、空洞判定、dup ACK、愈合跳变都必须用 int32 差值法而非直接比大小。
func TestSeqWrapAround(t *testing.T) {
	_, srv, net0, rec := testPair(t, func(c *Config) { c.HealDelay = 40 * time.Millisecond }, nil)

	sc := srv.(*Conn).c
	base := uint32(0xFFFFFFF0) // 变量（非 const）：后续 base+16/24 需要在运行时回绕
	sc.mu.Lock()
	sc.rcvNxt = base
	sc.mu.Unlock()

	cfg := Config{}
	cfg.defaults()
	cliEP := testEndpoint(testClientIP, 40000)
	srvEP := testEndpoint(testServerIP, 8080)
	deliver := func(seq uint32, payload string, ipID uint16) {
		t.Helper()
		bs := buildPacket(&cfg, cliEP, srvEP, seq, 0, flagPSH|flagACK,
			clockMS(), 0, []byte(payload), ipID)
		net0.deliver(testClientIP, testServerIP, bs)
	}
	rcvNxt := func() uint32 {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		return sc.rcvNxt
	}

	// 1) 回绕前的正序段：seq=base, 8 字节 → rcvNxt=base+8
	deliver(base, "AAAAAAAA", 1)
	waitFor(t, time.Second, func() bool { return rcvNxt() == base+8 }, "in-order advance before wrap")

	// 2) 越过回绕点的空洞段：seq=base+16（已回绕为 0x00000000）→ dup ACK 停在 base+8
	deliver(base+16, "CCCCCCCC", 2)
	waitFor(t, time.Second, func() bool {
		for _, p := range rec.snapshot() {
			if p.Src.Port == 8080 && p.Flags == flagACK && p.Ack == base+8 {
				return true
			}
		}
		return false
	}, "dup ack held at hole (wrap-safe)")
	if got := rcvNxt(); got != base+8 {
		t.Fatalf("hole must not advance rcvNxt: got %#x want %#x", got, base+8)
	}

	// 3) 愈合：rcvNxt 跳到空洞末端 base+24（回绕后 0x00000008）
	waitFor(t, 2*time.Second, func() bool { return rcvNxt() == base+24 }, "heal across wrap boundary")

	// 4) 愈合后回绕点之后的段按正序继续推进
	deliver(base+24, "DDDD", 3)
	waitFor(t, time.Second, func() bool { return rcvNxt() == base+28 }, "post-wrap in-order advance")

	// 数据全部投递（A、C、D；B 本就没发）
	got := map[string]bool{}
	buf := make([]byte, 64)
	srv.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	for {
		n, err := srv.Read(buf)
		if err != nil {
			break
		}
		got[string(buf[:n])] = true
	}
	for _, want := range []string{"AAAAAAAA", "CCCCCCCC", "DDDD"} {
		if !got[want] {
			t.Fatalf("wrap data lost: want %q in %v", want, got)
		}
	}
}

// slowLink 可在建连后切换为"阻塞写"的链路包装，用于背压测试
type slowLink struct {
	Link
	blocked atomic.Bool
	release chan struct{}
}

func (s *slowLink) WritePacket(bs []byte) error {
	if s.blocked.Load() {
		<-s.release
	}
	return s.Link.WritePacket(bs)
}

// TestWriteBackpressure 出站队列满时 Write 按写超时返回 ErrWriteTimeout，
// 链路恢复后写恢复正常且队列按序发出
func TestWriteBackpressure(t *testing.T) {
	cfg := Config{}
	cfg.defaults()

	net0 := newMemNet()
	ln := listenWithLink(cfg, net0.link(testServerIP), testEndpoint(testServerIP, 8080))
	defer ln.Close()
	recvCh := make(chan net.Conn, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			recvCh <- c
		}
	}()

	slow := &slowLink{Link: net0.link(testClientIP), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := dialWithLink(ctx, cfg, slow,
		testEndpoint(testClientIP, 40000), testEndpoint(testServerIP, 8080))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()
	var srv net.Conn
	select {
	case srv = <-recvCh:
	case <-time.After(3 * time.Second):
		t.Fatal("accept timeout")
	}
	defer srv.Close()
	defer func() {
		slow.blocked.Store(false)
		select {
		case <-slow.release:
		default:
			close(slow.release)
		}
	}()

	// 堵住链路：writeLoop 卡在第一个报文上，队列很快填满
	slow.blocked.Store(true)
	cli.SetWriteDeadline(time.Now().Add(150 * time.Millisecond))
	var werr error
	sent := 0
	for i := 0; i < outQueueCap+64; i++ {
		if _, err := cli.Write(make([]byte, 64)); err != nil {
			werr = err
			break
		}
		sent++
	}
	if werr == nil {
		t.Fatal("write should time out when queue is full and link blocked")
	}
	if code := errors.AsCode(werr); code == nil || code.Code() != ErrWriteTimeout.Code() {
		t.Fatalf("want ErrWriteTimeout, got %v", werr)
	}
	if sent == 0 {
		t.Fatal("no packet was accepted before backpressure")
	}

	// 恢复链路：出站队列排空后写恢复正常
	slow.blocked.Store(false)
	close(slow.release)
	waitFor(t, 5*time.Second, func() bool {
		return len(cli.(*Conn).c.outCh) == 0
	}, "out queue drained after link unblocked")
	cli.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := cli.Write([]byte("after-unblock")); err != nil {
		t.Fatalf("write after unblock: %v", err)
	}
	// 服务端能读到数据（链路确实恢复）
	srv.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 2048)
	if _, err := srv.Read(buf); err != nil {
		t.Fatalf("read after unblock: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 基准 / 上层集成
// ---------------------------------------------------------------------------

// BenchmarkThroughput 内存链路上的吞吐基准（构包+状态机开销）
func BenchmarkThroughput(b *testing.B) {
	cfg := Config{}
	cfg.defaults()
	net0 := newMemNet()
	ln := listenWithLink(cfg, net0.link(testServerIP), testEndpoint(testServerIP, 8080))
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 2048)
		for {
			if _, err := c.Read(buf); err != nil {
				return
			}
		}
	}()
	ctx := context.Background()
	cli, err := dialWithLink(ctx, cfg, net0.link(testClientIP),
		testEndpoint(testClientIP, 40000), testEndpoint(testServerIP, 8080))
	if err != nil {
		b.Fatal(err)
	}
	defer cli.Close()

	payload := make([]byte, cfg.MSS)
	b.SetBytes(int64(cfg.MSS))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
}

// TestSynAckEchoesTimestamp 服务端 SYN+ACK 必须回显客户端 SYN 的 TSval。
// 真实 Linux 一定回显；置 0 会被部分状态化 NAT/防火墙判为无效而丢弃
// （真机实测服务端 SYN+ACK 一直是 ecr 0，是本次修复的动机）。
func TestSynAckEchoesTimestamp(t *testing.T) {
	cli, _, _, rec := testPair(t, nil, nil)
	_ = cli
	waitFor(t, time.Second, func() bool { return len(rec.snapshot()) >= 2 }, "handshake packets")
	pkts := rec.snapshot()
	if pkts[0].TSval == 0 {
		t.Fatal("client SYN should carry TSval")
	}
	if !pkts[1].Has(flagSYN) || !pkts[1].Has(flagACK) {
		t.Fatalf("pkt1 should be SYN+ACK, got flags=%x", pkts[1].Flags)
	}
	if pkts[1].TSecr != pkts[0].TSval {
		t.Fatalf("SYN+ACK TSecr=%d, want echo of SYN TSval=%d", pkts[1].TSecr, pkts[0].TSval)
	}
}
