package faux_tcp

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
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

// TestAckSemanticsWithHole 精确丢一个数据段，验证：
//  1. 接收端 ack 停在空洞处（语义自洽）
//  2. 不产生 dup ACK（空洞后的 ACK 数量 == 0）
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
	cli, srv, _, rec := testPair(t, nil, hook)

	for i := 0; i < 6; i++ {
		if _, err := cli.Write([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	// 找到被丢段的 seq 与前一个段的 end
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

	// 服务端（被动侧）在空洞后不应发出任何 ACK（无 dup ACK），
	// 且 ack 绝不超过空洞起点
	for _, p := range rec.snapshot() {
		if p.Src.Port == 8080 && p.Has(flagACK) && len(p.Payload) == 0 && !p.Has(flagSYN) {
			if seqAfter(p.Ack, holeSeq) {
				t.Fatalf("ack %d beyond hole %d", p.Ack, holeSeq)
			}
		}
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
	if err != errHandshakeTimeout {
		t.Fatalf("want handshake timeout, got %v", err)
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
