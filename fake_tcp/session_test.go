package fake_tcp

import (
	"bytes"
	"context"
	"io"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lxt1045/errors"
)

// session_test.go：基于内存管道 LinkIO 的状态机测试。
// pipe 不做报文序列化（Segment 直传），因此可无机权限制地完整测试 RawTCP 会话语义。

// pipeLink 内存管道 LinkIO：两个端点互连，WriteSegment 的报文出现在对端 recv
type pipeLink struct {
	send chan *Segment
	recv chan *Segment

	maxPayload int // MaxPayload() 返回值（默认 1300，测试可覆盖）

	closed   atomic.Bool
	closeCh  chan struct{}
	closeOne sync.Once

	// 测试钩子：记录发出的报文；drop 过滤器返回 true 则丢包（模拟丢包）
	mu      sync.Mutex
	written []Segment
	drop    func(seg *Segment) bool
}

func pipePair() (a, b *pipeLink) {
	ab := make(chan *Segment, 4096)
	ba := make(chan *Segment, 4096)
	a = &pipeLink{send: ab, recv: ba, closeCh: make(chan struct{}), maxPayload: 1300}
	b = &pipeLink{send: ba, recv: ab, closeCh: make(chan struct{}), maxPayload: 1300}
	return a, b
}

func (p *pipeLink) ReadSegment() (*Segment, error) {
	select {
	case seg := <-p.recv:
		return seg, nil
	case <-p.closeCh:
		return nil, ErrConnClosed.New()
	}
}

func (p *pipeLink) WriteSegment(seg *Segment) error {
	if p.closed.Load() {
		return ErrConnClosed.New()
	}
	// 深拷贝（模拟真实链路的序列化语义：调用方缓冲可能被复用）
	cp := *seg
	if seg.Payload != nil {
		cp.Payload = append([]byte(nil), seg.Payload...)
	}
	p.mu.Lock()
	p.written = append(p.written, cp)
	drop := p.drop
	p.mu.Unlock()
	if drop != nil && drop(&cp) {
		return nil
	}
	select {
	case p.send <- &cp:
		return nil
	case <-p.closeCh:
		return ErrConnClosed.New()
	}
}

func (p *pipeLink) MaxPayload() int {
	if p.maxPayload > 0 {
		return p.maxPayload
	}
	return 1300
}

func (p *pipeLink) Close() error {
	p.closeOne.Do(func() {
		p.closed.Store(true)
		close(p.closeCh)
	})
	return nil
}

// writtenSnapshot 已发报文快照
func (p *pipeLink) writtenSnapshot() []Segment {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Segment(nil), p.written...)
}

// setDrop 设置丢包过滤器（返回 true 丢弃）
func (p *pipeLink) setDrop(f func(seg *Segment) bool) {
	p.mu.Lock()
	p.drop = f
	p.mu.Unlock()
}

// pipeTestEnv 一组 pipe 互联的 c/s
type pipeTestEnv struct {
	ctx    context.Context
	cancel context.CancelFunc

	srvLink *pipeLink
	cliLink *pipeLink
	l       *Listener
	cli     *Conn
	srv     *Conn

	cliPeer PeerAddr
	srvPeer PeerAddr
}

func newPipeTestEnv(t *testing.T, mutateCfg func(*Config)) *pipeTestEnv {
	t.Helper()
	cfg := Config{
		Mode:             ModeRawTCP,
		MTU:              1400,
		Keepalive:        time.Hour, // 测试默认关闭保活干扰，需要的用例自行调小
		HandshakeRetries: 3,
		// 大于测试总量对应的分段数（10MB/1300≈8050），保证管道测试零丢包、确定性
		RecvQueue: 16384,
	}
	if mutateCfg != nil {
		mutateCfg(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srvLink, cliLink := pipePair()
	env := &pipeTestEnv{
		ctx:     ctx,
		cancel:  cancel,
		srvLink: srvLink,
		cliLink: cliLink,
		cliPeer: PeerAddr{IP: netip.MustParseAddr("10.1.1.2"), Port: 54321},
		srvPeer: PeerAddr{IP: netip.MustParseAddr("10.1.1.1"), Port: 8443},
	}
	env.l = listenLink(ctx, cfg, srvLink, env.srvPeer, nil)

	conn, err := dialLink(ctx, cfg, cliLink, env.cliPeer, env.srvPeer, nil)
	if err != nil {
		cancel()
		t.Fatalf("dialLink: %v", err)
	}
	env.cli = conn

	// 等服务端 Accept（dialLink 成功即握手完成，Accept 应立即返回）
	lc, err := env.l.Accept()
	if err != nil {
		cancel()
		t.Fatalf("Accept: %v", err)
	}
	env.srv = lc
	t.Cleanup(func() {
		_ = env.l.Close()
		_ = cliLink.Close()
		_ = srvLink.Close()
		cancel()
	})
	return env
}

// TestPipeEndToEnd 管道全双工 10MB 字节级哈希校验（有序无损链路上的确定性锚点）
func TestPipeEndToEnd(t *testing.T) {
	env := newPipeTestEnv(t, nil)

	const total = 10 << 20
	up := make([]byte, total)
	down := make([]byte, total)
	for i := range up {
		up[i] = byte(i * 31)
		down[i] = byte(i * 17)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // c→s
		defer wg.Done()
		for sent := 0; sent < total; {
			n, err := env.cli.Write(up[sent:])
			if err != nil {
				t.Errorf("cli.Write: %v", err)
				return
			}
			sent += n
		}
	}()
	go func() { // s→c
		defer wg.Done()
		for sent := 0; sent < total; {
			n, err := env.srv.Write(down[sent:])
			if err != nil {
				t.Errorf("srv.Write: %v", err)
				return
			}
			sent += n
		}
	}()

	readExact := func(c *Conn, want []byte) bool {
		got := make([]byte, 0, total)
		buf := make([]byte, 64*1024)
		for len(got) < total {
			n, err := c.Read(buf)
			if err != nil {
				t.Errorf("Read: %v", err)
				return false
			}
			got = append(got, buf[:n]...)
		}
		return bytes.Equal(got, want)
	}
	okUp := readExact(env.srv, up)
	okDown := readExact(env.cli, down)
	wg.Wait()
	if !okUp || !okDown {
		t.Fatalf("字节级校验失败: up=%v down=%v", okUp, okDown)
	}
}

// TestSessionHandshakeRawTCP 三次握手的报文序列与 seq/ack 关系（plan.md §4.2）
func TestSessionHandshakeRawTCP(t *testing.T) {
	env := newPipeTestEnv(t, nil)

	cliWire := env.cliLink.writtenSnapshot()
	srvWire := env.srvLink.writtenSnapshot()
	if len(cliWire) != 2 || len(srvWire) != 1 {
		t.Fatalf("握手报文数不符: cli=%d srv=%d", len(cliWire), len(srvWire))
	}
	syn, synack, ack := cliWire[0], srvWire[0], cliWire[1]
	if syn.Flags != FlagSYN {
		t.Fatalf("第 1 包应为 SYN: %#x", syn.Flags)
	}
	if synack.Flags != FlagSYN|FlagACK {
		t.Fatalf("第 2 包应为 SYN|ACK: %#x", synack.Flags)
	}
	if ack.Flags != FlagACK {
		t.Fatalf("第 3 包应为 ACK: %#x", ack.Flags)
	}
	if synack.Ack != syn.Seq+1 {
		t.Fatalf("SYNACK.ack 应为 SYN.seq+1: %d != %d", synack.Ack, syn.Seq+1)
	}
	if ack.Seq != syn.Seq+1 || ack.Ack != synack.Seq+1 {
		t.Fatalf("ACK 序号不符: seq=%d(want %d) ack=%d(want %d)", ack.Seq, syn.Seq+1, ack.Ack, synack.Seq+1)
	}
	if syn.TSecr != 0 {
		t.Fatalf("SYN 的 TSecr 应为 0")
	}
	if synack.TSecr != syn.TSval {
		t.Fatalf("SYNACK 应回显客户端 TSval: %d != %d", synack.TSecr, syn.TSval)
	}
}

// TestSessionDataRawTCP 数据传输：切片、seq 连续性、双向、delayed ACK（plan.md §4.3）
func TestSessionDataRawTCP(t *testing.T) {
	env := newPipeTestEnv(t, nil)

	// 客户端 → 服务端 3000 字节（1300 切片 → 3 段）
	const total = 3000
	data := make([]byte, total)
	for i := range data {
		data[i] = byte(i)
	}
	if n, err := env.cli.Write(data); err != nil || n != total {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	got := make([]byte, 0, total)
	buf := make([]byte, 1024)
	for len(got) < total {
		n, err := env.srv.Read(buf)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got = append(got, buf[:n]...)
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("数据不符 at %d", i)
		}
	}

	// seq 连续性：客户端数据段 seq 严格相接
	cliWire := env.cliLink.writtenSnapshot()
	var dataSegs []Segment
	for _, s := range cliWire {
		if len(s.Payload) > 0 {
			dataSegs = append(dataSegs, s)
		}
	}
	if len(dataSegs) != 3 {
		t.Fatalf("应为 3 个数据段，实际 %d", len(dataSegs))
	}
	for i := 1; i < len(dataSegs); i++ {
		prev := dataSegs[i-1]
		if dataSegs[i].Seq != prev.Seq+uint32(len(prev.Payload)) {
			t.Fatalf("seq 不连续: seg%d seq=%d, 上一段 end=%d", i, dataSegs[i].Seq, prev.Seq+uint32(len(prev.Payload)))
		}
		if dataSegs[i].Flags != FlagPSH|FlagACK {
			t.Fatalf("数据段应为 PSH|ACK: %#x", dataSegs[i].Flags)
		}
	}

	// 反向传输 500 字节
	back := make([]byte, 500)
	for i := range back {
		back[i] = byte(i * 7)
	}
	if _, err := env.srv.Write(back); err != nil {
		t.Fatalf("srv.Write: %v", err)
	}
	rgot := make([]byte, 0, 500)
	for len(rgot) < 500 {
		n, err := env.cli.Read(buf)
		if err != nil {
			t.Fatalf("cli.Read: %v", err)
		}
		rgot = append(rgot, buf[:n]...)
	}
	for i := range back {
		if rgot[i] != back[i] {
			t.Fatalf("反向数据不符 at %d", i)
		}
	}

	// delayed ACK：3 个数据段应至少触发 1 个纯 ACK（每 2 个包一次）
	srvWire := env.srvLink.writtenSnapshot()
	ackCount := 0
	for _, s := range srvWire {
		if s.Flags == FlagACK && len(s.Payload) == 0 {
			ackCount++
		}
	}
	if ackCount == 0 {
		t.Fatalf("服务端未回纯 ACK（delayed ACK 应至少触发一次）")
	}
}

// TestSessionGapDupLoss RawTCP 收包规则（plan.md §4.3 表格）：空洞照收、位图合并、重复丢弃
func TestSessionGapDupLoss(t *testing.T) {
	env := newPipeTestEnv(t, nil)

	// 找到服务端 session，直接注入报文（绕过 pipe，精准控制 seq）
	var srvSess *session
	env.l.sessions.Range(func(_, v any) bool {
		srvSess = v.(*session)
		return false
	})
	if srvSess == nil {
		t.Fatalf("服务端会话不存在")
	}
	base := srvSess.rcvNxt.Load()
	peer := PeerAddr{IP: netip.MustParseAddr("10.1.1.2"), Port: 54321}

	inject := func(seq uint32, payload string) {
		srvSess.handle(env.ctx, &Segment{
			Peer:    peer,
			Flags:   FlagPSH | FlagACK,
			Seq:     seq,
			Ack:     srvSess.sndNxt.Load(),
			TSval:   1,
			Payload: []byte(payload),
		})
	}

	// 空洞注入：seq 跳过 "AAAA"（4 字节）先发 "BBBB"
	inject(base+4, "BBBB")
	// 重复段（位图命中）：应丢弃
	inject(base+4, "BBBB")
	// 填补空洞："AAAA"
	inject(base, "AAAA")
	// 连续段
	inject(base+8, "CCCC")
	// 完全重复的旧段（end <= rcvNxt）：应丢弃
	inject(base, "AAAA")

	// 到达即交：读到的顺序应是 BBBB AAAA CCCC（空洞段先交，补洞段后交）
	want := []string{"BBBB", "AAAA", "CCCC"}
	buf := make([]byte, 4)
	for i, w := range want {
		n, err := env.srv.Read(buf)
		if err != nil || n != 4 || string(buf[:n]) != w {
			t.Fatalf("第 %d 块: n=%d err=%v data=%q want %q", i, n, err, buf[:n], w)
		}
	}

	// rcvNxt 应推进到 base+12
	if got := srvSess.rcvNxt.Load(); got != base+12 {
		t.Fatalf("rcvNxt=%d, want %d", got, base+12)
	}
	// 位图应已清空（空洞被填平）
	srvSess.bitmapMu.Lock()
	blen := len(srvSess.bitmap)
	srvSess.bitmapMu.Unlock()
	if blen != 0 {
		t.Fatalf("位图应为空，实际 %d", blen)
	}
}

// TestSessionCloseRawTCP 关闭流程：FIN|ACK → FIN|ACK → ACK（合并段的三段式四次挥手），读侧 EOF
func TestSessionCloseRawTCP(t *testing.T) {
	env := newPipeTestEnv(t, nil)

	if err := env.cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 服务端读侧应收到 EOF（读超时不算，继续等到 EOF 或总超时）
	buf := make([]byte, 16)
	deadline := time.Now().Add(3 * time.Second)
	for {
		_ = env.srv.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := env.srv.Read(buf)
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.AsCode(err) == nil || errors.AsCode(err).Code() != ErrTimeout.Code() {
			t.Fatalf("服务端读错误: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("服务端未收到 EOF")
		}
	}

	// 等双方 teardown
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cnt := 0
		env.l.sessions.Range(func(_, _ any) bool { cnt++; return true })
		if cnt == 0 && env.cli.sess.isClosed() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cnt := 0
	env.l.sessions.Range(func(_, _ any) bool { cnt++; return true })
	if cnt != 0 || !env.cli.sess.isClosed() {
		t.Fatalf("会话未全部关闭: srv 剩余 %d, cli closed=%v", cnt, env.cli.sess.isClosed())
	}

	// 报文序列：FIN|ACK（客户端）→ FIN|ACK（服务端）→ ACK（客户端）
	cliWire := env.cliLink.writtenSnapshot()
	srvWire := env.srvLink.writtenSnapshot()
	var srvFin bool
	for _, s := range srvWire {
		if s.Flags == FlagFIN|FlagACK {
			srvFin = true
		}
	}
	var cliFin, cliAck bool
	for _, s := range cliWire {
		if s.Flags == FlagFIN|FlagACK {
			cliFin = true
			continue
		}
		if s.Flags == FlagACK && cliFin { // FIN 之后的纯 ACK = 挥手最后一击
			cliAck = true
		}
	}
	if !cliFin || !srvFin || !cliAck {
		t.Fatalf("关闭报文序列不完整: cliFIN=%v srvFIN=%v cliFinalACK=%v", cliFin, srvFin, cliAck)
	}
}

// TestSessionReadTimeout 读超时
func TestSessionReadTimeout(t *testing.T) {
	env := newPipeTestEnv(t, nil)
	_ = env.cli.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	start := time.Now()
	_, err := env.cli.Read(make([]byte, 16))
	if err == nil {
		t.Fatalf("应返回超时错误")
	}
	if errors.AsCode(err) == nil || errors.AsCode(err).Code() != ErrTimeout.Code() {
		t.Fatalf("应为 ErrTimeout: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("超时未生效")
	}
}

// TestSessionHalfOpenReap 半开连接被保活扫描回收
func TestSessionHalfOpenReap(t *testing.T) {
	cfg := Config{
		Mode:             ModeRawTCP,
		Keepalive:        100 * time.Millisecond,
		HandshakeRetries: 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srvLink, cliLink := pipePair()
	defer srvLink.Close()
	defer cliLink.Close()
	l := listenLink(ctx, cfg, srvLink, PeerAddr{IP: netip.MustParseAddr("10.2.2.1"), Port: 8443}, nil)
	defer l.Close()

	// 客户端只发 SYN 后消失（不发最后一个 ACK）
	sess := newSession(ctx, cfg, cliLink,
		PeerAddr{IP: netip.MustParseAddr("10.2.2.2"), Port: 54321},
		PeerAddr{IP: netip.MustParseAddr("10.2.2.1"), Port: 8443}, 0)
	sess.sndNxt.Store(1000)
	if err := sess.sendSeg(FlagSYN, nil); err != nil {
		t.Fatalf("SYN: %v", err)
	}

	// 服务端应建立 SynReceived 会话
	deadline := time.Now().Add(2 * time.Second)
	for {
		cnt := 0
		l.sessions.Range(func(_, _ any) bool { cnt++; return true })
		if cnt > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("服务端未创建会话")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 等保活扫描回收（半开超时 = (retries+1)*1s）
	deadline = time.Now().Add(5 * time.Second)
	for {
		cnt := 0
		l.sessions.Range(func(_, _ any) bool { cnt++; return true })
		if cnt == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("半开会话未被回收")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
