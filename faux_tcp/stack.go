package faux_tcp

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/utils/log"
)

// state TCP 状态机的状态
type state int32

const (
	stSynSent     state = iota + 1 // 主动侧：已发 SYN
	stSynRcvd                      // 被动侧：已收 SYN，已发 SYN+ACK
	stEstablished                  // 已建立
	stFinWait                      // 本端已发 FIN（主动关闭方），等对端 FIN
	stCloseWait                    // 对端已发 FIN，本端未关（半关闭，仍可写）
	stClosing                      // stCloseWait 后本端也发了 FIN，等最后的 ACK
	stClosed
)

// 错误码见 errno.go；此处不再定义裸 error 哨兵。

// seqAfter 判断 a 是否在 b 之后（处理序号回绕）
func seqAfter(a, b uint32) bool { return int32(a-b) > 0 }

// ---------------------------------------------------------------------------
// demux：链路收包分发器
// ---------------------------------------------------------------------------

// connKey 连接四元组中用于路由的部分（本地端口 + 对端端点）
type connKey struct {
	localPort  uint16
	remoteIP   [4]byte
	remotePort uint16
}

// demux 拥有一条链路的读循环，把 inbound 报文分发给连接或监听器。
type demux struct {
	cfg  Config
	link Link

	mu      sync.Mutex
	conns   map[connKey]*fconn
	ln      *Listener // 非 nil 时表示有监听器
	done    chan struct{}
	once    sync.Once
	onClose func(c *fconn) // 连接关闭回调（listener 清理用）

	fwCleanup func() // 自动安装的 RST 抑制规则清理钩子（可为 nil）
}

func newDemux(cfg Config, link Link) *demux {
	d := &demux{
		cfg:   cfg,
		link:  link,
		conns: make(map[connKey]*fconn),
		done:  make(chan struct{}),
	}
	go d.readLoop()
	return d
}

func (d *demux) keyOf(localPort uint16, remote Endpoint) connKey {
	ip4 := remote.IP.As4()
	return connKey{localPort: localPort, remoteIP: ip4, remotePort: remote.Port}
}

func (d *demux) readLoop() {
	for {
		bs, err := d.link.ReadPacket()
		if err != nil {
			d.closeAll()
			return
		}
		p, err := parsePacket(bs)
		if err != nil {
			continue // 非 TCP/校验失败/分片：静默丢弃
		}
		d.dispatch(p)
	}
}

func (d *demux) dispatch(p *Packet) {
	key := d.keyOf(p.Dst.Port, p.Src)

	d.mu.Lock()
	c := d.conns[key]
	ln := d.ln
	d.mu.Unlock()

	if c != nil {
		select {
		case c.chPkt <- p:
		case <-c.done:
		default:
			// 连接事件队列满：丢包（与真实网络一致，上层 KCP 负责重传）
		}
		return
	}

	// 未知名连接：SYN 交给监听器；RST 忽略；其余回 RST（真实协议栈行为）
	switch {
	case p.Has(flagSYN) && !p.Has(flagACK):
		if ln != nil {
			ln.onSyn(d, p)
		}
	case p.Has(flagRST):
	default:
		d.sendRst(p)
	}
}

// sendRst 对未知名连接的报文回 RST（seq/ack 按 RFC 793）
func (d *demux) sendRst(p *Packet) {
	var seq, ack uint32
	var flags uint8
	if p.Has(flagACK) {
		seq, flags = p.Ack, flagRST
	} else {
		seq, ack, flags = 0, p.Seq+uint32(len(p.Payload)), flagRST|flagACK
		if p.Has(flagSYN) {
			ack++
		}
		if p.Has(flagFIN) {
			ack++
		}
	}
	src := Endpoint{IP: p.Dst.IP, Port: p.Dst.Port}
	if d.cfg.ReplySrc.IsValid() {
		src.IP = d.cfg.ReplySrc
	}
	bs := buildPacket(&d.cfg, src, p.Src, seq, ack, flags, clockMS(), p.TSval, nil, 0)
	_ = d.link.WritePacket(bs)
}

func (d *demux) add(c *fconn) {
	d.mu.Lock()
	d.conns[d.keyOf(c.local.Port, c.remote)] = c
	d.mu.Unlock()
}

func (d *demux) remove(c *fconn) {
	d.mu.Lock()
	delete(d.conns, d.keyOf(c.local.Port, c.remote))
	empty := len(d.conns) == 0 && d.ln == nil
	d.mu.Unlock()
	// 客户端场景（无监听器）最后一条连接消失后回收整条链路，
	// 否则 readLoop 会永远阻塞在 ReadPacket 上（goroutine/fd 泄漏），
	// 同时触发防火墙规则清理。异步避免在 c.mu 持锁路径上嵌套。
	if empty {
		go d.closeAll()
	}
}

func (d *demux) closeAll() {
	d.once.Do(func() {
		close(d.done)
		d.mu.Lock()
		conns := make([]*fconn, 0, len(d.conns))
		for _, c := range d.conns {
			conns = append(conns, c)
		}
		d.mu.Unlock()
		for _, c := range conns {
			c.closeWithErr(io.ErrClosedPipe)
		}
		_ = d.link.Close()
		if d.fwCleanup != nil {
			d.fwCleanup()
		}
	})
}

// ---------------------------------------------------------------------------
// fconn：单连接的 TCP 状态机
// ---------------------------------------------------------------------------

// fconn 一个伪装 TCP 连接：线上表象完整，无重传/拥塞控制/滑动窗口。
//
// 丢包外观策略（与真实 Linux 接收端对齐）：
//   - 空洞段：照常投递上层，同时回 dup ACK + SACK（真实接收端行为）；
//   - 空洞在 HealDelay 内不会被填补（本层永不重传），随后执行"虚拟重传愈合"：
//     累计确认直接越过空洞——线上呈现丢包→dup ACK/SACK→快速恢复的完整闭环，
//     避免 ack 永久冻结在第一个空洞处（那对 DPI 是显著异常）。
type fconn struct {
	cfg    Config
	local  Endpoint
	remote Endpoint
	d      *demux

	mu       sync.Mutex
	state    state
	isn      uint32 // 本端 ISN
	sndNxt   uint32 // 下一个发送序号（只增不减，与对端 ack 无关）
	rcvNxt   uint32 // 对端最高连续序号（ack 语义自洽的核心）
	tsRecent uint32 // 对端最近 TSval（回显到 TSecr）
	peerMSS  uint16 // 对端通告的 MSS（路径 MTU 受限时可据此校准本端分段）
	ipID     uint16 // IPv4 ID（递增，仿 Linux）
	lastSent time.Time
	lastRecv time.Time // 最近一次入站报文（死亡检测/半开回收）

	// 空洞跟踪（SACK 外观 + 愈合）
	bitmap     map[uint32]uint32 // 空洞之后的已收段：seq -> end
	healTimer  *time.Timer
	healTimerC <-chan time.Time

	// delayed ACK
	ackPending int
	ackTimer   *time.Timer
	ackTimerC  <-chan time.Time

	chPkt    chan *Packet // demux 投递的 inbound 报文
	outCh    chan []byte  // 已构好的待发报文（writeLoop 消费）
	chData   chan []byte  // 投递给上层的数据（满则丢包，无积压）
	chReady  chan error   // 握手结果（cap 1）
	done     chan struct{}
	once     sync.Once
	dataOnce sync.Once    // chData 只关闭一次
	err      atomic.Value // 关闭原因

	// outMu 串行化"构包 + 入队"（仅数据路径 Write 使用：保证并发 Write 的
	// seq 分配与入队顺序一致，同时允许队列满时等待而不持有 c.mu）。
	// 锁序恒为 outMu → c.mu，单一方向，无死锁。
	outMu sync.Mutex

	// onEstablished 连接进入 Established 时回调（仅 Listener 侧设置：
	// 此时才推入 accept 队列——Accept 只返回完成三次握手的连接）。
	// 在持有 c.mu 的路径上调用，实现必须非阻塞。
	onEstablished func(c *fconn)

	// 统计（测试/观测用；握手超时诊断也依赖它们）
	SentPackets atomic.Int64
	RecvPackets atomic.Int64
	// SendErrors 链路发包失败次数（raw socket 错误此前被静默丢弃，
	// 会让"本机发不出去"与"对端没回"都表现为握手超时）
	SendErrors atomic.Int64
	sendErrMu  sync.Mutex
	sendErr    error
}

func newFConn(cfg Config, d *demux, local, remote Endpoint) *fconn {
	return &fconn{
		cfg:     cfg,
		local:   local,
		remote:  remote,
		d:       d,
		isn:     newISN(),
		ipID:    uint16(newISN()), // IP ID 随机起点（Linux 按流计数器并非从 0 开始）
		bitmap:  make(map[uint32]uint32),
		chPkt:   make(chan *Packet, 1024),
		outCh:   make(chan []byte, outQueueCap),
		chData:  make(chan []byte, recvQueueCap),
		chReady: make(chan error, 1),
		done:    make(chan struct{}),
	}
}

// start 启动事件循环（状态机/定时器）与出站协程
func (c *fconn) start() {
	go c.loop()
	go c.writeLoop()
}

func (c *fconn) loop() {
	// 保活/回收粒度取 KeepAlive/2，保证空闲不超过 KeepAlive
	tick := c.cfg.KeepAlive / 2
	if tick <= 0 {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		// 定时器通道在 c.mu 下增删，每次循环持锁取快照（避免与 disarm 竞争）
		c.mu.Lock()
		ackC, healC := c.ackTimerC, c.healTimerC
		c.mu.Unlock()
		select {
		case p := <-c.chPkt:
			c.handlePacket(p)
		case <-ackC:
			c.onAckTimer()
		case <-healC:
			c.onHealTimer()
		case <-ticker.C:
			c.onTick()
		case <-c.done:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// 发包（异步出站队列）
// ---------------------------------------------------------------------------
//
// 构包在调用方 goroutine 内完成（保证 seq/ipID 分配顺序），发出交给 writeLoop：
// 读循环/事件循环绝不阻塞在链路写系统调用上（否则慢链路会拖垮 ACK 与状态机，
// 两个对端互相等待时甚至形成 ABBA 死锁）。
//   - 控制段（ACK/SYN/FIN/RST/探测）：非阻塞入队，队列满时同步兜底直发
//     （控制段不可丢：FIN/ACK 丢失会拖垮挥手与确认外观）；
//   - 数据段（Write）：入队可等待（本地队列背压，非线上限速），
//     由 writeDeadline/连接关闭打断。

// buildLocked 构造报文（不发出，payload 被拷入报文缓冲）。
// 持有 c.mu 调用；seq 推进（SYN/FIN/数据）由调用方按语义负责。
func (c *fconn) buildLocked(flags uint8, payload []byte, sack [][2]uint32) []byte {
	bs := buildPacketSack(&c.cfg, c.local, c.remote, c.sndNxt, c.rcvNxt, flags,
		clockMS(), c.tsRecent, payload, c.ipID, sack)
	c.ipID++
	c.lastSent = time.Now()
	c.SentPackets.Add(1)
	return bs
}

// recordSendErr 记录一次链路发送失败（供握手/连接错误信息诊断）；
// 前几次同时打到 debug 日志，服务端"回不出 SYNACK"这类问题可以直接从日志看到。
func (c *fconn) recordSendErr(err error) {
	if err == nil {
		return
	}
	n := c.SendErrors.Add(1)
	c.sendErrMu.Lock()
	c.sendErr = err
	c.sendErrMu.Unlock()
	if n <= 3 {
		log.Ctx(context.Background()).Debug().
			Msgf("faux_tcp: 发包失败(%d) %s -> %s: %v", n, c.local, c.remote, err)
	}
}

// lastSendErr 返回最近一次发送失败（无则 nil）。
func (c *fconn) lastSendErr() error {
	c.sendErrMu.Lock()
	defer c.sendErrMu.Unlock()
	return c.sendErr
}

// dispatchControlLocked 发出控制段：优先异步队列，满则同步兜底。持有 c.mu 调用。
func (c *fconn) dispatchControlLocked(bs []byte) {
	select {
	case c.outCh <- bs:
	default:
		c.recordSendErr(c.d.link.WritePacket(bs))
	}
}

// writeLoop 出站协程：排空队列后才退出（保证 FIN/ACK 等收尾报文发出）
func (c *fconn) writeLoop() {
	for {
		select {
		case bs := <-c.outCh:
			c.recordSendErr(c.d.link.WritePacket(bs))
		case <-c.done:
			for {
				select {
				case bs := <-c.outCh:
					c.recordSendErr(c.d.link.WritePacket(bs))
				default:
					return
				}
			}
		}
	}
}

// notePeerMSS 记录对端/路径通告的 MSS；若小于本端 MSS，说明路径 MTU 受限
// （常见于 VPN/隧道出口改写 MSS），必须把 faux_tcp.mss 与 KCP MTU 一起调小，
// 否则大于路径 MTU 的报文会被丢弃（本层报文带 DF，不会被分片）。
func (c *fconn) notePeerMSS(peerMSS uint16) {
	if peerMSS == 0 {
		return
	}
	c.peerMSS = peerMSS
	if int(peerMSS) < c.cfg.MSS {
		log.Ctx(context.Background()).Warn().
			Msgf("faux_tcp: 对端/路径通告 MSS=%d < 本端 MSS=%d（路径 MTU 受限）："+
				"请把 faux_tcp.mss 调到 ≤%d，并把 KCP MTU(kcp_mtu) 调到 ≤ faux_tcp.mss，"+
				"否则大于路径 MTU 的报文会被丢弃", peerMSS, c.cfg.MSS, peerMSS)
	}
}

// PeerMSS 返回对端在握手时通告的 MSS（0 表示未通告）。
func (c *fconn) PeerMSS() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int(c.peerMSS)
}

// sendLocked 构造并入队一个控制段；调用方按语义推进 sndNxt。持有 c.mu 调用。
func (c *fconn) sendLocked(flags uint8, payload []byte) {
	c.sendWithSackLocked(flags, payload, nil)
}

// sendWithSackLocked 同 sendLocked，可携带 SACK 块（空洞 dup ACK 用）
func (c *fconn) sendWithSackLocked(flags uint8, payload []byte, sack [][2]uint32) {
	c.dispatchControlLocked(c.buildLocked(flags, payload, sack))
}

// sendSynLocked 发送 SYN（isn 不变，支持握手重试）
func (c *fconn) sendSynLocked() {
	c.sndNxt = c.isn // SYN 占一个序号，发送后推进
	bs := buildPacket(&c.cfg, c.local, c.remote, c.isn, 0, flagSYN,
		clockMS(), 0, nil, c.ipID)
	c.ipID++
	c.lastSent = time.Now()
	c.SentPackets.Add(1)
	c.dispatchControlLocked(bs)
	c.sndNxt = c.isn + 1
}

// sendSynAckLocked 被动侧 SYN+ACK
func (c *fconn) sendSynAckLocked() {
	bs := buildPacket(&c.cfg, c.local, c.remote, c.isn, c.rcvNxt, flagSYN|flagACK,
		clockMS(), c.tsRecent, nil, c.ipID)
	c.ipID++
	c.lastSent = time.Now()
	c.SentPackets.Add(1)
	c.dispatchControlLocked(bs)
	c.sndNxt = c.isn + 1
}

// sendKeepaliveLocked 发送标准 TCP keepalive 探测包：
// seq=sndNxt-1 的纯 ACK（窗口前一格，不消耗序号；对端按规则应答）。
// 持有 c.mu 调用。
func (c *fconn) sendKeepaliveLocked() {
	seq := c.sndNxt - 1
	bs := buildPacket(&c.cfg, c.local, c.remote, seq, c.rcvNxt, flagACK,
		clockMS(), c.tsRecent, nil, c.ipID)
	c.ipID++
	c.lastSent = time.Now()
	c.SentPackets.Add(1)
	c.dispatchControlLocked(bs)
}

// ---------------------------------------------------------------------------
// delayed ACK / 空洞愈合 定时器
// ---------------------------------------------------------------------------

// armAckTimerLocked 首个未确认数据包到达后启动 40ms 冲刷定时器
func (c *fconn) armAckTimerLocked() {
	if c.ackTimer == nil {
		c.ackTimer = time.NewTimer(ackMaxDelay)
		c.ackTimerC = c.ackTimer.C
	}
}

// disarmAckTimerLocked 停止冲刷定时器（立即 ACK 或连接关闭时）
func (c *fconn) disarmAckTimerLocked() {
	if c.ackTimer != nil {
		c.ackTimer.Stop()
		c.ackTimer = nil
		c.ackTimerC = nil
	}
}

// ackDataLocked 正序数据包的 ACK 策略：凑满 ackEveryPackets 立即回，
// 否则等 40ms 冲刷（仿 Linux delayed ACK）
func (c *fconn) ackDataLocked() {
	c.ackPending++
	if c.ackPending >= ackEveryPackets {
		c.flushAckLocked()
		return
	}
	c.armAckTimerLocked()
}

// flushAckLocked 立即发送累计确认并重置 delayed ACK 状态
func (c *fconn) flushAckLocked() {
	c.ackPending = 0
	c.disarmAckTimerLocked()
	c.sendLocked(flagACK, nil)
}

func (c *fconn) onAckTimer() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ackTimer = nil
	c.ackTimerC = nil
	if c.state == stClosed || c.ackPending == 0 {
		return
	}
	c.ackPending = 0
	c.sendLocked(flagACK, nil)
}

// armHealTimerLocked 空洞出现后启动"虚拟重传愈合"定时器
func (c *fconn) armHealTimerLocked() {
	if c.healTimer == nil {
		c.healTimer = time.NewTimer(c.cfg.HealDelay)
		c.healTimerC = c.healTimer.C
	}
}

func (c *fconn) disarmHealTimerLocked() {
	if c.healTimer != nil {
		c.healTimer.Stop()
		c.healTimer = nil
		c.healTimerC = nil
	}
}

func (c *fconn) onHealTimer() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.healTimer = nil
	c.healTimerC = nil
	if c.state == stClosed || len(c.bitmap) == 0 {
		return
	}
	c.healLocked()
}

// healLocked 虚拟重传愈合：真实 TCP 此刻应已收到重传并推进累计确认，
// 本层直接越过第一个空洞（该数据早已投递给上层 KCP，无需线上重演）：
// rcvNxt 跳到第一空洞末端，吸收位图中紧随的连续段，然后发推进后的 ACK。
// 线上观察：ack 停顿 HealDelay（≈重传时延）后跳变——与真实快速恢复外观一致。
func (c *fconn) healLocked() {
	// 第一空洞末端 = 位图最小 key
	var minKey uint32
	first := true
	for k := range c.bitmap {
		if first || int32(k-minKey) < 0 {
			minKey = k
			first = false
		}
	}
	if first || !seqAfter(minKey, c.rcvNxt) {
		// 位图异常（不含超前段）：清空兜底，不后退 rcvNxt
		c.bitmap = make(map[uint32]uint32)
		return
	}
	c.rcvNxt = minKey
	for {
		end, ok := c.bitmap[c.rcvNxt]
		if !ok {
			break
		}
		delete(c.bitmap, c.rcvNxt)
		c.rcvNxt = end
	}
	// 愈合后仍有空洞：重新计时（下一空洞的"重传时延"）
	if len(c.bitmap) > 0 {
		c.armHealTimerLocked()
	}
	// 推进后的累计确认立即发出（对应真实栈收到重传后的 ACK 跳变）
	c.flushAckLocked()
}

// ---------------------------------------------------------------------------
// 保活 / 会话回收
// ---------------------------------------------------------------------------

// onTick 周期任务：保活探测、对端死亡检测、半开/卡死连接回收
func (c *fconn) onTick() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == stClosed {
		return
	}
	idle := time.Since(c.lastRecv)
	switch c.state {
	case stSynRcvd:
		// 半开连接超时回收：SYNACK 发出多次仍收不到最后一个 ACK，通常是
		// 回程被 NAT/防火墙/容器网络丢弃（客户端会表现为"握手超时、收到 0 个报文"）
		if idle > c.cfg.handshakeWindow() {
			log.Ctx(context.Background()).Debug().
				Msgf("faux_tcp: 半开连接超时回收 peer=%s（已发 %d 个报文未收到 ACK；"+
					"若对端报握手超时，请检查回程：NAT/安全组/容器网络是否放行本端发出的 SYN+ACK）",
					c.remote, c.SentPackets.Load())
			c.closeLocked(nil)
		}
	case stEstablished:
		if idle > 3*c.cfg.KeepAlive {
			// 对端死亡（NAT 表项消失/进程崩溃无 RST）
			c.closeLocked(ErrPeerTimeout.New())
			return
		}
		if time.Since(c.lastSent) >= c.cfg.KeepAlive {
			c.sendKeepaliveLocked()
		}
	case stFinWait, stClosing, stCloseWait:
		// 关闭流程卡死（对端消失）：一个保活周期后回收
		if idle > c.cfg.KeepAlive {
			c.closeLocked(nil)
		}
	}
}

// ---------------------------------------------------------------------------
// 收包处理（状态机核心）
// ---------------------------------------------------------------------------

func (c *fconn) handlePacket(p *Packet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == stClosed {
		return
	}
	c.RecvPackets.Add(1)
	c.lastRecv = time.Now()
	if p.TSval != 0 {
		c.tsRecent = p.TSval
	}

	if p.Has(flagRST) {
		c.closeLocked(ErrConnReset.New())
		return
	}

	switch c.state {
	case stSynSent:
		c.handleSynSent(p)
	case stSynRcvd:
		c.handleSynRcvd(p)
	default:
		c.handleEstablished(p)
	}
}

func (c *fconn) handleSynSent(p *Packet) {
	if p.Has(flagSYN) && p.Has(flagACK) && p.Ack == c.sndNxt {
		c.rcvNxt = p.Seq + 1
		c.notePeerMSS(p.MSS)
		c.state = stEstablished
		c.sendLocked(flagACK, nil)
		select {
		case c.chReady <- nil:
		default:
		}
	}
	// 其它报文（如重传的 SYN 以外的段）在握手未完成前忽略
}

func (c *fconn) handleSynRcvd(p *Packet) {
	if p.Has(flagSYN) && !p.Has(flagACK) {
		// 对端 SYN 重传：重发 SYN+ACK（握手期重传是正常 TCP 行为）
		c.sendSynAckLocked()
		return
	}
	if p.Has(flagACK) && p.Ack == c.sndNxt {
		c.state = stEstablished
		if c.onEstablished != nil {
			c.onEstablished(c)
		}
		if len(p.Payload) > 0 {
			c.deliverLocked(p)
		}
	}
}

func (c *fconn) handleEstablished(p *Packet) {
	if len(p.Payload) == 0 && !p.Has(flagFIN) {
		if c.state == stClosing {
			// 最后的 ACK：四次挥手完成
			c.closeLocked(nil)
			return
		}
		// 纯 ACK：无重传队列、无窗口跟踪，唯一需要响应的是对端 keepalive
		// 探测包（seq=对端 sndNxt-1，即本端视角 rcvNxt-1）——回当前 ACK；
		// 普通 ACK（seq==rcvNxt）不应答，避免乒乓。
		if p.Seq+1 == c.rcvNxt && p.Flags == flagACK {
			c.sendLocked(flagACK, nil)
		}
		return
	}

	end := p.Seq + uint32(len(p.Payload))
	if p.Has(flagFIN) {
		end++ // FIN 占一个序号
	}

	switch {
	case p.Seq == c.rcvNxt:
		// 正序：推进累计确认；位图中紧随其后的乱序段一并吸收
		c.rcvNxt = end
		if len(p.Payload) > 0 {
			c.deliverLocked(p)
		}
		for {
			nxt, ok := c.bitmap[c.rcvNxt]
			if !ok {
				break
			}
			delete(c.bitmap, c.rcvNxt)
			c.rcvNxt = nxt
		}
		if p.Has(flagFIN) {
			c.onPeerFinLocked()
			return
		}
		c.ackDataLocked() // delayed ACK：凑 2 个立即回，否则等 40ms
	case seqAfter(p.Seq, c.rcvNxt):
		// 空洞段：位图去重后照常投递（可靠性是上层 KCP 的事），
		// 立即回 dup ACK + SACK（仿真实接收端）；空洞逾 HealDelay 未愈
		// 则按"虚拟重传"推进 rcvNxt（见 healLocked）。
		if len(p.Payload) > 0 {
			if _, dup := c.bitmap[p.Seq]; !dup {
				if len(c.bitmap) < bitmapCap {
					if len(c.bitmap) == 0 {
						c.armHealTimerLocked()
					}
					c.bitmap[p.Seq] = end
				}
				c.deliverLocked(p)
			}
		}
		c.sendDupAckLocked()
		if p.Has(flagFIN) {
			// 带空洞的 FIN：空洞永远不会被本层填补，FIN 照常生效
			c.onPeerFinLocked()
		}
	default:
		// 完全重复的段（end <= rcvNxt）：补一个当前 ACK（真实栈的重复段应答）
		c.sendLocked(flagACK, nil)
	}
}

// sendDupAckLocked 空洞时的 dup ACK + SACK（SACK 块来自位图，最多 4 块）。
// 持有 c.mu 调用。
func (c *fconn) sendDupAckLocked() {
	blocks := make([][2]uint32, 0, 4)
	for l, r := range c.bitmap {
		blocks = append(blocks, [2]uint32{l, r})
		if len(blocks) >= 4 {
			break
		}
	}
	c.sendWithSackLocked(flagACK, nil, blocks)
}

// deliverLocked 把载荷投递给上层；Packet 拥有 Payload 所有权，直接转交。
// chData 满时丢包（无积压，背压以丢包形式传导给上层，由 KCP 重传）。
func (c *fconn) deliverLocked(p *Packet) {
	select {
	case c.chData <- p.Payload:
	case <-c.done:
	default:
	}
}

// onPeerFinLocked 对端 FIN 到达（FIN 序号已处理）。
// stEstablished → 回 ACK、读侧 EOF、转 stCloseWait（本端仍可写，等应用 Close）；
// stFinWait（本端先关）→ 回最后的 ACK，四次挥手完成；
// stClosing/stCloseWait 收到重复 FIN → 重发累计 ACK（ack 已覆盖其 FIN）。
func (c *fconn) onPeerFinLocked() {
	switch c.state {
	case stFinWait:
		c.sendLocked(flagACK, nil)
		c.closeLocked(nil)
	case stClosing:
		// 对端重试的 FIN（≈LAST_ACK 状态）：重发累计 ACK（ack 已覆盖其 FIN）
		c.sendLocked(flagACK, nil)
	default: // stEstablished 首次 / stCloseWait 重复 FIN
		c.sendLocked(flagACK, nil)
		if c.state == stEstablished {
			c.state = stCloseWait
		}
		// 读侧 EOF（保留 chData 里已投递的数据，由 Read 排空后返回 EOF）
		c.dataOnce.Do(func() { close(c.chData) })
	}
}

// closeLocked 完全关闭连接
func (c *fconn) closeLocked(err error) {
	if c.state == stClosed {
		return
	}
	c.state = stClosed
	if err != nil {
		c.err.Store(err)
	}
	c.disarmAckTimerLocked()
	c.disarmHealTimerLocked()
	c.once.Do(func() {
		close(c.done)
		c.d.remove(c)
		c.dataOnce.Do(func() { close(c.chData) })
	})
}

func (c *fconn) closeWithErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked(err)
}

// getState 读取当前状态（测试/观测用）
func (c *fconn) getState() state {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}
