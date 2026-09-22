package faux_tcp

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/errors"
)

// state TCP 状态机的状态
type state int32

const (
	stSynSent     state = iota + 1 // 主动侧：已发 SYN
	stSynRcvd                      // 被动侧：已收 SYN，已发 SYN+ACK
	stEstablished                  // 已建立
	stClosing                      // 任一端已发 FIN
	stClosed
)

var (
	errHandshakeTimeout = errors.New("handshake timeout")
	errReadTimeout      = errors.New("read timeout")
	errReset            = errors.New("connection reset by peer")
	errClosed           = errors.New("connection closed")
)

// seqAfter 判断 a 是否在 b 之后（处理序号回绕）
func seqAfter(a, b uint32) bool { return int32(a-b) > 0 }

// ---------------------------------------------------------------------------
// demux：链路收包分发器
// ---------------------------------------------------------------------------

// connKey 连接四元组中用于路由的部分（本地端口 + 对端端点）
type connKey struct {
	localPort uint16
	remoteIP  [4]byte
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
	d.mu.Unlock()
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
	})
}

// ---------------------------------------------------------------------------
// fconn：单连接的 TCP 状态机
// ---------------------------------------------------------------------------

// fconn 一个伪装 TCP 连接：线上表象完整，无重传/拥塞控制/滑动窗口。
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
	ipID     uint16 // IPv4 ID（递增，仿 Linux）
	lastSent time.Time
	finSent  bool
	finAcked bool

	chPkt   chan *Packet // demux 投递的 inbound 报文
	chData  chan []byte  // 投递给上层的数据（满则丢包，无积压）
	chReady chan error    // 握手结果（cap 1）
	done    chan struct{}
	once    sync.Once
	dataOnce sync.Once   // chData 只关闭一次
	err     atomic.Value // 关闭原因

	// 统计（测试/观测用）
	SentPackets atomic.Int64
	RecvPackets atomic.Int64
}

func newFConn(cfg Config, d *demux, local, remote Endpoint) *fconn {
	return &fconn{
		cfg:     cfg,
		local:   local,
		remote:  remote,
		d:       d,
		isn:     newISN(),
		chPkt:   make(chan *Packet, 1024),
		chData:  make(chan []byte, 256),
		chReady: make(chan error, 1),
		done:    make(chan struct{}),
	}
}

// start 启动事件循环（保活定时器）
func (c *fconn) start() {
	go c.loop()
}

func (c *fconn) loop() {
	// 保活粒度取 KeepAlive/2，保证空闲不超过 KeepAlive
	tick := c.cfg.KeepAlive / 2
	if tick <= 0 {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case p := <-c.chPkt:
			c.handlePacket(p)
		case <-ticker.C:
			c.maybeKeepalive()
		case <-c.done:
			return
		}
	}
}

// maybeKeepalive 空闲超时发裸 ACK 保活（无数据载荷，普通 ACK 段，不是
// TCP keepalive 探测包——探测包用 sndNxt-1 会暴露特征）。
func (c *fconn) maybeKeepalive() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != stEstablished {
		return
	}
	if time.Since(c.lastSent) >= c.cfg.KeepAlive {
		c.sendLocked(flagACK, nil)
	}
}

// ---------------------------------------------------------------------------
// 发包
// ---------------------------------------------------------------------------

// sendLocked 构造并发送一个 TCP 段；SYN/FIN/数据消耗序号，调用方按语义推进 sndNxt。
// 持有 c.mu 调用。
func (c *fconn) sendLocked(flags uint8, payload []byte) {
	seq := c.sndNxt
	bs := buildPacket(&c.cfg, c.local, c.remote, seq, c.rcvNxt, flags,
		clockMS(), c.tsRecent, payload, c.ipID)
	c.ipID++
	c.lastSent = time.Now()
	c.SentPackets.Add(1)
	// 写出即发出，失败不重试（fire-and-forget）
	_ = c.d.link.WritePacket(bs)
}

// sendSynLocked 发送 SYN（isn 不变，支持握手重试）
func (c *fconn) sendSynLocked() {
	c.sndNxt = c.isn // SYN 占一个序号，发送后推进
	bs := buildPacket(&c.cfg, c.local, c.remote, c.isn, 0, flagSYN,
		clockMS(), 0, nil, c.ipID)
	c.ipID++
	c.lastSent = time.Now()
	c.SentPackets.Add(1)
	_ = c.d.link.WritePacket(bs)
	c.sndNxt = c.isn + 1
}

// sendSynAckLocked 被动侧 SYN+ACK
func (c *fconn) sendSynAckLocked() {
	bs := buildPacket(&c.cfg, c.local, c.remote, c.isn, c.rcvNxt, flagSYN|flagACK,
		clockMS(), c.tsRecent, nil, c.ipID)
	c.ipID++
	c.lastSent = time.Now()
	c.SentPackets.Add(1)
	_ = c.d.link.WritePacket(bs)
	c.sndNxt = c.isn + 1
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
	if p.TSval != 0 {
		c.tsRecent = p.TSval
	}

	if p.Has(flagRST) {
		c.closeLocked(errReset)
		return
	}

	switch c.state {
	case stSynSent:
		c.handleSynSent(p)
	case stSynRcvd:
		c.handleSynRcvd(p)
	case stEstablished, stClosing:
		c.handleEstablished(p)
	}
}

func (c *fconn) handleSynSent(p *Packet) {
	if p.Has(flagSYN) && p.Has(flagACK) && p.Ack == c.sndNxt {
		c.rcvNxt = p.Seq + 1
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
		if len(p.Payload) > 0 {
			c.deliverLocked(p)
		}
	}
}

func (c *fconn) handleEstablished(p *Packet) {
	if len(p.Payload) == 0 && !p.Has(flagFIN) {
		return // 纯 ACK：不据此做任何事（无重传队列、无窗口跟踪）
	}

	end := p.Seq + uint32(len(p.Payload))
	if p.Has(flagFIN) {
		end++ // FIN 占一个序号
	}

	switch {
	case p.Seq == c.rcvNxt:
		// 正序：推进累计确认，回 ACK
		c.rcvNxt = end
		if len(p.Payload) > 0 {
			c.deliverLocked(p)
		}
		c.sendLocked(flagACK, nil)
		if p.Has(flagFIN) {
			c.onPeerFinLocked()
		}
	case seqAfter(p.Seq, c.rcvNxt):
		// 空洞：数据直接投递给上层（KCP 负责排序/重传），
		// 不回 ACK——真实栈此时会发 dup ACK，会让 DPI 看到丢包特征
		if len(p.Payload) > 0 {
			c.deliverLocked(p)
		}
		if p.Has(flagFIN) {
			// 空洞不会愈合（不重传），FIN 照常生效
			c.onPeerFinLocked()
		}
	default:
		// 完全重复的段（end <= rcvNxt）：补一个 ACK（合理栈行为）
		c.sendLocked(flagACK, nil)
	}
}

// deliverLocked 把载荷投递给上层；chData 满时丢包（无积压，
// 背压以丢包形式传导给上层，由 KCP 重传）。
func (c *fconn) deliverLocked(p *Packet) {
	data := append([]byte(nil), p.Payload...)
	select {
	case c.chData <- data:
	case <-c.done:
	default:
	}
}

// onPeerFinLocked 对端 FIN：回 ACK，读侧 EOF；若我们也已发 FIN 则直接关闭
func (c *fconn) onPeerFinLocked() {
	if c.state == stClosing && c.finSent {
		c.sendLocked(flagACK, nil)
		c.closeLocked(nil)
		return
	}
	c.sendLocked(flagACK, nil)
	// 读侧 EOF（保留 chData 里已投递的数据，由 Read 排空后返回 EOF）
	c.dataOnce.Do(func() { close(c.chData) })
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
