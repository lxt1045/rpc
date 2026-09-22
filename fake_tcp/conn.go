package fake_tcp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync/atomic"
	"time"
)

// conn.go：Conn —— 面向应用的连接对象，实现 io.ReadWriteCloser 与 net.Conn 语义子集。
// 可直接喂给 trunk_kcp.NewTrunkKCP / rpc.NewPeer（plan.md §8）。

// Conn 一条 fake_tcp 虚拟连接
type Conn struct {
	sess *session

	rBuf []byte // 当前正在消费的接收块

	rd atomic.Int64 // 读截止时间（unixnano，0 为无）
	wd atomic.Int64 // 写截止时间（保留，写路径不阻塞故暂不使用）
}

func newSession(ctx context.Context, cfg Config, link LinkIO, local, peer PeerAddr, connID uint64) *session {
	if cfg.RecvQueue <= 0 {
		cfg.RecvQueue = recvQueueCap
	}
	s := &session{
		cfg:     cfg,
		link:    link,
		local:   local,
		peer:    peer,
		connID:  connID,
		recvCh:  make(chan []byte, cfg.RecvQueue),
		outCh:   make(chan *Segment, outQueueCap),
		finCh:   make(chan struct{}),
		closeCh: make(chan struct{}),
		estCh:   make(chan struct{}),
		tsEpoch: time.Now(),
		bitmap:  make(map[uint32]uint32),
	}
	s.ipID.Store(randUint32()) // IP ID 随机起步（仿 Linux per-flow 计数器）
	s.touch()
	go s.writeLoop(ctx)
	return s
}

// randUint32 加密随机数（ISN / Magic / IP ID 起步用）
func randUint32() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint32(time.Now().UnixNano())
	}
	return binary.LittleEndian.Uint32(b[:])
}

// Read 读取数据。语义：到达即交、不保序（plan.md §4.3；需要有序请在上层叠 KCP）。
// 对端 FIN 且接收队列排空后返回 io.EOF；对端 RST 或本地关闭返回对应错误。
func (c *Conn) Read(p []byte) (n int, err error) {
	s := c.sess
	for {
		if len(c.rBuf) > 0 {
			n = copy(p, c.rBuf)
			c.rBuf = c.rBuf[n:]
			return n, nil
		}
		// 非阻塞先取一次
		select {
		case b := <-s.recvCh:
			c.rBuf = b
			continue
		default:
		}
		// 队列已空：对端 FIN → EOF；会话关闭 → 错误
		if s.finReceived() && len(s.recvCh) == 0 {
			return 0, io.EOF
		}
		if s.isClosed() && len(s.recvCh) == 0 {
			return 0, s.closeErr()
		}

		var timer *time.Timer
		var timeout <-chan time.Time
		if dl := c.rd.Load(); dl > 0 {
			d := time.Until(time.Unix(0, dl))
			if d <= 0 {
				return 0, ErrTimeout.New("读超时")
			}
			timer = time.NewTimer(d)
			timeout = timer.C
		}
		select {
		case b := <-s.recvCh:
			c.rBuf = b
		case <-s.finCh:
		case <-s.closeCh:
		case <-timeout:
			return 0, ErrTimeout.New("读超时")
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// Write 写入数据。两种语义（Config.DatagramOnly）：
//   - 数据报模式：一次 Write = 一个 TCP 段，超过 MaxPayload 直接报错（由上层分段；
//     叠加 trunk_kcp 时必须开启，杜绝半帧丢失导致流式重组错位）；
//   - 流式模式（默认关闭时）：内部按 MaxPayload 切片为多个段直发。
//
// 不重传、不限速、不做线上背压（仅本地出站队列容量等待）。
func (c *Conn) Write(p []byte) (n int, err error) {
	s := c.sess
	switch s.getState() {
	case stateEstablished, stateCloseWait:
		// CloseWait：对端已 FIN 但本端未关，仍允许写（半关闭语义）
	default:
		return 0, ErrConnClosed.New()
	}
	max := s.link.MaxPayload()
	if max <= 0 {
		max = defaultMTU
	}
	if s.cfg.DatagramOnly {
		if len(p) > max {
			return 0, ErrPacketTooBig.Newf("%d > %d", len(p), max)
		}
		if len(p) == 0 {
			return 0, nil
		}
		if err = s.sendData(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	for len(p) > 0 {
		m := max
		if m > len(p) {
			m = len(p)
		}
		if err = s.sendData(p[:m]); err != nil {
			return n, err
		}
		n += m
		p = p[m:]
	}
	return n, nil
}

// Close 主动关闭：发起 FIN 交互后异步收尾（不阻塞等待对端应答），
// 会话资源由保活扫描兜底回收。
func (c *Conn) Close() error {
	c.sess.closeInitiate(context.Background())
	return nil
}

// Addr 实现 net.Addr（按模式伪装成 TCPAddr/UDPAddr 外观）
type Addr struct {
	network string
	ip      netip.Addr
	port    uint16
}

func (a Addr) Network() string { return a.network }
func (a Addr) String() string {
	return net.JoinHostPort(a.ip.String(), strconv.Itoa(int(a.port)))
}

// LocalAddr 本地地址（线路上是 TCP，按 TCPAddr 外观呈现）
func (c *Conn) LocalAddr() net.Addr {
	return Addr{network: "tcp", ip: c.sess.local.IP, port: c.sess.local.Port}
}

// RemoteAddr 对端地址
func (c *Conn) RemoteAddr() net.Addr {
	return Addr{network: "tcp", ip: c.sess.peer.IP, port: c.sess.peer.Port}
}

// SetDeadline 读/写截止时间
func (c *Conn) SetDeadline(t time.Time) error {
	c.rd.Store(t.UnixNano())
	c.wd.Store(t.UnixNano())
	return nil
}

// SetReadDeadline 读截止时间
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.rd.Store(t.UnixNano())
	return nil
}

// SetWriteDeadline 写截止时间（写路径不阻塞，仅记录）
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wd.Store(t.UnixNano())
	return nil
}

var (
	_ io.ReadWriteCloser = (*Conn)(nil)
	_ net.Conn           = (*Conn)(nil)
)
