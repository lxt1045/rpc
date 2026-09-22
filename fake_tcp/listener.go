package fake_tcp

import (
	"context"
	"net"
	"sync"

	"github.com/lxt1045/utils/log"
)

// listener.go：服务端。持有 LinkIO 与会话路由表，把报文分发到各 session；
// 新 SYN 创建 session 并在 Established 后推入 acceptCh。
// Listener 实现标准 net.Listener 接口。

// Listener 服务端监听器
type Listener struct {
	cfg   Config
	link  LinkIO
	local PeerAddr

	sessions sync.Map // sessKey -> *session
	acceptCh chan *Conn

	ctx     context.Context
	cancel  context.CancelFunc
	closeCh chan struct{}

	fwCleanup func() // RST 抑制规则清理
}

var _ net.Listener = (*Listener)(nil)

// Listen 创建服务端（RawTCP 模式；需要 Linux + root/CAP_NET_RAW，
// 无权限时请使用外挂 udp2raw 方案，见 deploy/udp2raw/README.md）。
func Listen(ctx context.Context, cfg Config) (l *Listener, err error) {
	if err = cfg.setDefaults(); err != nil {
		return nil, err
	}
	if cfg.LocalAddr == "" {
		return nil, ErrAddrRequired.New("LocalAddr 为空")
	}
	return listenRawTCP(ctx, cfg)
}

// listenLink 以指定 LinkIO 启动服务端（pipe 测试也走这里）
func listenLink(ctx context.Context, cfg Config, link LinkIO, local PeerAddr, fwCleanup func()) *Listener {
	cctx, cancel := context.WithCancel(ctx)
	l := &Listener{
		cfg:       cfg,
		link:      link,
		local:     local,
		acceptCh:  make(chan *Conn, 128),
		ctx:       cctx,
		cancel:    cancel,
		closeCh:   make(chan struct{}),
		fwCleanup: fwCleanup,
	}
	go l.readLoop()
	go keepaliveLoop(cctx, cfg, func(f func(*session) bool) {
		l.sessions.Range(func(k, v any) bool {
			return f(v.(*session))
		})
	})
	return l
}

// readLoop 收包分发：已有会话直接 handle；新 SYN 创建会话；其余静默丢弃
func (l *Listener) readLoop() {
	ctx := l.ctx
	for {
		seg, err := l.link.ReadSegment()
		if err != nil {
			l.closeAll()
			return
		}
		key := sessKey{Peer: seg.Peer, ConnID: seg.ConnID}
		if v, ok := l.sessions.Load(key); ok {
			v.(*session).handle(ctx, seg)
			continue
		}
		if seg.Flags&FlagSYN == 0 {
			continue // 非 SYN 的陌生报文：静默丢弃（plan.md §4.2）
		}
		l.newServerSession(ctx, key, seg)
	}
}

// newServerSession 处理新连接 SYN
func (l *Listener) newServerSession(ctx context.Context, key sessKey, seg *Segment) {
	s := newSession(ctx, l.cfg, l.link, l.local, seg.Peer, seg.ConnID)
	s.state.Store(int32(stateSynReceived))
	s.onClose = func(k sessKey, _ *session) { l.sessions.Delete(k) }
	s.onEstablished = func(s *session) {
		select {
		case l.acceptCh <- &Conn{sess: s}:
		default:
			log.Ctx(ctx).Info().Msgf("fake_tcp: accept 队列满，拒绝新连接, peer=%v", s.peer)
			_ = s.sendSeg(FlagRST, nil)
			s.teardown()
		}
	}

	s.rcvNxt.Store(seg.Seq + 1)
	s.rcvMax.Store(seg.Seq + 1)
	s.tsRecent.Store(seg.TSval)
	s.sndNxt.Store(randUint32()) // 服务端 ISN
	l.sessions.Store(key, s)

	// 回 SYNACK（等三次握手最后一个 ACK 后 Established）
	if err := s.sendSeg(FlagSYN|FlagACK, nil); err != nil {
		log.Ctx(ctx).Debug().Msgf("fake_tcp: SYNACK 发送失败: %v", err)
		s.teardown()
		return
	}
}

// Accept 接受新连接（net.Listener 语义）
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.acceptCh:
		return c, nil
	case <-l.closeCh:
		return nil, ErrConnClosed.New()
	case <-l.ctx.Done():
		return nil, ErrConnClosed.New()
	}
}

// Addr 实际监听地址（net.Listener 语义；端口为 0 时取到真实端口）
func (l *Listener) Addr() net.Addr {
	return Addr{network: "tcp", ip: l.local.IP, port: l.local.Port}
}

func (l *Listener) closeAll() {
	l.closeOnce()
}

func (l *Listener) closeOnce() {
	select {
	case <-l.closeCh:
		return
	default:
	}
	close(l.closeCh)
	l.cancel()
	_ = l.link.Close()
	if l.fwCleanup != nil {
		l.fwCleanup()
	}
	l.sessions.Range(func(k, v any) bool {
		v.(*session).teardown()
		return true
	})
}

// Close 关闭监听器及全部会话
func (l *Listener) Close() error {
	l.closeAll()
	return nil
}
