package fake_tcp

import (
	"context"
	"net"
	"net/netip"
	"sync"

	"github.com/lxt1045/utils/log"
)

// listener.go：服务端。持有 LinkIO 与会话路由表，把报文分发到各 session；
// 新 SYN 创建 session 并在 Established 后推入 acceptCh。

// Listener 服务端监听器
type Listener struct {
	cfg  Config
	link LinkIO
	local PeerAddr

	sessions sync.Map // sessKey -> *session
	acceptCh chan *Conn

	ctx    context.Context
	cancel context.CancelFunc
	closeCh chan struct{}

	fwCleanup func() // RawTCP 模式的 RST 抑制规则清理
}

// Listen 创建服务端（plan.md §7）。RawTCP 模式失败（无权限/防火墙）时自动降级 ModeUDP 并打日志。
func Listen(ctx context.Context, cfg Config) (l *Listener, err error) {
	if err = cfg.setDefaults(); err != nil {
		return nil, err
	}
	if cfg.LocalAddr == "" {
		return nil, ErrAddrRequired.New("LocalAddr 为空")
	}

	if cfg.Mode == ModeRawTCP {
		l, err = listenRawTCP(ctx, cfg)
		if err == nil {
			return l, nil
		}
		log.Ctx(ctx).Warn().Err(err).Msgf("fake_tcp: RawTCP 模式不可用，降级为 UDP 模式: %v", err)
		cfg.Mode = ModeUDP
	}
	return listenUDP(ctx, cfg)
}

// listenUDP UDP 模式服务端
func listenUDP(ctx context.Context, cfg Config) (*Listener, error) {
	uaddr, err := net.ResolveUDPAddr("udp", cfg.LocalAddr)
	if err != nil {
		return nil, ErrInvalidConfig.Newf("LocalAddr 解析失败: %v", err)
	}
	conn, err := net.ListenUDP("udp", uaddr)
	if err != nil {
		return nil, err
	}
	link := newUDPLinkIO(conn, cfg.Magic, cfg.MTU)
	local := PeerAddr{IP: mustAddrIP(conn.LocalAddr().(*net.UDPAddr)), Port: uint16(conn.LocalAddr().(*net.UDPAddr).Port)}
	return listenLink(ctx, cfg, link, local, nil), nil
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

	if l.cfg.Mode == ModeRawTCP {
		s.rcvNxt.Store(seg.Seq + 1)
		s.rcvMax.Store(seg.Seq + 1)
		s.tsRecent.Store(seg.TSval)
		s.sndNxt.Store(randUint32()) // 服务端 ISN
	}
	l.sessions.Store(key, s)

	// 回 SYNACK
	if err := s.sendSeg(FlagSYN|FlagACK, nil); err != nil {
		log.Ctx(ctx).Debug().Msgf("fake_tcp: SYNACK 发送失败: %v", err)
		s.teardown()
		return
	}
	if l.cfg.Mode == ModeUDP {
		// UDP 模式无三次握手第三个包，SYNACK 即建立
		s.markEstablished()
	}
}

// Accept 接受新连接
func (l *Listener) Accept() (*Conn, error) {
	select {
	case c := <-l.acceptCh:
		return c, nil
	case <-l.closeCh:
		return nil, ErrConnClosed.New()
	case <-l.ctx.Done():
		return nil, ErrConnClosed.New()
	}
}

// Addr 实际监听地址（端口为 0 时取到真实端口）
func (l *Listener) Addr() PeerAddr { return l.local }

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

// mustAddrIP 从 UDPAddr 取 netip 地址
func mustAddrIP(a *net.UDPAddr) netip.Addr {
	ip, _ := netip.AddrFromSlice(a.IP)
	return ip
}
