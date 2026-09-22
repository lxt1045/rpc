package fake_tcp

import (
	"context"
	"net"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/utils/log"
)

// dialer.go：客户端。构造 LinkIO、发起握手（含应用层重试）、启动收包与保活协程。

// Dial 创建客户端连接（plan.md §7）。RawTCP 模式失败（无权限/防火墙）时自动降级 ModeUDP。
func Dial(ctx context.Context, cfg Config) (c *Conn, err error) {
	if err = cfg.setDefaults(); err != nil {
		return nil, err
	}
	if cfg.RemoteAddr == "" {
		return nil, ErrAddrRequired.New("RemoteAddr 为空")
	}

	if cfg.Mode == ModeRawTCP {
		c, err = dialRawTCP(ctx, cfg)
		if err == nil {
			return c, nil
		}
		log.Ctx(ctx).Warn().Err(err).Msgf("fake_tcp: RawTCP 模式不可用，降级为 UDP 模式: %v", err)
		cfg.Mode = ModeUDP
	}
	return dialUDP(ctx, cfg)
}

// dialUDP UDP 模式客户端
func dialUDP(ctx context.Context, cfg Config) (*Conn, error) {
	raddr, err := net.ResolveUDPAddr("udp", cfg.RemoteAddr)
	if err != nil {
		return nil, ErrInvalidConfig.Newf("RemoteAddr 解析失败: %v", err)
	}
	var laddr *net.UDPAddr
	if cfg.LocalAddr != "" {
		laddr, err = net.ResolveUDPAddr("udp", cfg.LocalAddr)
		if err != nil {
			return nil, ErrInvalidConfig.Newf("LocalAddr 解析失败: %v", err)
		}
	}
	// 非 connected socket：WriteSegment 统一走 WriteToUDP
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}
	link := newUDPLinkIO(conn, cfg.Magic, cfg.MTU)
	lu := conn.LocalAddr().(*net.UDPAddr)
	local := PeerAddr{IP: mustAddrIP(lu), Port: uint16(lu.Port)}
	remote := PeerAddr{IP: mustAddrIP(raddr), Port: uint16(raddr.Port)}
	return dialLink(ctx, cfg, link, local, remote, nil)
}

// dialLink 以指定 LinkIO 完成握手并返回 Conn（pipe 测试也走这里）。
// fwCleanup 为 RawTCP 模式防火墙规则清理函数，其它模式传 nil。
func dialLink(ctx context.Context, cfg Config, link LinkIO, local, remote PeerAddr, fwCleanup func()) (conn *Conn, err error) {
	connID := uint64(0)
	if cfg.Mode == ModeUDP {
		connID = randUint64() // UDP 模式由客户端生成虚拟连接标识
	}
	s := newSession(ctx, cfg, link, local, remote, connID)
	s.state.Store(int32(stateSynSent))
	if cfg.Mode == ModeRawTCP {
		s.sndNxt.Store(randUint32()) // 客户端 ISN
	}
	cleanupFirewall := true
	defer func() {
		if err != nil {
			_ = link.Close()
			if cleanupFirewall && fwCleanup != nil {
				fwCleanup()
			}
		}
	}()

	// 收包协程：单连接，直接喂给本 session
	go func() {
		for {
			seg, rerr := link.ReadSegment()
			if rerr != nil {
				s.teardown()
				return
			}
			s.handle(ctx, seg)
		}
	}()
	// 保活/超时扫描协程（session 关闭后退出，避免泄漏）
	kctx, kcancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			kcancel()
		}
	}()
	go func() {
		select {
		case <-s.closeCh:
			kcancel()
		case <-kctx.Done():
		}
	}()
	go keepaliveLoop(kctx, cfg, func(f func(*session) bool) {
		f(s)
	})

	// 握手：SYN 应用层重试（此时连接未建立，不算"数据重传"）
	for i := 0; i <= cfg.HandshakeRetries; i++ {
		if err = s.sendSeg(FlagSYN, nil); err != nil {
			return nil, err
		}
		timer := time.NewTimer(handshakeRetryInterval)
		select {
		case <-s.estCh:
			timer.Stop()
			return &Conn{sess: s}, nil
		case <-s.closeCh:
			timer.Stop()
			return nil, ErrHandshakeRejected.Newf("对端 %v 拒绝", remote)
		case <-timer.C:
			// 重试
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.WithErr(ctx.Err())
		}
	}
	return nil, ErrHandshakeTimeout.Newf("对端 %v 握手超时（重试 %d 次）", remote, cfg.HandshakeRetries)
}
