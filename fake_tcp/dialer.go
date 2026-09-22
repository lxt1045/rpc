package fake_tcp

import (
	"context"
	"time"

	"github.com/lxt1045/errors"
)

// dialer.go：客户端。构造 LinkIO、发起握手（含应用层重试）、启动收包与保活协程。

// Dial 创建客户端连接（RawTCP 模式；需要 Linux + root/CAP_NET_RAW，
// 无权限时请使用外挂 udp2raw 方案，见 deploy/udp2raw/README.md）。
func Dial(ctx context.Context, cfg Config) (c *Conn, err error) {
	if err = cfg.setDefaults(); err != nil {
		return nil, err
	}
	if cfg.RemoteAddr == "" {
		return nil, ErrAddrRequired.New("RemoteAddr 为空")
	}
	return dialRawTCP(ctx, cfg)
}

// dialLink 以指定 LinkIO 完成握手并返回 Conn（pipe 测试也走这里）。
// fwCleanup 为防火墙规则清理函数，pipe 测试传 nil。
func dialLink(ctx context.Context, cfg Config, link LinkIO, local, remote PeerAddr, fwCleanup func()) (conn *Conn, err error) {
	s := newSession(ctx, cfg, link, local, remote, 0) // ConnID 由链路层按四元组哈希计算
	s.state.Store(int32(stateSynSent))
	s.sndNxt.Store(randUint32()) // 客户端 ISN

	defer func() {
		if err != nil {
			_ = link.Close()
			if fwCleanup != nil {
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
