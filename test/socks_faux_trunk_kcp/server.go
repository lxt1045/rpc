package socks_faux_kcp

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/pb"
	"github.com/lxt1045/rpc/trunk_kcp"
	"github.com/lxt1045/utils/log"
)

// Server 服务端：持有一份 RPC 方法表（反射一次），每条底层连接 Clone 一个 peer。
// 底层连接来自 faux_tcp 监听器（真实链路见 cmd/socks-faux-trunk-kcp-server，
// 内存链路见测试）。每条 faux_tcp 连接的首字节是类型标记（tlsvconn.go）：
//   - 控制连接：再叠 迷你 trunk(KCP) → 控制 vconn → TLS → RPC（Auth 等，token 加密）
//   - 物理连接：裸 RPC 只做 TrunkUpgrade，随后整连接交给数据面 trunk 跑 KCP
type Server struct {
	ctx   context.Context
	gPeer rpc.Peer

	mu   sync.Mutex
	svcs map[*SocksSvc]struct{}
}

// NewServer 构建服务端方法表（不做 I/O，不监听）。
// TLS 配置经 SetServerTLSConfig 注入（全局，供控制通道与数据面 vconn 共用）。
func NewServer(ctx context.Context) (*Server, error) {
	gPeer, err := rpc.NewPeer(ctx, &SocksSvc{}, pb.NewSocksCliClient, pb.RegisterSocksSvcServer)
	if err != nil {
		return nil, err
	}
	return &Server{
		ctx:   ctx,
		gPeer: gPeer,
		svcs:  make(map[*SocksSvc]struct{}),
	}, nil
}

// Serve 在 listener 上循环 Accept；按首字节类型分流。
// listener 关闭（或 ctx 结束）时返回 nil。
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") || s.ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.dispatchConn(conn)
	}
}

// dispatchConn 读取首字节类型标记并分流。
func (s *Server) dispatchConn(conn net.Conn) {
	var t [1]byte
	if _, err := io.ReadFull(conn, t[:]); err != nil {
		log.Ctx(s.ctx).Debug().Caller().Err(err).Str("remote", conn.RemoteAddr().String()).Msg("read conn type failed")
		_ = conn.Close()
		return
	}
	switch t[0] {
	case connTypeControl:
		s.serveControlConn(conn)
	case connTypePhysical:
		s.serveConn(conn)
	default:
		log.Ctx(s.ctx).Debug().Caller().Str("remote", conn.RemoteAddr().String()).
			Msgf("unknown conn type %d", t[0])
		_ = conn.Close()
	}
}

// serveControlConn 控制连接：faux_tcp 之上叠迷你 trunk（KCP 可靠层）→
// 控制 vconn（固定 connID=0）→ TLS → RPC。
func (s *Server) serveControlConn(conn net.Conn) {
	var ctrl *trunk_kcp.TrunkKCP // 先声明：下面的 onNewConn 闭包要引用它
	ctrl = trunk_kcp.NewTrunkKCP(ctrlConv, func(vconn *trunk_kcp.VirtualConn) {
		if vconn.ConnID() != 0 {
			// 控制通道只用 vconn 0
			_ = vconn.Close()
			return
		}
		var c net.Conn = wrapVConn(vconn, conn.LocalAddr(), conn.RemoteAddr())
		if cfg := serverTLSConfig(); cfg != nil {
			tlsConn, err := wrapTLSServer(s.ctx, vconn, conn.LocalAddr(), conn.RemoteAddr(), cfg, 10*time.Second)
			if err != nil {
				log.Ctx(s.ctx).Debug().Caller().Err(err).Str("remote", conn.RemoteAddr().String()).
					Msg("control channel tls handshake failed")
				_ = vconn.Close()
				return
			}
			c = tlsConn
		}
		// peer 关闭时连带关闭迷你 trunk 与其承载的 faux_tcp 连接
		s.serveRWC(&ctrlConn{Conn: c, ctrl: ctrl}, conn)
	}, conn)
	go func() {
		if err := ctrl.Run(s.ctx); err != nil && s.ctx.Err() == nil {
			log.Ctx(s.ctx).Debug().Caller().Err(err).Msg("control mini-trunk stopped")
		}
	}()
}

// serveConn 物理连接：裸 RPC（只做 TrunkUpgrade，无秘密），Clone 一个 SocksSvc。
func (s *Server) serveConn(conn net.Conn) {
	s.serveRWC(conn, conn)
}

// serveRWC 在给定流上 Clone 出独立的 SocksSvc（carrier 仅用于地址展示）。
func (s *Server) serveRWC(rwc io.ReadWriteCloser, carrier net.Conn) {
	svc := &SocksSvc{}
	if carrier != nil {
		svc.RemoteAddr = carrier.RemoteAddr().String()
		svc.LocalAddr = carrier.LocalAddr().String()
	}
	peer, err := s.gPeer.Clone(s.ctx, rwc, svc)
	if err != nil {
		log.Ctx(s.ctx).Warn().Caller().Err(err).Str("remote", svc.RemoteAddr).Msg("clone peer failed")
		_ = rwc.Close()
		return
	}
	svc.Peer = peer
	s.mu.Lock()
	s.svcs[svc] = struct{}{}
	s.mu.Unlock()
}

// Reap 周期清理已关闭的 RPC 连接对应的会话（由 RunReaper 调用）。
func (s *Server) Reap() {
	s.mu.Lock()
	for svc := range s.svcs {
		if svc.Peer.IsClosed() {
			SvcClosed(svc)
			delete(s.svcs, svc)
		}
	}
	s.mu.Unlock()
}

// RunReaper 起一个周期协程回收已关闭连接的会话，ctx 结束即退出。
func (s *Server) RunReaper(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.Reap()
		}
	}
}

// Close 关闭所有会话（trunk + 物理连接）。
func (s *Server) Close() {
	CloseAllSessions()
}
