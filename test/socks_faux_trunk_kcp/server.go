package socks_faux_kcp

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/pb"
	"github.com/lxt1045/utils/log"
)

// Server 服务端：持有一份 RPC 方法表（反射一次），每条底层连接 Clone 一个 peer。
// 底层连接来自 faux_tcp 监听器（真实链路见 cmd/socks-faux-trunk-kcp-server，
// 内存链路见测试），对 RPC 层而言就是一个 net.Conn。
type Server struct {
	ctx   context.Context
	gPeer rpc.Peer

	mu   sync.Mutex
	svcs map[*SocksSvc]struct{}
}

// NewServer 构建服务端方法表（不做 I/O，不监听）。
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

// Serve 在 listener 上循环 Accept；每条连接 Clone 出独立的 SocksSvc。
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
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	svc := &SocksSvc{
		RemoteAddr: conn.RemoteAddr().String(),
		LocalAddr:  conn.LocalAddr().String(),
	}
	peer, err := s.gPeer.Clone(s.ctx, conn, svc)
	if err != nil {
		log.Ctx(s.ctx).Warn().Err(err).Str("remote", svc.RemoteAddr).Msg("clone peer failed")
		_ = conn.Close()
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
