package socks

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/proxy/pb"
	"github.com/lxt1045/utils/log"
)

type SocksCli struct {
	Name         string
	SocksAddr    string
	ChPeer       chan *Peer
	ChPeerReuser chan *Peer
	TlsConf      *tls.Config
	PeerAddr     string
}

type Peer struct {
	TsCreate    int64
	LocalAddrs  string
	RemoteAddrs string
	rpc.Peer
}

// ConfigureTCPConn applies the socket options needed by long-lived proxy
// connections. TLS connections are unwrapped only to configure the socket;
// callers must continue using the original connection for I/O.
func ConfigureTCPConn(conn net.Conn) {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	_ = tcpConn.SetNoDelay(true)
}

// GetPeer keeps the legacy blocking API. New code should use the context-aware
// connection path through RunTCP.
func (p *SocksCli) GetPeer() *Peer { return p.getPeer(context.Background()) }

func (p *SocksCli) getPeer(ctx context.Context) *Peer {
	cutoff := time.Now().Add(-3 * time.Minute).Unix()
	for {
		select {
		case peer := <-p.ChPeerReuser:
			if peer != nil && peer.TsCreate > cutoff && !peer.Peer.IsClosed() {
				return peer
			}
			if peer != nil {
				_ = peer.Close(ctx)
			}
		default:
		}
		select {
		case peer := <-p.ChPeer:
			if peer != nil && peer.TsCreate > cutoff && !peer.Peer.IsClosed() {
				return peer
			}
			if peer != nil {
				_ = peer.Close(ctx)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (p *SocksCli) close(ctx context.Context) (err error) {
	closePeers := func(ch <-chan *Peer) {
		for {
			select {
			case peer := <-ch:
				if peer != nil {
					if e := peer.Close(ctx); e != nil {
						err = e
					}
				}
			default:
				return
			}
		}
	}
	closePeers(p.ChPeer)
	closePeers(p.ChPeerReuser)
	return err
}

func (p *SocksCli) Close(ctx context.Context, _ *pb.CloseReq) (*pb.CloseRsp, error) {
	return &pb.CloseRsp{}, p.close(ctx)
}

// RunTCP listens on addr and forwards each accepted connection to targetAddr.
func (p *SocksCli) RunTCP(ctx context.Context, addr, targetAddr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to listen on %s", addr)
		return err
	}
	defer l.Close()
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			return err
		}
		go func() {
			if err := p.connect(ctx, targetAddr, c); err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Msg("proxy connection failed")
			}
		}()
	}
}

func (p *SocksCli) connect(ctx context.Context, targetAddr string, rc net.Conn) (err error) {
	defer rc.Close()
	ConfigureTCPConn(rc)
	peer := p.getPeer(ctx)
	if peer == nil {
		return ctx.Err()
	}
	defer peer.Close(context.Background())
	upgrade, err := peer.Upgrade(ctx, "ConnUpgrade", &pb.ConnUpgradeReq{Addr: targetAddr}, &pb.ConnUpgradeRsp{})
	if err != nil {
		return err
	}

	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{}, 2)
	closeBoth := sync.OnceFunc(func() {
		_ = rc.Close()
		_ = upgrade.Close()
	})
	go func() {
		<-copyCtx.Done()
		closeBoth()
	}()
	go func() {
		_, _ = Copy(copyCtx, upgrade, rc)
		closeBoth()
		done <- struct{}{}
	}()
	go func() {
		_, _ = Copy(copyCtx, rc, upgrade)
		closeBoth()
		done <- struct{}{}
	}()
	<-done
	cancel()
	closeBoth()
	<-done
	return nil
}

// RunConnLoop maintains idle RPC peers for TCP proxy requests.
func (p *SocksCli) RunConnLoop(ctx context.Context, cancel context.CancelFunc, addr string, tlsConfig *tls.Config) {
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		dialCtx, dialCancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := socket.DialTLS(dialCtx, "tcp", addr, tlsConfig)
		dialCancel()
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("dial proxy service")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		ConfigureTCPConn(conn)
		peer, err := rpc.NewPeer(ctx, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
		if err == nil {
			err = peer.Conn(ctx, conn)
		}
		if err != nil {
			_ = conn.Close()
			_ = peer.Close(context.Background())
			continue
		}
		item := &Peer{
			TsCreate:    time.Now().Unix(),
			Peer:        peer,
			LocalAddrs:  conn.LocalAddr().String(),
			RemoteAddrs: conn.RemoteAddr().String(),
		}
		select {
		case p.ChPeer <- item:
		case <-ctx.Done():
			_ = item.Close(context.Background())
			return
		}
	}
}
