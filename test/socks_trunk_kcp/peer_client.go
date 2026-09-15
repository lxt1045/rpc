package socks_kcp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/socks_trunk_kcp/pb"
	"github.com/lxt1045/rpc/trunk_kcp"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
	"github.com/lxt1045/utils/socks/http/utils"
	"golang.org/x/sync/errgroup"
)

type Peer struct {
	LocalAddrs  string
	RemoteAddrs string
	rpc.Peer
}

type SocksCli struct {
	Name     string
	PeerAddr string
	TlsConf  *tls.Config
	Token    string
	TrunkCfg TrunkKCPConfig
	ChPeer   chan *Peer

	trunk     *trunk_kcp.TrunkKCP
	trunkPeer *Peer
	connID    atomic.Uint32
	mu        sync.Mutex
}

var _ pb.SocksCliServer = &SocksCli{}

func (p *SocksCli) Close(ctx context.Context, req *pb.CloseReq) (*pb.CloseRsp, error) {
	p.mu.Lock()
	if p.trunk != nil {
		_ = p.trunk.Close()
		p.trunk = nil
	}
	p.mu.Unlock()
	return &pb.CloseRsp{}, nil
}

func (p *SocksCli) authPeer(ctx context.Context, peer rpc.Peer) error {
	if p.Token == "" {
		return errors.New("client token is empty")
	}
	resp := &pb.AuthRsp{}
	if err := peer.Invoke(ctx, "Auth", &pb.AuthReq{Name: p.Token}, resp); err != nil {
		return err
	}
	if resp.Status != pb.AuthRsp_Succ {
		msg := "auth failed"
		if resp.Err != nil {
			msg = resp.Err.Msg
		}
		return errors.New(msg)
	}
	return nil
}

func (p *SocksCli) GetPeer() *Peer {
	for {
		select {
		case peer := <-p.ChPeer:
			if peer.IsClosed() {
				_ = peer.Close(context.Background())
				continue
			}
			return peer
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// RunConnLoop 建立并认证一条控制 RPC 连接。
func (p *SocksCli) RunConnLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		conn, err := socket.DialTLSTimeout1(ctx, "tcp", p.PeerAddr, p.TlsConf, 3*time.Second)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		peer, err := rpc.NewPeer(ctx, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
		if err != nil {
			_ = conn.Close()
			time.Sleep(time.Second)
			continue
		}
		if err = peer.Conn(ctx, conn); err != nil {
			_ = conn.Close()
			time.Sleep(time.Second)
			continue
		}
		if err = p.authPeer(ctx, peer); err != nil {
			_ = conn.Close()
			time.Sleep(time.Second)
			continue
		}
		select {
		case p.ChPeer <- &Peer{Peer: peer, LocalAddrs: conn.LocalAddr().String(), RemoteAddrs: conn.RemoteAddr().String()}:
		case <-ctx.Done():
			return
		}
	}
}

// InitTrunk 创建 N 条 trunk_kcp 底层连接并启动。
func (p *SocksCli) InitTrunk(ctx context.Context) error {
	// 先关闭旧的 trunk（如果存在）
	p.mu.Lock()
	if p.trunk != nil {
		oldTrunk := p.trunk
		p.trunk = nil
		p.mu.Unlock()
		log.Ctx(ctx).Info().Msg("closing old client trunk before creating new one")
		_ = oldTrunk.Close()
	} else {
		p.mu.Unlock()
	}

	p.TrunkCfg.defaults()
	conv := p.TrunkCfg.Conv
	n := p.TrunkCfg.MaxConns
	if n <= 0 {
		n = p.TrunkCfg.MinConns
	}
	conns, err := p.TrunkConn(ctx, conv, n)
	if err != nil {
		return err
	}
	p.trunkPeer = p.GetPeer()

	req := &pb.TrunkStartReq{
		TrunkId:      conv,
		UpgradeCount: uint32(n),
	}
	if err := p.trunkPeer.Invoke(ctx, "TrunkStart", req, &pb.TrunkStartRsp{}); err != nil {
		return err
	}

	trunk := trunk_kcp.NewTrunkKCP(conv, nil, conns...)
	p.mu.Lock()
	p.trunk = trunk
	p.mu.Unlock()
	go trunk.Run(ctx)
	return nil
}

// RemoveTrunkConn 优雅剔除一条底层物理连接：先本地 CloseWrite，
// 再通知服务端也 CloseWrite/RemoveConn，最后本地 RemoveConn。
func (p *SocksCli) RemoveTrunkConn(ctx context.Context, id int) error {
	p.mu.Lock()
	trunk := p.trunk
	peer := p.trunkPeer
	p.mu.Unlock()
	if trunk == nil || peer == nil {
		return errors.New("trunk or control peer not ready")
	}
	_ = trunk.CloseWriteConn(id)
	_ = peer.Invoke(ctx, "TrunkRemoveConn", &pb.TrunkUpgradeReq{TrunkId: p.TrunkCfg.Conv, UpgradeId: uint32(id)}, &pb.TrunkUpgradeRsp{})
	_ = trunk.RemoveConn(id)
	return nil
}

// AddTrunkConn 向已运行的 trunk_kcp 动态加入一条新的底层连接。
func (p *SocksCli) AddTrunkConn(ctx context.Context, n int) error {
	p.mu.Lock()
	trunk := p.trunk
	p.mu.Unlock()
	if trunk == nil {
		return errors.New("trunk not ready")
	}
	conns, err := p.TrunkConn(ctx, p.TrunkCfg.Conv, n)
	if err != nil {
		return err
	}
	for _, c := range conns {
		if _, err := trunk.AddConn(c); err != nil {
			_ = c.Close()
			return err
		}
	}
	return nil
}

// MaintainTrunk 检查底层连接数量，低于目标时自动补充。
func (p *SocksCli) MaintainTrunk(ctx context.Context) {
	p.TrunkCfg.defaults()
	target := p.TrunkCfg.MaxConns
	if target <= 0 {
		target = p.TrunkCfg.MinConns
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			trunk := p.trunk
			p.mu.Unlock()
			if trunk == nil {
				continue
			}
			cur := trunk.ConnCount()
			if cur >= target {
				continue
			}
			add := target - cur
			log.Ctx(ctx).Info().Int("current", cur).Int("target", target).Int("add", add).Msg("trunk conn missing, adding")
			if err := p.AddTrunkConn(ctx, add); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("add trunk conn failed")
			}
		}
	}
}

func (p *SocksCli) TrunkConn(ctx context.Context, conv uint32, n int) ([]io.ReadWriteCloser, error) {
	g := errgroup.Group{}
	var mu sync.Mutex
	conns := make([]io.ReadWriteCloser, 0, n)
	for i := 0; i < n; i++ {
		i := i
		g.Go(func() error {
			conn, err := socket.DialTLSTimeout1(ctx, "tcp", p.PeerAddr, p.TlsConf, 3*time.Second)
			if err != nil {
				return err
			}
			peer, err := rpc.NewPeer(ctx, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
			if err != nil {
				_ = conn.Close()
				return err
			}
			if err = peer.Conn(ctx, conn); err != nil {
				_ = conn.Close()
				return err
			}
			if err = p.authPeer(ctx, peer); err != nil {
				_ = conn.Close()
				return err
			}
			req := &pb.TrunkUpgradeReq{
				TrunkId:   conv,
				UpgradeId: uint32(i),
			}
			upgrade, err := peer.Upgrade(ctx, "TrunkUpgrade", req, &pb.TrunkUpgradeRsp{})
			if err != nil {
				_ = conn.Close()
				return err
			}
			mu.Lock()
			conns = append(conns, upgrade)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return conns, nil
}

// RunSocks 启动 SOCKS5 TCP 监听。
func (p *SocksCli) RunSocks(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Ctx(ctx).Info().Str("addr", addr).Msg("SOCKS5 listening")
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go p.handleSocks(ctx, c)
	}
}

func (p *SocksCli) handleSocks(ctx context.Context, rc net.Conn) {
	defer rc.Close()
	if tcp, ok := rc.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
	}
	addr, err := socks.Handshake(rc)
	if err != nil {
		log.Ctx(ctx).Debug().Err(err).Msg("socks handshake failed")
		return
	}
	_ = p.openProxy(ctx, rc, addr.String(), nil)
}

// RunHTTPProxy 启动 HTTP CONNECT 监听。
func (p *SocksCli) RunHTTPProxy(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Ctx(ctx).Info().Str("addr", addr).Msg("HTTP proxy listening")
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go p.handleHTTP(ctx, c)
	}
}

func (p *SocksCli) handleHTTP(ctx context.Context, inConn net.Conn) {
	defer inConn.Close()
	req, err := utils.NewHTTPRequest(&inConn, 4096, false, nil)
	if err != nil {
		return
	}
	if !req.IsHTTPS() {
		// 普通 HTTP 代理也需要目标地址；这里只保留 CONNECT 的清晰实现。
		return
	}
	req.HTTPSReply()
	_ = p.openProxy(ctx, inConn, req.Host, nil)
}

func (p *SocksCli) openProxy(ctx context.Context, local net.Conn, addr string, head []byte) error {
	p.mu.Lock()
	trunk := p.trunk
	p.mu.Unlock()
	if trunk == nil {
		return errors.New("trunk not ready")
	}
	maxV := int(p.TrunkCfg.MaxVirtualConn)
	if maxV <= 0 {
		maxV = 256
	}
	id := int(p.connID.Add(1))%maxV + 1
	vconn := trunk.GetConn(uint16(id))
	if vconn == nil {
		return errors.New("get virtual conn failed")
	}
	if err := WriteOpenHeader(vconn, addr, head); err != nil {
		_ = vconn.Close()
		return err
	}
	relay(ctx, vconn, local)
	return nil
}
