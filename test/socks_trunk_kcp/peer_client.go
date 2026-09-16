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

// connInfo 记录连接的ID和创建时间
type connInfo struct {
	id        int
	createdAt time.Time
}

type SocksCli struct {
	Name     string
	PeerAddr string
	TlsConf  *tls.Config
	Token    string
	TrunkCfg TrunkKCPConfig
	ChPeer   chan *Peer

	trunk       *trunk_kcp.TrunkKCP
	trunkPeer   *Peer
	connID      atomic.Uint32
	connInfos   []connInfo // 记录所有活跃连接的信息
	stopRefresh chan struct{}
	mu          sync.Mutex
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
		// 停止连接刷新
		if p.stopRefresh != nil {
			close(p.stopRefresh)
			p.stopRefresh = nil
		}
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

	// 获取一个新的控制连接peer
	trunkPeer := p.GetPeer()

	req := &pb.TrunkStartReq{
		TrunkId:      conv,
		UpgradeCount: uint32(n),
	}
	if err := trunkPeer.Invoke(ctx, "TrunkStart", req, &pb.TrunkStartRsp{}); err != nil {
		return err
	}

	trunk := trunk_kcp.NewTrunkKCP(conv, nil, conns...)

	// 记录初始连接信息
	now := time.Now()
	connInfos := make([]connInfo, n)
	for i := 0; i < n; i++ {
		connInfos[i] = connInfo{
			id:        i,
			createdAt: now,
		}
	}

	stopRefresh := make(chan struct{})
	p.mu.Lock()
	p.trunk = trunk
	p.trunkPeer = trunkPeer
	p.connInfos = connInfos
	p.stopRefresh = stopRefresh
	p.mu.Unlock()

	// 启动trunk并监控其状态
	go func() {
		trunk.Run(ctx)
		log.Ctx(ctx).Warn().Msg("trunk stopped, will attempt to reconnect")

		// trunk停止后，清理状态
		p.mu.Lock()
		if p.trunk == trunk {
			p.trunk = nil
			// 停止连接刷新
			if p.stopRefresh != nil {
				close(p.stopRefresh)
				p.stopRefresh = nil
			}
			// 将控制连接放回，让它继续被其他地方使用或被心跳检测关闭
			if p.trunkPeer == trunkPeer && !trunkPeer.IsClosed() {
				select {
				case p.ChPeer <- trunkPeer:
				default:
					_ = trunkPeer.Close(ctx)
				}
			}
			p.trunkPeer = nil
			p.connInfos = nil
		}
		p.mu.Unlock()

		// 延迟后尝试重连
		time.Sleep(3 * time.Second)
		log.Ctx(ctx).Info().Msg("attempting to reconnect trunk")
		if err := p.InitTrunk(ctx); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to reconnect trunk")
		} else {
			log.Ctx(ctx).Info().Msg("trunk reconnected successfully")
		}
	}()

	// 启动定期刷新连接的goroutine
	go p.refreshConnLoop(ctx, stopRefresh)

	return nil
}


// refreshConnLoop 定期刷新连接：每隔10秒删除最老的一条连接，同时加入一条新连接
func (p *SocksCli) refreshConnLoop(ctx context.Context, stopRefresh chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopRefresh:
			return
		case <-ticker.C:
			if err := p.refreshOldestConn(ctx); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("failed to refresh oldest connection")
			}
		}
	}
}

// refreshOldestConn 删除最老的连接并添加一条新连接
func (p *SocksCli) refreshOldestConn(ctx context.Context) error {
	p.mu.Lock()
	trunk := p.trunk
	if trunk == nil || len(p.connInfos) == 0 {
		p.mu.Unlock()
		return errors.New("trunk not ready or no connections")
	}

	// 找到最老的连接
	oldestIdx := 0
	oldestTime := p.connInfos[0].createdAt
	for i := 1; i < len(p.connInfos); i++ {
		if p.connInfos[i].createdAt.Before(oldestTime) {
			oldestIdx = i
			oldestTime = p.connInfos[i].createdAt
		}
	}
	oldestID := p.connInfos[oldestIdx].id
	p.mu.Unlock()

	log.Ctx(ctx).Info().Int("conn_id", oldestID).Time("created_at", oldestTime).Msg("refreshing oldest connection")

	// 先创建新连接
	conns, err := p.TrunkConn(ctx, p.TrunkCfg.Conv, 1)
	if err != nil {
		return errors.New("failed to create new connection: " + err.Error())
	}
	newConn := conns[0]

	// 添加新连接到trunk
	newID, err := trunk.AddConn(newConn)
	if err != nil {
		_ = newConn.Close()
		return errors.New("failed to add new connection: " + err.Error())
	}

	// 添加新连接信息到列表
	p.mu.Lock()
	p.connInfos = append(p.connInfos, connInfo{
		id:        newID,
		createdAt: time.Now(),
	})
	p.mu.Unlock()

	// 删除旧连接
	if err := p.RemoveTrunkConn(ctx, oldestID); err != nil {
		log.Ctx(ctx).Warn().Err(err).Int("old_conn_id", oldestID).Msg("failed to remove old connection")
		// 即使删除失败，新连接也已经添加了，继续更新记录
	}

	// 从连接信息列表中移除旧连接
	p.mu.Lock()
	for i, info := range p.connInfos {
		if info.id == oldestID {
			p.connInfos = append(p.connInfos[:i], p.connInfos[i+1:]...)
			break
		}
	}
	p.mu.Unlock()

	log.Ctx(ctx).Info().Int("old_conn_id", oldestID).Int("new_conn_id", newID).Msg("connection refreshed successfully")
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
