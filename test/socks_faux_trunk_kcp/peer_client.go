package socks_faux_kcp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/pb"
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
	Name      string
	PeerAddr  string // 服务端 "ip:port"（faux_tcp 对端）
	LocalAddr string // 可选本地 "ip:port"
	Token     string
	TrunkCfg  TrunkKCPConfig
	FauxCfg   FauxTCPConfig
	// TLSCfg 端到端 TLS（跑在 trunk_kcp VirtualConn 上）：nil = 明文模式。
	// 控制通道与每条代理数据连接都会做一次 TLS 握手。
	TLSCfg *tls.Config
	ChPeer chan *Peer

	// Dialer 底层拨号器；nil 时按 PeerAddr/LocalAddr/FauxCfg 构造 FauxDialer。
	// 测试可注入内存链路拨号器（见 faux_mem_test.go）。
	Dialer ConnDialer

	trunk     *trunk_kcp.TrunkKCP
	trunkPeer *Peer
	dflt      ConnDialer
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

// dialer 返回注入的拨号器或默认 faux_tcp 拨号器。
func (p *SocksCli) dialer() ConnDialer {
	if p.Dialer != nil {
		return p.Dialer
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dflt == nil {
		// 拨号超时必须大于握手预算（单次超时 ×(重试+1)），否则 ctx 先超时，
		// 握手超时的自诊断信息（收到几个报文/发送失败几次）就看不到。
		timeout := p.FauxCfg.HandshakeBudget() + 2*time.Second
		if timeout < 5*time.Second {
			timeout = 5 * time.Second
		}
		p.dflt = &FauxDialer{
			Config:  p.FauxCfg,
			Local:   p.LocalAddr,
			Remote:  p.PeerAddr,
			Timeout: timeout,
		}
	}
	return p.dflt
}

func (p *SocksCli) dial(ctx context.Context) (net.Conn, error) {
	conn, err := p.dialer().Dial(ctx)
	if err != nil {
		return nil, err
	}
	if p.Name != "" {
		log.Ctx(ctx).Debug().Caller().Str("local", conn.LocalAddr().String()).
			Str("remote", conn.RemoteAddr().String()).Msgf("%s faux_tcp connected", p.Name)
	}
	return conn, nil
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

// RunConnLoop 建立并认证一条控制 RPC 连接（承载 Auth/TrunkStart/TrunkRemoveConn）。
// 控制通道分层：faux_tcp → 迷你 trunk(KCP 可靠层) → 控制 vconn → TLS → RPC。
// faux_tcp 不重传，迷你 trunk 为控制面补上可靠性；token 只在 TLS 之内传输。
func (p *SocksCli) RunConnLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		peer, err := p.dialControlPeer(ctx)
		if err != nil {
			log.Ctx(ctx).Warn().Caller().Err(err).Msg("dial control conn failed")
			time.Sleep(time.Second)
			continue
		}
		select {
		case p.ChPeer <- peer:
		case <-ctx.Done():
			return
		}
	}
}

// dialControlPeer 建立一条控制连接并完成认证。
func (p *SocksCli) dialControlPeer(ctx context.Context) (*Peer, error) {
	conn, err := p.dial(ctx)
	if err != nil {
		return nil, err
	}
	var ctrl *trunk_kcp.TrunkKCP
	fail := func(err error) (*Peer, error) {
		if ctrl != nil {
			_ = ctrl.Close() // 连带关闭 faux_tcp 物理连接
		}
		_ = conn.Close()
		return nil, err
	}

	// 类型标记：服务端按首字节分流（控制/物理）
	if _, err := conn.Write([]byte{connTypeControl}); err != nil {
		return fail(err)
	}
	// 迷你 trunk：为控制 RPC 提供可靠性（faux_tcp 不重传，丢包会打断裸 RPC）
	ctrl = trunk_kcp.NewTrunkKCP(ctrlConv, nil, conn)
	go ctrl.Run(ctx)
	vconn := ctrl.GetConn(0)
	if vconn == nil {
		return fail(errors.New("control vconn open failed"))
	}

	// TLS（可选；token 只在 TLS 之内传输）
	var c net.Conn = wrapVConn(vconn, conn.LocalAddr(), conn.RemoteAddr())
	if p.TLSCfg != nil {
		tlsConn, err := wrapTLSClient(ctx, vconn, conn.LocalAddr(), conn.RemoteAddr(), p.TLSCfg, 10*time.Second)
		if err != nil {
			return fail(err)
		}
		c = tlsConn
	}

	peer, err := rpc.NewPeer(ctx, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
	if err != nil {
		return fail(err)
	}
	if err = peer.Conn(ctx, &ctrlConn{Conn: c, ctrl: ctrl}); err != nil {
		return fail(err)
	}
	if err = p.authPeer(ctx, peer); err != nil {
		_ = peer.Close(ctx)
		return fail(err)
	}
	return &Peer{Peer: peer, LocalAddrs: conn.LocalAddr().String(), RemoteAddrs: conn.RemoteAddr().String()}, nil
}

// InitTrunk 创建 N 条物理连接（faux_tcp）并启动 trunk_kcp。
func (p *SocksCli) InitTrunk(ctx context.Context) error {
	p.mu.Lock()
	if p.trunk != nil {
		oldTrunk := p.trunk
		p.trunk = nil
		p.mu.Unlock()
		log.Ctx(ctx).Info().Caller().Msg("closing old client trunk before creating new one")
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

	// 顺序与 socks_trunk_kcp 相反：先在控制连接上 TrunkStart（服务端建
	// 0 连接 trunk 并登记 conv→会话），再逐条拨物理连接 TrunkUpgrade 加入。
	// 因为 TrunkUpgrade（物理连接上的裸 RPC）依赖 conv 对应的已授权会话。
	trunkPeer := p.GetPeer()

	req := &pb.TrunkStartReq{
		TrunkId:      conv,
		UpgradeCount: uint32(n),
	}
	if err := trunkPeer.Invoke(ctx, "TrunkStart", req, &pb.TrunkStartRsp{}); err != nil {
		return err
	}

	conns, err := p.TrunkConn(ctx, conv, n)
	if err != nil {
		return err
	}

	trunk := trunk_kcp.NewTrunkKCP(conv, nil) // 本地 0 连接启动
	// 应用配置的 KCP NoDelay 参数（物理层是 faux_tcp，默认快速模式即可）
	p.TrunkCfg.ApplyKCPParam(trunk)
	for _, c := range conns {
		if _, err := trunk.AddConn(c); err != nil {
			_ = c.Close()
			_ = trunk.Close()
			return err
		}
	}

	// 空闲连接检测：1 分钟未收到数据则替换（faux_tcp 保活包不产生上层数据，
	// 因此长期无代理流量时物理连接会被主动轮换，避免 NAT/防火墙表项老化）。
	trunk.SetIdleTimeout(60*time.Second, func(connID int) io.ReadWriteCloser {
		conns, err := p.TrunkConn(ctx, conv, 1)
		if err != nil {
			log.Ctx(ctx).Warn().Caller().Err(err).Msg("failed to create replacement conn for idle detection")
			return nil
		}
		return conns[0]
	})

	// 慢速连接检测：创建超过 30 分钟且速率低于平均 10% 的连接替换
	trunk.SetSlowConnDetection(0.1, 30*time.Minute, func(connID int) io.ReadWriteCloser {
		conns, err := p.TrunkConn(ctx, conv, 1)
		if err != nil {
			log.Ctx(ctx).Warn().Caller().Err(err).Msg("failed to create replacement conn for slow connection detection")
			return nil
		}
		return conns[0]
	})

	p.mu.Lock()
	p.trunk = trunk
	p.trunkPeer = trunkPeer
	p.mu.Unlock()

	go func() {
		trunk.Run(ctx)
		log.Ctx(ctx).Warn().Caller().Msg("trunk stopped, will attempt to reconnect")

		p.mu.Lock()
		if p.trunk == trunk {
			p.trunk = nil
			if p.trunkPeer == trunkPeer && !trunkPeer.IsClosed() {
				select {
				case p.ChPeer <- trunkPeer:
				default:
					_ = trunkPeer.Close(ctx)
				}
			}
			p.trunkPeer = nil
		}
		p.mu.Unlock()

		time.Sleep(3 * time.Second)
		log.Ctx(ctx).Info().Caller().Msg("attempting to reconnect trunk")
		if err := p.InitTrunk(ctx); err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("failed to reconnect trunk")
		} else {
			log.Ctx(ctx).Info().Caller().Msg("trunk reconnected successfully")
		}
	}()

	return nil
}

// RemoveTrunkConn closes a local physical connection. The remote recvLoop
// removes its matching connection on EOF; physical IDs are local to each trunk.
func (p *SocksCli) RemoveTrunkConn(ctx context.Context, id int) error {
	p.mu.Lock()
	trunk := p.trunk
	p.mu.Unlock()
	if trunk == nil {
		return errors.New("trunk not ready")
	}
	return trunk.RemoveConn(id)
}

// AddTrunkConn 向已运行的 trunk_kcp 动态加入 n 条新的底层连接（faux_tcp）。
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
			log.Ctx(ctx).Info().Caller().Int("current", cur).Int("target", target).Int("add", add).Msg("trunk conn missing, adding")
			if err := p.AddTrunkConn(ctx, add); err != nil {
				log.Ctx(ctx).Warn().Caller().Err(err).Msg("add trunk conn failed")
			}
		}
	}
}

// TrunkConn 建立 n 条物理连接：faux_tcp 拨号 → 类型字节 → 裸 RPC →
// TrunkUpgrade 交给 trunk_kcp。物理连接上不做 Auth（无秘密可传；服务端按
// conv 关联控制通道里已授权的会话）。Upgrade 之后该连接只跑 KCP 段。
func (p *SocksCli) TrunkConn(ctx context.Context, conv uint32, n int) ([]io.ReadWriteCloser, error) {
	g := errgroup.Group{}
	var mu sync.Mutex
	conns := make([]io.ReadWriteCloser, 0, n)
	for i := 0; i < n; i++ {
		i := i
		g.Go(func() error {
			conn, err := p.dial(ctx)
			if err != nil {
				return err
			}
			if _, err := conn.Write([]byte{connTypePhysical}); err != nil {
				_ = conn.Close()
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
	log.Ctx(ctx).Info().Caller().Str("addr", addr).Msg("SOCKS5 listening")
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
		log.Ctx(ctx).Debug().Caller().Err(err).Msg("socks handshake failed")
		return
	}
	if err := p.openProxy(ctx, rc, addr.String(), nil); err != nil {
		log.Ctx(ctx).Warn().Caller().Err(err).Str("addr", addr.String()).Msg("open SOCKS proxy failed")
	}
}

// RunHTTPProxy 启动 HTTP CONNECT 监听。
func (p *SocksCli) RunHTTPProxy(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Ctx(ctx).Info().Caller().Str("addr", addr).Msg("HTTP proxy listening")
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
	if err := req.HTTPSReply(); err != nil {
		return
	}
	if err := p.openProxy(ctx, inConn, req.Host, nil); err != nil {
		log.Ctx(ctx).Warn().Caller().Err(err).Str("addr", req.Host).Msg("open HTTP proxy failed")
	}
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
	vconn, err := trunk.OpenConn(maxV)
	if err != nil {
		return err
	}
	defer vconn.Close()

	// 数据面端到端 TLS：open header（含目标地址）与代理数据都在 TLS 之内
	var rwc io.ReadWriteCloser = vconn
	if p.TLSCfg != nil {
		tlsConn, err := wrapTLSClient(ctx, vconn, nil, nil, p.TLSCfg, 10*time.Second)
		if err != nil {
			_ = vconn.Close()
			return err
		}
		rwc = tlsConn
	}
	if err := WriteOpenHeader(rwc, addr, head); err != nil {
		_ = rwc.Close()
		return err
	}
	relay(ctx, rwc, local)
	return nil
}
