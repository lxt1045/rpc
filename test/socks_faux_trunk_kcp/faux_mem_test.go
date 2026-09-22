package socks_faux_kcp

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/faux_tcp"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/pb"
)

// 本文件提供内存 faux_tcp 网络，让整个代理链路（faux_tcp 状态机 + RPC 控制面 +
// trunk_kcp 数据面 + 目标回显）在没有 root/CAP_NET_RAW 的环境下也能端到端跑通。
//
// 拓扑对齐真实实现：
//   - 服务端：一个"网卡"（一条 Link）承载所有四元组，由一个 demux 分流；
//   - 客户端：每条连接一个 Link（真实实现里是每条连接一个 AF_PACKET raw socket），
//     互相之间按目的端口路由，保证每个 demux 只收到自己四元组的报文。

var (
	memSrvIP    = netip.AddrFrom4([4]byte{10, 0, 0, 2})
	memCliIP    = netip.AddrFrom4([4]byte{10, 0, 0, 1})
	memSrvPort  = uint16(18099)
	memCliPort0 = uint16(40000)
)

// memLink 内存链路：写出交给 memNet 按目的端口路由，读入取本链路专属队列。
type memLink struct {
	net       *memNet
	ownerPort uint16 // 0 表示服务端链路
	rx        chan []byte
	done      chan struct{}
	once      sync.Once
	closed    atomic.Bool
}

// memNet 内存网络：按目的 TCP 端口把报文路由到对应链路（无丢包，可注入）。
type memNet struct {
	mu      sync.Mutex
	srv     *memLink
	clients map[uint16]*memLink
	// loss 返回 true 表示丢弃该报文（dstPort 为报文目的端口）
	loss func(dstPort uint16, bs []byte) bool
}

func newMemNet() *memNet {
	n := &memNet{clients: make(map[uint16]*memLink)}
	n.srv = n.newLink(0)
	return n
}

func (n *memNet) newLink(ownerPort uint16) *memLink {
	return &memLink{
		net:       n,
		ownerPort: ownerPort,
		rx:        make(chan []byte, 8192),
		done:      make(chan struct{}),
	}
}

// serverLink 服务端链路（唯一）。
func (n *memNet) serverLink() *memLink { return n.srv }

// clientLink 为客户端的某条连接创建专属链路（真实实现：每条连接一个 raw socket）。
func (n *memNet) clientLink(localPort uint16) *memLink {
	l := n.newLink(localPort)
	n.mu.Lock()
	n.clients[localPort] = l
	n.mu.Unlock()
	return l
}

// setLoss 安装丢包钩子（需在链路运行期间调用，故加锁）。
func (n *memNet) setLoss(f func(dstPort uint16, bs []byte) bool) {
	n.mu.Lock()
	n.loss = f
	n.mu.Unlock()
}

// route 按目的端口投递报文（队列满 = 丢包，与真实网络一致）。
func (n *memNet) route(dstPort uint16, bs []byte) {
	n.mu.Lock()
	loss := n.loss
	var target *memLink
	if dstPort == memSrvPort {
		target = n.srv
	} else {
		target = n.clients[dstPort]
	}
	n.mu.Unlock()

	if loss != nil && loss(dstPort, bs) {
		return
	}
	if target == nil {
		return // 目的不存在：静默丢弃
	}
	cp := append([]byte(nil), bs...)
	select {
	case target.rx <- cp:
	case <-target.done:
	default:
		// 队列满丢包
	}
}

func (l *memLink) WritePacket(bs []byte) error {
	if l.closed.Load() {
		return io.ErrClosedPipe
	}
	if len(bs) < 24 {
		return io.ErrShortBuffer
	}
	// faux_tcp 构包 IPv4 头无选项（IHL=5）：TCP 目的端口位于偏移 22:24
	l.net.route(binary.BigEndian.Uint16(bs[22:24]), bs)
	return nil
}

func (l *memLink) ReadPacket() ([]byte, error) {
	select {
	case bs := <-l.rx:
		return bs, nil
	case <-l.done:
		return nil, io.EOF
	}
}

func (l *memLink) Close() error {
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.done)
	})
	return nil
}

// memDialer 注入 SocksCli 的内存拨号器：每次 Dial 新建一条客户端链路
// （对应真实实现里每条连接一个 raw socket + 独立本地端口）。
type memDialer struct {
	net      *memNet
	nextPort uint32
}

func (d *memDialer) Dial(ctx context.Context) (net.Conn, error) {
	port := memCliPort0 + uint16(atomic.AddUint32(&d.nextPort, 1)-1)
	link := d.net.clientLink(port)
	return faux_tcp.DialWithLink(ctx, faux_tcp.Config{}, link,
		faux_tcp.Endpoint{IP: memCliIP, Port: port},
		faux_tcp.Endpoint{IP: memSrvIP, Port: memSrvPort})
}

// newEchoTarget 启动一个 TCP 回显服务，返回其地址。
func newEchoTarget(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

// proxyRoundTrip 经代理做 rounds 次"写→读回显"，验证数据面完整性。
func proxyRoundTrip(t *testing.T, ctx context.Context, cli *SocksCli, target string, rounds int) {
	t.Helper()
	local, proxyEnd := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- cli.openProxy(ctx, proxyEnd, target, nil) }()
	defer func() {
		_ = local.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("openProxy did not return after local close")
		}
	}()

	buf := make([]byte, 2048)
	for i := 0; i < rounds; i++ {
		msg := []byte(fmt.Sprintf("round-%02d-payload", i))
		if _, err := local.Write(msg); err != nil {
			t.Fatalf("write round %d: %v", i, err)
		}
		_ = local.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := local.Read(buf)
		if err != nil {
			t.Fatalf("read round %d: %v", i, err)
		}
		if string(buf[:n]) != string(msg) {
			t.Fatalf("echo mismatch round %d: got %q want %q", i, buf[:n], msg)
		}
	}
}

// startFauxStack 启动内存 faux_tcp 服务端 + 客户端（控制面与数据面都走内存链路）。
// useTLS=true 时控制通道与数据面都跑端到端 TLS（内存自签 CA，无文件依赖）。
func startFauxStack(t *testing.T, ctx context.Context, trunkCfg TrunkKCPConfig, useTLS bool) (*SocksCli, *memNet) {
	t.Helper()
	const token = "e2e-faux-token"

	if err := InitServerSecurity(token, 8, trunkCfg.Conv, trunkCfg.MaxVirtualConn); err != nil {
		t.Fatal(err)
	}
	SetServerTrunkConfig(trunkCfg)
	t.Cleanup(CloseAllSessions)

	var clientTLS *tls.Config
	if useTLS {
		certs := newTestCerts(t)
		SetServerTLSConfig(certs.serverTLS(t))
		t.Cleanup(func() { SetServerTLSConfig(nil) })
		clientTLS = certs.clientTLS(t)
	} else {
		SetServerTLSConfig(nil)
	}

	srv, err := NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	netw := newMemNet()
	ln := faux_tcp.ListenWithLink(faux_tcp.Config{}, netw.serverLink(),
		faux_tcp.Endpoint{IP: memSrvIP, Port: memSrvPort})
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = srv.Serve(ln) }()

	cli := &SocksCli{
		Name:     "e2e-client",
		Token:    token,
		TrunkCfg: trunkCfg,
		TLSCfg:   clientTLS,
		Dialer:   &memDialer{net: netw},
		ChPeer:   make(chan *Peer, 2),
	}
	go cli.RunConnLoop(ctx)
	if err := cli.InitTrunk(ctx); err != nil {
		t.Fatalf("InitTrunk: %v", err)
	}
	t.Cleanup(func() { _, _ = cli.Close(ctx, &pb.CloseReq{}) })
	return cli, netw
}

// TestFauxTrunkProxyEndToEnd 全链路端到端：openProxy → KCP 虚拟连接 →
// 服务端拨号目标 → 双向中继，全程跑在内存 faux_tcp 上（无需 root）。
func TestFauxTrunkProxyEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	target := newEchoTarget(t)
	trunkCfg := TrunkKCPConfig{Conv: 0x66aa55, MinConns: 2, MaxConns: 2, MaxVirtualConn: 16}
	cli, _ := startFauxStack(t, ctx, trunkCfg, true)

	// 两条并发虚拟连接（验证多路复用 + 多条物理连接的负载分担）
	proxyRoundTrip(t, ctx, cli, target, 32)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			proxyRoundTrip(t, ctx, cli, target, 16)
		}()
	}
	wg.Wait()

	if n := cli.trunk.ConnCount(); n != trunkCfg.MaxConns {
		t.Fatalf("physical conn count = %d, want %d", n, trunkCfg.MaxConns)
	}
}

// TestFauxTrunkProxySurvivesLoss 物理链路注入丢包（安装于建链之后，只影响数据面）：
// faux_tcp 不重传，可靠性完全由 KCP 提供——回显必须 100% 正确。
func TestFauxTrunkProxySurvivesLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	target := newEchoTarget(t)
	trunkCfg := TrunkKCPConfig{Conv: 0x66aa56, MinConns: 2, MaxConns: 2, MaxVirtualConn: 16}
	cli, netw := startFauxStack(t, ctx, trunkCfg, true)

	// 建链完成后注入 ~12% 丢包（丢每 8 个报文中的 1 个）
	var n uint64
	var mu sync.Mutex
	netw.setLoss(func(dstPort uint16, bs []byte) bool {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		return cur%8 == 0
	})

	proxyRoundTrip(t, ctx, cli, target, 64)
}

// TestFauxTrunkProxyPlaintext 明文模式（tls.enabled=false）：兼容路径仍可用。
func TestFauxTrunkProxyPlaintext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := newEchoTarget(t)
	trunkCfg := TrunkKCPConfig{Conv: 0x66aa57, MinConns: 2, MaxConns: 2, MaxVirtualConn: 16}
	cli, _ := startFauxStack(t, ctx, trunkCfg, false)
	proxyRoundTrip(t, ctx, cli, target, 16)
}

// TestTrunkUpgradeRequiresAuthorization 物理连接的 TrunkUpgrade 必须对应
// 已授权会话（控制通道 TLS 内完成 Auth）；未授权直接拒绝。
func TestTrunkUpgradeRequiresAuthorization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := InitServerSecurity("tok-unauth", 8, 0x7788, 16); err != nil {
		t.Fatal(err)
	}
	SetServerTrunkConfig(TrunkKCPConfig{Conv: 0x7788, MinConns: 1, MaxConns: 1})
	t.Cleanup(CloseAllSessions)

	srv, err := NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	netw := newMemNet()
	ln := faux_tcp.ListenWithLink(faux_tcp.Config{}, netw.serverLink(),
		faux_tcp.Endpoint{IP: memSrvIP, Port: memSrvPort})
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()

	// 直接拨一条物理连接（0x02）并调 TrunkUpgrade：没有任何认证/TrunkStart
	d := &memDialer{net: netw}
	conn, err := d.Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{connTypePhysical}); err != nil {
		t.Fatal(err)
	}
	cli := &SocksCli{}
	peer, err := rpc.NewPeer(ctx, cli, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Conn(ctx, conn); err != nil {
		t.Fatal(err)
	}
	err = peer.Invoke(ctx, "TrunkUpgrade", &pb.TrunkUpgradeReq{TrunkId: 0x7788, UpgradeId: 0}, &pb.TrunkUpgradeRsp{})
	if err == nil {
		t.Fatal("unauthorized TrunkUpgrade should be rejected")
	}
	t.Logf("rejected as expected: %v", err)
}
