package socks_faux_kcp

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/pb"
	"github.com/lxt1045/rpc/trunk_kcp"
	"github.com/lxt1045/utils/log"
)

var sessionManager = newSessionManager()

// serverTLS 服务端 TLS 配置（数据面 VirtualConn 与控制通道共用）；nil = 明文模式。
var serverTLS atomic.Pointer[tls.Config]

// SetServerTLSConfig 设置服务端 TLS 配置（main 在监听前调用一次）。
func SetServerTLSConfig(cfg *tls.Config) {
	serverTLS.Store(cfg)
}

func serverTLSConfig() *tls.Config { return serverTLS.Load() }

type session struct {
	clientID string
	conv     uint32
	maxVConn int

	// authorized 会话级授权：由控制通道（迷你 trunk + TLS）上的 Auth 置位。
	// 物理连接的 TrunkUpgrade 只认已授权会话。
	authorized atomic.Bool

	mu    sync.Mutex
	trunk *trunk_kcp.TrunkKCP
	svcs  map[*SocksSvc]struct{}
}

type sessionManagerType struct {
	mu         sync.Mutex
	token      string
	maxClients int
	maxVConn   int
	kcpCfg     TrunkKCPConfig // KCP NoDelay 参数（NoDelayParam 为全 -1 时用库默认）
	sessions   map[string]*session
	byConv     map[uint32]*session // 数据面 trunk conv → 会话（TrunkUpgrade 鉴权用）
}

func newSessionManager() *sessionManagerType {
	return &sessionManagerType{
		sessions:   make(map[string]*session),
		byConv:     make(map[uint32]*session),
		maxClients: 1024,
		maxVConn:   256,
	}
}

func initServerSecurity(token string, maxClients int, conv uint32, maxVConn int) error {
	if token == "" {
		return fmt.Errorf("server token is required")
	}
	sessionManager.mu.Lock()
	sessionManager.token = token
	if maxClients > 0 {
		sessionManager.maxClients = maxClients
	}
	if maxVConn > 0 {
		sessionManager.maxVConn = maxVConn
	}
	sessionManager.mu.Unlock()
	return nil
}

// InitServerSecurity 是 main 调用的服务端初始化入口。
func InitServerSecurity(token string, maxClients int, conv uint32, maxVConn int) error {
	return initServerSecurity(token, maxClients, conv, maxVConn)
}

// SetServerTrunkConfig 设置服务端创建 TrunkKCP 时使用的链路参数
// （主要是 KCP NoDelay 参数，需与客户端配置一致）。
// 应在 InitServerSecurity 之后、接受连接之前调用。
func SetServerTrunkConfig(cfg TrunkKCPConfig) {
	cfg.defaults()
	sessionManager.mu.Lock()
	sessionManager.kcpCfg = cfg
	sessionManager.mu.Unlock()
}

// registerConv 建立 conv → 会话索引（TrunkStart 时调用）。
func (m *sessionManagerType) registerConv(conv uint32, sess *session) {
	m.mu.Lock()
	m.byConv[conv] = sess
	m.mu.Unlock()
}

// lookupConv 按 conv 查会话（物理连接 TrunkUpgrade 鉴权用）。
func (m *sessionManagerType) lookupConv(conv uint32) *session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byConv[conv]
}

// unregisterConv 摘除 conv 索引（会话关闭时调用）。
func (m *sessionManagerType) unregisterConv(conv uint32, sess *session) {
	m.mu.Lock()
	if m.byConv[conv] == sess {
		delete(m.byConv, conv)
	}
	m.mu.Unlock()
}

// SocksSvc 是服务端每个 RPC 连接对应的 service 实例。
type SocksSvc struct {
	Name       string
	LocalAddr  string
	RemoteAddr string
	Peer       rpc.Peer

	mu         sync.Mutex
	authorized bool
	clientID   string
	sess       *session

	trunk *trunk_kcp.TrunkKCP
}

var _ pb.SocksSvcServer = &SocksSvc{}

func (p *SocksSvc) close(ctx context.Context) error {
	sessionManager.unregister(p)
	return p.Peer.Close(ctx)
}

func (p *SocksSvc) Close(ctx context.Context, req *pb.CloseReq) (*pb.CloseRsp, error) {
	_ = p.close(ctx)
	return &pb.CloseRsp{}, nil
}

// Auth 认证。本示例中 Auth 只从控制通道（迷你 trunk + TLS）发起，
// token 不会以明文出现在 faux_tcp 链路上。
func (p *SocksSvc) Auth(ctx context.Context, req *pb.AuthReq) (*pb.AuthRsp, error) {
	if req == nil || req.Name == "" {
		return &pb.AuthRsp{Status: pb.AuthRsp_Fail, Err: &pb.Err{Msg: "empty token"}}, nil
	}
	sessionManager.mu.Lock()
	expected := sessionManager.token
	maxClients := sessionManager.maxClients
	maxVConn := sessionManager.maxVConn
	sessionManager.mu.Unlock()
	if expected == "" {
		return &pb.AuthRsp{Status: pb.AuthRsp_Fail, Err: &pb.Err{Msg: "server security not configured"}}, nil
	}
	if subtle.ConstantTimeCompare([]byte(expected), []byte(req.Name)) != 1 {
		log.Ctx(ctx).Warn().Str("remote", p.RemoteAddr).Msg("auth failed")
		return &pb.AuthRsp{Status: pb.AuthRsp_Fail, Err: &pb.Err{Msg: "invalid token"}}, nil
	}

	clientID := req.Name
	sessionManager.mu.Lock()
	sess, ok := sessionManager.sessions[clientID]
	if !ok {
		if len(sessionManager.sessions) >= maxClients {
			sessionManager.mu.Unlock()
			return &pb.AuthRsp{Status: pb.AuthRsp_Fail, Err: &pb.Err{Msg: "too many clients"}}, nil
		}
		sess = &session{
			clientID: clientID,
			svcs:     make(map[*SocksSvc]struct{}),
			maxVConn: maxVConn,
		}
		sessionManager.sessions[clientID] = sess
	}
	sessionManager.mu.Unlock()

	// 注意：不在此做"旧 trunk 清理"——0 连接启动的新 trunk（TrunkStart 刚建、
	// 物理连接还没 AddConn）ConnCount() 也是 0，误杀会造成 TrunkUpgrade 被拒
	// 的竞态；重连时旧 trunk 由 TrunkStart 的"先关旧"逻辑回收。
	sess.mu.Lock()
	sess.svcs[p] = struct{}{}
	sess.mu.Unlock()

	p.mu.Lock()
	p.authorized = true
	p.clientID = clientID
	p.sess = sess
	p.mu.Unlock()
	p.Name = req.Name

	// 会话级授权（物理连接的 TrunkUpgrade 依此判定）
	sess.authorized.Store(true)
	return &pb.AuthRsp{Status: pb.AuthRsp_Succ}, nil
}

func (p *SocksSvc) Conn(ctx context.Context, req *pb.ConnReq) (*pb.ConnRsp, error) {
	return nil, fmt.Errorf("Conn is not used by socks_faux_trunk_kcp")
}

func (p *SocksSvc) ConnUpgrade(ctx context.Context, req *pb.ConnUpgradeReq) (*pb.ConnUpgradeRsp, error) {
	return nil, fmt.Errorf("ConnUpgrade is not used by socks_faux_trunk_kcp")
}

// TrunkUpgrade 由物理连接（裸 RPC，无秘密）发起：把当前连接升级为数据面
// trunk 的物理连接。鉴权方式：req.TrunkId 必须对应一个已授权且已 TrunkStart
// 的会话（授权发生在控制通道的 TLS 内）。
func (p *SocksSvc) TrunkUpgrade(ctx context.Context, req *pb.TrunkUpgradeReq) (*pb.TrunkUpgradeRsp, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	upgrade := codec.GetUpgrade(ctx)
	if upgrade == nil {
		return nil, fmt.Errorf("upgrade is nil")
	}
	sess := sessionManager.lookupConv(req.TrunkId)
	if sess == nil || !sess.authorized.Load() {
		upgrade.Close()
		return nil, fmt.Errorf("trunk %d not authorized", req.TrunkId)
	}
	sess.mu.Lock()
	trunk := sess.trunk
	sess.mu.Unlock()
	if trunk == nil {
		upgrade.Close()
		return nil, fmt.Errorf("trunk %d not started", req.TrunkId)
	}
	id, err := trunk.AddConn(upgrade)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("AddConn to trunk failed")
		upgrade.Close()
		return nil, err
	}
	log.Ctx(ctx).Info().Int("conn_id", id).Uint32("trunk_id", req.TrunkId).Msg("physical conn upgraded into trunk")
	return &pb.TrunkUpgradeRsp{}, nil
}

// TrunkRemoveConn 由对端发起，优雅剔除一条底层物理连接。
func (p *SocksSvc) TrunkRemoveConn(ctx context.Context, req *pb.TrunkUpgradeReq) (*pb.TrunkUpgradeRsp, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	if !p.isAuthorized() {
		return nil, fmt.Errorf("not authenticated")
	}
	sess, err := p.session()
	if err != nil {
		return nil, err
	}
	sess.mu.Lock()
	trunk := sess.trunk
	sess.mu.Unlock()
	if trunk == nil {
		return &pb.TrunkUpgradeRsp{}, nil
	}
	id := int(req.UpgradeId)
	_ = trunk.CloseWriteConn(id)
	_ = trunk.RemoveConn(id)
	return &pb.TrunkUpgradeRsp{}, nil
}

// TrunkStart 由控制通道（已认证）发起：创建数据面 trunk（0 物理连接启动，
// 物理连接随后由各条连接的 TrunkUpgrade 逐个 AddConn 加入）。
func (p *SocksSvc) TrunkStart(ctx context.Context, req *pb.TrunkStartReq) (*pb.TrunkStartRsp, error) {
	if !p.isAuthorized() {
		return nil, fmt.Errorf("not authenticated")
	}
	sess, err := p.session()
	if err != nil {
		return nil, err
	}
	if !sess.authorized.Load() {
		return nil, fmt.Errorf("session not authorized")
	}
	maxVConn := p.maxVirtualConns()

	sess.mu.Lock()
	// 如果已存在 trunk，先关闭旧的
	if sess.trunk != nil {
		oldTrunk := sess.trunk
		sess.trunk = nil
		sess.mu.Unlock()
		log.Ctx(ctx).Info().Msg("closing old trunk before creating new one")
		_ = oldTrunk.Close()
		sess.mu.Lock()
	}

	onNewConn := func(vconn *trunk_kcp.VirtualConn) {
		p.serveVirtualConn(ctx, vconn)
	}

	// 0 物理连接启动（trunk_kcp 原生支持 AddConn 动态加入）
	trunk := trunk_kcp.NewTrunkKCP(req.TrunkId, onNewConn)
	sessionManager.mu.Lock()
	kcpCfg := sessionManager.kcpCfg
	sessionManager.mu.Unlock()
	kcpCfg.ApplyKCPParam(trunk)
	sess.conv = req.TrunkId
	sess.maxVConn = maxVConn
	sess.trunk = trunk
	sess.mu.Unlock()
	p.mu.Lock()
	p.trunk = trunk
	p.mu.Unlock()

	sessionManager.registerConv(req.TrunkId, sess)
	go trunk.Run(ctx)
	return &pb.TrunkStartRsp{}, nil
}

func (p *SocksSvc) isAuthorized() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.authorized
}

func (p *SocksSvc) session() (*session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sess == nil {
		return nil, fmt.Errorf("not authenticated")
	}
	return p.sess, nil
}

func (p *SocksSvc) maxVirtualConns() int {
	if p.sess != nil {
		p.sess.mu.Lock()
		v := p.sess.maxVConn
		p.sess.mu.Unlock()
		if v > 0 {
			return v
		}
	}
	sessionManager.mu.Lock()
	defer sessionManager.mu.Unlock()
	if sessionManager.maxVConn <= 0 {
		return 256
	}
	return sessionManager.maxVConn
}

// serveVirtualConn 处理数据面虚拟连接：启用 TLS 时先把 vconn 包成 TLS 连接
// （握手在内进行），open header（含目标地址）与应用数据都在 TLS 之内。
func (p *SocksSvc) serveVirtualConn(ctx context.Context, vconn *trunk_kcp.VirtualConn) {
	if vconn == nil {
		return
	}
	defer vconn.Close()

	var rwc io.ReadWriteCloser = vconn
	if cfg := serverTLSConfig(); cfg != nil {
		tlsConn, err := wrapTLSServer(ctx, vconn, nil, nil, cfg, 10*time.Second)
		if err != nil {
			log.Ctx(ctx).Debug().Err(err).Msg("virtual conn tls handshake failed")
			return
		}
		rwc = tlsConn
	}

	msg, err := ReadOpenHeader(rwc)
	if err != nil {
		if err != io.EOF {
			log.Ctx(ctx).Debug().Err(err).Msg("virtual conn closed before open")
		}
		return
	}
	if !CheckACL(msg.Addr) {
		log.Ctx(ctx).Warn().Str("addr", msg.Addr).Msg("acl deny")
		_ = rwc.Close()
		return
	}
	d := net.Dialer{Timeout: 30 * time.Second}
	rc, err := d.Dial("tcp", msg.Addr)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("addr", msg.Addr).Msg("dial target failed")
		_ = rwc.Close()
		return
	}
	if len(msg.Body) > 0 {
		if _, err := rc.Write(msg.Body); err != nil {
			rc.Close()
			_ = rwc.Close()
			return
		}
	}
	log.Ctx(ctx).Info().Str("addr", msg.Addr).Msg("proxy connected")
	relay(ctx, rwc, rc)
}

func (m *sessionManagerType) unregister(svc *SocksSvc) {
	svc.mu.Lock()
	clientID := svc.clientID
	sess := svc.sess
	svc.mu.Unlock()
	if clientID == "" || sess == nil {
		return
	}
	sess.mu.Lock()
	delete(sess.svcs, svc)
	empty := len(sess.svcs) == 0
	sess.mu.Unlock()
	if empty {
		m.closeSession(clientID)
	}
}

func (m *sessionManagerType) closeSession(clientID string) {
	m.mu.Lock()
	sess, ok := m.sessions[clientID]
	if ok {
		delete(m.sessions, clientID)
	}
	m.mu.Unlock()
	if ok {
		sess.mu.Lock()
		conv := sess.conv
		if sess.trunk != nil {
			_ = sess.trunk.Close()
		}
		sess.mu.Unlock()
		if conv != 0 {
			m.unregisterConv(conv, sess)
		}
	}
}

// SvcClosed 通知 manager 某个 RPC 连接关闭。
func SvcClosed(svc *SocksSvc) {
	sessionManager.unregister(svc)
}

// CloseAllSessions 关闭所有会话。
func CloseAllSessions() {
	sessionManager.mu.Lock()
	ids := make([]string, 0, len(sessionManager.sessions))
	for id := range sessionManager.sessions {
		ids = append(ids, id)
	}
	sessionManager.mu.Unlock()
	for _, id := range ids {
		sessionManager.closeSession(id)
	}
}
