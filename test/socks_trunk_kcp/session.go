package socks_kcp

import (
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/test/socks_trunk_kcp/pb"
	"github.com/lxt1045/rpc/trunk_kcp"
	"github.com/lxt1045/utils/log"
)

var sessionManager = newSessionManager()

type session struct {
	clientID string
	conv     uint32
	maxVConn int

	mu    sync.Mutex
	conns []sessionConn
	trunk *trunk_kcp.TrunkKCP
	svcs  map[*SocksSvc]struct{}
}

type sessionConn struct {
	rw io.ReadWriteCloser
}

type sessionManagerType struct {
	mu         sync.Mutex
	token      string
	maxClients int
	maxVConn   int
	sessions   map[string]*session
}

func newSessionManager() *sessionManagerType {
	return &sessionManagerType{
		sessions:   make(map[string]*session),
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

	// 如果是重连，检查是否需要清理旧的 trunk
	sess.mu.Lock()
	if sess.trunk != nil && sess.trunk.ConnCount() == 0 {
		// trunk 存在但没有活跃连接，说明是旧的，需要清理
		oldTrunk := sess.trunk
		sess.trunk = nil
		sess.conns = nil
		log.Ctx(ctx).Info().Msg("cleaning up stale trunk in Auth")
		go func() { _ = oldTrunk.Close() }()
	}
	sess.svcs[p] = struct{}{}
	sess.mu.Unlock()

	p.mu.Lock()
	p.authorized = true
	p.clientID = clientID
	p.sess = sess
	p.mu.Unlock()
	p.Name = req.Name
	return &pb.AuthRsp{Status: pb.AuthRsp_Succ}, nil
}

func (p *SocksSvc) Conn(ctx context.Context, req *pb.ConnReq) (*pb.ConnRsp, error) {
	return nil, fmt.Errorf("Conn is not used by socks_trunk_kcp")
}

func (p *SocksSvc) ConnUpgrade(ctx context.Context, req *pb.ConnUpgradeReq) (*pb.ConnUpgradeRsp, error) {
	return nil, fmt.Errorf("ConnUpgrade is not used by socks_trunk_kcp")
}

func (p *SocksSvc) TrunkUpgrade(ctx context.Context, req *pb.TrunkUpgradeReq) (*pb.TrunkUpgradeRsp, error) {
	if !p.isAuthorized() {
		return nil, fmt.Errorf("not authenticated")
	}
	upgrade := codec.GetUpgrade(ctx)
	if upgrade == nil {
		return nil, fmt.Errorf("upgrade is nil")
	}
	sess, err := p.session()
	if err != nil {
		upgrade.Close()
		return nil, err
	}
	sess.mu.Lock()
	if sess.trunk != nil {
		trunk := sess.trunk
		sess.mu.Unlock()
		// 检查 trunk 是否健康（有物理连接且有虚拟连接）
		connCount := trunk.ConnCount()
		virtualCount := trunk.VirtualConnCount()
		if connCount > 0 && virtualCount > 0 {
			if _, err := trunk.AddConn(upgrade); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("AddConn to existing trunk failed")
				upgrade.Close()
				return nil, err
			}
			log.Ctx(ctx).Info().Int("conn_count", connCount).Int("virtual_count", virtualCount).Msg("added conn to existing trunk")
			return &pb.TrunkUpgradeRsp{}, nil
		}
		// trunk 没有活跃连接或虚拟连接，说明正在关闭或已废弃，需要重新创建
		log.Ctx(ctx).Warn().Int("conn_count", connCount).Int("virtual_count", virtualCount).Msg("trunk exists but is stale or closing, treating as new session")
		sess.mu.Lock()
	}
	sess.conns = append(sess.conns, sessionConn{rw: upgrade})
	sess.mu.Unlock()
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

func (p *SocksSvc) TrunkStart(ctx context.Context, req *pb.TrunkStartReq) (*pb.TrunkStartRsp, error) {
	if !p.isAuthorized() {
		return nil, fmt.Errorf("not authenticated")
	}
	sess, err := p.session()
	if err != nil {
		return nil, err
	}
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
	conns := append([]sessionConn(nil), sess.conns...)
	// 清空 conns 列表，防止重复使用
	sess.conns = nil
	sess.mu.Unlock()
	if len(conns) != int(req.UpgradeCount) {
		return nil, fmt.Errorf("upgrade count mismatch: have %d want %d", len(conns), req.UpgradeCount)
	}
	rws := make([]io.ReadWriteCloser, 0, len(conns))
	for _, c := range conns {
		rws = append(rws, c.rw)
	}
	maxVConn := p.maxVirtualConns()

	// 创建回调函数，按需处理新的虚拟连接
	onNewConn := func(vconn *trunk_kcp.VirtualConn) {
		p.serveVirtualConn(ctx, vconn)
	}

	trunk := trunk_kcp.NewTrunkKCP(req.TrunkId, onNewConn, rws...)
	sess.mu.Lock()
	sess.conv = req.TrunkId
	sess.maxVConn = maxVConn
	sess.trunk = trunk
	sess.mu.Unlock()
	p.mu.Lock()
	p.trunk = trunk
	p.mu.Unlock()

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

func (p *SocksSvc) serveVirtualConn(ctx context.Context, vconn *trunk_kcp.VirtualConn) {
	if vconn == nil {
		return
	}
	msg, err := ReadOpenHeader(vconn)
	if err != nil {
		if err != io.EOF {
			log.Ctx(ctx).Debug().Err(err).Msg("virtual conn closed before open")
		}
		return
	}
	if !CheckACL(msg.Addr) {
		log.Ctx(ctx).Warn().Str("addr", msg.Addr).Msg("acl deny")
		_ = vconn.Close()
		return
	}
	d := net.Dialer{Timeout: 30 * time.Second}
	rc, err := d.Dial("tcp", msg.Addr)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("addr", msg.Addr).Msg("dial target failed")
		_ = vconn.Close()
		return
	}
	if len(msg.Body) > 0 {
		if _, err := rc.Write(msg.Body); err != nil {
			rc.Close()
			_ = vconn.Close()
			return
		}
	}
	log.Ctx(ctx).Info().Str("addr", msg.Addr).Msg("proxy connected")
	relay(ctx, vconn, rc)
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
	empty := len(sess.svcs) == 0 && len(sess.conns) == 0
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
		if sess.trunk != nil {
			_ = sess.trunk.Close()
		}
		for _, c := range sess.conns {
			if c.rw != nil {
				_ = c.rw.Close()
			}
		}
		sess.mu.Unlock()
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
