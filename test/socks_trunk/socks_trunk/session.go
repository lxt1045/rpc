package socks

import (
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lxt1045/rpc/test/socks_trunk/socks_trunk/pb"
	"github.com/lxt1045/rpc/trunk"
)

// sessionManager is a process-level manager for authenticated clients and their
// Trunk resources. Unlike the previous package-level mTrunkConn map, every
// resource is owned by an authenticated client session and can be cleaned up.
var sessionManager = NewSessionManager()

// Session holds all Trunk state belonging to one authenticated client.
type Session struct {
	ClientID  string
	TrunkID   uint32
	CreatedAt time.Time
	LastSeen  time.Time

	mu        sync.Mutex
	svcs      map[*SocksSvc]struct{}
	conns     []Conn
	trunk     *trunk.Trunk
	maxConns  int
	closeOnce sync.Once
}

func newSession(clientID string) *Session {
	return &Session{
		ClientID:  clientID,
		CreatedAt: time.Now(),
		LastSeen:  time.Now(),
		svcs:      make(map[*SocksSvc]struct{}),
		maxConns:  128,
	}
}

func (s *Session) touch() {
	s.mu.Lock()
	s.LastSeen = time.Now()
	s.mu.Unlock()
}

func (s *Session) addService(svc *SocksSvc) {
	s.mu.Lock()
	s.svcs[svc] = struct{}{}
	s.LastSeen = time.Now()
	s.mu.Unlock()
}

func (s *Session) addConn(c Conn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trunk != nil {
		return fmt.Errorf("session %s trunk already started", s.ClientID)
	}
	if len(s.conns) >= s.maxConns {
		return fmt.Errorf("session %s exceeds max trunk conns %d", s.ClientID, s.maxConns)
	}
	s.conns = append(s.conns, c)
	s.LastSeen = time.Now()
	return nil
}

func (s *Session) prepareRestartIfNeeded() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trunk == nil || !s.trunk.IsClosed() {
		return
	}
	_ = s.trunk.Close()
	s.trunk = nil
	for _, c := range s.conns {
		if c.rw != nil {
			_ = c.rw.Close()
		}
		metricTrunkConnsAdd(-1)
	}
	s.conns = nil
}

func (s *Session) connsSnapshot() []Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Conn(nil), s.conns...)
}

func (s *Session) takeConns() []Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	conns := s.conns
	s.conns = nil
	return conns
}

func (s *Session) setTrunk(t *trunk.Trunk) {
	s.mu.Lock()
	s.trunk = t
	s.mu.Unlock()
}

func (s *Session) removeService(svc *SocksSvc) (empty bool, conns []Conn) {
	s.mu.Lock()
	delete(s.svcs, svc)
	rest := s.conns[:0]
	for _, c := range s.conns {
		if c.owner == svc {
			if c.rw != nil {
				_ = c.rw.Close()
			}
			metricTrunkConnsAdd(-1)
			continue
		}
		rest = append(rest, c)
	}
	s.conns = rest
	empty = len(s.svcs) == 0 && len(s.conns) == 0
	conns = nil
	s.mu.Unlock()
	return empty, nil
}

func (s *Session) Close() {
	s.closeOnce.Do(func() {
		conns := s.takeConns()
		for _, c := range conns {
			if c.rw != nil {
				_ = c.rw.Close()
			}
			metricTrunkConnsAdd(-1)
		}
		s.mu.Lock()
		if s.trunk != nil {
			_ = s.trunk.Close()
			s.trunk = nil
		}
		s.mu.Unlock()
	})
}

// SessionManager stores all authenticated sessions and their Trunk resources.
type SessionManager struct {
	mu         sync.Mutex
	token      string
	maxClients int
	maxConns   int
	sessions   map[string]*Session
}

// NewSessionManager returns a manager with conservative defaults.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions:   make(map[string]*Session),
		maxClients: 1024,
		maxConns:   128,
	}
}

// Configure sets the server-wide token and capacity limits.
func (m *SessionManager) Configure(token string, maxClients, maxConnsPerClient int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.token = token
	if maxClients > 0 {
		m.maxClients = maxClients
	}
	if maxConnsPerClient > 0 {
		m.maxConns = maxConnsPerClient
	}
	for _, s := range m.sessions {
		s.mu.Lock()
		s.maxConns = m.maxConns
		s.mu.Unlock()
	}
}

func (m *SessionManager) config() (token string, maxClients, maxConns int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.token, m.maxClients, m.maxConns
}

// InitServerSecurity configures the process-level session manager from config.
func InitServerSecurity(token string, maxClients, maxConnsPerClient int) error {
	if token == "" {
		return fmt.Errorf("server token is required")
	}
	sessionManager.Configure(token, maxClients, maxConnsPerClient)
	return nil
}

// Authenticate checks the presented credential and binds the svc connection to
// the session identified by clientID. req.Name is the configured client token
// for this deployment.
func (m *SessionManager) Authenticate(svc *SocksSvc, clientID, token string) error {
	expected, maxClients, maxConns := m.config()
	if expected == "" {
		return fmt.Errorf("server security is not configured")
	}
	if subtle.ConstantTimeCompare([]byte(expected), []byte(token)) != 1 {
		metricAuthFailureInc()
		return fmt.Errorf("invalid token")
	}
	if clientID == "" {
		metricAuthFailureInc()
		return fmt.Errorf("client id is empty")
	}

	m.mu.Lock()
	sess, ok := m.sessions[clientID]
	created := false
	if !ok {
		if len(m.sessions) >= maxClients {
			m.mu.Unlock()
			metricAuthFailureInc()
			return fmt.Errorf("too many clients")
		}
		sess = newSession(clientID)
		m.sessions[clientID] = sess
		created = true
	}
	sess.mu.Lock()
	sess.maxConns = maxConns
	sess.mu.Unlock()
	m.mu.Unlock()
	if created {
		metricSessionsAdd(1)
	}

	sess.addService(svc)

	svc.mu.Lock()
	svc.authorized = true
	svc.clientID = clientID
	svc.session = sess
	svc.mu.Unlock()
	return nil
}

// AddTrunkConn stores an upgrade connection belonging to svc's session.
func (m *SessionManager) AddTrunkConn(svc *SocksSvc, req *pb.TrunkUpgradeReq, rw io.ReadWriteCloser) error {
	sess, err := m.requireSession(svc)
	if err != nil {
		return err
	}
	sess.prepareRestartIfNeeded()
	sess.mu.Lock()
	trunkID := sess.TrunkID
	sess.mu.Unlock()
	if trunkID != 0 && trunkID != req.TrunkId {
		return fmt.Errorf("session trunk id mismatch")
	}
	if trunkID == 0 {
		sess.mu.Lock()
		sess.TrunkID = req.TrunkId
		sess.mu.Unlock()
	}
	if err = sess.addConn(Conn{Req: *req, rw: rw, owner: svc}); err == nil {
		metricTrunkConnsAdd(1)
	}
	return err
}

// StartTrunk assembles all stored upgrade connections into one Trunk.
func (m *SessionManager) StartTrunk(svc *SocksSvc, req *pb.TrunkStartReq) error {
	sess, err := m.requireSession(svc)
	if err != nil {
		return err
	}
	sess.prepareRestartIfNeeded()
	sess.mu.Lock()
	trunkID := sess.TrunkID
	sess.mu.Unlock()
	if trunkID != 0 && trunkID != req.TrunkId {
		return fmt.Errorf("session trunk id mismatch")
	}
	conns := sess.connsSnapshot()
	if len(conns) != int(req.UpgradeCount) {
		return fmt.Errorf("len(conns) != int(req.UpgradeCount),%d != %d", len(conns), int(req.UpgradeCount))
	}
	rws := make([]io.ReadWriteCloser, 0, len(conns))
	for _, c := range conns {
		rws = append(rws, c.rw)
	}
	trunk0 := trunk.NewTrunk(rws...)
	trunk0.SetEventHandler(svc.trunkEvent)
	sess.setTrunk(trunk0)
	svc.mu.Lock()
	svc.trunk = trunk0
	svc.mu.Unlock()
	go trunk0.Run(context.Background())
	return nil
}

func (m *SessionManager) requireSession(svc *SocksSvc) (*Session, error) {
	svc.mu.Lock()
	ok := svc.authorized
	clientID := svc.clientID
	sess := svc.session
	svc.mu.Unlock()
	if !ok || clientID == "" || sess == nil {
		return nil, fmt.Errorf("connection is not authenticated")
	}
	sess.touch()
	return sess, nil
}

// UnregisterSvc is called when one RPC connection is closed. It drops that
// connection's session reference and any Trunk upgrades owned by it.
func (m *SessionManager) UnregisterSvc(svc *SocksSvc) {
	svc.mu.Lock()
	clientID := svc.clientID
	sess := svc.session
	svc.mu.Unlock()
	if clientID == "" || sess == nil {
		return
	}
	empty, _ := sess.removeService(svc)
	if empty {
		m.CloseSession(clientID)
	}
}

// SvcClosed notifies the process-level manager that one RPC connection closed.
func SvcClosed(svc *SocksSvc) {
	sessionManager.UnregisterSvc(svc)
}

// CloseAllSessions closes all authenticated sessions.
func CloseAllSessions() {
	sessionManager.CloseAll()
}

// SessionCount returns the number of active authenticated clients.
func SessionCount() int {
	return sessionManager.Count()
}

// CloseSession removes and closes all resources of clientID.
func (m *SessionManager) CloseSession(clientID string) {
	m.mu.Lock()
	sess, ok := m.sessions[clientID]
	if ok {
		delete(m.sessions, clientID)
	}
	m.mu.Unlock()
	if ok {
		metricSessionsAdd(-1)
		sess.Close()
	}
}

// CloseAll closes all sessions; used during graceful server shutdown.
func (m *SessionManager) CloseAll() {
	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.sessions = make(map[string]*Session)
	m.mu.Unlock()
	if len(sessions) > 0 {
		metricSessionsAdd(-int64(len(sessions)))
	}
	for _, s := range sessions {
		s.Close()
	}
}

func (m *SessionManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}
