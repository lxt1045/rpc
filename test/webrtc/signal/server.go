package signal

import (
	"context"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/lxt1045/utils/log"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server 是内存版 rendezvous: 记录 id -> 连接的映射, 按 To 字段转发。
//
// 安全提示: 这个实现不带任何鉴权, 任何人都能注册 id 并向他人发 offer,
// 等于开放任意人借用出口节点。仅适用于本地联调, 对外暴露前必须加接入认证。
type Server struct {
	mu    sync.RWMutex
	peers map[string]*conn
}

func NewServer() *Server {
	return &Server{peers: make(map[string]*conn)}
}

// conn 每个连接一个写 goroutine + 写队列: gorilla/websocket 并发写会 panic。
type conn struct {
	id   string
	ws   *websocket.Conn
	send chan Msg
	once sync.Once
	done chan struct{}
}

func (c *conn) close() {
	c.once.Do(func() {
		close(c.done)
		c.ws.Close()
	})
}

// push 把消息投进写队列。连接已关闭时丢弃, 不阻塞调用方。
func (c *conn) push(m Msg) {
	select {
	case c.send <- m:
	case <-c.done:
	}
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serve)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("websocket upgrade")
		return
	}

	c := &conn{
		ws:   ws,
		send: make(chan Msg, 16),
		done: make(chan struct{}),
	}
	defer func() {
		c.close()
		s.remove(c)
	}()

	go c.writeLoop(ctx)
	s.readLoop(ctx, c)
}

func (c *conn) writeLoop(ctx context.Context) {
	for {
		select {
		case m := <-c.send:
			if err := c.ws.WriteJSON(m); err != nil {
				log.Ctx(ctx).Info().Caller().Err(err).Str("id", c.id).Msg("write")
				c.close()
				return
			}
		case <-c.done:
			return
		}
	}
}

func (s *Server) readLoop(ctx context.Context, c *conn) {
	for {
		var m Msg
		if err := c.ws.ReadJSON(&m); err != nil {
			log.Ctx(ctx).Info().Caller().Err(err).Str("id", c.id).Msg("read")
			return
		}

		switch m.Type {
		case TypeRegister:
			if m.From == "" {
				c.push(Msg{Type: TypeError, Err: "register: empty id"})
				continue
			}
			s.register(c, m.From)
			log.Ctx(ctx).Info().Caller().Str("id", m.From).Msg("registered")

		case TypeOffer, TypeAnswer, TypeCandidate:
			if m.From == "" {
				m.From = c.id
			}
			target := s.lookup(m.To)
			if target == nil {
				log.Ctx(ctx).Info().Caller().Str("to", m.To).Str("type", m.Type).Msg("peer offline")
				c.push(Msg{Type: TypeError, To: m.From, Err: m.To + " not online"})
				continue
			}
			target.push(m)

		default:
			c.push(Msg{Type: TypeError, To: c.id, Err: "unknown type: " + m.Type})
		}
	}
}

func (s *Server) register(c *conn, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.peers[id]; ok && old != c {
		old.close()
	}
	c.id = id
	s.peers[id] = c
}

func (s *Server) lookup(id string) *conn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peers[id]
}

// remove 只在映射仍指向 c 时删除, 避免把同 id 的新连接误删。
func (s *Server) remove(c *conn) {
	if c.id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.peers[c.id]; ok && cur == c {
		delete(s.peers, c.id)
	}
}
