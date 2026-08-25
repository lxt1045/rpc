package signal

import (
	"context"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/lxt1045/errors"
	"github.com/lxt1045/utils/log"
)

// Client 是信令客户端: 注册自己的 id, 之后收发消息。
//
// 收到的消息统一投进 Recv() 返回的 channel, 由调用方按 Type 分派。
// 写方向经队列串行化, 可以并发调用 Send。
type Client struct {
	id   string
	ws   *websocket.Conn
	send chan Msg
	recv chan Msg
	once sync.Once
	done chan struct{}
}

// Dial 连上信令服务并注册 id。url 形如 ws://127.0.0.1:18080/signal。
func Dial(ctx context.Context, url, id string) (c *Client, err error) {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, errors.Errorf("dial %s: %v", url, err)
	}

	c = &Client{
		id:   id,
		ws:   ws,
		send: make(chan Msg, 16),
		recv: make(chan Msg, 16),
		done: make(chan struct{}),
	}

	go c.writeLoop(ctx)
	go c.readLoop(ctx)

	if err = c.Send(Msg{Type: TypeRegister, From: id}); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// ID 返回本端注册的 id。
func (c *Client) ID() string { return c.id }

// Recv 返回接收队列。连接断开时 channel 关闭。
func (c *Client) Recv() <-chan Msg { return c.recv }

// Done 在连接断开时关闭。
func (c *Client) Done() <-chan struct{} { return c.done }

// Send 把消息投进发送队列。From 为空时自动填本端 id。
func (c *Client) Send(m Msg) error {
	if m.From == "" {
		m.From = c.id
	}
	select {
	case c.send <- m:
		return nil
	case <-c.done:
		return errors.Errorf("signal client closed")
	}
}

func (c *Client) Close() error {
	c.once.Do(func() {
		close(c.done)
		c.ws.Close()
	})
	return nil
}

func (c *Client) writeLoop(ctx context.Context) {
	for {
		select {
		case m := <-c.send:
			if err := c.ws.WriteJSON(m); err != nil {
				log.Ctx(ctx).Info().Caller().Err(err).Msg("signal write")
				c.Close()
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) readLoop(ctx context.Context) {
	defer close(c.recv)
	defer c.Close()
	for {
		var m Msg
		if err := c.ws.ReadJSON(&m); err != nil {
			select {
			case <-c.done:
			default:
				log.Ctx(ctx).Info().Caller().Err(err).Msg("signal read")
			}
			return
		}
		select {
		case c.recv <- m:
		case <-c.done:
			return
		}
	}
}
