package faux_tcp

import (
	"io"
	"sync"
)

// Link 报文链路层接口：收发的是完整 IPv4 报文（不含以太网头）。
// 真实实现见 link_linux.go；测试用内存实现见 newMemNet。
type Link interface {
	// WritePacket 发送一个 IPv4 报文（尽力而为，失败返回错误）
	WritePacket(bs []byte) error
	// ReadPacket 阻塞接收一个 IPv4 报文；关闭后返回 io.EOF
	ReadPacket() ([]byte, error)
	// Close 关闭链路
	Close() error
}

// LinkDescriber 可选接口：链路实现可提供一句本端链路描述（网卡/本地地址/
// 收包模式），握手或连接失败时拼进错误信息，便于跨机排查（例如网卡选错）。
type LinkDescriber interface {
	Describe() string
}

// ---------------------------------------------------------------------------
// 内存链路（测试用）
// ---------------------------------------------------------------------------

// memNet 一个虚拟网络：按 IPv4 报文的目的 IP 路由到注册的链路。
// 可通过 SetHook 注入丢包/乱序用于状态机测试。
type memNet struct {
	mu    sync.Mutex
	links map[[4]byte]*memLink // key: 目的 IPv4

	// 测试注入钩子：返回 false 表示丢包。
	// 默认 nil（不丢包）。
	hook func(src, dst [4]byte, bs []byte) bool
}

func newMemNet() *memNet {
	return &memNet{links: make(map[[4]byte]*memLink)}
}

// link 注册（或获取）指定 IPv4 的链路。一个 IP 对应一条链路（端口在报文内）。
func (n *memNet) link(ip [4]byte) *memLink {
	n.mu.Lock()
	defer n.mu.Unlock()
	l, ok := n.links[ip]
	if !ok {
		l = &memLink{
			net:  n,
			ip:   ip,
			ch:   make(chan []byte, 4096),
			done: make(chan struct{}),
		}
		n.links[ip] = l
	}
	return l
}

func (n *memNet) deliver(src, dst [4]byte, bs []byte) {
	if n.hook != nil && !n.hook(src, dst, bs) {
		return // 丢包
	}
	n.mu.Lock()
	l := n.links[dst]
	n.mu.Unlock()
	if l == nil {
		return // 目的不存在：静默丢弃（与真实网络一致）
	}
	cp := append([]byte(nil), bs...)
	select {
	case l.ch <- cp:
	case <-l.done:
	default:
		// 队列满 = 丢包（真实网络的队列溢出行为；阻塞会死锁：
		// 发送方持锁等待投递，接收方的处理循环也需要同一把锁）
	}
}

type memLink struct {
	net  *memNet
	ip   [4]byte
	ch   chan []byte
	done chan struct{}
	once sync.Once
}

func (l *memLink) WritePacket(bs []byte) error {
	if len(bs) < ipv4HeaderLen+tcpHeaderLen {
		return io.ErrClosedPipe
	}
	var src, dst [4]byte
	copy(src[:], bs[12:16])
	copy(dst[:], bs[16:20])
	l.net.deliver(src, dst, bs)
	return nil
}

func (l *memLink) ReadPacket() ([]byte, error) {
	select {
	case bs, ok := <-l.ch:
		if !ok {
			return nil, io.EOF
		}
		return bs, nil
	case <-l.done:
		return nil, io.EOF
	}
}

func (l *memLink) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}
