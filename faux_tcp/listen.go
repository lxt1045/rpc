package faux_tcp

import (
	"context"
	"net"
	"net/netip"
	"sync"

	"github.com/lxt1045/errors"
)

var _ net.Listener = &Listener{}

// Listener 伪装 TCP 监听器
type Listener struct {
	cfg      Config
	d        *demux
	addr     Endpoint
	chAccept chan *fconn

	once sync.Once
}

// Listen 在指定地址监听伪装 TCP 连接。
// addr 形如 "0.0.0.0:5678"；IP 必须为 IPv4（0.0.0.0 表示绑定所有接口，
// 会话仍以报文中的目的 IP 为准建立）。
func Listen(ctx context.Context, cfg Config, addr string) (net.Listener, error) {
	cfg.defaults()

	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return nil, errors.Errorf("parse addr: %s", err)
	}
	if !ap.Addr().Is4() && !ap.Addr().IsUnspecified() {
		return nil, errors.Errorf("only IPv4 supported: %s", addr)
	}

	link, err := newRawLink(ap.Addr())
	if err != nil {
		return nil, err
	}
	return listenWithLink(cfg, link, Endpoint{IP: ap.Addr(), Port: ap.Port()}), nil
}

// listenWithLink 在指定链路上监听（测试可注入内存链路）
func listenWithLink(cfg Config, link Link, addr Endpoint) *Listener {
	l := &Listener{
		cfg:      cfg,
		addr:     addr,
		chAccept: make(chan *fconn, 64),
	}
	d := newDemux(cfg, link)
	d.mu.Lock()
	d.ln = l
	d.mu.Unlock()
	l.d = d
	return l
}

// onSyn 处理新 SYN（demux 在无匹配连接时调用）：创建半连接并回 SYN+ACK
func (l *Listener) onSyn(d *demux, p *Packet) {
	// 目的端口不匹配监听器：忽略（demux 的 RST 由 dispatch 逻辑保证只调监听端口？
	// 这里再校验一次，0.0.0.0 监听时按端口匹配）
	if l.addr.Port != p.Dst.Port {
		return
	}
	// 监听地址非 0.0.0.0 时还要求目的 IP 匹配
	if !l.addr.IP.IsUnspecified() && l.addr.IP != p.Dst.IP {
		return
	}
	local := Endpoint{IP: p.Dst.IP, Port: p.Dst.Port}
	c := newFConn(l.cfg, d, local, p.Src)
	c.mu.Lock()
	c.state = stSynRcvd
	c.rcvNxt = p.Seq + 1
	if p.TSval != 0 {
		c.tsRecent = p.TSval
	}
	c.sendSynAckLocked()
	c.mu.Unlock()
	d.add(c)
	c.start()

	// 交给 Accept 队列（等 ACK 到达后状态转 Established）
	select {
	case l.chAccept <- c:
	default:
		// 队列满：丢弃半连接（真实 listen backlog 溢出行为）
		c.closeWithErr(nil)
	}
}

// Accept 接受一个新连接
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.chAccept:
		return wrapConn(c), nil
	case <-l.d.done:
		return nil, errClosed
	}
}

// Close 关闭监听器（及其下所有连接）
func (l *Listener) Close() error {
	l.once.Do(func() {
		l.d.closeAll()
	})
	return nil
}

// Addr 监听地址
func (l *Listener) Addr() net.Addr {
	return &net.TCPAddr{IP: l.addr.IP.AsSlice(), Port: int(l.addr.Port)}
}

// outboundIP 用 UDP dial 技巧探测去往 remoteIP 的出站本地 IP（不发实际流量）
func outboundIP(remoteIP netip.Addr) (netip.Addr, error) {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{
		IP:   remoteIP.AsSlice(),
		Port: 9, // discard 端口，不会真的发包
	})
	if err != nil {
		return netip.Addr{}, errors.Errorf("probe outbound ip: %s", err)
	}
	defer conn.Close()
	la, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || la.IP == nil {
		return netip.Addr{}, errors.New("probe outbound ip: no local addr")
	}
	ip, ok := netip.AddrFromSlice(la.IP)
	if !ok || !ip.Is4() {
		return netip.Addr{}, errors.Errorf("outbound ip not IPv4: %s", la.IP)
	}
	return ip, nil
}
