package faux_tcp

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"

	"github.com/lxt1045/utils/log"
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
// cfg.ManualFirewall 为 false 时自动安装监听端口的内核 RST 抑制规则（Close 时卸载）。
func Listen(ctx context.Context, cfg Config, addr string) (net.Listener, error) {
	cfg.defaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return nil, ErrInvalidAddr.Newf("parse addr: %s", err)
	}
	if !ap.Addr().Is4() && !ap.Addr().IsUnspecified() {
		return nil, ErrInvalidAddr.Newf("only IPv4 supported: %s", addr)
	}

	// ReplySrc 只说清"回包源地址从哪来"，很多平台会因此静默丢包，启动时必须留下判据。
	if cfg.ReplySrc.IsValid() {
		if isLocalAddr(cfg.ReplySrc) {
			log.Ctx(ctx).Info().Msgf("faux_tcp: reply_src=%s 生效（出站报文源 IP 统一改写为本机地址）", cfg.ReplySrc)
		} else {
			log.Ctx(ctx).Warn().Msgf("faux_tcp: reply_src=%s 不是本机网卡地址。"+
				"无状态 DNAT/端口映射（Docker/K8s）下这是预期配置；"+
				"但云主机 1:1 NAT/弹性公网 IP（腾讯云 VPC/轻量、阿里云 ECS 等）会对虚拟网卡做"+
				"源地址校验，出站报文源 IP 不是网卡地址时会被平台丢弃——"+
				"典型现象：内核 TCP 监听同端口能通、faux_tcp 却握手超时，且服务端 tcpdump 里"+
				"SYN+ACK 的源地址是公网 IP（不是网卡内网地址），客户端一个包都收不到。"+
				"此场景请把 reply_src 留空：回包源地址取报文目的 IP（内网地址），"+
				"由云平台 NAT 转成公网——与内核 TCP 完全同路。", cfg.ReplySrc)
		}
	}

	link, err := newRawLink(cfg, ap.Addr(), ap.Port())
	if err != nil {
		return nil, err
	}
	var fwCleanup func()
	if !cfg.ManualFirewall {
		fwCleanup, err = installRSTDrop(ap.Port())
		if err != nil {
			_ = link.Close()
			return nil, err
		}
	}
	l := listenWithLink(cfg, link, Endpoint{IP: ap.Addr(), Port: ap.Port()})
	l.d.fwCleanup = fwCleanup
	return l, nil
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
	if l.cfg.ReplySrc.IsValid() {
		// 无状态 DNAT 环境：回包源地址用对外服务地址（见 Config.ReplySrc）
		local.IP = l.cfg.ReplySrc
	}

	// 可见性：服务端不创建内核监听，ss/netstat 看不到端口，这条日志是判断
	// "客户端 SYN 是否真的到达服务端"的直接依据（配合 debug 日志级别）。
	// 同时打印对端通告的 MSS 与回包源地址：
	//   - MSS 可判断路径上是否有 MSS-clamp 中间盒（客户端通告 1460、这里看到 1280）；
	//   - 回包源地址若不是本机网卡地址、而客户端收不到包，就是云平台源地址校验丢弃。
	peerMSS := "未通告"
	if p.MSS > 0 {
		peerMSS = fmt.Sprint(p.MSS)
	}
	log.Ctx(context.Background()).Debug().
		Msgf("faux_tcp: 收到 SYN %s -> %s, seq=%d, 对端通告 MSS=%s, 回包源=%s", p.Src, p.Dst, p.Seq, peerMSS, local.IP)

	c := newFConn(l.cfg, d, local, p.Src)
	// Accept 队列只收完成三次握手的连接（真实 listen 队列语义；
	// 半开连接由 stSynRcvd 超时回收兜底，不会进入应用视野）
	c.onEstablished = func(c *fconn) {
		select {
		case l.chAccept <- c:
		default:
			// 队列满：丢弃连接（真实 listen backlog 溢出行为）
			c.closeWithErr(nil)
		}
	}
	c.mu.Lock()
	c.state = stSynRcvd
	c.rcvNxt = p.Seq + 1
	if p.TSval != 0 {
		c.tsRecent = p.TSval
	}
	c.notePeerMSS(p.MSS)
	c.sendSynAckLocked()
	c.mu.Unlock()
	d.add(c)
	c.start()
}

// Accept 接受一个新连接
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.chAccept:
		return wrapConn(c), nil
	case <-l.d.done:
		return nil, ErrConnClosed.New()
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

// isLocalAddr 判断 ip 是否为本机网卡地址（含回环）。
// 用于区分 reply_src 的两种用途：Docker/K8s 端口映射下它必然不是本机地址（对外
// 地址在宿主上），而云主机 1:1 NAT 下填公网地址则会被平台源地址校验丢弃。
// 查询失败时返回 true，宁可漏报也不误报。
func isLocalAddr(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}
	want := ip.Unmap()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return true
	}
	for _, a := range addrs {
		var aip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			aip = v.IP
		case *net.IPAddr:
			aip = v.IP
		}
		if got, ok := netip.AddrFromSlice(aip); ok && got.Unmap() == want {
			return true
		}
	}
	return false
}

// outboundIP 用 UDP dial 技巧探测去往 remoteIP 的出站本地 IP（不发实际流量）
func outboundIP(remoteIP netip.Addr) (netip.Addr, error) {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{
		IP:   remoteIP.AsSlice(),
		Port: 9, // discard 端口，不会真的发包
	})
	if err != nil {
		return netip.Addr{}, ErrInvalidAddr.Newf("probe outbound ip: %s", err)
	}
	defer conn.Close()
	la, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || la.IP == nil {
		return netip.Addr{}, ErrInvalidAddr.New("probe outbound ip: no local addr")
	}
	ip, ok := netip.AddrFromSlice(la.IP)
	if !ok || !ip.Is4() {
		return netip.Addr{}, ErrInvalidAddr.Newf("outbound ip not IPv4: %s", la.IP)
	}
	return ip, nil
}
