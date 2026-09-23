package faux_tcp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var _ net.Conn = &Conn{}

// Conn 伪装 TCP 连接的 net.Conn 适配。
// Read 按报文返回（内部保留数据报边界，剩余部分缓冲到下次 Read）；
// Write 保留数据报边界且有界排队（队列满时按写超时等待，见 Write 注释）。
type Conn struct {
	c *fconn

	rl   sync.Mutex // Read 串行化
	rBuf []byte     // 一次没读完的数据

	readDeadline  atomic.Int64 // UnixNano，0 表示无超时
	writeDeadline atomic.Int64 // UnixNano，0 表示无限等待出站队列
}

func wrapConn(c *fconn) *Conn { return &Conn{c: c} }

// LocalAddr 本地地址
func (cn *Conn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: cn.c.local.IP.AsSlice(), Port: int(cn.c.local.Port)}
}

// RemoteAddr 对端地址
func (cn *Conn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: cn.c.remote.IP.AsSlice(), Port: int(cn.c.remote.Port)}
}

// SetDeadline 读写超时
func (cn *Conn) SetDeadline(t time.Time) error {
	cn.SetReadDeadline(t)
	cn.SetWriteDeadline(t)
	return nil
}

// SetReadDeadline 读超时
func (cn *Conn) SetReadDeadline(t time.Time) error {
	cn.readDeadline.Store(t.UnixNano())
	return nil
}

// SetWriteDeadline 写超时（出站队列满时的最长等待时间；0/过去时间立即超时）
func (cn *Conn) SetWriteDeadline(t time.Time) error {
	cn.writeDeadline.Store(t.UnixNano())
	return nil
}

// Read 读取一个报文（缓冲不足时剩余部分留到下次 Read）
func (cn *Conn) Read(bs []byte) (n int, err error) {
	if len(bs) == 0 {
		return 0, nil
	}
	cn.rl.Lock()
	defer cn.rl.Unlock()

	for {
		if len(cn.rBuf) > 0 {
			m := copy(bs, cn.rBuf)
			cn.rBuf = cn.rBuf[m:]
			return m, nil
		}
		var timer *time.Timer
		if dl := cn.readDeadline.Load(); dl > 0 {
			d := time.Until(time.Unix(0, dl))
			if d <= 0 {
				return 0, ErrReadTimeout.New()
			}
			timer = time.NewTimer(d)
		}
		select {
		case data, ok := <-cn.c.chData:
			if timer != nil {
				timer.Stop()
			}
			if !ok {
				return 0, cn.closeErr(io.EOF)
			}
			cn.rBuf = data
		case <-timerChan(timer):
			return 0, ErrReadTimeout.New()
		case <-cn.c.done:
			if timer != nil {
				timer.Stop()
			}
			// 排空残留数据后再报 EOF
			select {
			case data, ok := <-cn.c.chData:
				if !ok {
					return 0, cn.closeErr(io.EOF)
				}
				cn.rBuf = data
			default:
				return 0, cn.closeErr(io.EOF)
			}
		}
	}
}

func timerChan(t *time.Timer) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

// closeErr 返回连接关闭时的真实原因（RST 等），否则返回 def
func (cn *Conn) closeErr(def error) error {
	if e := cn.c.err.Load(); e != nil {
		return e.(error)
	}
	return def
}

// Write 发送一个报文：**保留数据报边界，一次 Write = 一个 TCP 段**。
// 超过 MSS 直接报错（由上层分段——KCP 已按 MSS 1376 分段）。
// 不重传、不做线上滑窗/限速；但报文要进本连接出站队列（本地背压，约 1.4MB）：
// 队列持续满时按 SetWriteDeadline 返回 ErrWriteTimeout，未设超时则一直等待。
// 半关闭语义：对端 FIN 之后（stCloseWait）本端仍可写，直到本端 Close。
//
// 为什么不能像 TCP 一样拆分大写请求：本层不保序不重传，拆分后任何一个片段丢失
// 都会让上层（如 trunk_kcp 的按长度流式重组）永久错位。整段投递使"丢包=丢整条
// 报文"，与 UDP 语义一致，上层 KCP 可正确重传。
func (cn *Conn) Write(bs []byte) (n int, err error) {
	c := cn.c
	if len(bs) > c.cfg.MSS {
		return 0, ErrPacketTooBig.Newf("%d > MSS %d", len(bs), c.cfg.MSS)
	}
	// outMu 保证并发 Write 的"取 seq → 构包 → 入队"整体有序；
	// 等待队列空间时不持有 c.mu（读循环/状态机不受影响）。
	c.outMu.Lock()
	defer c.outMu.Unlock()

	c.mu.Lock()
	if c.state != stEstablished && c.state != stCloseWait {
		c.mu.Unlock()
		return 0, ErrConnClosed.New()
	}
	pkt := c.buildLocked(flagPSH|flagACK, bs, nil)
	c.sndNxt += uint32(len(bs)) // 序号在构包时即消耗（未发出即空洞，靠愈合/上层兜底）
	c.mu.Unlock()

	var timer *time.Timer
	var timeout <-chan time.Time
	if dl := cn.writeDeadline.Load(); dl > 0 {
		d := time.Until(time.Unix(0, dl))
		if d <= 0 {
			return 0, ErrWriteTimeout.New()
		}
		timer = time.NewTimer(d)
		timeout = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	select {
	case c.outCh <- pkt:
		return len(bs), nil
	case <-c.done:
		return 0, ErrConnClosed.New()
	case <-timeout:
		return 0, ErrWriteTimeout.New()
	}
}

// PeerMSS 对端在握手时通告的 MSS（0=未通告）。路径 MTU 受限时可据此校准
// 上层分段大小（本层 cfg.MSS 需相应调小）。
func (cn *Conn) PeerMSS() int { return cn.c.PeerMSS() }

// PacketCounters 报文级计数快照（诊断用）：
//
//	sent = 本连接交给链路发出的报文数（含握手/FIN 等控制段）
//	recv = 链路收到并归属本连接的报文数（**在 demux 之后、投递给上层之前**）
//
// 这两个数放在两端对比，就能把"丢包发生在哪一段"钉死（真机排查用）：
//
//	对端 sent ≈ 本端 recv  → 公网路径没丢，问题在本端用户态（收包队列/上层处理）
//	对端 sent ≫ 本端 recv  → 公网路径在丢（或本端内核 socket 缓冲溢出）
//
// 再与本端上层（如 trunk_kcp 的"收线/收段"）对比，可区分"链路没收到"与
// "收到了但没交给上层"（如 faux_tcp 接收队列满时丢包，见 stack.go deliverLocked）。
func (cn *Conn) PacketCounters() (sent, recv int64) {
	return cn.c.SentPackets.Load(), cn.c.RecvPackets.Load()
}

// Done 连接完全关闭后关闭（诊断/收尾协程可据此退出，避免泄露）。
func (cn *Conn) Done() <-chan struct{} { return cn.c.done }

// Close 关闭连接：发 FIN（消耗一个序号），随后走完整四次挥手；
// 宽限期后对端仍无回应则强制关闭（FIN 不重传）。幂等。
func (cn *Conn) Close() error {
	c := cn.c
	c.mu.Lock()
	switch c.state {
	case stClosed:
		c.mu.Unlock()
		return ErrConnClosed.New()
	case stEstablished:
		c.sendLocked(flagFIN|flagACK, nil)
		c.sndNxt++
		c.state = stFinWait
	case stCloseWait:
		// 对端先 FIN（读侧已 EOF），本端应用随后关闭：回自己的 FIN，等最后的 ACK
		c.sendLocked(flagFIN|flagACK, nil)
		c.sndNxt++
		c.state = stClosing
	case stSynSent, stSynRcvd:
		// 握手未完成：RST 让对端立即知道（真实栈行为）
		c.sendLocked(flagRST|flagACK, nil)
		c.closeLocked(nil)
		c.mu.Unlock()
		return nil
	default:
		// stFinWait/stClosing：FIN 已发出，挥手进行中
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	// 宽限期后强制关闭（对端 FIN/ACK 到达时会提前在状态机里关闭）
	time.AfterFunc(c.cfg.CloseGrace, func() {
		c.closeWithErr(nil)
	})
	return nil
}

// ---------------------------------------------------------------------------
// Dial
// ---------------------------------------------------------------------------

// Dial 主动建立伪装 TCP 连接（完整三次握手表象）。
// laddr/raddr 形如 "1.2.3.4:5678"；laddr 为空时自动选择出站 IP 与随机端口。
// cfg.ManualFirewall 为 false 时自动安装本端口的内核 RST 抑制规则（连接关闭时卸载）。
func Dial(ctx context.Context, cfg Config, laddr, raddr string) (net.Conn, error) {
	cfg.defaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	remote, err := netip.ParseAddrPort(raddr)
	if err != nil {
		return nil, ErrInvalidAddr.Newf("parse raddr: %s", err)
	}
	if !remote.Addr().Is4() {
		return nil, ErrInvalidAddr.Newf("only IPv4 supported: %s", raddr)
	}

	local, err := resolveLocal(laddr, remote.Addr())
	if err != nil {
		return nil, err
	}

	link, err := newRawLink(cfg, local.IP, local.Port)
	if err != nil {
		return nil, err
	}
	// RST 抑制必须在第一个 SYN 发出前装好（否则对端会先收到内核的 RST）
	var fwCleanup func()
	if !cfg.ManualFirewall {
		fwCleanup, err = installRSTDrop(local.Port)
		if err != nil {
			_ = link.Close()
			return nil, err
		}
	}
	conn, err := dialWithLink(ctx, cfg, link, local, Endpoint{IP: remote.Addr(), Port: remote.Port()})
	if err != nil {
		if fwCleanup != nil {
			fwCleanup()
		}
		return nil, err
	}
	conn.(*Conn).c.d.fwCleanup = fwCleanup
	return conn, nil
}

// dialWithLink 在指定链路上发起握手（测试可注入内存链路）
func dialWithLink(ctx context.Context, cfg Config, link Link, local, remote Endpoint) (net.Conn, error) {
	d := newDemux(cfg, link)
	c := newFConn(cfg, d, local, remote)
	d.add(c)
	c.start()

	if err := c.handshake(ctx); err != nil {
		c.closeWithErr(err)
		d.closeAll()
		return nil, err
	}
	return wrapConn(c), nil
}

// resolveLocal 解析本地端点：laddr 为空时自动选择出站 IP 和随机端口
func resolveLocal(laddr string, remoteIP netip.Addr) (Endpoint, error) {
	if laddr != "" {
		ap, err := netip.ParseAddrPort(laddr)
		if err != nil {
			return Endpoint{}, ErrInvalidAddr.Newf("parse laddr: %s", err)
		}
		return Endpoint{IP: ap.Addr(), Port: ap.Port()}, nil
	}
	ip, err := outboundIP(remoteIP)
	if err != nil {
		return Endpoint{}, err
	}
	port, err := pickLocalPort()
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{IP: ip, Port: port}, nil
}

// pickLocalPort 选一个"像内核挑的"本地源端口：范围取自
// /proc/sys/net/ipv4/ip_local_port_range（真实客户端就是从这里选源端口），
// 并避开本机已占用的端口。
//
// 为什么不能用固定区间：某些网络设备/运营商只放行"源端口落在本机 ephemeral 范围
// 内"的 TCP 会话的回程报文。实测某客户端 ip_local_port_range=44620-48715，而本包
// 原先硬编码 49152-65535，结果 SYN 正常到达服务端、服务端也回了 SYN+ACK，但客户端
// 一个包都收不到（同端口的内核 TCP 一切正常）——源端口落在范围外，回程被上游丢弃。
func pickLocalPort() (uint16, error) {
	lo, hi := 32768, 60999 // 内核默认范围（读不到 /proc 时的兜底）
	if b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range"); err == nil {
		var a, c int
		if _, err := fmt.Sscanf(string(b), "%d %d", &a, &c); err == nil && a >= 1024 && c > a && c <= 65535 {
			lo, hi = a, c
		}
	}
	span := hi - lo + 1
	var b [2]byte
	for i := 0; i < 32; i++ {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, err
		}
		p := lo + int(binary.BigEndian.Uint16(b[:]))%span
		if localPortFree(p) {
			return uint16(p), nil
		}
	}
	// 兜底：范围内随机取一个（可能与本机端口撞车，交给上层重试）
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return uint16(lo + int(binary.BigEndian.Uint16(b[:]))%span), nil
}

// localPortFree 本机是否未占用该端口（用一次 bind 探测；TIME_WAIT 里的端口仍可用）
func localPortFree(port int) bool {
	l, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return false
	}
	_ = l.Close()
	return true
}

// handshakeTimeoutErr 组装可自诊断的握手超时错误：区分"对端没回"与"本机发不出去"。
func (c *fconn) handshakeTimeoutErr() error {
	recv := c.RecvPackets.Load()
	sentErr := c.SendErrors.Load()
	msg := fmt.Sprintf("握手超时：无 SYN+ACK；已发 %d 个报文，收到 %d 个报文，发送失败 %d 次",
		c.SentPackets.Load(), recv, sentErr)
	if e := c.lastSendErr(); e != nil {
		msg += fmt.Sprintf("；最后发送错误: %v", e)
	}
	switch {
	case sentErr > 0:
		msg += "；本机 raw socket 发包失败，检查权限/路由/源地址"
	case recv == 0:
		msg += "；对端无任何回包：① 服务端是否运行、云安全组/入站防火墙是否放行该 TCP 端口；" +
			"② 用 tcpdump 对比（与收包同一挂钩点）：tcpdump 能看到回包而这里计数为 0 = 收包过滤/帧格式问题，" +
			"tcpdump 也看不到 = 包没出去或对端没回（含同出口公网 IP 的 NAT 回环）"
	default:
		msg += "；有回包但无有效 SYN+ACK：① 对端 RST 抑制规则（iptables/nft）未生效；" +
			"② 对端/中间设备是 TCP 代理（SYN-cookie），SYN+ACK 的 ack 不是本端 ISN" +
			"（日志搜 \"SYNACK 的 ack 不符\"）；③ 端口被别的内核服务占用"
	}
	if d, ok := c.d.link.(LinkDescriber); ok {
		msg += "；本端链路: " + d.Describe()
	}
	return ErrHandshakeTimeout.New(msg)
}

// handshake 三次握手（带超时与有限重试——SYN 重试是正常 TCP 行为）
func (c *fconn) handshake(ctx context.Context) error {
	c.mu.Lock()
	c.state = stSynSent
	c.mu.Unlock()

	for i := 0; ; i++ {
		c.mu.Lock()
		c.sendSynLocked()
		c.mu.Unlock()
		select {
		case err := <-c.chReady:
			return err
		case <-c.done:
			return ErrConnClosed.New()
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.cfg.HandshakeTimeout):
			if i >= c.cfg.HandshakeRetries-1 {
				return c.handshakeTimeoutErr()
			}
		}
	}
}
