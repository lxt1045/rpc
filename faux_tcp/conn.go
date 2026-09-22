package faux_tcp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/errors"
)

var _ net.Conn = &Conn{}

// Conn 伪装 TCP 连接的 net.Conn 适配。
// Read 按报文返回（内部保留数据报边界，剩余部分缓冲到下次 Read）；
// Write 即发即走，永不阻塞（背压以丢包形式上抛）。
type Conn struct {
	c *fconn

	rl   sync.Mutex // Read 串行化
	rBuf []byte     // 一次没读完的数据

	readDeadline  atomic.Int64 // UnixNano，0 表示无超时
	writeDeadline atomic.Int64
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

// SetWriteDeadline 写超时（写出即走，实际不会超时；仅为满足接口）
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
				return 0, errReadTimeout
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
			return 0, errReadTimeout
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

// errPacketTooBig 单次 Write 超过 MSS
var errPacketTooBig = errors.New("packet exceeds MSS")

// Write 发送一个报文：**保留数据报边界，一次 Write = 一个 TCP 段**。
// 超过 MSS 直接报错（由上层分段——KCP 已按 MSS 1376 分段）。
// 即发即走：不保留副本、不重传、不做窗口检查。
//
// 为什么不能像 TCP 一样拆分大写请求：本层不保序不重传，拆分后任何一个片段丢失
// 都会让上层（如 trunk_kcp 的按长度流式重组）永久错位。整段投递使"丢包=丢整条
// 报文"，与 UDP 语义一致，上层 KCP 可正确重传。
func (cn *Conn) Write(bs []byte) (n int, err error) {
	c := cn.c
	if len(bs) > c.cfg.MSS {
		return 0, errors.Errorf("%w: %d > %d", errPacketTooBig, len(bs), c.cfg.MSS)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != stEstablished {
		return 0, errClosed
	}
	c.sendLocked(flagPSH|flagACK, bs)
	c.sndNxt += uint32(len(bs))
	return len(bs), nil
}

// Close 关闭连接：发 FIN（消耗一个序号），宽限期后无论对端是否回应都关闭
// （FIN 不重传）。幂等。
func (cn *Conn) Close() error {
	c := cn.c
	c.mu.Lock()
	switch c.state {
	case stClosed:
		c.mu.Unlock()
		return errClosed
	case stEstablished, stSynRcvd:
		c.finSent = true
		c.sendLocked(flagFIN|flagACK, nil)
		c.sndNxt++
		c.state = stClosing
	}
	c.mu.Unlock()
	// 宽限期后强制关闭（对端 FIN 到达时会提前在 onPeerFinLocked 关闭）
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
func Dial(ctx context.Context, cfg Config, laddr, raddr string) (net.Conn, error) {
	cfg.defaults()

	remote, err := netip.ParseAddrPort(raddr)
	if err != nil {
		return nil, errors.Errorf("parse raddr: %s", err)
	}
	if !remote.Addr().Is4() {
		return nil, errors.Errorf("only IPv4 supported: %s", raddr)
	}

	local, err := resolveLocal(laddr, remote.Addr())
	if err != nil {
		return nil, err
	}

	link, err := newRawLink(local.IP)
	if err != nil {
		return nil, err
	}
	return dialWithLink(ctx, cfg, link, local, Endpoint{IP: remote.Addr(), Port: remote.Port()})
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
			return Endpoint{}, errors.Errorf("parse laddr: %s", err)
		}
		return Endpoint{IP: ap.Addr(), Port: ap.Port()}, nil
	}
	ip, err := outboundIP(remoteIP)
	if err != nil {
		return Endpoint{}, err
	}
	// 随机高端口（真实 TCP 客户端行为：49152~65535）
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return Endpoint{}, err
	}
	port := 49152 + binary.BigEndian.Uint16(b[:])%16384
	return Endpoint{IP: ip, Port: port}, nil
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
			return errClosed
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.cfg.HandshakeTimeout):
			if i >= c.cfg.HandshakeRetries-1 {
				return errHandshakeTimeout
			}
		}
	}
}
