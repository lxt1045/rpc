package socks_faux_kcp

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/lxt1045/rpc/trunk_kcp"
)

// 连接类型标记：每条 faux_tcp 链路上的首字节（一个独立 TCP 段），服务端据此分流。
const (
	// connTypeControl 控制连接：迷你 trunk(KCP) + TLS + RPC（Auth/TrunkStart/...）
	connTypeControl byte = 0x01
	// connTypePhysical 数据面物理连接：裸 RPC 只做 TrunkUpgrade（无秘密），
	// Upgrade 后交给 trunk_kcp 只跑 KCP 段。
	connTypePhysical byte = 0x02
)

// ctrlConv 控制通道迷你 trunk 的固定 KCP conv。每条控制连接是独立 trunk
// （独占一条 faux_tcp 连接），不存在同链路多路复用，故用固定值即可。
const ctrlConv = 0xC0DE0001

// vconnConn 把 trunk_kcp.VirtualConn（io.ReadWriteCloser）适配为 net.Conn，
// 供 crypto/tls 使用。deadline 方法仅记录不生效（VirtualConn 无底层支持；
// 超时由 tlsHandshake 的包装与上层 ctx 兜底）。
type vconnConn struct {
	rwc    io.ReadWriteCloser
	local  net.Addr
	remote net.Addr
}

type vconnAddr string

func (a vconnAddr) Network() string { return "kcp" }
func (a vconnAddr) String() string  { return string(a) }

// wrapVConn 把 VirtualConn 包装成 net.Conn。local/remote 可取承载它的
// 物理连接的地址（仅用于日志展示）。
func wrapVConn(vconn io.ReadWriteCloser, local, remote net.Addr) net.Conn {
	if local == nil {
		local = vconnAddr("kcp-local")
	}
	if remote == nil {
		remote = vconnAddr("kcp-remote")
	}
	return &vconnConn{rwc: vconn, local: local, remote: remote}
}

func (c *vconnConn) Read(p []byte) (int, error)         { return c.rwc.Read(p) }
func (c *vconnConn) Write(p []byte) (int, error)        { return c.rwc.Write(p) }
func (c *vconnConn) Close() error                       { return c.rwc.Close() }
func (c *vconnConn) LocalAddr() net.Addr                { return c.local }
func (c *vconnConn) RemoteAddr() net.Addr               { return c.remote }
func (c *vconnConn) SetDeadline(t time.Time) error      { return nil }
func (c *vconnConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *vconnConn) SetWriteDeadline(t time.Time) error { return nil }

// tlsHandshake 带超时的 TLS 握手：超时/ctx 取消时关闭底层连接打断握手。
func tlsHandshake(ctx context.Context, conn *tls.Conn, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- conn.Handshake() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = conn.Close()
		return ctx.Err()
	case <-time.After(timeout):
		_ = conn.Close()
		return fmt.Errorf("tls handshake timeout after %v", timeout)
	}
}

// ctrlConn 控制通道连接：外层是 TLS/VirtualConn，Close 时连带关闭迷你 trunk
// 及其承载的 faux_tcp 物理连接（否则 RPC peer 关闭后迷你 trunk 会泄漏）。
type ctrlConn struct {
	net.Conn
	ctrl *trunk_kcp.TrunkKCP
}

func (c *ctrlConn) Close() error {
	err := c.Conn.Close()
	if c.ctrl != nil {
		_ = c.ctrl.Close()
	}
	return err
}

// Handshake 转发到底层（*tls.Conn 已握手时是幂等的）；Peer.Conn/Clone 会探测此接口。
func (c *ctrlConn) Handshake() error {
	if h, ok := c.Conn.(interface{ Handshake() error }); ok {
		return h.Handshake()
	}
	return nil
}

// wrapTLSClient 在可靠流上完成 TLS 客户端握手（rwc 必须是可靠有序流，如
// trunk_kcp VirtualConn；faux_tcp 裸连接不满足）。
func wrapTLSClient(ctx context.Context, rwc io.ReadWriteCloser, local, remote net.Addr,
	cfg *tls.Config, timeout time.Duration) (net.Conn, error) {
	conn := tls.Client(wrapVConn(rwc, local, remote), cfg)
	if err := tlsHandshake(ctx, conn, timeout); err != nil {
		return nil, err
	}
	return conn, nil
}

// wrapTLSServer 在可靠流上完成 TLS 服务端握手。
func wrapTLSServer(ctx context.Context, rwc io.ReadWriteCloser, local, remote net.Addr,
	cfg *tls.Config, timeout time.Duration) (net.Conn, error) {
	conn := tls.Server(wrapVConn(rwc, local, remote), cfg)
	if err := tlsHandshake(ctx, conn, timeout); err != nil {
		return nil, err
	}
	return conn, nil
}
