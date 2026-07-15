package socket

import (
	"context"
	"crypto/tls"
	"net"
	"syscall"
	"time"

	"github.com/lxt1045/errors"
)

func control(network, address string, c syscall.RawConn) error {
	// windows
	if TCP_SYNCNT == 0 {
		return c.Control(func(fd uintptr) {
			h := newHandle(fd).H
			syscall.SetsockoptInt(h, syscall.SOL_SOCKET, SO_REUSEADDR, 1)
			syscall.SetsockoptInt(h, syscall.SOL_SOCKET, SO_REUSEPORT, 1)
		})
	}
	// linux
	return c.Control(func(fd uintptr) {
		h := newHandle(fd).H
		syscall.SetsockoptInt(h, syscall.SOL_SOCKET, SO_REUSEADDR, 1)
		syscall.SetsockoptInt(h, syscall.SOL_SOCKET, SO_REUSEPORT, 1)
		syscall.SetsockoptInt(h, syscall.IPPROTO_TCP, TCP_SYNCNT, 1)
	})
}

func Listen(ctx context.Context, network, addr string) (ln net.Listener, err error) {
	cfg := net.ListenConfig{
		Control: control,
	}
	ln, err = cfg.Listen(ctx, network, addr)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	return
}

func Dial(ctx context.Context, network, localAddr, remoteAddr string) (conn net.Conn, err error) {
	la, err := net.ResolveTCPAddr("tcp4", localAddr)
	if err != nil {
		return
	}
	cfg := net.Dialer{
		Control:   control,
		LocalAddr: la,
	}
	conn, err = cfg.DialContext(ctx, network, remoteAddr)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	return
}

func DialWithTimeout(ctx context.Context, network, localAddr, remoteAddr string, timeout time.Duration) (conn net.Conn, err error) {
	la, err := net.ResolveTCPAddr("tcp4", localAddr)
	if err != nil {
		return
	}
	cfg := net.Dialer{
		Timeout:   timeout, // 默认 3 minutes.
		Control:   control,
		LocalAddr: la,
	}
	conn, err = cfg.DialContext(ctx, network, remoteAddr)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	return

}

func DialTLS(ctx context.Context, network, addr string, conf *tls.Config) (conn *tls.Conn, err error) {
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{
			Control: control,
		},
		Config: conf,
	}
	c, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	conn = c.(*tls.Conn)
	return
}

func DialTLSTimeout(ctx context.Context, network, addr string, tlsConfig *tls.Config, timeout time.Duration) (conn *tls.Conn, err error) {
	dialer := &net.Dialer{
		Timeout: timeout, // 默认 3 minutes.
		// 启用端口重用
		// Control: control,
	}
	conn, err = tls.DialWithDialer(dialer, network, addr, tlsConfig)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	return

}
func DialTLSTimeout1(ctx context.Context, network, addr string, conf *tls.Config, timeout time.Duration) (conn *tls.Conn, err error) {
	dialer := net.Dialer{
		Timeout: timeout, // 默认 3 minutes.
		// 启用端口重用
		Control: control,
	}
	conn, err = tls.DialWithDialer(&dialer, network, addr, conf)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	return

}
