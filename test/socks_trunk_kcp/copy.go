package socks_kcp

import (
	"context"
	stderr "errors"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/utils/log"
)

// relay 双向复制，任一端结束后关闭两端，避免 goroutine 泄漏。
func relay(ctx context.Context, left, right io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)

	cp := func(dst, src io.ReadWriteCloser) {
		defer wg.Done()
		// _, _ = io.Copy(dst, src) // TODO: 使用带大缓存的Copy 函数
		// _ = dst.Close()
		// _ = src.Close()
		Copy(ctx, dst, src)
	}
	go cp(left, right)
	go cp(right, left)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		_ = left.Close()
		_ = right.Close()
	}
}

// Copy 将 src 的内容搬运到 dst，直到任一方出错或 ctx 取消。
//
// 设计要点：
//  1. reader 协程专注 Read；writer（主协程）专注 Write，两者通过 channel 配合。
//  2. writer 出错时会 cancel ctx；但 src.Read 本身不受 ctx 约束，
//     因此还会调用 src.Close() / SetDeadline 来唤醒阻塞中的 reader。
//  3. 对于 "对端正常关闭" 一类 benign error（浏览器关 tab、keep-alive 断开、RST 等），
//     不在本函数内打 error 日志，交由调用方决定。
func Copy(ctx context.Context, dst io.WriteCloser, src io.ReadCloser) (written int64, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan []byte, 1024)

	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("Copy reader")
			}
			close(ch)
		}()
		buf := make([]byte, 1024*64)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			n, er := src.Read(buf)
			if n > 0 {
				bs := make([]byte, n)
				copy(bs, buf[:n])
				select {
				case <-ctx.Done():
					return
				case ch <- bs:
				}
			}
			if er != nil {
				return
			}
		}
	}()

	defer func() {
		if e := recover(); e != nil {
			err = errors.Errorf("recover : %v", e)
		}
		cancel()

		// // 唤醒可能阻塞在 src.Read 的 reader 协程
		// if dl, ok := src.(interface{ SetDeadline(t time.Time) error }); ok {
		// 	_ = dl.SetDeadline(time.Now())
		// }
		if err != nil && !isBenignCloseErr(err) {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("Copy defer")
		} else if err != nil {
			log.Ctx(ctx).Debug().Caller().Err(err).Msg("Copy defer benign close")
			err = nil // 将良性关闭视为正常退出
		}
	}()

	for {
		var bs []byte
		var ok bool
		select {
		case bs, ok = <-ch:
			if !ok {
				if tcpConn, ok := dst.(*net.TCPConn); ok {
					// 不能直接直接调用 conn.Close()，会发送RST 直接断开tcp 链接
					tcpConn.CloseWrite()
					log.Ctx(ctx).Info().Caller().Msg("Copy CloseWrite, Send FIN")
				}
				return
			}
			var n int
			n, err = dst.Write(bs)
			written += int64(n)
			if err != nil {
				err = errors.New("err: %v", err)
				return
			}
			if n < len(bs) {
				err = errors.Errorf("short write: %d < %d", n, len(bs))
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// isBenignCloseErr 判断 err 是否属于 "对端/本端正常关闭连接" 的情况
// 这类错误在代理场景（浏览器主动关 tab、keep-alive 过期、RST 等）非常常见，
// 不应按 error 级别打印。
func isBenignCloseErr(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF || err == io.ErrClosedPipe {
		return true
	}
	if stderr.Is(err, net.ErrClosed) {
		return true
	}
	msg := err.Error()
	// 跨平台: Linux 常见 "connection reset by peer"、"broken pipe"；
	// Windows: wsasend/wsarecv + "forcibly closed" / "aborted by the software"。
	// 同时匹配 rpc 层的 "has been closed" / "Codec is closed" 等显式关闭错误。
	for _, s := range []string{
		"use of closed network connection",
		"connection reset by peer",
		"broken pipe",
		"forcibly closed",
		"aborted by the software",
		"An established connection was aborted",
		"An existing connection was forcibly closed",
		"has been closed",
		"Codec is closed",
		"upgrade closed",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
