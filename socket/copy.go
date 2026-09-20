package socket

import (
	"context"
	stderr "errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/utils/log"
)

var (
	pool = sync.Pool{
		New: func() any {
			return make([]byte, 1024*8)
		},
	}
)

// closeGrace 是连接 teardown 时的宽限时间：一端 EOF 后不立即硬关，
// 给反方向残留数据留出发送时间（避免截断响应），到期再强制回收，
// 防止 TLS 连接 / 目标连接 / goroutine 永久泄漏。
// （泄漏长期累积曾耗尽服务端 fd / 网关会话表，导致新连接 SYN 被静默丢弃、
// client 建连成功率骤降，重启后才恢复。）
const closeGrace = time.Second * 10

// Copy 将 src 的内容搬运到 dst，直到任一方出错或 ctx 取消。
//
// 设计要点：
//  1. reader 协程专注 Read；writer（主协程）专注 Write，两者通过 channel 配合。
//     reader 只把实际读到的 buf[:n] 交给 writer，归还 pool 时必须恢复完整长度。
//  2. 正常 EOF（reader 关闭 channel）时不能毒化 src —— 反方向的 Copy 可能
//     还在向 src 写残留数据；仅在写失败或父 ctx 取消时 SetDeadline，
//     唤醒可能阻塞在 src.Read 的 reader。
//  3. 读错误通过 close(ch) 的 happens-before 关系传递给 writer；
//     benign 的关闭错误（对端正常关闭、RST 等）不作为错误返回。
func Copy(ctx context.Context, dst io.WriteCloser, src io.ReadCloser) (written int64, err error) {
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 读进程和写进程分开，增加延时/增加吞吐率
	ch := make(chan []byte, 128)
	var readErr error // reader 在 close(ch) 前写入；writer 仅在 channel 关闭后读取

	// 读
	go func() {
		defer func() {
			if e := recover(); e != nil {
				readErr = errors.Errorf("recover : %v", e)
			}
			close(ch)

			if readErr != nil {
				local, remote := "", ""
				if tcpConn, ok := src.(*net.TCPConn); ok {
					local, remote = tcpConn.LocalAddr().String(), tcpConn.RemoteAddr().String()
				}
				if !isBenignCloseErr(readErr) {
					log.Ctx(ctx).Error().Caller().Err(errors.WithErr(readErr)).Str("local", local).Str("remote", remote).Msg("src.Read")
				} else {
					log.Ctx(ctx).Debug().Caller().Err(errors.WithErr(readErr)).Str("local", local).Str("remote", remote).Msg("src.Read benign close")
				}
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			buf := pool.Get().([]byte)
			n, er := src.Read(buf)
			if n > 0 {
				select {
				case <-ctx.Done():
					pool.Put(buf[:cap(buf)])
					return
				case ch <- buf[:n]: // 只发送实际读到的内容，不能把整个 buffer 写出去
				}
			}
			if er != nil {
				if n <= 0 {
					pool.Put(buf[:cap(buf)])
				}
				readErr = er
				if e := CloseRead(src); e != nil {
					log.Ctx(ctx).Debug().Caller().Err(e).Msg("CloseRead(src)")
				}
				return
			}
		}
	}()

	// 写
	var eofReadErr error // 仅在 !ok 分支（channel 已关闭）赋值，与 reader 有 happens-before 关系
	func() {
		defer func() {
			if e := recover(); e != nil {
				err = errors.Errorf("recover : %v", e)
			}
			cancel()
			if err != nil || parent.Err() != nil {
				// 写失败或 ctx 取消：唤醒可能阻塞在 src.Read 的 reader。
				// 正常 EOF 时 reader 已自行退出，这里绝不能毒化 src。
				if set, ok := src.(interface{ SetDeadline(t time.Time) error }); ok {
					set.SetDeadline(time.Now())
				}
			}

			if err != nil {
				local, remote := "", ""
				if tcpConn, ok := dst.(*net.TCPConn); ok {
					local, remote = tcpConn.LocalAddr().String(), tcpConn.RemoteAddr().String()
				}
				if !isBenignCloseErr(err) {
					log.Ctx(ctx).Error().Caller().Err(err).Str("local", local).Str("remote", remote).Msg("dst.Write")
				} else {
					log.Ctx(ctx).Error().Caller().Err(err).Str("local", local).Str("remote", remote).Msg("Copy defer benign close")
					err = nil // 将良性关闭视为正常退出
				}
			}
		}()

		for {
			var bs []byte
			var ok bool
			select {
			case bs, ok = <-ch:
				if !ok {
					eofReadErr = readErr
					return
				}
				buf := bs // 保留原始切片用于归还 pool
				for len(bs) > 0 {
					var n int
					n, err = dst.Write(bs)
					written += int64(n)
					if err != nil {
						pool.Put(buf[:cap(buf)])
						err = errors.New("err: %v", err)
						if e := CloseWrite(dst); e != nil {
							log.Ctx(ctx).Debug().Caller().Err(e).Msg("CloseWrite(dst)")
						}
						return
					}
					if n <= 0 {
						pool.Put(buf[:cap(buf)])
						err = errors.New("short write: n=%d", n)
						if e := CloseWrite(dst); e != nil {
							log.Ctx(ctx).Debug().Caller().Err(e).Msg("CloseWrite(dst)")
						}
						return
					}
					bs = bs[n:]
				}
				pool.Put(buf[:cap(buf)])
			case <-ctx.Done():
				return
			}
		}
	}()

	// 读错误优先级低于写错误；benign 读错误（EOF、对端正常关闭）不上报
	if err == nil {
		if eofReadErr != nil && !isBenignCloseErr(eofReadErr) {
			err = eofReadErr
		} else if parent.Err() != nil {
			// defer 里的 cancel 只影响派生 ctx，这里如实上报父 ctx 的取消
			err = parent.Err()
		}
	}
	return
}

// CloseRead 半关闭 rw 的读端；不支持半关闭时延迟 closeGrace 后完全关闭，
// 异步执行，避免阻塞调用方（宽限期的意义见 closeGrace 注释）。
func CloseRead(rw io.ReadCloser) (err error) {
	if close, ok := rw.(interface{ CloseRead() error }); ok {
		// 不能直接直接调用 conn.Close()，会发送RST 直接断开tcp 链接
		return close.CloseRead()
	}

	time.AfterFunc(closeGrace, func() {
		rw.Close()
	})
	return nil
}

// CloseWrite 半关闭 rw 的写端（发送 FIN）；不支持半关闭时延迟 closeGrace 后完全关闭，
// 异步执行，避免阻塞调用方（宽限期的意义见 closeGrace 注释）。
func CloseWrite(rw io.WriteCloser) (err error) {
	if close, ok := rw.(interface{ CloseWrite() error }); ok {
		// 不能直接直接调用 conn.Close()，会发送RST 直接断开tcp 链接
		return close.CloseWrite()
	}

	time.AfterFunc(closeGrace, func() {
		rw.Close()
	})
	return nil
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
