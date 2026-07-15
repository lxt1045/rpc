package socks

import (
	"context"
	stderr "errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/test/socks/pb"
	"github.com/lxt1045/utils/log"
)

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

type SocksSvc struct {
	Name       string
	LocalAddr  string
	RemoteAddr string
	Peer       rpc.Peer
}

func (p *SocksSvc) close(ctx context.Context) (err error) {
	err = p.Peer.Close(ctx)
	return
}

func (p *SocksSvc) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = p.close(ctx)
	return &pb.CloseRsp{}, err
}

func (s *SocksSvc) Auth(ctx context.Context, req *pb.AuthReq) (resp *pb.AuthRsp, err error) {

	return
}

func (p *SocksSvc) Conn(ctx context.Context, req *pb.ConnReq) (resp *pb.ConnRsp, err error) {
	// defer p.close(ctx)
	if stream := codec.GetStream(ctx); stream != nil {
		d := net.Dialer{
			Timeout: time.Second * 30,
		}
		rc, err1 := d.Dial("tcp", req.Addr)
		if err1 != nil {
			err = err1
			log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to connect to target: %v", err)
			return
		}
		rc.(*net.TCPConn).SetKeepAlive(true)

		log.Ctx(ctx).Info().Caller().Err(err).Msgf("proxy %s <-> %s", p.RemoteAddr, req.Addr)

		go func() {
			var n int
			ch := make(chan []byte, 1024)
			go func() {
				// TODO: Read 和Send 分两个进程处理
				defer func() {
					// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
					close(ch)
					// stream.Close(ctx)
					// cancel()
				}()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if l := len(req.Body); l > 0 {
						ch <- req.Body
					}
					iface, err := stream.Recv(ctx)
					if err != nil {
						log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
						return
					}
					req = iface.(*pb.ConnReq)
				}
			}()
			defer func() {
				e := recover()
				if e != nil {
					err = errors.Errorf("recover : %v", e)
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
				// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				// stream.Close(ctx)
			}()
			for {
				var bs []byte
				select {
				case bs = <-ch:
				case <-ctx.Done():
					return
				}
				n, err = rc.Write(bs)
				if n < 0 || n < len(bs) {
					if err == nil {
						err = errors.Errorf(" n < 0 || n < l")
					}
				}
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
					return
				}
			}
		}()

		go func() {
			defer func() {
				e := recover()
				if e != nil {
					err = errors.Errorf("recover : %v", e)
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
				// wg.Wait()
				rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				// stream.Close(ctx)
				// p.close(ctx)
			}()
			// buf := make([]byte, math.MaxUint16/2)
			// buf := make([]byte, 1<<20)

			ch := make(chan []byte, 1024)
			go func() {
				defer func() {
					close(ch)
					// stream.Close(ctx)
					rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				}()
				for {
					// buf := make([]byte, math.MaxUint16/2)
					buf := make([]byte, 1024*8)
					nr, er := rc.Read(buf)
					if er != nil {
						if er != io.EOF {
							err = er
							log.Ctx(ctx).Error().Caller().Err(err).Msgf("err : %v", err)
						}
						break
					}
					if nr <= 0 {
						continue
					}
					ch <- buf[:nr]
				}
			}()
			// TODO: Read 和Send 分两个进程处理
			for {
				var bs []byte
				select {
				case bs = <-ch:
				case <-ctx.Done():
					return
				}
				err1 := stream.Send(ctx, &pb.ConnRsp{Body: bs})
				if err1 != nil {
					log.Ctx(ctx).Info().Caller().Err(err1).Msg("err")
					return
				}
			}
		}()

		return
	}

	// 在阿里云再试试
	return
}

func (p *SocksSvc) ConnUpgrade(ctx context.Context, req *pb.ConnUpgradeReq) (resp *pb.ConnUpgradeRsp, err error) {
	upgrade := codec.GetUpgrade(ctx)
	if upgrade == nil {
		err = errors.New("upgrade is nil")
		log.Ctx(ctx).Error().Caller().Err(err).Msg("ConnUpgrade")
		return
	}
	// rc, err1 := net.Dial("tcp", req.Addr)
	d := net.Dialer{
		Timeout: time.Second * 30,
	}
	rc, err1 := d.Dial("tcp", req.Addr)
	if err1 != nil {
		err = err1
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to connect to target: %v", err)
		return
	}
	rc.(*net.TCPConn).SetKeepAlive(true)

	log.Ctx(ctx).Info().Caller().Err(err).Msgf("proxy %s <-> %s", p.RemoteAddr, req.Addr)

	if len(req.Body) > 0 {
		n := 0
		n, err = rc.Write(req.Body)
		if n < 0 || n < len(req.Body) {
			err = errors.Errorf("n < 0 || n < l, err: %s", err.Error())
			return
		}
	}

	// 两个方向的拷贝必须同生共死：一旦一端出错/EOF，应立即唤醒另一端，
	// 否则会出现 goroutine 泄漏（reader 阻塞在 Read 上），同时产生大量
	// "wsasend: An established connection was aborted" 类噪声日志。
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("ConnUpgrade rc->upgrade")
			}
			rc.SetDeadline(time.Now()) // 唤醒另一方向在 rc 上阻塞的读
			rc.Close()
			upgrade.Close() // 同步关闭 upgrade, 对端会收到 EOF
		}()
		Copy(ctx, upgrade, rc)
	}()

	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("ConnUpgrade upgrade->rc")
			}
			//rc.SetDeadline(time.Now())
			//rc.Close()
			//upgrade.Close()
		}()
		Copy(ctx, rc, upgrade)
	}()

	return &pb.ConnUpgradeRsp{}, nil
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
