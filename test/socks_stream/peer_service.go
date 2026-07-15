package socks

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/test/socks/pb"
	"github.com/lxt1045/utils/log"
)

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
		ctx, cancel := context.WithCancel(ctx)
		// rc, err1 := net.Dial("tcp", req.Addr)
		d := net.Dialer{
			Timeout: time.Second * 3,
		}
		rc, err1 := d.Dial("tcp", req.Addr)
		if err1 != nil {
			err = err1
			log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to connect to target: %v", err)
			cancel()
			return
		}
		rc.(*net.TCPConn).SetKeepAlive(true)

		log.Ctx(ctx).Info().Caller().Err(err).Msgf("proxy %s <-> %s", p.RemoteAddr, req.Addr)

		go func() {
			defer func() {
				e := recover()
				if e != nil {
					err = errors.Errorf("recover : %v", e)
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
				rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				stream.Close(ctx)
				cancel()
			}()
			var n int
			ch := make(chan []byte, 1024)
			go func() {
				// TODO: Read 和Send 分两个进程处理
				defer func() {
					// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
					close(ch)
					stream.Close(ctx)
					cancel()
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
				stream.Close(ctx)
				// p.close(ctx)
				cancel()
			}()
			// buf := make([]byte, math.MaxUint16/2)
			// buf := make([]byte, 1<<20)

			ch := make(chan []byte, 1024)
			go func() {
				defer func() {
					close(ch)
					cancel()
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

	// ctx, cancel := context.WithCancel(ctx)
	// go io.Copy(rc, upgrade)
	// go io.Copy(upgrade, rc)
	go func() {
		defer func() {
			rc.SetDeadline(time.Now()) // 唤醒因读写conn而阻塞的协程
			// cancel()
			rc.Close()
			// upgrade.Close() service段不能主动关闭upgrade，避免错误： wsasend: An established connection was aborted by the software in your host machine
		}()
		Copy(ctx, upgrade, rc)
		// io.Copy(upgrade, rc)
	}()

	go func() {
		defer func() {
			rc.SetDeadline(time.Now()) // 唤醒因读写conn而阻塞的协程
			// cancel()
			rc.Close()
			// upgrade.Close()
		}()
		Copy(ctx, rc, upgrade)
		// io.Copy(rc, upgrade)
	}()

	return &pb.ConnUpgradeRsp{}, nil
}

func Copy(ctx context.Context, dst io.WriteCloser, src io.ReadCloser) (written int64, err error) {
	var n int
	ch := make(chan []byte, 1024)
	go func() {
		var err error
		// TODO: Read 和Send 分两个进程处理
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
			}
			// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
			close(ch)
			// src.Close()
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Msg("Copy defer")
			}
		}()
		buf := make([]byte, 1024*64)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			n, err := src.Read(buf)
			if n > 0 {
				bs := make([]byte, n)
				copy(bs, buf[:n])
				ch <- bs
			}
			if err != nil {
				err = errors.Errorf("Read:%s", err.Error())
				return
			}
		}
	}()
	defer func() {
		e := recover()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
		}
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("Copy defer")
			return
		}
		// dst.Close()
		// src.Close()
	}()
	for {
		var bs []byte
		var ok bool
		select {
		case bs, ok = <-ch:
			if !ok {
				return
			}
			n, err = dst.Write(bs)
			if n < len(bs) || err != nil {
				err = errors.Errorf(" n < l, err: %v", err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
