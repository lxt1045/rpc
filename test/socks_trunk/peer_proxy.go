package socks

import (
	"context"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/test/socks_trunk/pb"
	"github.com/lxt1045/utils/log"
)

var _ pb.SocksCliServer = &SocksProxy{}

type SocksProxy struct {
	SocksSvc

	Svc SocksCli
}

func (p *SocksProxy) Close(ctx context.Context, req *pb.CloseReq) (resp *pb.CloseRsp, err error) {
	resp, err = p.SocksSvc.Close(ctx, req)
	if err == nil {
		resp, err = p.Svc.Close(ctx, req)
	}
	return
}
func (p *SocksProxy) Conn(ctx context.Context, req *pb.ConnReq) (resp *pb.ConnRsp, err error) {
	svcPeer := p.Svc.GetPeer()

	// defer p.close(ctx)
	if cliStream := codec.GetStream(ctx); cliStream != nil {
		svcStream, err1 := svcPeer.StreamAsync(ctx, "Conn")
		if err1 != nil {
			err = err1
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}

		log.Ctx(ctx).Info().Caller().Err(err).Msgf("proxy %s <-> %s", p.RemoteAddr, req.Addr)

		go func() {
			defer func() {
				e := recover()
				if e != nil {
					err = errors.Errorf("recover : %v", e)
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
				// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
			}()
			ch := make(chan []byte, 1024)
			go func() {
				// TODO: Read 和Send 分两个进程处理
				defer close(ch)
				for {
					if l := len(req.Body); l > 0 {
						ch <- req.Body
					}
					iface, err := cliStream.Recv(ctx)
					if err != nil {
						log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
						return
					}
					req = iface.(*pb.ConnReq)
				}
			}()
			for bs := range ch {
				err = svcStream.Send(ctx, &pb.ConnReq{
					Addr: req.Addr,
					Body: bs,
				})
				if err1 != nil {
					log.Ctx(ctx).Info().Caller().Err(err1).Msg("err")
					return
				}
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
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
				cliStream.Close(ctx)
				// p.close(ctx)
			}()
			// buf := make([]byte, math.MaxUint16/2)
			// buf := make([]byte, 1<<20)

			ch := make(chan []byte, 1024)
			go func() {
				// TODO: Read 和Send 分两个进程处理
				defer close(ch)
				for {
					iface, err := svcStream.Recv(ctx)
					if err != nil {
						log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
						return
					}
					rsp := iface.(*pb.ConnRsp)
					if rsp.Err != nil && rsp.Err.Code != 0 {
						err = errors.NewErr(int(rsp.Err.Code), rsp.Err.Msg)
						log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
						return
					}
					ch <- rsp.Body
				}
			}()
			// TODO: Read 和Send 分两个进程处理
			for bs := range ch {
				err1 := cliStream.Send(ctx, &pb.ConnRsp{Body: bs})
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
