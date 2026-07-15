package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/test/tcp_upgrade/pb"
	"github.com/lxt1045/utils/log"
)

type socksSvc struct {
	Name       string
	LocalAddr  string
	RemoteAddr string
	Peer       rpc.Peer
}

func (p *socksSvc) close(ctx context.Context) (err error) {
	err = p.Peer.Close(ctx)
	return
}

func (p *socksSvc) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = p.close(ctx)
	return &pb.CloseRsp{}, err
}

func (s *socksSvc) Auth(ctx context.Context, req *pb.AuthReq) (resp *pb.AuthRsp, err error) {
	log.Ctx(ctx).Debug().Caller().Interface("req", req).Msg("Auth")
	// resp = &pb.AuthRsp{
	// 	Status: pb.AuthRsp_Fail,
	// 	Err: &pb.Err{
	// 		Code: 9999,
	// 		Msg:  "Just Test!",
	// 	},
	// }

	err = errors.NewCode(0, 888, "test error")
	log.Ctx(ctx).Debug().Caller().Interface("req", req).Msg("resp error")
	return
}

func (p *socksSvc) Hello(ctx context.Context, req *pb.HelloReq) (resp *pb.HelloRsp, err error) {
	log.Ctx(ctx).Debug().Caller().Interface("req", req).Msg("Hello")

	{
		time.Sleep(time.Second)
		req := &pb.HelloReq{
			Name: req.Name,
		}
		resp := &pb.HelloRsp{}
		err = p.Peer.Invoke(ctx, "Hello", req, resp)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			log.Ctx(ctx).Warn().Err(err).Caller().Interface("resp", resp).Msg("Hello")
		}
		log.Ctx(ctx).Debug().Caller().Interface("resp", resp).Msg("Hello")
	}

	return &pb.HelloRsp{
		Name: "server-1",
		Msg:  req.Msg,
		Err: &pb.Err{
			Code: 9999,
			Msg:  "Just Test!",
		},
	}, err
}
func (p *socksSvc) Conn(ctx context.Context, req *pb.ConnReq) (resp *pb.ConnRsp, err error) {
	// defer p.close(ctx)
	if stream := codec.GetStream(ctx); stream != nil {

		return
	}

	// 在阿里云再试试
	return
}

func (p *socksSvc) testConn(ctx context.Context, req *pb.ConnReq) (resp *pb.ConnRsp, err error) {
	// defer p.close(ctx)
	if stream := codec.GetStream(ctx); stream != nil {
		rc, err1 := net.Dial("tcp", req.Addr)
		if err1 != nil {
			err = err1
			log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to connect to target: %v", err)
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
				// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
			}()
			var n int
			ch := make(chan []byte, 1024)
			go func() {
				// TODO: Read 和Send 分两个进程处理
				defer close(ch)
				for {
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
			for bs := range ch {
				n, err = rc.Write(bs)
				if n < 0 || n < len(bs) {
					if err == nil {
						err = errors.Errorf(" n < 0 || n < l")
					}
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
				// wg.Wait()
				rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				stream.Close(ctx)
				// p.close(ctx)
			}()
			// buf := make([]byte, math.MaxUint16/2)
			// buf := make([]byte, 1<<20)

			ch := make(chan []byte, 1024)
			go func() {
				defer close(ch)
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
			for bs := range ch {
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

func (p *socksSvc) ConnUpgrade(ctx context.Context, req *pb.ConnUpgradeReq) (resp *pb.ConnUpgradeRsp, err error) {
	upgrade := codec.GetUpgrade(ctx)
	if upgrade == nil {
		err = errors.New("upgrade is nil")
		log.Ctx(ctx).Error().Caller().Err(err).Msg("ConnUpgrade")
		return
	}
	go func() {
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
			}
		}()

		go func() {
			defer func() {
				upgrade.Close()
				log.Ctx(ctx).Info().Caller().Msg("upgrade closed")
			}()
			buf := make([]byte, 1024)
			for {
				n, err := upgrade.Read(buf)
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
					return
				}
				log.Ctx(ctx).Info().Caller().Str("upgrade read", string(buf[:n])).Send()
			}
		}()

		buf := []byte("22222222222222")
		_, err = upgrade.Write(buf)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		log.Ctx(ctx).Info().Caller().Str("upgrade write", string(buf)).Send()
		buf = []byte("66666666666666")
		_, err = upgrade.Write(buf)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		log.Ctx(ctx).Info().Caller().Str("upgrade write", string(buf)).Send()
	}()
	return &pb.ConnUpgradeRsp{}, nil
}
