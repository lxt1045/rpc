package main

import (
	"context"
	"io"
	"math"
	"net"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/test/socks_nat/pb"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
)

type peerSvc struct {
	Name       string
	LocalAddr  string
	RemoteAddr string
	Peer       rpc.Peer

	AfterConnUpgradeClose func()
}

func (p *peerSvc) close(ctx context.Context) (err error) {
	err = p.Peer.Close(ctx)
	return
}

func (p *peerSvc) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = p.close(ctx)
	return &pb.CloseRsp{}, err
}

func (s *peerSvc) Auth(ctx context.Context, req *pb.AuthReq) (resp *pb.AuthRsp, err error) {

	return
}

func (p *peerSvc) Stream(ctx context.Context, req *pb.StreamReq) (resp *pb.StreamRsp, err error) {
	stream := codec.GetStream(ctx)
	if stream == nil {
		err = errors.Errorf("stream==nil")
		return
	}
	rc, err1 := net.Dial("tcp", req.Addr)
	if err1 != nil {
		err = err1
		log.Ctx(ctx).Error().Caller().Err(err).Str("req.Addr", req.Addr).Msgf("failed to connect to target: %v", err)
		return
	}
	rc.(*net.TCPConn).SetKeepAlive(true)

	log.Ctx(ctx).Error().Caller().Err(err).Msgf("proxy %s <-> %s", req.LocalAddr, req.Addr)

	go func() {
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
			}
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
			// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
			// p.close(ctx)
		}()
		var n int
		for {
			if l := len(req.Body); l > 0 {
				n, err = rc.Write(req.Body)
				if n < 0 || n < l {
					if err == nil {
						err = errors.Errorf(" n < 0 || n < l")
					}
				}
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
					return
				}
			}
			var iface interface{}
			iface, err = stream.Recv(ctx)
			if err != nil {
				log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
				return
			}
			req = iface.(*pb.StreamReq)
		}
	}()

	go func() {
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
			}
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
			rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
			rc.Close()
			stream.Close(ctx)
		}()
		for {
			buf := make([]byte, math.MaxUint16/2)
			nr, er := rc.Read(buf)
			if nr > 0 {
				err1 := stream.Send(ctx, &pb.StreamRsp{Body: buf[:nr]})
				if err1 != nil {
					log.Ctx(ctx).Info().Caller().Err(err1).Msg("err")
					return
				}
			}
			if er != nil {
				if er != io.EOF {
					err = er
					log.Ctx(ctx).Error().Caller().Err(err).Msgf("err : %v", err)
				}
				break
			}
		}
	}()

	resp = &pb.StreamRsp{}
	return
}

func (p *peerSvc) Conn(ctx context.Context, req *pb.ConnReq) (resp *pb.ConnRsp, err error) {
	return
}

func (p *peerSvc) ConnUpgrade(ctx context.Context, req *pb.ConnUpgradeReq) (resp *pb.ConnUpgradeRsp, err error) {
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

			if p.AfterConnUpgradeClose != nil {
				p.AfterConnUpgradeClose()
			}
		}()
		socks.Copy(ctx, upgrade, rc)
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
		socks.Copy(ctx, rc, upgrade)
	}()

	return &pb.ConnUpgradeRsp{}, nil
}
