package main

import (
	"context"
	"io"
	"math"
	"net"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/socks_nat/pb"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
)

type peerCli struct {
	Name       string
	LocalAddr  string
	RemoteAddr string
	Peer       rpc.Peer

	AfterConnUpgradeClose func()
}

func (p *peerCli) close(ctx context.Context) (err error) {
	err = p.Peer.Close(ctx)
	return
}

func (p *peerCli) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = p.Peer.Close(ctx)
	return &pb.CloseRsp{}, err
}

func (p *peerCli) connect(ctx context.Context, addr string, rc net.Conn) (err error) {
	stream, err := p.Peer.Stream(ctx, "Stream")
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	req := &pb.StreamReq{
		Addr:      addr,
		LocalAddr: rc.LocalAddr().String(),
	}
	err = stream.Send(ctx, req)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

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
			stream.Close(ctx)
		}()
		var n int
		for {
			iface, err := stream.Recv(ctx)
			if err != nil {
				log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
				return
			}
			resp := iface.(*pb.StreamRsp)
			if l := len(resp.Body); l > 0 {
				n, err = rc.Write(resp.Body)
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
		}
	}()

	defer func() {
		e := recover()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
		}
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
		// defer rc.Close()
		// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
	}()
	for {
		buf := make([]byte, math.MaxUint16/2)
		nr, er := rc.Read(buf)
		if nr > 0 {
			err1 := stream.Send(ctx, &pb.StreamReq{Body: buf[:nr]})
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
	return
}

func (p *peerCli) connect2(ctx context.Context, addr string, rc net.Conn) (err error) {
	// 第一个请求很大概率是HTTP，提前握手可以减少一次RTT
	buf := make([]byte, 1024*64)
	n, err := rc.Read(buf)
	if err != nil {
		err = errors.Errorf("Read:%s", err.Error())
		return
	}
	req := &pb.ConnUpgradeReq{
		Addr: addr,
		Body: buf[:n],
	}

	resp := &pb.ConnUpgradeRsp{}
	// stream, err := peer.StreamAsync(ctx, "Conn") ConnpUgrade
	upgrade, err := p.Peer.Upgrade(ctx, "ConnUpgrade", req, resp)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	// ctx, cancel := context.WithCancel(ctx)
	// go io.Copy(rc, upgrade)
	// io.Copy(upgrade, rc)
	go func() {
		defer func() {
			// // cancel()
			// // rc.SetDeadline(time.Now()) // 唤醒因读写conn而阻塞的协程
			// // rc.Close()
			// time.Sleep(time.Second * 3)
			upgrade.Close()
			if p.AfterConnUpgradeClose != nil {
				p.AfterConnUpgradeClose()
			}
		}()
		socks.Copy(ctx, rc, upgrade)
		// io.Copy(rc, upgrade)
	}()

	defer func() {
		// // cancel()
		// time.Sleep(time.Second * 3)
		// rc.SetDeadline(time.Now()) // 唤醒因读写conn而阻塞的协程
		// rc.Close()
		// upgrade.Close()
	}()
	socks.Copy(ctx, upgrade, rc)
	// io.Copy(upgrade, rc)
	return
}

// Listen on addr and proxy to server to reach target from getAddr.
func (p *peerCli) TCPLocal_222(ctx context.Context, socksAddr string) {
	l, err := net.Listen("tcp", socksAddr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to listen on %s: %v", socksAddr, err)
		return
	}

	for {
		c, err := l.Accept()
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("failed to accept")
			continue
		}

		go func() {
			c.(*net.TCPConn).SetKeepAlive(true)
			tgtAddr, err := socks.Handshake(c)
			if err != nil {
				// UDP: keep the connection until disconnect then free the UDP socket
				if err == socks.InfoUDPAssociate {
					buf := make([]byte, 1)
					// block here
					for {
						_, err := c.Read(buf)
						if err, ok := err.(net.Error); ok && err.Timeout() {
							continue
						}
						log.Ctx(ctx).Error().Caller().Err(err).Msgf("UDP Associate End.")
						return
					}
				}

				log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to get target address: %v", err)
				return
			}

			err = p.connect(ctx, tgtAddr.String(), c)
			if err != nil {
				//
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
		}()
	}
}
