package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/socks_quic/pb"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
)

type socksCli struct {
	Name      string
	socksAddr string
	chPeer    chan *Peer
}

type Peer struct {
	LocalAddrs  string
	RemoteAddrs string
	rpc.Peer
}

func (p *socksCli) close(ctx context.Context) (err error) {
	for peer := range p.chPeer {
		err1 := peer.Close(ctx)
		if err1 != nil {
			err = err1
		}
	}
	return
}

func (p *socksCli) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = p.close(ctx)
	return &pb.CloseRsp{}, err
}

// Listen on addr and proxy to server to reach target from getAddr.
func (p *socksCli) RunLocal(ctx context.Context, socksAddr string) {
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

		go p.connect(ctx, c)
	}
}

func (p *socksCli) connect(ctx context.Context, rc net.Conn) (err error) {
	rc.(*net.TCPConn).SetKeepAlive(true)
	tgtAddr, err := socks.Handshake(rc)
	if err != nil {
		// UDP: keep the connection until disconnect then free the UDP socket
		if err == socks.InfoUDPAssociate {
			buf := make([]byte, 1)
			// block here
			for {
				_, err = rc.Read(buf)
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
	peer := <-p.chPeer
	stream, err := peer.StreamAsync(ctx, "Conn")
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	go func() {
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
			rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
			// wg.Done()
			rc.Close()

			// select {
			// case p.chPeer <- peer:
			// 	log.Ctx(ctx).Info().Caller().Int("len(peers)", len(p.chPeer)).Msg("reuser peer++++++++++++++++++++++++")
			// case <-time.After(time.Second * 5):
			// 	log.Ctx(ctx).Info().Caller().Msg("close  peer------------------------")
			// 	peer.Close(ctx)
			// }
			peer.Close(ctx)
		}()
		var n int
		ch := make(chan []byte, 1024)
		go func() {
			// TODO: Read 和Send 分两个进程处理
			defer close(ch)
			for {
				iface, err := stream.Recv(ctx)
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
		for bs := range ch {
			if l := len(bs); l > 0 {
				n, err = rc.Write(bs)
				if n < 0 || n < l {
					if err == nil {
						err = errors.Errorf(" n < 0 || n < l")
					}
				}
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
			}
		}
	}()

	defer func() {
		e := recover()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
		// wg.Wait()
		// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
	}()

	ch := make(chan []byte, 1024)
	go func() {
		defer close(ch)
		for {
			buf := make([]byte, 1024*8)
			// buf := make([]byte, math.MaxUint16/2)
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
	addr := tgtAddr.String()
	for bs := range ch {
		err1 := stream.Send(ctx, &pb.ConnReq{
			Addr: addr,
			Body: bs,
		})
		if err1 != nil {
			log.Ctx(ctx).Info().Caller().Err(err1).Msg("err")
			return
		}
		addr = ""
	}
	return
}
