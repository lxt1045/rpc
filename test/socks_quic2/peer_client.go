package socks

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/conn"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/socks_quic2/pb"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
	"github.com/quic-go/quic-go"
)

type SocksCli struct {
	Name      string
	SocksAddr string
	ChPeer    chan *Peer
}

type Peer struct {
	TsLast      int64
	LocalAddrs  string
	RemoteAddrs string
	rpc.Peer
}

func (p *SocksCli) GetPeer(ctx context.Context) (peer *Peer) {
	peer = <-p.ChPeer
	for tsLast := time.Now().Unix() - int64(time.Second*60); peer.TsLast < tsLast; {
		err := peer.Close(ctx)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
		peer = <-p.ChPeer
	}
	return
}

func (p *SocksCli) close(ctx context.Context) (err error) {
	for peer := range p.ChPeer {
		err1 := peer.Close(ctx)
		if err1 != nil {
			err = err1
		}
	}
	return
}

func (p *SocksCli) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = p.close(ctx)
	return &pb.CloseRsp{}, err
}

// Listen on addr and proxy to server to reach target from getAddr.
func (p *SocksCli) RunLocal(ctx context.Context, cancel context.CancelFunc, socksAddr string) {
	defer cancel()

	l, err := net.Listen("tcp", socksAddr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to listen on %s: %v", socksAddr, err)
		return
	}
	defer l.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		c, err := l.Accept()
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("failed to accept")
			continue
		}
		ctx, calcelInner := context.WithCancel(ctx)
		go p.connect(ctx, calcelInner, c)
	}
}

func (p *SocksCli) connect(ctx context.Context, cancel context.CancelFunc, rc net.Conn) (err error) {
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
	peer := p.GetPeer(ctx)
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

			if false {
				peer.TsLast = time.Now().Unix()
				select {
				case p.ChPeer <- peer:
					log.Ctx(ctx).Info().Caller().Int("len(peers)", len(p.ChPeer)).Msg("reuser peer++++++++++++++++++++++++")
				case <-time.After(time.Second * 5):
					log.Ctx(ctx).Info().Caller().Msg("close  peer------------------------")
					peer.Close(ctx)
				}
			} else {
				peer.Close(ctx)
			}
			cancel()
		}()
		var n int
		ch := make(chan []byte, 1024)
		go func() {
			// TODO: Read 和Send 分两个进程处理
			defer close(ch)
			defer cancel()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

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
		cancel()
		// wg.Wait()
		// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
	}()

	ch := make(chan []byte, 1024)
	go func() {
		defer close(ch)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

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

func (p *SocksCli) connect2(ctx context.Context, cancel context.CancelFunc, rc net.Conn) (err error) {
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
	peer := p.GetPeer(ctx)
	addr := tgtAddr.String()
	go p.CopyLoop(ctx, cancel, rc, peer, addr)

	return
}

func (p *SocksCli) CopyLoop(ctx context.Context, cancel context.CancelFunc, rwc io.ReadWriteCloser, peer *Peer, addr string) (err error) {
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
			if dl, ok := rwc.(interface{ SetDeadline(t time.Time) error }); ok {
				// rwc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				dl.SetDeadline(time.Now())
			}
			// wg.Done()
			rwc.Close()
			cancel()

			if true {
				peer.TsLast = time.Now().Unix()
				select {
				case p.ChPeer <- peer:
					log.Ctx(ctx).Info().Caller().Int("len(peers)", len(p.ChPeer)).Msg("reuser peer++++++++++++++++++++++++")
				case <-time.After(time.Second * 5):
					log.Ctx(ctx).Info().Caller().Msg("close  peer------------------------")
					peer.Close(ctx)
				}
			} else {
				peer.Close(ctx)
			}
		}()
		var n int
		ch := make(chan []byte, 8)
		go func() {
			// TODO: Read 和Send 分两个进程处理
			defer close(ch)
			defer cancel()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

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
				n, err = rwc.Write(bs)
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
		cancel()
		// wg.Wait()
		// rwc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
	}()

	ch := make(chan []byte, 8)
	go func() {
		defer close(ch)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			buf := make([]byte, 1024*8)
			// buf := make([]byte, math.MaxUint16/2)
			nr, er := rwc.Read(buf)
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

// 创建备用connect，提前三次握手较少延时
func (p *SocksCli) RunConn(ctx context.Context, cancel context.CancelFunc, addr string, tlsConfig *tls.Config) {
	var err error
	defer func() {
		e := recover()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
		cancel()
	}()
	for {
		// conn, err := tls.Dial("tcp", conf.ClientConn.Addr, tlsConfig)
		conn, err := socket.DialTLS(ctx, "tcp", addr, tlsConfig)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		log.Ctx(ctx).Info().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()
		peer, err1 := rpc.StartPeer(ctx, conn, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
		if err1 != nil {
			err = err1
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}

		select {
		case <-ctx.Done():
			return
		case p.ChPeer <- &Peer{
			TsLast:      time.Now().Unix(),
			Peer:        peer,
			LocalAddrs:  conn.LocalAddr().String(),
			RemoteAddrs: conn.RemoteAddr().String(),
		}:
		case <-time.After(time.Second * 60):
			peer.Close(ctx)
		}
	}
}

// 创建备用connect，提前三次握手较少延时
func (p *SocksCli) RunQuicConn(ctx context.Context, cancel context.CancelFunc, addr string, tlsConfig *tls.Config) {
	var err error
	defer func() {
		e := recover()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
		cancel()
	}()
	for {
		// conn, err := tls.Dial("tcp", conf.ClientConn.Addr, tlsConfig)
		// conn, err := socket.DialTLS(ctx, "tcp", addr, tlsConfig)
		c, err := quic.DialAddr(ctx, addr, tlsConfig, nil)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			continue
		}
		cliConn, err := conn.WrapQuicClient(ctx, c)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			continue
		}
		// zcli, err := conn.NewZip(ctx, cliConn)
		// if err != nil {
		// 	log.Ctx(ctx).Error().Caller().Err(err).Send()
		// 	return
		// }
		log.Ctx(ctx).Info().Caller().Str("local", cliConn.LocalAddr().String()).Str("remote", cliConn.RemoteAddr().String()).Send()
		peer, err1 := rpc.StartPeer(ctx, cliConn, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
		if err1 != nil {
			err = err1
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			continue
		}

		select {
		case <-ctx.Done():
			log.Ctx(ctx).Error().Caller().Msg("ctx.Done()")
			return
		case p.ChPeer <- &Peer{
			TsLast:      time.Now().Unix(),
			Peer:        peer,
			LocalAddrs:  cliConn.LocalAddr().String(),
			RemoteAddrs: cliConn.RemoteAddr().String(),
		}:
		case <-time.After(time.Second * 60):
			peer.Close(ctx)
		}
	}
}
