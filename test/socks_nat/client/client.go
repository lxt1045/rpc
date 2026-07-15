package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/socks_nat/pb"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
)

type client struct {
	Name       string
	LocalAddr  string
	RemoteAddr string
	Peer       rpc.Peer
	ClientInfo *pb.ClientInfo
	TlsConf    *tls.Config

	bClient   bool
	peerCli   *peerCli
	peerSvc   *peerSvc
	socksAddr string

	ChPeer    chan *peerCli
	ChPeerSvc chan *peerSvc
	ChP2P     chan error // p2p成功时发送
}

func (cli *client) Latency(ctx context.Context, in *pb.LatencyReq) (out *pb.LatencyRsp, err error) {
	return &pb.LatencyRsp{
		Ts: time.Now().UnixNano(),
	}, nil
}

func (cli *client) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = cli.Peer.Close(ctx)
	return &pb.CloseRsp{}, err
}

// Listen on addr and proxy to server to reach target from getAddr.
func (cli *client) TCPLocal(ctx context.Context, socksAddr string) {
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

			var peerCli *peerCli
			select {
			case peerCli = <-cli.ChPeer:
			default:
			}
			if peerCli != nil {
				log.Ctx(ctx).Info().Caller().Err(err).Msgf("[proxy by Upgrade] %s <-> %s", c.LocalAddr().String(), tgtAddr.String())
				err = peerCli.connect2(ctx, tgtAddr.String(), c)
			} else {
				log.Ctx(ctx).Info().Caller().Err(err).Msgf("[proxy by Stream] %s <-> %s", c.LocalAddr().String(), tgtAddr.String())
				err = cli.peerCli.connect(ctx, tgtAddr.String(), c)
			}
			if err != nil {
				//
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
		}()
	}
}

func (cli *client) ConnPeer(ctx context.Context, in *pb.ConnPeerReq) (resp *pb.ConnPeerRsp, err error) {
	if !cli.bClient {
		defer func() {
			select {
			case cli.ChP2P <- err:
			default:
			}
		}()
	}

	// 先 listen 再 连接, 或者同时打开。后者的成功概率好像更大
	tsDo := in.Timestamp
	tsNow := time.Now().UnixNano()
	if tsDo < tsNow {
		err = errors.Errorf("tsDo: %d, tsNow: %d, time out", tsDo, tsNow)
		return
	}

	tsDiff := tsNow - tsDo - int64(time.Millisecond)*1
	time.Sleep(time.Duration(tsDiff))
	for {
		tsNow := time.Now().UnixNano()
		if tsDo <= tsNow {
			break
		}
		runtime.Gosched()
	}

	// conn, err := socket.Dial(ctx, "tcp4", c.LocalAddr, in.Client.Addr)
	conn, err := socket.DialWithTimeout(ctx, "tcp4", cli.LocalAddr, in.Client.Addr, time.Millisecond*100)
	if err != nil {
		for i := range 300 {
			log.Ctx(ctx).Info().Caller().Int("i", i).Err(err).Msg("Dial failed")
			conn, err = socket.DialWithTimeout(ctx, "tcp4", cli.LocalAddr, in.Client.Addr, time.Millisecond*time.Duration(100+rand.Int31n(30)))
			if err == nil {
				break
			}
		}
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("Dial failed")
			return
		}
	}
	log.Ctx(ctx).Info().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Msg("connected")

	if cli.bClient {
		me := &peerCli{
			LocalAddr:  conn.LocalAddr().String(),
			RemoteAddr: conn.RemoteAddr().String(),
		}
		// A 作为 TLS 客户端
		tlsConn := tls.Client(conn, cli.TlsConf)
		err = tlsConn.Handshake()
		if err != nil {
			fmt.Println("A TLS client error:", err)
			return
		}

		// me.Peer, err = rpc.StartPeer(ctx, conn, me, pb.RegisterPeerCliServer, pb.NewPeerSvcClient)
		me.Peer, err = rpc.StartPeer(ctx, tlsConn, me, pb.RegisterPeerCliServer, pb.NewPeerSvcClient)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		cli.peerCli = me
	} else {
		me := &peerSvc{
			Name:       cli.Name,
			LocalAddr:  conn.LocalAddr().String(),
			RemoteAddr: conn.RemoteAddr().String(),
		}

		// B 作为 TLS 服务器
		tlsConn := tls.Server(conn, cli.TlsConf)
		err = tlsConn.Handshake()
		if err != nil {
			fmt.Println("B TLS server error:", err)
			return
		}

		// me.Peer, err = rpc.StartPeer(ctx, conn, me, pb.RegisterPeerSvcServer, pb.NewPeerCliClient)
		me.Peer, err = rpc.StartPeer(ctx, tlsConn, me, pb.RegisterPeerSvcServer, pb.NewPeerCliClient)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}

		cli.peerSvc = me
	}
	return
}

func (cli *client) auth(ctx context.Context) (err error) {
	clientType := pb.AuthReq_Client
	if !cli.bClient {
		clientType = pb.AuthReq_Sevice
	}
	req := &pb.AuthReq{
		Name:       cli.Name,
		ClientType: clientType,
	}
	resp := &pb.AuthRsp{}
	err = cli.Peer.Invoke(ctx, "Auth", req, resp)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	return
}

func (cli *client) clients(ctx context.Context) (clients []*pb.ClientInfo, err error) {
	req := &pb.ClientsReq{
		MyName: cli.Name,
	}
	resp := &pb.ClientsRsp{}
	err = cli.Peer.Invoke(ctx, "Clients", req, resp)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	clients = resp.Clients
	return
}

func (c *client) RunConnLoop(ctx context.Context, cancel context.CancelFunc, addr string, tlsConfig *tls.Config) {
	var err error
	defer func() {
		e := recover()
		cancel()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
	}()
	for {
		ctx := context.TODO()
		ctx, _ = log.WithLogid(ctx, gid.GetGID())

		select {
		case <-ctx.Done():
			log.Ctx(ctx).Info().Caller().Str("addr", addr).Msg("ctx.Done")
			return
		default:
		}

		// conn, err := tls.Dial("tcp", conf.ClientConn.Addr, tlsConfig)
		conn, err := socket.DialTLS(ctx, "tcp", addr, tlsConfig)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}

		// 创建新的 client
		cli := *c
		cli.LocalAddr = conn.LocalAddr().String()
		cli.RemoteAddr = conn.RemoteAddr().String()
		var _ pb.ClientServer = &cli

		// c.Peer.Clone()
		cli.Peer, err = rpc.StartPeer(ctx, conn, &cli, pb.RegisterClientServer, pb.NewServiceClient)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}

		err = cli.auth(ctx)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}

		// TCP 同时打开失败时，需要此 listen 做防护守卫
		if false {
			go func(addr string) {
				ln, err := socket.Listen(ctx, "tcp4", addr)
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
					return
				}

				buf := make([]byte, 1024)
				for {
					connPeer, err := ln.Accept()
					if err != nil {
						fmt.Println("accept failed", err)
						continue
					}
					log.Ctx(ctx).Info().Caller().Str("local", connPeer.LocalAddr().String()).Str("remote", connPeer.RemoteAddr().String()).Send()
					for {
						n, err := connPeer.Read(buf)
						if err != nil {
							fmt.Println("read failed", err)
							break
						}
						log.Ctx(ctx).Info().Caller().Str("read", string(buf[:n])).
							Str("local", connPeer.LocalAddr().String()).Str("remote", connPeer.RemoteAddr().String()).Send()
					}
					// err = connectPeer(ctx, connPeer)
					// if err != nil {
					// 	log.Ctx(ctx).Error().Caller().Err(err).Send()
					// 	return
					// }
				}
			}(":" + localPort(conn))
		}

		if !cli.bClient {
			// 如何等待

			err = <-cli.ChP2P
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				continue
			}

			if cli.peerSvc != nil {
				select {
				case cli.ChPeerSvc <- cli.peerSvc:
				default:
				}
				cli.peerSvc.AfterConnUpgradeClose = func() {
					select {
					case <-cli.ChPeerSvc:
					default:
					}
					cli.Close(ctx, nil)
					log.Ctx(ctx).Info().Caller().Str("local", cli.LocalAddr).Str("remote", cli.RemoteAddr).Msg("close client")
				}
			} else {
				log.Ctx(ctx).Info().Caller().Str("local", cli.LocalAddr).Str("remote", cli.RemoteAddr).Msg("--- close client")
				cli.Close(ctx, nil)
			}
			continue
		}

		cli.TlsConf = tlsConfig
		clients, err := cli.clients(ctx)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		if len(clients) == 0 {
			err = errors.New("没有可连的服务端")
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		log.Ctx(ctx).Info().Caller().Interface("clients", clients).Send()

		// peer client 方
		target := clients[0]
		for _, c := range clients {
			if c.Count == 0 {
				target = c
				break
			}
		}
		log.Ctx(ctx).Info().Caller().Int32("count", target.Count).Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()

		req := &pb.ConnPeerReq{
			Client: target,
		}
		resp := &pb.ConnPeerRsp{}
		err = cli.Peer.Invoke(ctx, "ConnPeer", req, resp)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		log.Ctx(ctx).Info().Caller().Interface("resp", resp).Send()

		func() {
			req := &pb.ConnPeerReq{
				Timestamp: resp.Timestamp,
				Client:    target,
			}
			resp, err := cli.ConnPeer(ctx, req)
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				return
			}
			_ = resp

			cli.peerCli.AfterConnUpgradeClose = func() {
				cli.Close(ctx, nil)
				log.Ctx(ctx).Info().Caller().Str("local", cli.LocalAddr).Str("remote", cli.RemoteAddr).Msg("close client")
			}
			c.ChPeer <- cli.peerCli
		}()

	}

}
