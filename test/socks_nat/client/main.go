package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/socks_nat/filesystem"
	"github.com/lxt1045/rpc/test/socks_nat/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
)

type Config struct {
	Debug      bool
	Pprof      bool
	Dev        bool
	Conn       config.Conn
	ClientConn config.Conn
	Log        config.Log
}

/*
p2p service 端
./client  -c=test2 -socks=""

p2p client 端
./client
*/

func main() {
	var flags struct {
		Client  string
		Server  string
		Verbose bool
		Socks   string // 有则是 peerCli， 无则是 peerSvc
		Proxy   bool
	}

	flag.BoolVar(&flags.Verbose, "verbose", true, "verbose mode")
	flag.BoolVar(&flags.Proxy, "proxy", false, "verbose mode")
	flag.StringVar(&flags.Server, "s", "", "server listen address or url")
	flag.StringVar(&flags.Client, "c", "client-952701", "client connect address or url")
	flag.StringVar(&flags.Socks, "socks", ":20080", "(client-only) SOCKS listen address")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 解析配置文件
	conf := &Config{}
	file := "static/conf/default.yml"
	err := config.UnmarshalFS(file, filesystem.Static, conf)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	// 初始化Log
	err = log.Init(ctx, conf.Log)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	// log.Init()
	ctx, _ = log.WithLogid(ctx, gid.GetGID())

	cmtls := conf.ClientConn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, cmtls.ClientCert, cmtls.ClientKey, cmtls.CACert)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	tlsConfig.ServerName = conf.ClientConn.Host

	// conn, err := tls.Dial("tcp", conf.ClientConn.Addr, tlsConfig)
	conn, err := socket.DialTLS(ctx, "tcp", conf.ClientConn.Addr, tlsConfig)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	log.Ctx(ctx).Info().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()

	cli := &client{
		Name:       flags.Client,
		LocalAddr:  conn.LocalAddr().String(),
		RemoteAddr: conn.RemoteAddr().String(),
		bClient:    flags.Socks != "",
		socksAddr:  flags.Socks,
		ChPeer:     make(chan *peerCli, 3),
		ChPeerSvc:  make(chan *peerSvc, 3),
		ChP2P:      make(chan error, 1),
	}
	var _ pb.ClientServer = cli
	cli.Peer, err = rpc.StartPeer(ctx, conn, cli, pb.RegisterClientServer, pb.NewServiceClient)
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
		cmtls := conf.Conn.TLS
		cli.TlsConf, err = config.LoadTLSConfig(filesystem.Static, cmtls.ServerCert, cmtls.ServerKey, cmtls.CACert)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		if flags.Socks == "" {
			// peer service 方
			log.Ctx(ctx).Error().Caller().Msg("peer svc is running")
		} else {
			log.Ctx(ctx).Error().Caller().Msg("peer client is not exist")
		}
		go cli.RunConnLoop(ctx, cancel, conf.ClientConn.Addr, tlsConfig)

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		return
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
		if c.Name == "client-952701" {
			target = c
			break
		}
	}
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

	go func() {
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

		// 连接没问题后，监听 socks 端口
		log.Ctx(ctx).Error().Caller().Msgf("SOCKS proxy on %s", cli.socksAddr)

		go cli.RunConnLoop(ctx, cancel, conf.ClientConn.Addr, tlsConfig)

		cli.TCPLocal(ctx, cli.socksAddr) // 开始监听本地

	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}

func localPort(conn net.Conn) string {
	addrs := strings.Split(conn.LocalAddr().String(), ":")
	return addrs[len(addrs)-1]
}

func main1() {

	var flags struct {
		Client  string
		Server  string
		Verbose bool
		Socks   string
		Proxy   bool
	}

	flag.BoolVar(&flags.Verbose, "verbose", true, "verbose mode")
	flag.BoolVar(&flags.Proxy, "proxy", false, "verbose mode")
	flag.StringVar(&flags.Server, "s", "", "server listen address or url")
	flag.StringVar(&flags.Client, "c", "", "client connect address or url")
	flag.StringVar(&flags.Socks, "socks", ":10086", "(client-only) SOCKS listen address")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = log.WithLogid(ctx, gid.GetGID())

	log.Ctx(ctx).Error().Caller().Msgf("SOCKS proxy on %s", flags.Socks)
	go socks.TCPLocalOnly(ctx, flags.Socks)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
