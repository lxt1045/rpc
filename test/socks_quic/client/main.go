package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/conn"
	"github.com/lxt1045/rpc/test/socks_quic/filesystem"
	"github.com/lxt1045/rpc/test/socks_quic/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	quic "github.com/quic-go/quic-go"
	_ "go.uber.org/automaxprocs"
)

type Config struct {
	Debug      bool
	Pprof      bool
	Dev        bool
	Conn       config.Conn
	ClientConn config.Conn
	Log        config.Log
}

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
	flag.StringVar(&flags.Client, "c", "client-952700", "client connect address or url")
	// flag.StringVar(&flags.Socks, "socks", ":10086", "(client-only) SOCKS listen address")
	flag.StringVar(&flags.Socks, "socks", ":10080", "(client-only) SOCKS listen address")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = log.WithLogid(ctx, gid.GetGID())

	// 解析配置文件
	conf := &Config{}
	file := "static/conf/default.yml"
	err := config.UnmarshalFS(file, filesystem.Static, conf)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}

	cmtls := conf.ClientConn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, cmtls.ClientCert, cmtls.ClientKey, cmtls.CACert)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	tlsConfig.ServerName = conf.ClientConn.Host

	cli := &socksCli{
		Name:      flags.Client,
		socksAddr: flags.Socks,
		chPeer:    make(chan *Peer, 3),
	}
	var _ pb.SocksCliServer = cli
	go func() {
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
		}()
		for {

			c, err := quic.DialAddr(ctx, conf.ClientConn.Addr, tlsConfig, nil)
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				continue
			}
			wc, err := conn.WrapQuicClient(ctx, c)
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				continue
			}
			zcli, err := conn.NewZip(ctx, wc)
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				continue
			}

			log.Ctx(ctx).Info().Caller().Str("local", c.LocalAddr().String()).Str("remote", c.RemoteAddr().String()).Send()
			peer, err1 := rpc.StartPeer(ctx, zcli, cli, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
			if err1 != nil {
				err = err1
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				continue
			}
			cli.chPeer <- &Peer{
				Peer:        peer,
				LocalAddrs:  c.LocalAddr().String(),
				RemoteAddrs: c.RemoteAddr().String(),
			}
		}
	}()

	//

	go cli.RunLocal(ctx, flags.Socks)
	log.Ctx(ctx).Info().Caller().Str("Socks", flags.Socks).Send()

	//

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
