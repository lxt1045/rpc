package main

import (
	"context"
	"os/signal"
	"syscall"

	proxy "github.com/lxt1045/rpc/test/proxy"
	"github.com/lxt1045/rpc/test/proxy/filesystem"
	"github.com/lxt1045/rpc/test/proxy/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
)

type Config struct {
	ClientConn config.Conn
	Log        config.Log
	TCPProxy   []TCPProxy
}

type TCPProxy struct {
	From string
	To   string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, _ = log.WithLogid(ctx, gid.New())

	conf := &Config{}
	if err := config.UnmarshalFS("static/conf/default.yml", filesystem.Static, conf); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	if err := log.Init(ctx, conf.Log); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	tlsConf := conf.ClientConn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, tlsConf.ClientCert, tlsConf.ClientKey, tlsConf.CACert)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	tlsConfig.ServerName = conf.ClientConn.Host

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cli := &proxy.SocksCli{ChPeer: make(chan *proxy.Peer, 1)}
	var _ pb.SocksCliServer = cli
	go cli.RunConnLoop(ctx, cancel, conf.ClientConn.Addr, tlsConfig)

	for _, mapping := range conf.TCPProxy {
		go func() {
			if err := cli.RunTCP(ctx, mapping.From, mapping.To); err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Msg("TCP proxy stopped")
				cancel()
			}
		}()
	}
	<-ctx.Done()
}
