package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	socks "github.com/lxt1045/rpc/test/socks_trunk_kcp"
	"github.com/lxt1045/rpc/test/socks_trunk_kcp/filesystem"
	"github.com/lxt1045/rpc/test/socks_trunk_kcp/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
)

type Config struct {
	Debug      bool
	Pprof      bool
	Dev        bool
	ClientConn config.Conn
	Log        config.Log

	Token    string               `mapstructure:"token"`
	TrunkKCP socks.TrunkKCPConfig `mapstructure:"trunk_kcp"`
	Socks    string               `mapstructure:"socks"`
	HTTP     string               `mapstructure:"http"`
}

func main() {
	var socksAddr string
	var httpAddr string
	flag.StringVar(&socksAddr, "socks", "", "SOCKS5 listen address")
	flag.StringVar(&httpAddr, "http", "", "HTTP CONNECT listen address")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	token := os.Getenv("SOCKS_TRUNK_TOKEN")
	if token == "" {
		token = conf.Token
	}
	if err := socks.ValidateClientConfig(&socks.ClientConfig{Token: token, Trunk: conf.TrunkKCP}); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	cmtls := conf.ClientConn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, cmtls.ClientCert, cmtls.ClientKey, cmtls.CACert)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	tlsConfig.ServerName = conf.ClientConn.Host

	if socksAddr == "" {
		socksAddr = conf.Socks
	}
	if httpAddr == "" {
		httpAddr = conf.HTTP
	}

	cli := &socks.SocksCli{
		Name:     "socks-trunk-kcp-client",
		PeerAddr: conf.ClientConn.Addr,
		TlsConf:  tlsConfig,
		Token:    token,
		TrunkCfg: conf.TrunkKCP,
		ChPeer:   make(chan *socks.Peer, 2),
	}
	var _ pb.SocksCliServer = cli

	for i := 0; i < 2; i++ {
		go cli.RunConnLoop(ctx)
	}
	go func() {
		for {
			if err := cli.InitTrunk(ctx); err != nil {
				log.Ctx(ctx).Warn().Caller().Err(err).Msg("InitTrunk failed, retrying")
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			break
		}
		go cli.MaintainTrunk(ctx)
		if socksAddr != "" {
			go func() {
				if err := cli.RunSocks(ctx, socksAddr); err != nil && err != context.Canceled {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
			}()
		}
		if httpAddr != "" {
			go func() {
				if err := cli.RunHTTPProxy(ctx, httpAddr); err != nil && err != context.Canceled {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
			}()
		}
		log.Ctx(ctx).Info().Str("socks", socksAddr).Str("http", httpAddr).Msg("client started")
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
	cancel()
	cli.Close(ctx, &pb.CloseReq{})
}
