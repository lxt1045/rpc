package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	socks "github.com/lxt1045/rpc/test/socks_faux_trunk_kcp"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/filesystem"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
)

type Config struct {
	Debug bool
	Pprof bool
	Dev   bool
	Log   config.Log

	Token      string               `yaml:"token"`
	ClientConn socks.ConnConfig     `yaml:"client_conn"`
	TrunkKCP   socks.TrunkKCPConfig `yaml:"trunk_kcp"`
	FauxTCP    socks.FauxTCPConfig  `yaml:"faux_tcp"`
	TLS        socks.TLSConfig      `yaml:"tls"`
	Socks      string               `yaml:"socks"`
	HTTP       string               `yaml:"http"`
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
	confSource, err := socks.LoadConfig("static/conf/default.yml", filesystem.Static, conf)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	if err := log.Init(ctx, conf.Log); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	log.Ctx(ctx).Info().Caller().Str("conf", confSource).Msg("config loaded")
	socks.LogEffectiveTrunkKCP(ctx, "client", &conf.TrunkKCP)

	token := os.Getenv("SOCKS_TRUNK_TOKEN")
	if token == "" {
		token = conf.Token
	}
	cliCfg := &socks.ClientConfig{
		Token:      token,
		ClientConn: conf.ClientConn,
		Trunk:      conf.TrunkKCP,
		FauxTCP:    conf.FauxTCP,
	}
	if err := socks.ValidateClientConfig(cliCfg); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	if socksAddr == "" {
		socksAddr = conf.Socks
	}
	if httpAddr == "" {
		httpAddr = conf.HTTP
	}

	// 端到端 TLS（与控制通道共用一套 CA/证书）
	tlsCfg, err := cliCfg.TLS.ClientTLS(filesystem.Static)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	if tlsCfg == nil {
		log.Ctx(ctx).Warn().Caller().Msg("tls.enabled=false：token 与代理数据明文传输（仅可信链路）")
	}

	cli := &socks.SocksCli{
		Name:      "socks-faux-trunk-kcp-client",
		PeerAddr:  cliCfg.ClientConn.Addr,
		LocalAddr: cliCfg.ClientConn.LocalAddr,
		Token:     cliCfg.Token,
		TrunkCfg:  cliCfg.Trunk,
		FauxCfg:   cliCfg.FauxTCP,
		TLSCfg:    tlsCfg,
		ChPeer:    make(chan *socks.Peer, 2),
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
		log.Ctx(ctx).Info().Caller().Str("socks", socksAddr).Str("http", httpAddr).
			Str("server", cli.PeerAddr).Msg("client started (faux_tcp transport)")
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
	cancel()
	cli.Close(ctx, &pb.CloseReq{})
}
