package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	socks "github.com/lxt1045/rpc/test/socks_trunk"
	"github.com/lxt1045/rpc/test/socks_trunk/filesystem"
	"github.com/lxt1045/rpc/test/socks_trunk/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
)

/*
$env:CGO_ENABLED=0; $env:GOOS="linux"; $env:GOARCH="amd64"; go build ./
*/

type Config struct {
	Debug      bool
	Pprof      bool
	Dev        bool
	Conn       config.Conn
	ClientConn config.Conn
	Log        config.Log

	Token string            `mapstructure:"token"`
	Trunk socks.TrunkConfig `mapstructure:"trunk"`
	Socks string            `mapstructure:"socks"`
	HTTP  string            `mapstructure:"http"`
}

func main() {
	var socksAddr string
	var httpAddr string
	flag.StringVar(&socksAddr, "socks", "", "SOCKS5 TCP listen address")
	flag.StringVar(&httpAddr, "http", "", "HTTP CONNECT listen address")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := &Config{}
	if err := config.UnmarshalFS("static/conf/default.yml", filesystem.Static, conf); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	if err := log.Init(ctx, conf.Log); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	ctx, _ = log.WithLogid(ctx, gid.New())

	token := conf.Token
	if token == "" {
		token = os.Getenv("SOCKS_TRUNK_TOKEN")
	}
	if err := socks.ValidateClientConfig(&socks.ClientConfig{Token: token, Trunk: conf.Trunk}); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("invalid client config")
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
		Name:         "client",
		SocksAddr:    socksAddr,
		ChPeer:       make(chan *socks.Peer, 2),
		ChPeerReuser: make(chan *socks.Peer, 10),
		TlsConf:      tlsConfig,
		PeerAddr:     conf.ClientConn.Addr,
		Token:        token,
		TrunkCfg:     conf.Trunk,
	}
	var _ pb.SocksCliServer = cli

	// Keep two ready control connections so GetPeer can return immediately.
	for range 2 {
		go cli.RunConnLoop(ctx, cancel, conf.ClientConn.Addr, tlsConfig)
	}

	if err = cli.InitTrunk(ctx); err != nil {
		// Do not exit: MaintainTrunk below will retry until the server is ready.
		log.Ctx(ctx).Warn().Caller().Err(err).Msg("initial InitTrunk failed, will retry")
	}
	go cli.MaintainTrunk(ctx)

	if socksAddr != "" {
		go cli.RunSocks(ctx, socksAddr)
	}
	if httpAddr != "" {
		// mode 3 is the Trunk-backed HTTP proxy path.
		go cli.RunHttpProxy(ctx, httpAddr, 3)
	}
	log.Ctx(ctx).Info().Caller().Str("socks", socksAddr).Str("http", httpAddr).Msg("client started")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	cli.Close(ctx, &pb.CloseReq{})
}
