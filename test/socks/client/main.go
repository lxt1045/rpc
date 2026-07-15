package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/lxt1045/rpc/test/socks"
	"github.com/lxt1045/rpc/test/socks/filesystem"
	"github.com/lxt1045/rpc/test/socks/pb"
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
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
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

	cli := &socks.SocksCli{
		Name:         flags.Client,
		SocksAddr:    flags.Socks,
		ChPeer:       make(chan *socks.Peer, 1),
		ChPeerReuser: make(chan *socks.Peer, 10),

		TlsConf:  tlsConfig,
		PeerAddr: conf.ClientConn.Addr,
	}
	var _ pb.SocksCliServer = cli
	for range 32 {
		go cli.RunConnLoop(ctx, cancel, conf.ClientConn.Addr, tlsConfig)
	}

	//

	go cli.RunSocks(ctx, flags.Socks)
	go cli.RunHttpProxy(ctx, ":18082", 1)

	go cli.RunHttpProxy(ctx, ":18081", 0)
	log.Ctx(ctx).Info().Caller().Str("Socks", flags.Socks).Send()

	//

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
