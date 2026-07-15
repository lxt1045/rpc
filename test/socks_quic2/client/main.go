package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	socks "github.com/lxt1045/rpc/test/socks_quic2"
	"github.com/lxt1045/rpc/test/socks_quic2/filesystem"
	"github.com/lxt1045/rpc/test/socks_quic2/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
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

	if false {
		go func() {
			runtime.SetBlockProfileRate(1)     // 开启对阻塞操作的跟踪，block
			runtime.SetMutexProfileFraction(1) // 开启对锁调用的跟踪，mutex

			err := http.ListenAndServe(":16061", nil)
			log.Ctx(ctx).Error().Err(err).Msg("http/pprof listen")
		}()
	}

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

	cli := &socks.SocksCli{
		Name:      flags.Client,
		SocksAddr: flags.Socks,
		ChPeer:    make(chan *socks.Peer),
	}
	var _ pb.SocksCliServer = cli
	go cli.RunQuicConn(ctx, cancel, conf.ClientConn.Addr, tlsConfig)

	//

	go cli.RunLocal(ctx, cancel, flags.Socks)
	log.Ctx(ctx).Info().Caller().Str("Socks", flags.Socks).Send()

	//

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
