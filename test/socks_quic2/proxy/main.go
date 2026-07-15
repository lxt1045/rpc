package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"runtime"
	"strings"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/conn"
	socks "github.com/lxt1045/rpc/test/socks_quic2"
	"github.com/lxt1045/rpc/test/socks_quic2/filesystem"
	"github.com/lxt1045/rpc/test/socks_quic2/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	"github.com/quic-go/quic-go"
	_ "go.uber.org/automaxprocs"
)

type Config struct {
	Debug      bool
	Pprof      bool
	Dev        bool
	Conn       config.Conn
	ClientConn config.Conn
	ProxyConn  config.Conn
	Log        config.Log
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = log.WithLogid(ctx, gid.GetGID())

	go socks.CheckMemLoop(1024)

	pidOld := 0
	flag.IntVar(&pidOld, "rerun", 0, "rerun")
	flag.Parse()
	if pidOld >= 0 {
		socks.CheckProcess(ctx, pidOld)
	}

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

	cmtls := conf.Conn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, cmtls.ServerCert, cmtls.ServerKey, cmtls.CACert)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	// listener, err := tls.Listen("tcp", conf.Conn.ProxyAddr, tlsConfig)
	listener, err := quic.ListenAddr(conf.Conn.ProxyAddr, tlsConfig, nil)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	defer listener.Close()
	log.Ctx(ctx).Info().Caller().Str("Listen", conf.Conn.ProxyAddr).Send()

	gPeer, err := rpc.StartPeer(ctx, nil, &socks.SocksProxy{}, pb.NewSocksCliClient, pb.RegisterSocksSvcServer)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}

	ClientName := "Client"
	tlsConfig.ServerName = conf.ProxyConn.Host
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		c, err := listener.Accept(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				log.Ctx(ctx).Error().Caller().Err(errors.Errorf(err.Error())).Send()
				// panic(err)
			} else {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
			continue
		}
		go func(c quic.Connection, i int) {
			// ctx := context.TODO()
			ctx, cancel := context.WithCancel(context.TODO())
			svcConn, err := conn.WrapQuic(ctx, c)
			if err != nil {
				log.Ctx(ctx).Fatal().Caller().Err(err).Send()
			}
			// zsvc, err := conn.NewZip(ctx, svcConn)
			// if err != nil {
			// 	log.Ctx(ctx).Fatal().Caller().Err(err).Send()
			// }
			log.Ctx(ctx).Info().Caller().Str("local", svcConn.LocalAddr().String()).Str("remote", svcConn.RemoteAddr().String()).Send()

			remoteAddr := svcConn.RemoteAddr().String()
			svcAddr := conf.ProxyConn.Addr
			name := fmt.Sprintf(ClientName+":%d", i)
			_ = socks.NewSocksProxy(ctx, cancel, remoteAddr, svcAddr, name, tlsConfig, gPeer, svcConn)
		}(c, i)
	}
}
