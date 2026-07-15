package main

import (
	"context"
	"flag"
	"net/http"
	"runtime"
	"strings"

	_ "net/http/pprof"

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
	Debug bool
	Pprof bool
	Dev   bool
	Conn  config.Conn
	Log   config.Log
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = log.WithLogid(ctx, gid.GetGID())

	go socks.CheckMemLoop(512)

	pidOld := 0
	flag.IntVar(&pidOld, "rerun", 0, "rerun")
	flag.Parse()
	if pidOld >= 0 {
		socks.CheckProcess(ctx, pidOld)
	}

	if true {
		go func() {
			runtime.SetBlockProfileRate(1)     // 开启对阻塞操作的跟踪，block
			runtime.SetMutexProfileFraction(1) // 开启对锁调用的跟踪，mutex

			log.Ctx(ctx).Info().Msg("http://127.0.0.1:6060/debug/pprof")
			// curl -o cpu.prof http://127.0.0.1:6060/debug/pprof/allocs?seconds=60
			// curl -o cpu.prof http://127.0.0.1:6060/debug/pprof/profile?seconds=60
			// curl -o cpu.prof http://127.0.0.1:6060/debug/pprof/block?seconds=20
			// go tool pprof ./service.exe cpu.prof

			err := http.ListenAndServe(":6060", nil)
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
	// listener, err := tls.Listen("tcp", conf.Conn.Addr, tlsConfig)
	listener, err := quic.ListenAddr(conf.Conn.Addr, tlsConfig, nil)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	defer listener.Close()
	log.Ctx(ctx).Info().Caller().Str("Listen", conf.Conn.Addr).Send()

	gPeer, err := rpc.StartPeer(ctx, nil, &socks.SocksSvc{}, pb.NewSocksCliClient, pb.RegisterSocksSvcServer)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	defer gPeer.Close(ctx)

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
		go func(ctx context.Context, c quic.Connection) {
			svcConn, err := conn.WrapQuic(ctx, c)
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				return
			}
			// zsvc, err := conn.NewZip(ctx, svcConn)
			// if err != nil {
			// 	log.Ctx(ctx).Fatal().Caller().Err(err).Send()
			// }

			log.Ctx(ctx).Info().Caller().Str("local", svcConn.LocalAddr().String()).Str("remote", svcConn.RemoteAddr().String()).Send()

			svc := &socks.SocksSvc{
				RemoteAddr: svcConn.RemoteAddr().String(),
			}
			peer, err := gPeer.Clone(ctx, svcConn, svc)
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				return
			}
			svc.Peer = peer
		}(ctx, c)
	}
}
