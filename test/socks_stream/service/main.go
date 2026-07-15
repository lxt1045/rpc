package main

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/socks"
	"github.com/lxt1045/rpc/test/socks/filesystem"
	"github.com/lxt1045/rpc/test/socks/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"
)

/*
$env:CGO_ENABLED=0; $env:GOOS="linux"; $env:GOARCH="amd64"; go build ./


*/

var (
	ErrUnexpected = errors.NewCode(0, -1, "unexpected error")
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

	// 解析配置文件
	conf := &Config{}
	file := "static/conf/default.yml"
	err := config.UnmarshalFS(file, filesystem.Static, conf)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	// 初始化Log
	err = log.Init(ctx, conf.Log)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	cmtls := conf.Conn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, cmtls.ServerCert, cmtls.ServerKey, cmtls.CACert)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	listener, err := tls.Listen("tcp", conf.Conn.Addr, tlsConfig)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	defer listener.Close()
	chSvcs := make(chan *socks.SocksSvc, 1024)
	var g errgroup.Group
	g.Go(func() (err error) {
		defer func() {
			e := recover()
			if e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Stack().Msg("listener defer")
			}
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Stack().Msg("listener defer")
			} else {
				log.Ctx(ctx).Info().Caller().Err(errors.Errorf("listener")).Stack().Msg("listener defer")
			}
			cancel()
		}()
		log.Ctx(ctx).Info().Caller().Str("Listen", conf.Conn.Addr).Msg("Listen")

		gPeer, err := rpc.NewPeer(ctx, &socks.SocksSvc{}, pb.NewSocksCliClient, pb.RegisterSocksSvcServer)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		// gPeer.ClientUse(rpc.CliLogid)
		// gPeer.ServiceUse(rpc.SvcLogid)
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				log.Ctx(ctx).Info().Caller().Str("Listen", conf.Conn.Addr).Msg("Done")
				return
			default:
			}
			conn, err := listener.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					log.Ctx(ctx).Error().Caller().Err(ErrUnexpected.WithErr(err)).Send()
					// panic(err)
				} else {
					err = ErrUnexpected.WithErr(err)
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
				// continue
				return err
			}
			// conn.SetReadDeadline(time.Second)

			go func(conn net.Conn) {
				log.Ctx(ctx).Info().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Msg("Accept")
				svc := &socks.SocksSvc{
					RemoteAddr: conn.RemoteAddr().String(),
					LocalAddr:  conn.LocalAddr().String(),
				}
				peer, err := gPeer.Clone(ctx, conn, svc)
				if err != nil {
					log.Ctx(ctx).Warn().Caller().Err(err).Send()
					return
				}
				svc.Peer = peer
				select {
				case chSvcs <- svc:
				default:
					log.Ctx(ctx).Warn().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Msg("send error")
				}
			}(conn)
		}
	})

	// g.Go(func() (err error) {
	go func() {
		var err error
		defer func() {
			e := recover()
			if e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Err(errors.Errorf("svcs check")).Stack().Msg("svcs check defer")
			}
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Stack().Msg("svcs check defer")
			} else {
				log.Ctx(ctx).Info().Caller().Err(errors.Errorf("svcs check")).Stack().Msg("svcs check defer")
			}
			cancel()
			listener.Close()
		}()

		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()

		// SocksSvc 需要存起来，要不有可能被垃圾回收
		svcs := make([]*socks.SocksSvc, 0, 1024)
		for {
			select {
			case <-ctx.Done():
				log.Ctx(ctx).Info().Caller().Str("Listen", conf.Conn.Addr).Msg("Done")
				return
			case svc := <-chSvcs:
				svcs = append(svcs, svc)
			case <-ticker.C:
				SvcsNew := svcs[:0]
				for _, svc := range svcs {
					if svc != nil && !svc.Peer.IsClosed() {
						SvcsNew = append(SvcsNew, svc)
						continue
					}
					log.Ctx(ctx).Info().Caller().Interface("svc", svc).Msg("svc closed")
				}
				svcs = SvcsNew
			}
			// log.Ctx(ctx).Info().Caller().Int("svcs", len(svcs)).Msg("check")
		}
		// })
	}()

	WaitSysSignal(ctx)
	cancel()
	listener.Close()
	// time.Sleep(time.Millisecond * 100)
	err = g.Wait()
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

}

func WaitSysSignal(ctx context.Context) {
	// SIGINT	2	Term	用户发送INTR字符(Ctrl+C)触发
	// SIGTERM	15	Term	结束程序(可以被捕获、阻塞或忽略)
	// SIGHUP	1	Term	终端控制进程结束(终端连接断开)
	// SIGQUIT	3	Core	用户发送QUIT字符(Ctrl+/)触发
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	for exit := false; !exit; {
		select {
		case s := <-ch:
			switch s {
			case syscall.SIGQUIT:
				log.Ctx(ctx).Info().Caller().Msg("SIGSTOP")
				exit = true
			case syscall.SIGHUP:
				log.Ctx(ctx).Info().Caller().Msg("SIGHUP")
				// exit = true
			case syscall.SIGINT:
				log.Ctx(ctx).Info().Caller().Msg("SIGINT")
				exit = true
			case syscall.SIGTERM:
				log.Ctx(ctx).Info().Caller().Msg("SIGINT")
				exit = true
			default:
				log.Ctx(ctx).Info().Caller().Msgf("default:%v", s)
				// exit = true
			}
		case <-ctx.Done():
			exit = true
			log.Ctx(ctx).Info().Caller().Msg("ctx.Done()")
		}
	}
}
