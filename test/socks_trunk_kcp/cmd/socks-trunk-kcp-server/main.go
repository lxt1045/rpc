package main

import (
	"context"
	"crypto/tls"
	"expvar"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lxt1045/rpc"
	socks "github.com/lxt1045/rpc/test/socks_trunk_kcp"
	"github.com/lxt1045/rpc/test/socks_trunk_kcp/filesystem"
	"github.com/lxt1045/rpc/test/socks_trunk_kcp/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	Debug bool
	Pprof bool
	Dev   bool
	Conn  config.Conn
	Log   config.Log

	Token       string               `mapstructure:"token"`
	MaxClients  int                  `mapstructure:"max_clients"`
	TrunkKCP    socks.TrunkKCPConfig `mapstructure:"trunk_kcp"`
	ACL         socks.ACLConfig      `mapstructure:"acl"`
	MetricsAddr string               `mapstructure:"metrics_addr"`
}

func main() {
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

	token := conf.Token
	if token == "" {
		token = os.Getenv("SOCKS_TRUNK_TOKEN")
	}
	if token == "" {
		if conf.Debug {
			token = "dev-insecure-token"
			log.Ctx(ctx).Warn().Caller().Msg("using dev-insecure-token")
		} else {
			log.Ctx(ctx).Error().Caller().Msg("server token is required")
			return
		}
	}
	socks.NormalizeTrunkKCPConfig(&conf.TrunkKCP)
	if err := socks.InitServerSecurity(token, conf.MaxClients, conf.TrunkKCP.Conv, conf.TrunkKCP.MaxVirtualConn); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	if err := socks.ConfigureACL(conf.ACL); err != nil {
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
	log.Ctx(ctx).Info().Str("addr", conf.Conn.Addr).Msg("server started")

	if conf.MetricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/debug/vars", expvar.Handler())
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("ok"))
			})
			_ = http.ListenAndServe(conf.MetricsAddr, mux)
		}()
	}

	var active struct {
		sync.Mutex
		svcs map[*socks.SocksSvc]struct{}
	}
	active.svcs = make(map[*socks.SocksSvc]struct{})

	var g errgroup.Group
	g.Go(func() error {
		gPeer, err := rpc.NewPeer(ctx, &socks.SocksSvc{}, pb.NewSocksCliClient, pb.RegisterSocksSvcServer)
		if err != nil {
			return err
		}
		for {
			conn, err := listener.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					return nil
				}
				return err
			}
			go func(conn net.Conn) {
				svc := &socks.SocksSvc{RemoteAddr: conn.RemoteAddr().String(), LocalAddr: conn.LocalAddr().String()}
				peer, err := gPeer.Clone(ctx, conn, svc)
				if err != nil {
					_ = conn.Close()
					return
				}
				svc.Peer = peer
				active.Lock()
				active.svcs[svc] = struct{}{}
				active.Unlock()
			}(conn)
		}
	})
	g.Go(func() error {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				active.Lock()
				for svc := range active.svcs {
					if svc.Peer.IsClosed() {
						socks.SvcClosed(svc)
						delete(active.svcs, svc)
					}
				}
				active.Unlock()
			}
		}
	})

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	select {
	case s := <-done:
		log.Ctx(ctx).Info().Str("signal", s.String()).Msg("shutdown")
	case <-ctx.Done():
	}
	cancel()
	listener.Close()
	socks.CloseAllSessions()
	_ = g.Wait()
}
