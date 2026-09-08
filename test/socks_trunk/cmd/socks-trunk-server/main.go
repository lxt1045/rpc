package main

import (
	"context"
	"crypto/tls"
	"expvar"
	"flag"
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
	socks "github.com/lxt1045/rpc/test/socks_trunk/socks_trunk"
	"github.com/lxt1045/rpc/test/socks_trunk/socks_trunk/filesystem"
	"github.com/lxt1045/rpc/test/socks_trunk/socks_trunk/pb"
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

	Token             string          `mapstructure:"token"`
	MaxClients        int             `mapstructure:"max_clients"`
	MaxConnsPerClient int             `mapstructure:"max_conns_per_client"`
	MetricsAddr       string          `mapstructure:"metrics_addr"`
	ACL               socks.ACLConfig `mapstructure:"acl"`
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "static/conf/default.yml", "config file path")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = log.WithLogid(ctx, gid.New())

	conf := &Config{}
	if err := loadServerConfig(configPath, conf); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Str("config", configPath).Send()
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
			log.Ctx(ctx).Warn().Caller().Msg("using dev-insecure-token because debug=true")
		} else {
			log.Ctx(ctx).Error().Caller().Msg("server token is required")
			return
		}
	}
	if err := socks.InitServerSecurity(token, conf.MaxClients, conf.MaxConnsPerClient); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	if err := socks.ConfigureACL(conf.ACL); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	tlsConfig, err := loadServerTLS(conf.Conn.TLS.CACert, conf.Conn.TLS.ServerCert, conf.Conn.TLS.ServerKey)
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
	log.Ctx(ctx).Info().Caller().Str("addr", conf.Conn.Addr).Msg("started")

	var active struct {
		sync.Mutex
		svcs map[*socks.SocksSvc]struct{}
	}
	active.svcs = make(map[*socks.SocksSvc]struct{})

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
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				_, _ = w.Write([]byte("ok"))
			})
			log.Ctx(ctx).Info().Str("addr", conf.MetricsAddr).Msg("metrics server listening")
			if err := http.ListenAndServe(conf.MetricsAddr, mux); err != nil && err != http.ErrServerClosed {
				log.Ctx(ctx).Warn().Caller().Err(err).Msg("metrics server stopped")
			}
		}()
	}

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
				log.Ctx(ctx).Warn().Caller().Err(err).Msg("accept failed")
				return err
			}
			go func(conn net.Conn) {
				svc := &socks.SocksSvc{
					RemoteAddr: conn.RemoteAddr().String(),
					LocalAddr:  conn.LocalAddr().String(),
				}
				peer, err := gPeer.Clone(ctx, conn, svc)
				if err != nil {
					log.Ctx(ctx).Warn().Caller().Err(err).Msg("clone failed")
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
		log.Ctx(ctx).Info().Caller().Str("signal", s.String()).Msg("shutdown")
	case <-ctx.Done():
	}
	cancel()
	listener.Close()
	socks.CloseAllSessions()
	if err := g.Wait(); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
	}
}

// loadServerConfig supports both an external production config file and the
// embedded demo config that was used by the original demo binaries.
func loadServerConfig(path string, conf interface{}) error {
	if path == "static/conf/default.yml" {
		if err := config.UnmarshalFS(path, filesystem.Static, conf); err == nil {
			return nil
		}
	}
	return socks.LoadConfig(path, conf)
}

func loadServerTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	if _, err := os.Stat(certFile); err == nil {
		return socks.LoadTLSConfigFromFiles(caFile, certFile, keyFile)
	}
	return config.LoadTLSConfig(filesystem.Static, certFile, keyFile, caFile)
}
