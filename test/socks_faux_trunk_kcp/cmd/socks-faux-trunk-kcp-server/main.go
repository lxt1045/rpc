package main

import (
	"context"
	"expvar"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lxt1045/rpc/faux_tcp"
	socks "github.com/lxt1045/rpc/test/socks_faux_trunk_kcp"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/filesystem"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"
)

/*
$env:CGO_ENABLED=0; $env:GOOS="linux"; $env:GOARCH="amd64"; go build ./
*/
type Config struct {
	Debug bool
	Pprof bool
	Dev   bool
	Log   config.Log

	Token       string               `yaml:"token"`
	MaxClients  int                  `yaml:"max_clients"`
	Conn        socks.ConnConfig     `yaml:"conn"`
	TrunkKCP    socks.TrunkKCPConfig `yaml:"trunk_kcp"`
	FauxTCP     socks.FauxTCPConfig  `yaml:"faux_tcp"`
	TLS         socks.TLSConfig      `yaml:"tls"`
	ACL         socks.ACLConfig      `yaml:"acl"`
	MetricsAddr string               `yaml:"metrics_addr"`
}

func main() {
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
	log.Ctx(ctx).Info().Str("conf", confSource).Msg("config loaded")

	token := os.Getenv("SOCKS_TRUNK_TOKEN")
	if token == "" {
		token = conf.Token
	}
	if token == "" {
		if conf.Debug {
			token = "skdkfjjdklfkljdnmkjkl"
			log.Ctx(ctx).Warn().Caller().Msg("using dev-insecure-token")
		} else {
			log.Ctx(ctx).Error().Caller().Msg("server token is required")
			return
		}
	}

	srvCfg := &socks.ServerConfig{
		Token:      token,
		MaxClients: conf.MaxClients,
		Conn:       conf.Conn,
		Trunk:      conf.TrunkKCP,
		FauxTCP:    conf.FauxTCP,
		ACL:        conf.ACL,
	}
	if err := socks.ValidateServerConfig(srvCfg); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	socks.NormalizeTrunkKCPConfig(&srvCfg.Trunk)
	socks.SetServerTrunkConfig(srvCfg.Trunk)
	if err := socks.InitServerSecurity(srvCfg.Token, srvCfg.MaxClients, srvCfg.Trunk.Conv, srvCfg.Trunk.MaxVirtualConn); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	// 诊断模式：内核 TCP 监听同一端口并回显（对照 faux_tcp 失败时用）。
	// 用途：容器/端口映射环境下判断"回程是否通"——若内核 TCP 能连通而 faux_tcp
	// 不通，说明端口映射与回程对内核 TCP 有效，问题在 raw socket 回程（需要
	// --network host 或宿主/平台配合）。
	if os.Getenv("SOCKS_FAUX_PLAIN_LISTEN") != "" {
		ln, lerr := net.Listen("tcp", srvCfg.Conn.Addr)
		if lerr != nil {
			log.Ctx(ctx).Error().Caller().Err(lerr).Send()
			return
		}
		log.Ctx(ctx).Warn().Str("addr", srvCfg.Conn.Addr).
			Msg("PLAIN TCP 诊断监听已启动（内核 TCP，非 faux_tcp）：用于对照验证端口映射/回程")
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				log.Ctx(ctx).Error().Caller().Err(aerr).Send()
				return
			}
			log.Ctx(ctx).Info().Str("remote", c.RemoteAddr().String()).Msg("PLAIN TCP 连接已建立")
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(c)
		}
	}

	if err := socks.ConfigureACL(srvCfg.ACL); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	// 端到端 TLS（跑在 trunk_kcp VirtualConn 上）：控制通道与数据面共用。
	// 证书生成：go test -run TestMake ./test/cert，然后把 test/cert/ca/ 下的
	// root/server/client pem 拷到 filesystem/static/ca/（见 README）。
	tlsCfg, err := srvCfg.TLS.ServerTLS(filesystem.Static)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	if tlsCfg == nil {
		log.Ctx(ctx).Warn().Caller().Msg("tls.enabled=false：token 与代理数据明文传输（仅可信链路）")
	}
	socks.SetServerTLSConfig(tlsCfg)

	// faux_tcp 监听：AF_PACKET 收 + raw IP 发，需要 Linux + root/CAP_NET_RAW；
	// 内核 RST 抑制规则默认自动装拆（faux_tcp.manual_firewall: true 时改为手工维护）。
	listener, err := faux_tcp.Listen(ctx, srvCfg.FauxTCP.ToFauxTCP(), srvCfg.Conn.Addr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).
			Msg("faux_tcp listen failed: 需要 Linux + root/CAP_NET_RAW（iptables/nft 规则自动安装）")
		return
	}
	defer listener.Close()

	server, err := socks.NewServer(ctx)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

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

	log.Ctx(ctx).Info().Str("addr", srvCfg.Conn.Addr).
		Int("trunk_conns", srvCfg.Trunk.MaxConns).
		Msg("server started (faux_tcp transport)")

	var g errgroup.Group
	g.Go(func() error {
		err := server.Serve(listener)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("serve stopped")
		}
		return err
	})
	g.Go(func() error {
		server.RunReaper(10 * time.Second)
		return nil
	})

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	select {
	case s := <-done:
		log.Ctx(ctx).Info().Str("signal", s.String()).Msg("shutdown")
	case <-ctx.Done():
	}
	cancel()
	_ = listener.Close()
	server.Close()
	_ = g.Wait()
}
