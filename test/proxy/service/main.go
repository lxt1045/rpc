package main

import (
	"context"
	"crypto/tls"
	"net"
	"os/signal"
	"syscall"

	"github.com/lxt1045/rpc"
	proxy "github.com/lxt1045/rpc/test/proxy"
	"github.com/lxt1045/rpc/test/proxy/filesystem"
	"github.com/lxt1045/rpc/test/proxy/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
)

type Config struct {
	Conn config.Conn
	Log  config.Log
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()
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

	tlsConf := conf.Conn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, tlsConf.ServerCert, tlsConf.ServerKey, tlsConf.CACert)
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
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	peerTemplate, err := rpc.NewPeer(ctx, &proxy.SocksSvc{}, pb.NewSocksCliClient, pb.RegisterSocksSvcServer)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	log.Ctx(ctx).Info().Caller().Str("listen", conf.Conn.Addr).Send()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				log.Ctx(ctx).Error().Caller().Err(err).Msg("accept")
			}
			return
		}
		proxy.ConfigureTCPConn(conn)
		go serveConn(ctx, peerTemplate, conn)
	}
}

func serveConn(ctx context.Context, peerTemplate rpc.Peer, conn net.Conn) {
	cloneDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-cloneDone:
		}
	}()
	svc := &proxy.SocksSvc{}
	peer, err := peerTemplate.Clone(ctx, conn, svc)
	close(cloneDone)
	if err != nil {
		_ = conn.Close()
		log.Ctx(ctx).Warn().Caller().Err(err).Send()
		return
	}
	svc.Peer = peer
}
