package main

import (
	"context"
	"crypto/tls"
	"os"
	"os/signal"
	"syscall"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/tcp/filesystem"
	"github.com/lxt1045/rpc/test/tcp/pb"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = log.WithLogid(ctx, gid.GetGID())

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

	cli := &socksCli{
		Name:   "test1",
		chPeer: make(chan *Peer, 20),
	}
	var _ pb.ClientServer = cli

	conn, err := tls.Dial("tcp", conf.ClientConn.Addr, tlsConfig)
	// conn, err := socket.DialTLS(ctx, "tcp", conf.ClientConn.Addr, tlsConfig)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	log.Ctx(ctx).Info().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()

	peer, err1 := rpc.StartPeer(ctx, conn, cli, pb.RegisterClientServer, pb.NewServiceClient)
	if err1 != nil {
		err = err1
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	// cli.chPeer <- &Peer{
	// 	Peer:        peer,
	// 	LocalAddrs:  conn.LocalAddr().String(),
	// 	RemoteAddrs: conn.RemoteAddr().String(),
	// }

	req := &pb.HelloReq{
		Name: cli.Name,
	}
	resp := &pb.HelloRsp{}
	// stream, err := peer.StreamAsync(ctx, "Conn")
	err = peer.Invoke(ctx, "Hello", req, resp)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	log.Ctx(ctx).Debug().Caller().Interface("resp", resp).Msg("Hello")
	//
	{
		req := &pb.AuthReq{
			Name: cli.Name,
		}
		resp := &pb.AuthRsp{}
		// stream, err := peer.StreamAsync(ctx, "Conn")
		err = peer.Invoke(ctx, "Auth", req, resp)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			// return
		}
		log.Ctx(ctx).Debug().Caller().Interface("resp", resp).Msg("Hello")
	}

	//

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
	case <-peer.Done():
	}
}
