package rpc

import (
	"context"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/lxt1045/errors"
	base "github.com/lxt1045/rpc/base"
	"github.com/lxt1045/rpc/conn"
	"github.com/lxt1045/rpc/test/filesystem"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/log"
	"github.com/quic-go/quic-go"
)

func TestQuic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	go func() {
		quicService(ctx, cancel, t, ch)
	}()

	<-ch
	time.Sleep(time.Second * 1)
	quicClient(ctx, cancel, t)
}

type Config struct {
	Debug      bool
	Pprof      bool
	Dev        bool
	Conn       config.Conn
	ClientConn config.Conn
	Log        config.Log
}

func quicService(ctx context.Context, cancel context.CancelFunc, t *testing.T, ch chan struct{}) {
	conf := &Config{}
	file := "static/conf/default.yml"
	bs, err := fs.ReadFile(filesystem.Static, file)
	if err != nil {
		err = errors.Errorf(err.Error())
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
	}
	err = config.Unmarshal(bs, conf)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
	}

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
	listener, err := quic.ListenAddr(conf.Conn.Addr, tlsConfig, nil)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(errors.Errorf(err.Error())).Send()
		return
	}
	defer listener.Close()

	// gPeer, err := StartService(ctx, nil, &server{Str: "test"}, base.RegisterHelloServer)
	gPeer, err := StartPeer(ctx, nil, &server{Str: "test"}, base.RegisterHelloServer)
	if err != nil {
		t.Fatal(err)
	}
	ch <- struct{}{}
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

		svc, err := conn.WrapQuic(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		zsvc, err := conn.NewZip(ctx, svc)
		if err != nil {
			t.Fatal(err)
		}

		_, err = gPeer.Clone(ctx, zsvc, &server{Str: "hello"})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func quicClient(ctx context.Context, cancel context.CancelFunc, t *testing.T) {
	// 解析配置文件
	conf := &Config{}
	file := "static/conf/default.yml"
	bs, err := fs.ReadFile(filesystem.Static, file)
	if err != nil {
		err = errors.Errorf(err.Error())
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
	}
	err = config.Unmarshal(bs, conf)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
	}

	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}

	cmtls := conf.ClientConn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, cmtls.ClientCert, cmtls.ClientKey, cmtls.CACert)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	tlsConfig.ServerName = conf.ClientConn.Host

	c, err := quic.DialAddr(ctx, conf.ClientConn.Addr, tlsConfig, nil)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	cli, err := conn.WrapQuicClient(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	zcli, err := conn.NewZip(ctx, cli)
	if err != nil {
		t.Fatal(err)
	}

	// client, err := StartClient(ctx, zcli, base.NewHelloClient)
	client, err := StartPeer(ctx, zcli, nil, base.NewHelloClient)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	req := base.HelloReq{
		Name: "call by quic 007",
	}
	resp := base.HelloRsp{}

	err = client.Invoke(ctx, "SayHello", &req, &resp)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("resp.Msg:\"%s\"", resp.Msg)
}
