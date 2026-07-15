package main

import (
	"context"
	"fmt"
	"io/fs"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/nat/filesystem"
	"github.com/lxt1045/rpc/test/nat/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
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
	bs, err := fs.ReadFile(filesystem.Static, file)
	if err != nil {
		err = errors.Errorf(err.Error())
		log.Ctx(ctx).Error().Caller().Err(err).Send()
	}
	err = config.Unmarshal(bs, conf)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
	}

	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	cmtls := conf.ClientConn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, cmtls.ClientCert, cmtls.ClientKey, cmtls.CACert)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	tlsConfig.ServerName = conf.ClientConn.Host

	// conn, err := tls.Dial("tcp", conf.ClientConn.Addr, tlsConfig)
	conn, err := socket.DialTLS(ctx, "tcp", conf.ClientConn.Addr, tlsConfig)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	cli := NewClient(conn.LocalAddr().String(), conn.RemoteAddr().String())
	cli.Peer, err = rpc.StartPeer(ctx, conn, cli, pb.RegisterClientServer, pb.NewServiceClient)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	req := &pb.ClientsReq{
		MyName: "client-" + strconv.Itoa(int(time.Now().UnixNano())),
	}
	resp := &pb.ClientsRsp{}
	err = cli.Peer.Invoke(ctx, "Clients", req, resp)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	log.Ctx(ctx).Info().Caller().Interface("resp", resp).Send()

	addrs := strings.Split(conn.LocalAddr().String(), ":")
	go func(addr string) {
		ln, err := socket.Listen(ctx, "tcp4", addr)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
			return
		}

		buf := make([]byte, 1024)
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("accept failed", err)
				continue
			}
			for {
				n, err := conn.Read(buf)
				if err != nil {
					fmt.Println("read failed", err)
					break
				}
				log.Ctx(ctx).Info().Caller().Str("read", string(buf[:n])).
					Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()
			}
		}
	}(":" + addrs[len(addrs)-1])

	if len(resp.Clients) > 0 {
		target := resp.Clients[0]
		req := &pb.ConnToReq{
			Client: target,
		}
		resp := &pb.ConnToRsp{}
		err = cli.Peer.Invoke(ctx, "ConnTo", req, resp)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
		log.Ctx(ctx).Info().Caller().Interface("resp", resp).Send()

		_, err = cli.ConnTo(ctx, &pb.ConnToReq{
			Client:    target,
			Timestamp: resp.Timestamp,
		})
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
	}

	select {}
	return
}

type client struct {
	MyName     string
	RemoteAddr string
	LocalAddr  string
	Peer       rpc.Peer
	*pb.ClientInfo
}

func NewClient(localAddr, remoteAddr string) *client {
	return &client{
		RemoteAddr: remoteAddr,
		LocalAddr:  localAddr,
	}
}

func (s *client) Latency(ctx context.Context, in *pb.LatencyReq) (out *pb.LatencyRsp, err error) {
	return &pb.LatencyRsp{
		Ts: time.Now().UnixNano(),
	}, nil
}
func (s *client) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	return &pb.CloseRsp{}, nil
}
func (s *client) ConnTo(ctx context.Context, in *pb.ConnToReq) (out *pb.ConnToRsp, err error) {
	// 先 listen

	// 再 连接
	tsDo := in.Timestamp
	tsNow := time.Now().UnixNano()
	if tsDo < tsNow {
		err = errors.Errorf("tsDo: %d, tsNow: %d, time out", tsDo, tsNow)
		return
	}

	tsDiff := tsNow - tsDo - int64(time.Millisecond)*1
	time.Sleep(time.Duration(tsDiff))
	for {
		tsNow := time.Now().UnixNano()
		if tsDo <= tsNow {
			break
		}
		runtime.Gosched()
	}

	conn, err := socket.Dial(ctx, "tcp4", s.LocalAddr, in.Client.Addr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("Dial failed")
		return
	}

	go func() {
		for i := 0; i < 10; i++ {
			str := "Hello " + in.Client.Name + ", time: " + time.Now().Format(time.RFC3339)
			_, err = conn.Write([]byte(str))
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Msg("Dial failed")
				return
			}
			log.Ctx(ctx).Info().Caller().Str("read", str).
				Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()
			time.Sleep(time.Second * 1)
		}
	}()
	out = &pb.ConnToRsp{
		Status: pb.ConnToRsp_Succ,
	}
	return
}

// deta == 对方时钟 - 我方时钟； 用于对时
func getLatency(ctx context.Context, peer rpc.Peer) (deta int64, err error) {
	reqLatency := &pb.LatencyReq{
		Ts: time.Now().UnixNano(),
	}
	respLatency := &pb.LatencyRsp{}

	err = peer.Invoke(ctx, "Latency", reqLatency, respLatency)
	if err != nil {
		return
	}
	tsNow := time.Now().UnixNano()

	half := (tsNow + reqLatency.Ts) / 2
	deta = respLatency.Ts - half
	return
}
