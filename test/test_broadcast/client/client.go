package main

import (
	"context"
	"fmt"
	"io/fs"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/filesystem"
	"github.com/lxt1045/rpc/test/pb"
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
	Broadcast  config.Conn // 广播
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

	err = BroadcastService(ctx, conf)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	err = BroadcastClient(ctx, conf)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	// Client(ctx, conf)
	select {}
}

func Client(ctx context.Context, conf *Config) {
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

	cli := NewClient("")
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
	log.Ctx(ctx).Error().Caller().Interface("resp", resp).Send()

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
		log.Ctx(ctx).Error().Caller().Interface("resp", resp).Send()

		_, err = cli.ConnTo(ctx, &pb.ConnToReq{
			Client:    target,
			Timestamp: resp.Timestamp,
		})
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			return
		}
	}

	return
}

func BroadcastService(ctx context.Context, conf *Config) (err error) {
	// listener, err := net.ListenPacket("udp4", conf.Broadcast.Addr)
	listener, err := net.ListenPacket("udp4", ":18081")
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	go func() {
		message := make([]byte, 1024)
		for {
			n, src, err := listener.ReadFrom(message)
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
			log.Ctx(ctx).Debug().Caller().Str("src", src.String()).Msg(string(message[:n]))
		}
	}()
	return
}

func BroadcastClient(ctx context.Context, conf *Config) (err error) {
	/*
		// 这里设置发送者的IP地址，自己查看一下自己的IP自行设定
		laddr := net.UDPAddr{
			IP:   net.IPv4(192, 168, 137, 224),
			Port: 3000,
		}
		// 这里设置接收者的IP地址为广播地址
		raddr := net.UDPAddr{
			IP:   net.IPv4(255, 255, 255, 255),
			Port: 3000,
		}
		conn, err := net.DialUDP("udp", &laddr, &raddr)
	*/

	addrs := strings.Split(conf.Broadcast.Addr, ":")

	conn, err := net.Dial("udp4", "255.255.255.255:"+addrs[len(addrs)-1])
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}

	go func() {
		var ticker *time.Ticker
		bsMsg := []byte("hello world")

		ticker = time.NewTicker(time.Duration(conf.Broadcast.Heartbeat) * time.Second)
		defer ticker.Stop()
		for times := 500; ; {
			n, err := conn.Write(bsMsg)
			_ = n
			if err != nil || n != len(bsMsg) {
				log.Ctx(ctx).Error().Caller().Err(err).Msg("conn.Write errorx")
			} else {
				log.Ctx(ctx).Debug().Caller().Msg(string(bsMsg))
			}
			if times < 500 {
				// 启动后，快速发送多次，之后降为普通频率
				time.Sleep(time.Millisecond * 200)
				times++
				continue
			}
			<-ticker.C
		}
	}()

	return
}

type client struct {
	MyName     string
	RemoteAddr string
	Peer       rpc.Peer
	*pb.ClientInfo
}

func NewClient(remoteAddr string) *client {
	return &client{
		RemoteAddr: remoteAddr,
		// ClientInfo
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

	conn, err := socket.Dial(ctx, "tcp4", "", in.Client.Addr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("Dial failed")
		return
	}

	str := "Hello " + in.Client.Name
	_, err = conn.Write([]byte(str))
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("Dial failed")
		return
	}
	log.Ctx(ctx).Info().Caller().Str("read", str).
		Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()

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
