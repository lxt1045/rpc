package main

import (
	"context"
	"crypto/tls"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/nat/filesystem"
	"github.com/lxt1045/rpc/test/nat/pb"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
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
	bs, err := fs.ReadFile(filesystem.Static, file)
	if err != nil {
		err = errors.Errorf(err.Error())
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
	}
	err = config.Unmarshal(bs, conf)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
	}

	cmtls := conf.Conn.TLS
	tlsConfig, err := config.LoadTLSConfig(filesystem.Static, cmtls.ServerCert, cmtls.ServerKey, cmtls.CACert)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	listener, err := tls.Listen("tcp", conf.Conn.TCP, tlsConfig)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	defer listener.Close()
	log.Ctx(ctx).Info().Caller().Str("Listen", conf.Conn.TCP).Send()

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		ctx := context.TODO()
		conn, err := listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				log.Ctx(ctx).Error().Caller().Err(errors.Errorf(err.Error())).Send()
				// panic(err)
			} else {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
			continue
		}

		svc := NewServer(conn.RemoteAddr().String())
		svc.Peer, err = rpc.StartPeer(ctx, conn, svc, pb.NewClientClient, pb.RegisterServiceServer)
		if err != nil {
			log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		}
		continue
	}
}

var (
	clients     = make(map[string]*server)
	clientsLock sync.Mutex
)

type server struct {
	MyName     string
	RemoteAddr string
	Network    pb.Network
	Peer       rpc.Peer
}

func NewServer(remoteAddr string) *server {
	return &server{
		RemoteAddr: remoteAddr,
		// ClientInfo
	}
}

func (s *server) Latency(ctx context.Context, in *pb.LatencyReq) (out *pb.LatencyRsp, err error) {
	return &pb.LatencyRsp{
		Ts: time.Now().UnixNano(),
	}, nil
}
func (s *server) Auth(ctx context.Context, in *pb.AuthReq) (out *pb.AuthRsp, err error) {
	return &pb.AuthRsp{}, nil
}
func (s *server) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	clientsLock.Lock()
	defer clientsLock.Unlock()
	delete(clients, s.MyName)
	return &pb.CloseRsp{}, nil
}
func (s *server) Clients(ctx context.Context, in *pb.ClientsReq) (out *pb.ClientsRsp, err error) {
	s.MyName = in.MyName

	clients := func() []*pb.ClientInfo {
		clientsLock.Lock()
		defer clientsLock.Unlock()
		m := make([]*pb.ClientInfo, 0, len(clients))
		for k, v := range clients {
			if v.Peer.Client.Codec.IsClosed() {
				delete(clients, k)
				continue
			}
			m = append(m, &pb.ClientInfo{
				Name:    v.MyName,
				Addr:    v.RemoteAddr,
				Network: pb.Network_TCP,
			})
		}
		clients[s.MyName] = s
		return m
	}()

	return &pb.ClientsRsp{Clients: clients}, nil
}

func (s *server) ConnTo(ctx context.Context, in *pb.ConnToReq) (out *pb.ConnToRsp, err error) {
	target := func() *server {
		clientsLock.Lock()
		defer clientsLock.Unlock()
		return clients[in.Client.Name]
	}()
	if target == nil {
		out = &pb.ConnToRsp{
			Status: pb.ConnToRsp_Fail,
			Err: &pb.Err{
				Msg: in.Client.Name + " not exist",
			},
		}
		return
	}

	// 这里注意可以先探测一下网络延时
	detaTar, err := getLatency(ctx, target.Peer)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	detaSvs, err := getLatency(ctx, s.Peer)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	tsNow := time.Now().UnixNano() + int64(time.Second)
	tsTar, tsSvs := tsNow+detaTar, tsNow+detaSvs
	go func() {
		req := &pb.ConnToReq{
			Timestamp: tsTar,
			Client: &pb.ClientInfo{
				Name:    s.MyName,
				Addr:    s.RemoteAddr,
				Network: s.Network,
			},
		}
		resp := &pb.ConnToRsp{}
		err := target.Peer.Invoke(ctx, "ConnTo", req, resp)
		if err != nil {
			return
		}
		log.Ctx(ctx).Info().Caller().Interface("resp", resp).Send()
	}()

	out = &pb.ConnToRsp{
		Timestamp: tsSvs,
		Client: &pb.ClientInfo{
			Name:    target.MyName,
			Addr:    target.RemoteAddr,
			Network: target.Network,
		},
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
