package main

import (
	"context"
	"sync"
	"time"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/socks_udp/pb"
	"github.com/lxt1045/utils/log"
)

var (
	svcs = struct {
		m map[string]*service
		sync.RWMutex
	}{
		m: make(map[string]*service),
	}
)

func AddSvc(svc *service) {
	svcs.Lock()
	defer svcs.Unlock()
	svcs.m[svc.Name] = svc
}

type service struct {
	Name       string
	RemoteAddr string
	Network    pb.Network
	Peer       rpc.Peer
}

func (s *service) Close(ctx context.Context, req *pb.CloseReq) (resp *pb.CloseRsp, err error) {
	svcs.Lock()
	defer svcs.Unlock()
	delete(svcs.m, s.Name)
	return &pb.CloseRsp{}, nil
}

func (s *service) Auth(ctx context.Context, req *pb.AuthReq) (resp *pb.AuthRsp, err error) {
	s.Name = req.Name
	resp = &pb.AuthRsp{}
	svcs.Lock()
	defer svcs.Unlock()
	svcs.m[s.Name] = s
	return
}

func (s *service) Clients(ctx context.Context, req *pb.ClientsReq) (resp *pb.ClientsRsp, err error) {
	clients := func() []*pb.ClientInfo {
		m := make([]*pb.ClientInfo, 0, len(svcs.m))
		svcs.Lock()
		defer svcs.Unlock()
		for k, v := range svcs.m {
			if v.Peer.Client.Codec.IsClosed() {
				delete(svcs.m, k)
				continue
			}
			if s.Name == v.Name {
				continue
			}
			m = append(m, &pb.ClientInfo{
				Name:    v.Name,
				Addr:    v.RemoteAddr,
				Network: pb.Network_TCP,
			})
		}
		return m
	}()

	return &pb.ClientsRsp{Clients: clients}, nil
}

func (s *service) Latency(ctx context.Context, req *pb.LatencyReq) (resp *pb.LatencyRsp, err error) {
	return &pb.LatencyRsp{
		Ts: time.Now().UnixNano(),
	}, nil
}

func (s *service) ConnPeer(ctx context.Context, req *pb.ConnPeerReq) (resp *pb.ConnPeerRsp, err error) {
	target := func() *service {
		svcs.Lock()
		defer svcs.Unlock()
		return svcs.m[req.Client.Name]
	}()
	if target == nil {
		resp = &pb.ConnPeerRsp{
			Status: pb.ConnPeerRsp_Fail,
			Err: &pb.Err{
				Msg: req.Client.Name + " not exist",
			},
		}
		return
	}

	// 这里注意可以先探测一下网络延时
	latencys := getLatencys(ctx, target.Peer, s.Peer)
	for _, latency := range latencys {
		if latency.err != nil {
			log.Ctx(ctx).Error().Caller().Err(latency.err).Send()
			return
		}
		if latency.deta == 0 {
			log.Ctx(ctx).Error().Caller().Msgf("deta is %d", latency.deta)
			// return
		}
	}
	detaTar, detaSvs := latencys[0].deta, latencys[1].deta
	tsNow := time.Now().UnixNano() + int64(time.Second)
	tsTar, tsSvs := tsNow+detaTar, tsNow+detaSvs
	go func() {
		req := &pb.ConnPeerReq{
			Timestamp: tsTar,
			Client: &pb.ClientInfo{
				Name:    s.Name,
				Addr:    s.RemoteAddr,
				Network: s.Network,
			},
		}
		resp := &pb.ConnPeerRsp{}
		err := target.Peer.Invoke(ctx, "ConnPeer", req, resp)
		if err != nil {
			log.Ctx(ctx).Info().Caller().Err(err).Interface("resp", resp).Send()
			return
		}
		log.Ctx(ctx).Info().Caller().Interface("resp", resp).Send()
	}()

	resp = &pb.ConnPeerRsp{
		Timestamp: tsSvs,
		Client: &pb.ClientInfo{
			Name:    target.Name,
			Addr:    target.RemoteAddr,
			Network: target.Network,
		},
	}

	log.Ctx(ctx).Info().Caller().Interface("from", s.RemoteAddr).Interface("to", target.RemoteAddr).Send()
	return
}

type respLatency struct {
	deta int64
	err  error
}

// deta == 对方时钟 - 我方时钟； 用于对时
func getLatencys(ctx context.Context, peers ...rpc.Peer) (resps []respLatency) {

	resps = make([]respLatency, len(peers))
	wg := sync.WaitGroup{}

	for i := range peers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resps[i].deta, resps[i].err = getLatency(ctx, peers[i])
		}(i)
	}
	wg.Wait()
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
