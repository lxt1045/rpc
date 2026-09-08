package socks

import (
	"context"
	"crypto/tls"
	stderr "errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/socks_trunk/socks_trunk/pb"
	"github.com/lxt1045/rpc/trunk"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
	"github.com/lxt1045/utils/socks/http/utils"
	"golang.org/x/sync/errgroup"
)

var _ pb.SocksCliServer = &SocksCli{}

type SocksCli struct {
	Name         string
	SocksAddr    string
	ChPeer       chan *Peer
	ChPeerReuser chan *Peer

	TlsConf  *tls.Config
	PeerAddr string

	Token    string
	TrunkCfg TrunkConfig

	trunk     *trunk.Trunk
	trunkPeer *Peer

	trunkID    uint16
	trunkCount int
	trunkMu    sync.Mutex

	trunkConnID  uint16
	trunkConnIDs [math.MaxInt16]bool
	lTrunkConnID sync.Mutex
}

type Peer struct {
	TsLast      int64
	LocalAddrs  string
	RemoteAddrs string
	rpc.Peer
}

func (p *SocksCli) GetPeer() (peer *Peer) {
	select {
	case peer = <-p.ChPeerReuser:
	default:
	}
	if peer != nil {
		if !peer.Peer.IsClosed() {
			log.Ctx(context.TODO()).Debug().Msg("reuser peer----------------------------")
			return
		}
		peer.Close(context.TODO())
	}
	peer = <-p.ChPeer
	// for tsLast := time.Now().Unix() - int64(time.Second*30); peer.TsLast < tsLast || peer.Peer.IsClosed(); {
	for peer.Peer.IsClosed() {
		peer.Close(context.TODO())
		peer = <-p.ChPeer
	}

	// var err error
	// for peer, err = p.GetConn(context.TODO()); peer == nil || err != nil; {
	// 	log.Ctx(context.TODO()).Error().Caller().Err(err).Send()
	// }
	return
}

func (p *SocksCli) authPeer(ctx context.Context, peer rpc.Peer) error {
	if p.Token == "" {
		return errors.New("client token is empty")
	}
	resp := &pb.AuthRsp{}
	if err := peer.Invoke(ctx, "Auth", &pb.AuthReq{Name: p.Token}, resp); err != nil {
		return err
	}
	if resp.Status != pb.AuthRsp_Succ {
		msg := "auth failed"
		if resp.Err != nil {
			msg = resp.Err.Msg
		}
		return errors.Errorf("%s", msg)
	}
	return nil
}

func (p *SocksCli) close(ctx context.Context) (err error) {
	closePeer := func(peer *Peer) {
		if peer == nil {
			return
		}
		err1 := peer.Close(ctx)
		if err1 != nil {
			err = err1
		}
	}
	drain := func(ch chan *Peer) {
		for {
			select {
			case peer, ok := <-ch:
				if !ok {
					return
				}
				closePeer(peer)
			default:
				return
			}
		}
	}
	drain(p.ChPeer)
	drain(p.ChPeerReuser)
	return
}

func (p *SocksCli) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = p.close(ctx)
	return &pb.CloseRsp{}, err
}

// Listen on addr and proxy to server to reach target from getAddr.
func (p *SocksCli) RunSocks(ctx context.Context, socksAddr string) {
	l, err := net.Listen("tcp", socksAddr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to listen on %s", socksAddr)
		return
	}
	defer l.Close()
	log.Ctx(ctx).Info().Caller().Str("socksAddr", socksAddr).Msg("SOCKS5 listening")

	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if stderr.Is(err, net.ErrClosed) {
				return
			}
			log.Ctx(ctx).Warn().Caller().Err(err).Msg("socks accept failed")
			continue
		}
		go func(conn net.Conn) {
			defer func() {
				if r := recover(); r != nil {
					log.Ctx(ctx).Error().Caller().Interface("panic", r).Stack().Msg("socks handler panic")
					conn.Close()
				}
			}()
			_ = p.connectSocks(ctx, conn)
		}(c)
	}
}

func (p *SocksCli) connectSocks(ctx context.Context, rc net.Conn) (err error) {
	rc.(*net.TCPConn).SetKeepAlive(true)
	tgtAddr, err := socks.Handshake(rc)
	if err != nil {
		// UDP: keep the connection until disconnect then free the UDP socket
		if err == socks.InfoUDPAssociate {
			buf := make([]byte, 1)
			// block here
			for {
				_, err = rc.Read(buf)
				if err, ok := err.(net.Error); ok && err.Timeout() {
					continue
				}
				log.Ctx(ctx).Error().Caller().Err(err).Msgf("UDP Associate End.")
				return
			}
		}

		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to get target address: %v", err)
		return
	}

	if p.trunk == nil {
		return errors.New("trunk is not ready")
	}
	return p.connectTrunk(ctx, tgtAddr.String(), rc)
}

func (p *SocksCli) RunHttpProxy(ctx context.Context, httpAddr string, mode int) {
	l, err := net.Listen("tcp", httpAddr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to listen on %s", httpAddr)
		return
	}
	defer l.Close()
	log.Ctx(ctx).Info().Caller().Str("httpAddr", httpAddr).Msg("HTTP proxy listening")

	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if stderr.Is(err, net.ErrClosed) {
				return
			}
			log.Ctx(ctx).Warn().Caller().Err(err).Msg("http accept failed")
			continue
		}
		go func(conn net.Conn) {
			defer func() {
				if r := recover(); r != nil {
					log.Ctx(ctx).Error().Caller().Interface("panic", r).Stack().Msg("http handler panic")
					conn.Close()
				}
			}()
			_ = p.connectHttp(ctx, conn, mode)
		}(c)
	}
}

func (p *SocksCli) connectHttp(ctx context.Context, inConn net.Conn, mode int) (err error) {
	// 上线只保留 Trunk 数据面（mode 3）。其它验证性 mode 不再使用。
	if mode != 3 {
		err = errors.Errorf("unsupported http proxy mode %d", mode)
		utils.CloseConn(&inConn)
		return
	}

	req, err := utils.NewHTTPRequest(&inConn, 4096, false, nil)
	if err != nil {
		if err != io.EOF {
			err = errors.Errorf("decoder error , form %s, ERR:%s", err, inConn.RemoteAddr())
			return
		}
		utils.CloseConn(&inConn)
		return
	}
	address := req.Host

	log.Ctx(ctx).Info().Str("address", address).Msg("use proxy")
	err = p.OutToTCPPeer2(ctx, address, inConn, &req)
	if err != nil {
		log.Ctx(ctx).Error().Str("address", address).Err(err).Msg("connect fail")
		utils.CloseConn(&inConn)
	}
	return
}

func (p *SocksCli) TrunkConn(ctx context.Context, trunkID uint16, n int) (conns []io.ReadWriteCloser, err error) {
	g := errgroup.Group{}
	l := sync.Mutex{}
	for i := range n {
		if i != 0 && i%10 == 0 {
			time.Sleep(time.Millisecond * 100)
		}
		func(i int) {
			g.Go(func() (err error) {
				conn, err := socket.DialTLSTimeout1(ctx, "tcp", p.PeerAddr, p.TlsConf, time.Second*3)
				if err != nil {
					return
				}
				log.Ctx(ctx).Info().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Msg("DialTLS OK")
				peer, err := rpc.NewPeer(ctx, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
				if err != nil {
					conn.Close()
					return
				}

				// peer.ClientUse(rpc.CliLogid)
				// peer.ServiceUse(rpc.SvcLogid)

				err = peer.Conn(ctx, conn)
				if err != nil {
					conn.Close()
					return
				}
				if err = p.authPeer(ctx, peer); err != nil {
					conn.Close()
					return
				}

				reqPeer := &pb.TrunkUpgradeReq{
					TrunkId:   uint32(trunkID),
					UpgradeId: uint32(i),
				}

				resp := &pb.TrunkUpgradeRsp{}
				upgrade, err := peer.Upgrade(ctx, "TrunkUpgrade", reqPeer, resp)
				if err != nil {
					return
				}

				l.Lock()
				defer l.Unlock()
				conns = append(conns, upgrade)
				return
			})
		}(i)
	}
	err = g.Wait()
	if err != nil {
		return
	}
	return
}

func (p *SocksCli) InitTrunk(ctx context.Context) (err error) {
	p.trunkMu.Lock()
	defer p.trunkMu.Unlock()
	return p.initTrunkLocked(ctx)
}

func (p *SocksCli) initTrunkLocked(ctx context.Context) (err error) {
	p.TrunkCfg.defaults()
	// Use a stable per-process random ID; the same ID is reused when rebuilding
	// the session after a trunk failure.
	if p.trunkID == 0 {
		p.trunkID = uint16(time.Now().UnixNano()&0x7fff) + uint16(10086)
	}
	if p.trunkCount <= 0 {
		p.trunkCount = p.TrunkCfg.MaxConns
	}
	if p.trunkCount <= 0 {
		p.trunkCount = p.TrunkCfg.MinConns
	}
	if p.trunkCount <= 0 {
		p.trunkCount = 32
	}

	conns, err := p.TrunkConn(ctx, p.trunkID, p.trunkCount)
	if err != nil {
		return
	}

	p.trunkPeer = p.GetPeer()

	reqPeer := &pb.TrunkStartReq{
		TrunkId:      uint32(p.trunkID),
		UpgradeCount: uint32(p.trunkCount),
	}

	resp := &pb.TrunkStartRsp{}
	err = p.trunkPeer.Invoke(ctx, "TrunkStart", reqPeer, resp)
	if err != nil {
		return
	}

	trunk0 := trunk.NewTrunk(conns...)
	p.trunk = trunk0
	go trunk0.Run(ctx)
	return nil
}

// RebuildTrunk closes a broken trunk and establishes a new one using the same
// session/trunk id. It is a best-effort production recovery mechanism until
// the underlying trunk library supports live AddConn/RemoveConn.
func (p *SocksCli) RebuildTrunk(ctx context.Context) (err error) {
	p.trunkMu.Lock()
	defer p.trunkMu.Unlock()

	old := p.trunk
	if old != nil && !old.IsClosed() {
		return nil
	}
	if old != nil {
		_ = old.Close()
		p.trunk = nil
	}

	log.Ctx(ctx).Warn().Caller().Msg("rebuilding trunk")
	if p.trunkPeer == nil || p.trunkPeer.IsClosed() {
		p.trunkPeer = p.GetPeer()
	}
	return p.initTrunkLocked(ctx)
}

// MaintainTrunk periodically checks trunk health and rebuilds it if needed.
func (p *SocksCli) MaintainTrunk(ctx context.Context) {
	if p.TrunkCfg.HealthCheckSec <= 0 {
		p.TrunkCfg.HealthCheckSec = 10
	}
	ticker := time.NewTicker(time.Duration(p.TrunkCfg.HealthCheckSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.trunk == nil || p.trunk.IsClosed() {
				if err := p.RebuildTrunk(ctx); err != nil {
					log.Ctx(ctx).Warn().Caller().Err(err).Msg("trunk rebuild failed")
				}
			}
		}
	}
}

func (p *SocksCli) trunkDo(connID uint16, data []byte) (err error) {
	req := &pb.TrunkStartData{}
	err = proto.Unmarshal(data, req)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	ctx := context.TODO()
	log.Ctx(ctx).Info().Caller().Err(err).Msgf("trunkDo: %+v", req)
	return
}

func (p *SocksCli) ResetTrunkConnID(connID uint16) {
	p.lTrunkConnID.Lock()
	defer p.lTrunkConnID.Unlock()
	p.trunkConnIDs[p.trunkConnID] = false
}
func (p *SocksCli) GetTrunkConnID() (connID uint16, err error) {
	p.lTrunkConnID.Lock()
	defer p.lTrunkConnID.Unlock()
	for range len(p.trunkConnIDs) {
		if p.trunkConnIDs[p.trunkConnID] {
			p.trunkConnID = (p.trunkConnID + 1) & 0x7f
			continue
		}
		connID = p.trunkConnID
		p.trunkConnID = (p.trunkConnID + 1) & 0x7f
		return
	}
	err = errors.New("[GetTrunkConnID] connID has been exhausted")
	return
}

var (
	bufPool = sync.Pool{
		New: func() any {
			return make([]byte, math.MaxUint16)
		},
	}
)

func (p *SocksCli) OutToTCPPeer2(ctx context.Context, address string, inConn net.Conn, req *utils.HTTPRequest) (err error) {
	inAddr := inConn.RemoteAddr().String()
	inLocalAddr := inConn.LocalAddr().String()
	//防止死循环
	if p.IsDeadLoop(inLocalAddr, req.Host) {
		utils.CloseConn(&inConn)
		err = fmt.Errorf("dead loop detected , %s", req.Host)
		return
	}
	connID, err := p.GetTrunkConnID()
	if err != nil {
		return
	}
	defer p.ResetTrunkConnID(connID)
	conn := p.trunk.GetConn(connID)
	if conn == nil {
		return errors.Errorf("trunk conn %d is nil", connID)
	}
	metricActiveConnsAdd(1)
	defer metricActiveConnsAdd(-1)

	reqPeer := pb.TrunkStartData{
		Addr: address,
	}
	if !req.IsHTTPS() {
		reqPeer.Body = req.HeadBuf
	}
	wbuf := bufPool.Get().([]byte)
	buf := proto.NewBuffer(wbuf[:0]) //
	err = buf.Marshal(&reqPeer)
	if err != nil {
		bufPool.Put(wbuf)
		return
	}
	err = conn.SendEvent(buf.Bytes())
	bufPool.Put(wbuf)
	if err != nil {
		return
	}
	if req.IsHTTPS() {
		req.HTTPSReply() // http 回复建立连接
	}
	log.Ctx(ctx).Info().Str("inAddr", inAddr).Str("inLocalAddr", inLocalAddr).Str("host", req.Host).Msg("conn connected")

	// 两个方向的拷贝：浏览器->upgrade / upgrade->浏览器。
	// 任一方向结束都应唤醒另一方向，避免 goroutine 泄漏。
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("OutToTCPPeer in->up")
			}
			// 唤醒阻塞在 (*inConn).Read 的协程
			//if c := *inConn; c != nil {
			//	_ = c.SetDeadline(time.Now())
			//}
			//upgrade.Close()
		}()
		Copy(ctx, conn, inConn)
	}()

	defer func() {
		if e := recover(); e != nil {
			log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("OutToTCPPeer up->in")
		}
		conn.Close()
		utils.CloseConn(&inConn)
		// upgrade 场景下连接会被 RPC 接管，单个 peer 只能服务一个 upgrade；
		// 结束后必须关闭 peer，否则复用会失败
		log.Ctx(ctx).Info().Str("inAddr", inAddr).Str("inLocalAddr", inLocalAddr).Str("host", req.Host).Msg("conn closed")
	}()
	Copy(ctx, inConn, conn)
	return
}

func (p *SocksCli) IsDeadLoop(inLocalAddr string, host string) bool {
	inIP, inPort, err := net.SplitHostPort(inLocalAddr)
	if err != nil {
		return false
	}
	outDomain, outPort, err := net.SplitHostPort(host)
	if err != nil {
		return false
	}
	if inPort == outPort {
		var outIPs []net.IP
		outIPs, err = net.LookupIP(outDomain)
		if err == nil {
			for _, ip := range outIPs {
				if ip.String() == inIP {
					return true
				}
			}
		}
		interfaceIPs, err := utils.GetAllInterfaceAddr()
		if err == nil {
			for _, localIP := range interfaceIPs {
				for _, outIP := range outIPs {
					if localIP.Equal(outIP) {
						return true
					}
				}
			}
		}
	}
	return false
}

// connectTrunk proxies a local TCP connection over the aggregated Trunk.
// It is the production data path used by SOCKS5 TCP; HTTP CONNECT uses
// OutToTCPPeer2, which follows the same wire protocol.
func (p *SocksCli) connectTrunk(ctx context.Context, tgtAddr string, rc net.Conn) (err error) {
	if rc == nil {
		return errors.New("nil local conn")
	}
	if p.trunk == nil {
		return errors.New("trunk is not ready")
	}
	rc.(*net.TCPConn).SetKeepAlive(true)
	connID, err := p.GetTrunkConnID()
	if err != nil {
		return err
	}
	defer p.ResetTrunkConnID(connID)
	conn := p.trunk.GetConn(connID)
	if conn == nil {
		return errors.Errorf("trunk conn %d is nil", connID)
	}
	metricActiveConnsAdd(1)
	defer metricActiveConnsAdd(-1)

	reqPeer := pb.TrunkStartData{Addr: tgtAddr}
	wbuf := bufPool.Get().([]byte)
	buf := proto.NewBuffer(wbuf[:0])
	err = buf.Marshal(&reqPeer)
	if err != nil {
		bufPool.Put(wbuf)
		return
	}
	err = conn.SendEvent(buf.Bytes())
	bufPool.Put(wbuf)
	if err != nil {
		return
	}

	log.Ctx(ctx).Debug().Str("addr", tgtAddr).Str("local", rc.RemoteAddr().String()).Msg("trunk conn connected")

	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("connectTrunk in->trunk")
			}
			conn.Close()
			rc.Close()
		}()
		Copy(ctx, conn, rc)
	}()

	defer func() {
		if e := recover(); e != nil {
			err = errors.Errorf("recover : %v", e)
		}
		conn.Close()
		rc.Close()
	}()
	Copy(ctx, rc, conn)
	return
}

func (p *SocksCli) RunConnLoop(ctx context.Context, cancel context.CancelFunc, addr string, tlsConfig *tls.Config) {
	var err error
	defer func() {
		e := recover()
		cancel()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Info().Caller().Str("addr", addr).Msg("ctx.Done")
			return
		default:
		}
		// conn, err := tls.Dial("tcp", conf.ClientConn.Addr, tlsConfig)
		// conn, err := socket.DialTLS(ctx, "tcp", addr, tlsConfig)
		// conn, err := socket.DialTLSTimeout(ctx, "tcp", addr, tlsConfig, time.Second*3)
		conn, err := socket.DialTLSTimeout1(ctx, "tcp", addr, tlsConfig, time.Second*3)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			time.Sleep(time.Millisecond * 1000)
			continue
		}
		log.Ctx(ctx).Info().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Msg("DialTLS OK")
		peer, err1 := rpc.NewPeer(ctx, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
		if err1 != nil {
			err = err1
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			conn.Close()
			continue
		}

		// peer.ClientUse(rpc.CliLogid)
		// peer.ServiceUse(rpc.SvcLogid)

		err = peer.Conn(ctx, conn)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			conn.Close()
			continue
		}
		if err = p.authPeer(ctx, peer); err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			conn.Close()
			continue
		}
		p.ChPeer <- &Peer{
			TsLast:      time.Now().Unix(),
			Peer:        peer,
			LocalAddrs:  conn.LocalAddr().String(),
			RemoteAddrs: conn.RemoteAddr().String(),
		}
	}
}

func (p *SocksCli) GetConn(ctx context.Context) (out *Peer, err error) {
	// conn, err := tls.Dial("tcp", conf.ClientConn.Addr, tlsConfig)
	// conn, err := socket.DialTLS(ctx, "tcp", p.PeerAddr, p.TlsConf)
	conn, err := socket.DialTLSTimeout(ctx, "tcp", p.PeerAddr, p.TlsConf, time.Second)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		time.Sleep(time.Millisecond * 1000)
		return
	}
	log.Ctx(ctx).Info().Caller().Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()
	peer, err := rpc.StartPeer(ctx, conn, p, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	if err = p.authPeer(ctx, peer); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		peer.Close(ctx)
		return
	}
	out = &Peer{
		TsLast:      time.Now().Unix(),
		Peer:        peer,
		LocalAddrs:  conn.LocalAddr().String(),
		RemoteAddrs: conn.RemoteAddr().String(),
	}
	return
}
