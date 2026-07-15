package socks

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/socks/pb"
	"github.com/lxt1045/utils/log"
	"github.com/lxt1045/utils/socks"
	"github.com/lxt1045/utils/socks/http/utils"
)

type SocksCli struct {
	Name         string
	SocksAddr    string
	ChPeer       chan *Peer
	ChPeerReuser chan *Peer

	TlsConf  *tls.Config
	PeerAddr string
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

func (p *SocksCli) close(ctx context.Context) (err error) {
	for peer := range p.ChPeer {
		err1 := peer.Close(ctx)
		if err1 != nil {
			err = err1
		}
	}
	for peer := range p.ChPeerReuser {
		err1 := peer.Close(ctx)
		if err1 != nil {
			err = err1
		}
	}
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
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to listen on %s: %v", socksAddr, err)
		return
	}
	defer func() {
		e := recover()
		if e != nil {
			log.Ctx(ctx).Error().Caller().Interface("recover", e).Stack().Msg("listener defer")
		}
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Stack().Msg("listener defer")
		} else {
			log.Ctx(ctx).Info().Caller().Err(errors.Errorf("listener")).Stack().Msg("listener defer")
		}
		l.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Info().Caller().Str("socksAddr", socksAddr).Msg("Done")
			return
		default:
		}
		c, err := l.Accept()
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("failed to accept")
			continue
		}

		go p.connectSocks(ctx, c)
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

	return p.connect(ctx, tgtAddr.String(), rc)
}

func (p *SocksCli) RunHttpProxy(ctx context.Context, httpAddr string, mode int) {
	l, err := net.Listen("tcp", httpAddr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to listen on %s: %v", httpAddr, err)
		return
	}
	defer func() {
		e := recover()
		if e != nil {
			log.Ctx(ctx).Error().Caller().Interface("recover", e).Stack().Msg("listener defer")
		}
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Stack().Msg("listener defer")
		} else {
			log.Ctx(ctx).Info().Caller().Err(errors.Errorf("listener")).Stack().Msg("listener defer")
		}
		l.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Info().Caller().Str("httpAddr", httpAddr).Msg("Done")
			return
		default:
		}
		c, err := l.Accept()
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("failed to accept")
			continue
		}

		go p.connectHttp(ctx, c, mode)
	}
}

func (p *SocksCli) RunHttpProxyLocal(ctx context.Context, httpAddr string, mode int) {
	l, err := net.Listen("tcp", httpAddr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to listen on %s: %v", httpAddr, err)
		return
	}
	defer func() {
		e := recover()
		if e != nil {
			log.Ctx(ctx).Error().Caller().Interface("recover", e).Stack().Msg("listener defer")
		}
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Stack().Msg("listener defer")
		} else {
			log.Ctx(ctx).Info().Caller().Err(errors.Errorf("listener")).Stack().Msg("listener defer")
		}
		l.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Info().Caller().Str("httpAddr", httpAddr).Msg("Done")
			return
		default:
		}
		c, err := l.Accept()
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("failed to accept")
			continue
		}

		go p.connectHttp(ctx, c, mode)
	}
}

func (p *SocksCli) connectHttp(ctx context.Context, inConn net.Conn, mode int) (err error) {
	// rc.(*net.TCPConn).SetKeepAlive(true)

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
	//os.Exit(0)
	switch mode {
	case 0:
		err = p.OutToTCPPeer(ctx, address, &inConn, &req)
		if err != nil {
			log.Ctx(ctx).Error().Str("address", address).Err(err).Msg("connect fail")

			utils.CloseConn(&inConn)
		}
	case 1:
		err = p.OutToTCPPeer1(ctx, address, inConn, &req)
		if err != nil {
			log.Ctx(ctx).Error().Str("address", address).Err(err).Msg("connect fail")

			utils.CloseConn(&inConn)
		}
	case 2:
		err = p.OutToTCPLocal(ctx, address, &inConn, &req)
		if err != nil {
			log.Ctx(ctx).Error().Str("address", address).Err(err).Msg("connect fail")

			utils.CloseConn(&inConn)
		}
	}

	// return p.connect(ctx, tgtAddr.String(), rc)
	return
}

func (p *SocksCli) OutToTCPPeer(ctx context.Context, address string, inConn *net.Conn, req *utils.HTTPRequest) (err error) {
	inAddr := (*inConn).RemoteAddr().String()
	inLocalAddr := (*inConn).LocalAddr().String()
	//防止死循环
	if p.IsDeadLoop(inLocalAddr, req.Host) {
		utils.CloseConn(inConn)
		err = fmt.Errorf("dead loop detected , %s", req.Host)
		return
	}
	// var outConn net.Conn
	// outConn, err = utils.ConnectHost(address, 10*1000)

	peer := p.GetPeer()

	reqPeer := &pb.ConnUpgradeReq{
		Addr: address,
		// Body: buf,
	}
	if !req.IsHTTPS() {
		reqPeer.Body = req.HeadBuf
	}

	resp := &pb.ConnUpgradeRsp{}
	// stream, err := peer.StreamAsync(ctx, "Conn")
	upgrade, err := peer.Upgrade(ctx, "ConnUpgrade", reqPeer, resp)
	if err != nil {
		err = errors.Errorf("connect to %s , err:%s", address, err)
		utils.CloseConn(inConn)
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
		Copy(ctx, upgrade, *inConn)
	}()

	defer func() {
		if e := recover(); e != nil {
			log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("OutToTCPPeer up->in")
		}
		upgrade.Close()
		utils.CloseConn(inConn)
		// upgrade 场景下连接会被 RPC 接管，单个 peer 只能服务一个 upgrade；
		// 结束后必须关闭 peer，否则复用会失败
		peer.Close(context.TODO())
		log.Ctx(ctx).Info().Str("inAddr", inAddr).Str("inLocalAddr", inLocalAddr).Str("host", req.Host).Msg("conn closed")
	}()
	Copy(ctx, *inConn, upgrade)
	return
}

func (p *SocksCli) OutToTCPPeer1(ctx context.Context, address string, inConn net.Conn, req *utils.HTTPRequest) (err error) {
	inAddr := inConn.RemoteAddr().String()
	inLocalAddr := inConn.LocalAddr().String()
	//防止死循环
	if p.IsDeadLoop(inLocalAddr, req.Host) {
		utils.CloseConn(&inConn)
		err = fmt.Errorf("dead loop detected , %s", req.Host)
		return
	}
	// var outConn net.Conn
	// outConn, err = utils.ConnectHost(address, 10*1000)

	peer := p.GetPeer()

	stream, err := peer.StreamAsync(ctx, "Conn")
	if err != nil {
		err = errors.Errorf("connect to %s , err:%s", address, err)
		utils.CloseConn(&inConn)
		return
	}

	if req.IsHTTPS() {
		req.HTTPSReply() // http 回复建立连接
	}
	log.Ctx(ctx).Info().Str("inAddr", inAddr).Str("inLocalAddr", inLocalAddr).Str("host", req.Host).Msg("conn connected")

	go func() {
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
			// wg.Wait()
			// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
		}()

		ch := make(chan []byte, 1024)
		go func() {
			defer close(ch)
			for {
				buf := make([]byte, 1024*8)
				// buf := make([]byte, math.MaxUint16/2)
				nr, er := inConn.Read(buf)
				if er != nil {
					if er != io.EOF {
						err = er
						log.Ctx(ctx).Error().Caller().Err(err).Msgf("err : %v", err)
					}
					break
				}
				if nr <= 0 {
					continue
				}

				ch <- buf[:nr]
			}
		}()
		addr := address
		for bs := range ch {
			err1 := stream.Send(ctx, &pb.ConnReq{
				Addr: addr,
				Body: bs,
			})
			if err1 != nil {
				log.Ctx(ctx).Info().Caller().Err(err1).Msg("err")
				return
			}
			addr = ""
		}
	}()

	defer func() {
		e := recover()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
		stream.Close(ctx)
		if true {
			peer.TsLast = time.Now().Unix()
			select {
			case p.ChPeerReuser <- peer:
				log.Ctx(ctx).Info().Caller().Int("len(ChPeerReuser)", len(p.ChPeerReuser)).Msg("reuser peer++++++++++++++++++++++++")
			case <-time.After(time.Second * 15):
				log.Ctx(ctx).Info().Caller().Msg("close  peer------------------------")
				peer.Close(ctx)
			}
		} else {
			peer.Close(ctx)
		}
	}()
	var n int
	ch := make(chan []byte, 1024)
	go func() {
		// TODO: Read 和Send 分两个进程处理
		defer close(ch)

		if !req.IsHTTPS() {
			ch <- req.HeadBuf
		}
		for {
			iface, err := stream.Recv(ctx)
			if err != nil {
				log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
				return
			}
			rsp := iface.(*pb.ConnRsp)
			if rsp.Err != nil && rsp.Err.Code != 0 {
				err = errors.NewErr(int(rsp.Err.Code), rsp.Err.Msg)
				log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
				return
			}
			ch <- rsp.Body
		}
	}()
	for bs := range ch {
		if l := len(bs); l > 0 {
			n, err = inConn.Write(bs)
			if n < 0 || n < l {
				if err == nil {
					err = errors.Errorf(" n < 0 || n < l")
				}
			}
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				return
			}
		}
	}

	return
}

func (p *SocksCli) OutToTCPLocal(ctx context.Context, address string, inConn *net.Conn, req *utils.HTTPRequest) (err error) {
	inAddr := (*inConn).RemoteAddr().String()
	inLocalAddr := (*inConn).LocalAddr().String()
	//防止死循环
	if p.IsDeadLoop(inLocalAddr, req.Host) {
		utils.CloseConn(inConn)
		err = fmt.Errorf("dead loop detected , %s", req.Host)
		return
	}
	var outConn net.Conn
	outConn, err = utils.ConnectHost(address, 10*1000)

	if err != nil {
		err = errors.Errorf("connect to %s , err:%s", address, err)
		utils.CloseConn(inConn)
		return
	}

	outAddr := outConn.RemoteAddr().String()
	outLocalAddr := outConn.LocalAddr().String()

	if req.IsHTTPS() {
		req.HTTPSReply() // http 回复建立连接
	} else {
		outConn.Write(req.HeadBuf)
	}
	utils.IoBind((*inConn), outConn, func(isSrcErr bool, err error) {
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("conn %s - %s - %s -%s released [%s]", inAddr, inLocalAddr, outLocalAddr, outAddr, req.Host)
		utils.CloseConn(inConn)
		utils.CloseConn(&outConn)
	}, func(n int, d bool) {}, 0)
	log.Ctx(ctx).Info().Str("inAddr", inAddr).Str("inLocalAddr", inLocalAddr).Str("outLocalAddr", outLocalAddr).Str("outAddr", outAddr).Str("host", req.Host).Msg("conn connected")
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

func (p *SocksCli) connect(ctx context.Context, tgtAddr string, rc net.Conn) (err error) {
	rc.(*net.TCPConn).SetKeepAlive(true)

	peer := p.GetPeer()

	// 第一个请求很大概率是HTTP，提前握手可以减少一次RTT
	buf := make([]byte, 1024*64)
	n, err := rc.Read(buf)
	if err != nil {
		err = errors.Errorf("Read:%s", err.Error())
		return
	}
	req := &pb.ConnUpgradeReq{
		Addr: tgtAddr,
		Body: buf[:n],
	}

	resp := &pb.ConnUpgradeRsp{}
	// stream, err := peer.StreamAsync(ctx, "Conn") ConnpUgrade
	upgrade, err := peer.Upgrade(ctx, "ConnUpgrade", req, resp)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	// ctx, cancel := context.WithCancel(ctx)
	// go io.Copy(rc, upgrade)
	// io.Copy(upgrade, rc)
	go func() {
		defer func() {
			// // cancel()
			// // rc.SetDeadline(time.Now()) // 唤醒因读写conn而阻塞的协程
			// // rc.Close()
			// time.Sleep(time.Second * 3)
			upgrade.Close()
		}()
		Copy(ctx, rc, upgrade)
		// io.Copy(rc, upgrade)
	}()

	defer func() {
		// // cancel()
		// time.Sleep(time.Second * 3)
		// rc.SetDeadline(time.Now()) // 唤醒因读写conn而阻塞的协程
		// rc.Close()
		// upgrade.Close()
	}()
	Copy(ctx, upgrade, rc)
	// io.Copy(upgrade, rc)
	return
}

func (p *SocksCli) connect1(ctx context.Context, rc net.Conn) (err error) {
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
	peer := p.GetPeer()
	// stream, err := peer.StreamAsync(ctx, "Conn")
	stream, err := peer.Stream(ctx, "Conn")
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	go func() {
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}

			if false {
				peer.TsLast = time.Now().Unix()
				select {
				case p.ChPeer <- peer:
					log.Ctx(ctx).Info().Caller().Int("len(peers)", len(p.ChPeer)).Msg("reuser peer++++++++++++++++++++++++")
				case <-time.After(time.Second * 5):
					log.Ctx(ctx).Info().Caller().Msg("close  peer------------------------")
					peer.Close(ctx)
				}
			} else {
				peer.Close(ctx)
			}
		}()
		var n int
		ch := make(chan []byte, 1024)
		go func() {
			// TODO: Read 和Send 分两个进程处理
			defer close(ch)
			for {
				iface, err := stream.Recv(ctx)
				if err != nil {
					log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
					return
				}
				rsp := iface.(*pb.ConnRsp)
				if rsp.Err != nil && rsp.Err.Code != 0 {
					err = errors.NewErr(int(rsp.Err.Code), rsp.Err.Msg)
					log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
					return
				}
				ch <- rsp.Body
			}
		}()
		for bs := range ch {
			if l := len(bs); l > 0 {
				n, err = rc.Write(bs)
				if n < 0 || n < l {
					if err == nil {
						err = errors.Errorf(" n < 0 || n < l")
					}
				}
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
			}
		}
	}()

	defer func() {
		e := recover()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
		// wg.Wait()
		// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
	}()

	ch := make(chan []byte, 1024)
	go func() {
		defer close(ch)
		for {
			buf := make([]byte, 1024*8)
			// buf := make([]byte, math.MaxUint16/2)
			nr, er := rc.Read(buf)
			if er != nil {
				if er != io.EOF {
					err = er
					log.Ctx(ctx).Error().Caller().Err(err).Msgf("err : %v", err)
				}
				break
			}
			if nr <= 0 {
				continue
			}

			ch <- buf[:nr]
		}
	}()
	addr := tgtAddr.String()
	for bs := range ch {
		err1 := stream.Send(ctx, &pb.ConnReq{
			Addr: addr,
			Body: bs,
		})
		if err1 != nil {
			log.Ctx(ctx).Info().Caller().Err(err1).Msg("err")
			return
		}
		addr = ""
	}
	return
}

func (p *SocksCli) connect2(ctx context.Context, rc net.Conn) (err error) {
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
	peer := p.GetPeer()
	addr := tgtAddr.String()
	go p.CopyLoop(ctx, rc, peer, addr)

	return
}

func (p *SocksCli) CopyLoop(ctx context.Context, rwc io.ReadWriteCloser, peer *Peer, addr string) (err error) {
	stream, err := peer.StreamAsync(ctx, "Conn")
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}

	go func() {
		defer func() {
			e := recover()
			if e != nil {
				err = errors.Errorf("recover : %v", e)
				log.Ctx(ctx).Error().Caller().Err(err).Send()
			}
			if dl, ok := rwc.(interface{ SetDeadline(t time.Time) error }); ok {
				// rwc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				dl.SetDeadline(time.Now())
			}
			// wg.Done()
			rwc.Close()

			if false {
				peer.TsLast = time.Now().Unix()
				select {
				case p.ChPeer <- peer:
					log.Ctx(ctx).Info().Caller().Int("len(peers)", len(p.ChPeer)).Msg("reuser peer++++++++++++++++++++++++")
				case <-time.After(time.Second * 5):
					log.Ctx(ctx).Info().Caller().Msg("close  peer------------------------")
					peer.Close(ctx)
				}
			} else {
				peer.Close(ctx)
			}
		}()
		var n int
		ch := make(chan []byte, 1024)
		go func() {
			// TODO: Read 和Send 分两个进程处理
			defer close(ch)
			for {
				iface, err := stream.Recv(ctx)
				if err != nil {
					log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
					return
				}
				rsp := iface.(*pb.ConnRsp)
				if rsp.Err != nil && rsp.Err.Code != 0 {
					err = errors.NewErr(int(rsp.Err.Code), rsp.Err.Msg)
					log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
					return
				}
				ch <- rsp.Body
			}
		}()
		for bs := range ch {
			if l := len(bs); l > 0 {
				n, err = rwc.Write(bs)
				if n < 0 || n < l {
					if err == nil {
						err = errors.Errorf(" n < 0 || n < l")
					}
				}
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
			}
		}
	}()

	defer func() {
		e := recover()
		if e != nil {
			err = errors.Errorf("recover : %v", e)
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
		// wg.Wait()
		// rwc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
	}()

	ch := make(chan []byte, 1024)
	go func() {
		defer close(ch)
		for {
			buf := make([]byte, 1024*8)
			// buf := make([]byte, math.MaxUint16/2)
			nr, er := rwc.Read(buf)
			if er != nil {
				if er != io.EOF {
					err = er
					log.Ctx(ctx).Error().Caller().Err(err).Msgf("err : %v", err)
				}
				break
			}
			if nr <= 0 {
				continue
			}

			ch <- buf[:nr]
		}
	}()

	for bs := range ch {
		err1 := stream.Send(ctx, &pb.ConnReq{
			Addr: addr,
			Body: bs,
		})
		if err1 != nil {
			log.Ctx(ctx).Info().Caller().Err(err1).Msg("err")
			return
		}
		addr = ""
	}
	return
}

// 创建备用connect，提前三次握手较少延时
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
	out = &Peer{
		TsLast:      time.Now().Unix(),
		Peer:        peer,
		LocalAddrs:  conn.LocalAddr().String(),
		RemoteAddrs: conn.RemoteAddr().String(),
	}
	return
}
