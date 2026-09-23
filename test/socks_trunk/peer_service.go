package socks

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/socket"
	"github.com/lxt1045/rpc/test/socks_trunk/pb"
	"github.com/lxt1045/rpc/trunk"
	"github.com/lxt1045/utils/log"
)

var _ pb.SocksSvcServer = &SocksSvc{}

type SocksSvc struct {
	Name       string
	LocalAddr  string
	RemoteAddr string
	Peer       rpc.Peer

	mu         sync.Mutex
	authorized bool
	clientID   string
	session    *Session

	trunk *trunk.Trunk
}

type Conn struct {
	Req   pb.TrunkUpgradeReq
	rw    io.ReadWriteCloser
	owner *SocksSvc
}

func (p *SocksSvc) close(ctx context.Context) (err error) {
	sessionManager.UnregisterSvc(p)
	err = p.Peer.Close(ctx)
	return
}

func (p *SocksSvc) Close(ctx context.Context, in *pb.CloseReq) (out *pb.CloseRsp, err error) {
	err = p.close(ctx)
	return &pb.CloseRsp{}, err
}

func (s *SocksSvc) Auth(ctx context.Context, req *pb.AuthReq) (resp *pb.AuthRsp, err error) {
	if req == nil || req.Name == "" {
		return &pb.AuthRsp{
			Status: pb.AuthRsp_Fail,
			Err:    &pb.Err{Msg: "empty token"},
		}, nil
	}

	// AuthReq.Name carries the shared secret token for this deployment.
	if err := sessionManager.Authenticate(s, req.Name, req.Name); err != nil {
		log.Ctx(ctx).Warn().Caller().Err(err).Str("remote", s.RemoteAddr).Msg("auth failed")
		return &pb.AuthRsp{
			Status: pb.AuthRsp_Fail,
			Err:    &pb.Err{Code: -1, Msg: err.Error()},
		}, nil
	}

	s.Name = req.Name
	log.Ctx(ctx).Info().Caller().Str("client", req.Name).Str("remote", s.RemoteAddr).Msg("auth ok")
	return &pb.AuthRsp{Status: pb.AuthRsp_Succ}, nil
}

func (p *SocksSvc) requireAuth(ctx context.Context) error {
	p.mu.Lock()
	ok := p.authorized
	p.mu.Unlock()
	if ok {
		return nil
	}
	err := errors.New("connection is not authenticated")
	log.Ctx(ctx).Warn().Caller().Err(err).Str("remote", p.RemoteAddr).Msg("unauthorized call")
	return err
}

func (p *SocksSvc) Conn(ctx context.Context, req *pb.ConnReq) (resp *pb.ConnRsp, err error) {
	if err = p.requireAuth(ctx); err != nil {
		return
	}
	if req != nil && !CheckACL(req.Addr) {
		err = errors.New("target address is denied by ACL")
		log.Ctx(ctx).Warn().Caller().Err(err).Str("addr", req.Addr).Msg("acl deny")
		return
	}
	// defer p.close(ctx)
	if stream := codec.GetStream(ctx); stream != nil {
		d := net.Dialer{
			Timeout: time.Second * 30,
		}
		rc, err1 := d.Dial("tcp", req.Addr)
		if err1 != nil {
			err = err1
			log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to connect to target: %v", err)
			return
		}
		rc.(*net.TCPConn).SetKeepAlive(true)

		log.Ctx(ctx).Info().Caller().Err(err).Msgf("proxy %s <-> %s", p.RemoteAddr, req.Addr)

		go func() {
			var n int
			ch := make(chan []byte, 1024)
			go func() {
				// TODO: Read 和Send 分两个进程处理
				defer func() {
					// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
					close(ch)
					// stream.Close(ctx)
					// cancel()
				}()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if l := len(req.Body); l > 0 {
						ch <- req.Body
					}
					iface, err := stream.Recv(ctx)
					if err != nil {
						log.Ctx(ctx).Info().Caller().Err(err).Msg("err")
						return
					}
					req = iface.(*pb.ConnReq)
				}
			}()
			defer func() {
				e := recover()
				if e != nil {
					err = errors.Errorf("recover : %v", e)
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
				// rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				// stream.Close(ctx)
			}()
			for {
				var bs []byte
				select {
				case bs = <-ch:
				case <-ctx.Done():
					return
				}
				n, err = rc.Write(bs)
				if n < 0 || n < len(bs) {
					if err == nil {
						err = errors.Errorf(" n < 0 || n < l")
					}
				}
				if err != nil {
					log.Ctx(ctx).Error().Caller().Err(err).Send()
					return
				}
			}
		}()

		go func() {
			defer func() {
				e := recover()
				if e != nil {
					err = errors.Errorf("recover : %v", e)
					log.Ctx(ctx).Error().Caller().Err(err).Send()
				}
				// wg.Wait()
				rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				// stream.Close(ctx)
				// p.close(ctx)
			}()
			// buf := make([]byte, math.MaxUint16/2)
			// buf := make([]byte, 1<<20)

			ch := make(chan []byte, 1024)
			go func() {
				defer func() {
					close(ch)
					// stream.Close(ctx)
					rc.SetDeadline(time.Now()) // wake up the other goroutine blocking on right
				}()
				for {
					// buf := make([]byte, math.MaxUint16/2)
					buf := make([]byte, 1024*8)
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
			// TODO: Read 和Send 分两个进程处理
			for {
				var bs []byte
				select {
				case bs = <-ch:
				case <-ctx.Done():
					return
				}
				err1 := stream.Send(ctx, &pb.ConnRsp{Body: bs})
				if err1 != nil {
					log.Ctx(ctx).Info().Caller().Err(err1).Msg("err")
					return
				}
			}
		}()

		return
	}

	// 在阿里云再试试
	return
}

func (p *SocksSvc) ConnUpgrade(ctx context.Context, req *pb.ConnUpgradeReq) (resp *pb.ConnUpgradeRsp, err error) {
	if err = p.requireAuth(ctx); err != nil {
		return
	}
	if req != nil && !CheckACL(req.Addr) {
		err = errors.New("target address is denied by ACL")
		log.Ctx(ctx).Warn().Caller().Err(err).Str("addr", req.Addr).Msg("acl deny")
		return
	}
	upgrade := codec.GetUpgrade(ctx)
	if upgrade == nil {
		err = errors.New("upgrade is nil")
		log.Ctx(ctx).Error().Caller().Err(err).Msg("ConnUpgrade")
		return
	}
	// rc, err1 := net.Dial("tcp", req.Addr)
	d := net.Dialer{
		Timeout: time.Second * 30,
	}
	rc, err1 := d.Dial("tcp", req.Addr)
	if err1 != nil {
		err = err1
		log.Ctx(ctx).Error().Caller().Err(err).Msgf("failed to connect to target: %v", err)
		return
	}
	rcTCP := rc.(*net.TCPConn)
	rcTCP.SetKeepAlive(true)

	log.Ctx(ctx).Info().Caller().Err(err).Msgf("proxy %s <-> %s", p.RemoteAddr, req.Addr)

	if len(req.Body) > 0 {
		n := 0
		n, err = rcTCP.Write(req.Body)
		if n < 0 || n < len(req.Body) {
			err = errors.Errorf("n < 0 || n < l, err: %s", err.Error())
			return
		}
	}

	// 两个方向的拷贝必须同生共死：一旦一端出错/EOF，应立即唤醒另一端，
	// 否则会出现 goroutine 泄漏（reader 阻塞在 Read 上），同时产生大量
	// "wsasend: An established connection was aborted" 类噪声日志。
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("ConnUpgrade rc->upgrade")
			}
			rcTCP.SetDeadline(time.Now()) // 唤醒另一方向在 rc 上阻塞的读
			rcTCP.Close()
			upgrade.Close() // 同步关闭 upgrade, 对端会收到 EOF
		}()
		socket.Copy(ctx, upgrade, rcTCP)
	}()

	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("ConnUpgrade upgrade->rc")
			}
			// 本方向结束后先半关闭 rc，通知目标端不再发送数据；
			// 另一方向（rc->upgrade）可能还有残留数据要发给 client，
			// 延迟 closeGrace 再强制关闭，既避免截断响应，又保证 rc 最终回收，
			// 防止 rc 与 goroutine 永久泄漏（见 closeGrace 注释）。
			if e := rcTCP.CloseWrite(); e != nil {
				log.Ctx(ctx).Debug().Caller().Err(e).Caller().Msg("rcTCP.CloseWrite()")
			}
			time.AfterFunc(time.Second*30, func() {
				rcTCP.SetDeadline(time.Now()) // 唤醒另一方向在 rc 上阻塞的读
				rcTCP.Close()
				upgrade.Close()
			})
		}()
		socket.Copy(ctx, rcTCP, upgrade)
	}()

	return &pb.ConnUpgradeRsp{}, nil
}

func (p *SocksSvc) TrunkUpgrade(ctx context.Context, req *pb.TrunkUpgradeReq) (resp *pb.TrunkUpgradeRsp, err error) {
	if err = p.requireAuth(ctx); err != nil {
		return
	}
	upgrade := codec.GetUpgrade(ctx)
	if upgrade == nil {
		err = errors.New("upgrade is nil")
		log.Ctx(ctx).Error().Caller().Err(err).Msg("TrunkUpgrade")
		return
	}
	if err = sessionManager.AddTrunkConn(p, req, upgrade); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("TrunkUpgrade add conn failed")
		upgrade.Close()
		return
	}
	return &pb.TrunkUpgradeRsp{}, nil
}

func (p *SocksSvc) TrunkStart(ctx context.Context, req *pb.TrunkStartReq) (resp *pb.TrunkStartRsp, err error) {
	if err = p.requireAuth(ctx); err != nil {
		return
	}
	if err = sessionManager.StartTrunk(p, req); err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("TrunkStart failed")
		return
	}
	return &pb.TrunkStartRsp{}, nil
}
func (p *SocksSvc) trunkEvent(connID uint16, data []byte) (err error) {
	req := &pb.TrunkStartData{}
	err = proto.Unmarshal(data, req)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}

	ctx := context.TODO()
	if !CheckACL(req.Addr) {
		err = errors.New("target address is denied by ACL")
		log.Ctx(ctx).Warn().Caller().Err(err).Str("addr", req.Addr).Msg("acl deny")
		return
	}
	// rc, err1 := net.Dial("tcp", req.Addr)
	d := net.Dialer{
		Timeout: time.Second * 30,
	}
	rc, err1 := d.Dial("tcp", req.Addr)
	if err1 != nil {
		err = errors.Errorf("Dial %s got err: %s", req.Addr, err1.Error())
		return
	}
	rcTCP := rc.(*net.TCPConn)
	rcTCP.SetKeepAlive(true)

	log.Ctx(ctx).Info().Caller().Err(err).Msgf("proxy %s <-> %s", p.RemoteAddr, req.Addr)

	if len(req.Body) > 0 {
		n := 0
		n, err = rcTCP.Write(req.Body)
		if n < 0 || n < len(req.Body) {
			err = errors.Errorf("n < 0 || n < l, err: %s", err.Error())
			return
		}
	}

	upgrade := p.trunk.GetConn(connID)

	// 两个方向的拷贝必须同生共死：一旦一端出错/EOF，应立即唤醒另一端，
	// 否则会出现 goroutine 泄漏（reader 阻塞在 Read 上），同时产生大量
	// "wsasend: An established connection was aborted" 类噪声日志。
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("trunkEvent rc->upgrade")
			}
			rcTCP.SetDeadline(time.Now()) // 唤醒另一方向在 rc 上阻塞的读
			rcTCP.Close()
			upgrade.Close() // 同步关闭 upgrade, 对端会收到 EOF
		}()
		socket.Copy(ctx, upgrade, rcTCP)
	}()

	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("trunkEvent upgrade->rc")
			}
			// 本方向结束后先半关闭 rc，通知目标端不再发送数据；
			// 另一方向（rc->upgrade）可能还有残留数据要发给 client，
			// 延迟 closeGrace 再强制关闭，既避免截断响应，又保证 rc 最终回收，
			// 防止 rc 与 goroutine 永久泄漏（见 closeGrace 注释）。
			if e := rcTCP.CloseWrite(); e != nil {
				log.Ctx(ctx).Debug().Caller().Err(e).Caller().Msg("rcTCP.CloseWrite()")
			}
			time.AfterFunc(time.Second*30, func() {
				rcTCP.SetDeadline(time.Now()) // 唤醒另一方向在 rc 上阻塞的读
				rcTCP.Close()
				upgrade.Close()
			})
		}()
		socket.Copy(ctx, rcTCP, upgrade)
	}()
	return
}
