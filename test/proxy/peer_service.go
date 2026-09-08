package socks

import (
	"context"
	stderr "errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	rpcerrors "github.com/lxt1045/errors"
	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/codec"
	"github.com/lxt1045/rpc/test/proxy/pb"
	"github.com/lxt1045/utils/log"
)

func isBenignCloseErr(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF || err == io.ErrClosedPipe || stderr.Is(err, net.ErrClosed) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{"use of closed network connection", "connection reset by peer", "broken pipe", "forcibly closed", "aborted by the software", "has been closed", "codec is closed", "upgrade closed"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

type SocksSvc struct {
	pb.UnimplementedSocksSvcServer
	Peer rpc.Peer
}

func (p *SocksSvc) close(ctx context.Context) error {
	return p.Peer.Close(ctx)
}

func (p *SocksSvc) Close(ctx context.Context, _ *pb.CloseReq) (*pb.CloseRsp, error) {
	return &pb.CloseRsp{}, p.close(ctx)
}
func (p *SocksSvc) Auth(context.Context, *pb.AuthReq) (*pb.AuthRsp, error) { return &pb.AuthRsp{}, nil }

func (p *SocksSvc) ConnUpgrade(ctx context.Context, req *pb.ConnUpgradeReq) (*pb.ConnUpgradeRsp, error) {
	if req == nil {
		return nil, rpcerrors.New("request is nil")
	}
	upgrade := codec.GetUpgrade(ctx)
	if upgrade == nil {
		return nil, rpcerrors.New("upgrade is nil")
	}
	rc, err := (&net.Dialer{Timeout: 30 * time.Second}).DialContext(ctx, "tcp", req.Addr)
	if err != nil {
		return nil, err
	}
	ConfigureTCPConn(rc)
	closeBoth := sync.OnceFunc(func() { _ = rc.Close(); _ = upgrade.Close() })
	if err := writeInitial(rc, req.Body); err != nil {
		closeBoth()
		return nil, err
	}
	done := make(chan struct{}, 2)
	copyOne := func(dst io.WriteCloser, src io.ReadCloser) {
		go func() {
			defer func() {
				if e := recover(); e != nil {
					log.Ctx(ctx).Error().Caller().Interface("recover", e).Msg("ConnUpgrade copy")
				}
				closeBoth()
				done <- struct{}{}
			}()
			_, _ = Copy(ctx, dst, src)
		}()
	}
	go func() {
		if err := upgrade.WaitReady(ctx); err != nil {
			closeBoth()
			done <- struct{}{}
			return
		}
		copyOne(upgrade, rc)
		copyOne(rc, upgrade)
	}()
	go func() {
		select {
		case <-ctx.Done():
			closeBoth()
		case <-done:
			closeBoth()
		}
	}()
	return &pb.ConnUpgradeRsp{}, nil
}

func writeInitial(dst io.Writer, body []byte) error {
	for len(body) > 0 {
		n, err := dst.Write(body)
		if n < 0 || n > len(body) {
			return io.ErrShortWrite
		}
		body = body[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Copy transfers bytes until src or dst stops. The caller owns endpoint closure.
func Copy(ctx context.Context, dst io.WriteCloser, src io.ReadCloser) (written int64, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("copy panic: %v", e)
		}
	}()
	buf := make([]byte, 64*1024)
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}
		nr, readErr := src.Read(buf)
		if nr > 0 {
			remaining := buf[:nr]
			for len(remaining) > 0 {
				nw, writeErr := dst.Write(remaining)
				if nw < 0 || nw > len(remaining) {
					return written, io.ErrShortWrite
				}
				written += int64(nw)
				remaining = remaining[nw:]
				if writeErr != nil {
					return written, writeErr
				}
				if nw == 0 {
					return written, io.ErrShortWrite
				}
			}
		}
		if readErr != nil {
			if isBenignCloseErr(readErr) {
				return written, nil
			}
			return written, readErr
		}
	}
}
