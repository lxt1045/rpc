package trunk_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lxt1045/rpc/trunk"
	"github.com/lxt1045/rpc/trunk_kcp"
)

type transportPeer struct {
	run   func(context.Context)
	close func() error
	conn  func(uint16) io.ReadWriteCloser
}

var transportFactories = map[string]func(...io.ReadWriteCloser) transportPeer{
	"trunk": func(rws ...io.ReadWriteCloser) transportPeer {
		p := trunk.NewTrunk(rws...)
		return transportPeer{p.Run, p.Close, func(id uint16) io.ReadWriteCloser { return p.GetConn(id) }}
	},
	"kcp": func(rws ...io.ReadWriteCloser) transportPeer {
		p := trunk_kcp.NewTrunkKCP(42, rws...)
		return transportPeer{func(ctx context.Context) { p.Run(ctx) }, p.Close, func(id uint16) io.ReadWriteCloser { return p.GetConn(id) }}
	},
}

func TestTransportPipeTransfer(t *testing.T) {
	for name, factory := range transportFactories {
		for _, lanes := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/%d", name, lanes), func(t *testing.T) {
				var left, right []io.ReadWriteCloser
				for range lanes {
					a, b := net.Pipe()
					left, right = append(left, a), append(right, b)
				}
				a, b := factory(left...), factory(right...)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				finished := make(chan struct{}, 2)
				go func() { a.run(ctx); finished <- struct{}{} }()
				go func() { b.run(ctx); finished <- struct{}{} }()
				defer func() {
					a.close()
					b.close()
					for range 2 {
						select {
						case <-finished:
						case <-time.After(time.Second):
							t.Error("Run did not exit after Close")
						}
					}
				}()
				src, dst := a.conn(32767), b.conn(32767)
				payload := bytes.Repeat([]byte("0123456789abcdef"), 16384)
				written := make(chan error, 1)
				go func() {
					if n, err := src.Write(payload); err != nil || n != len(payload) {
						written <- fmt.Errorf("large Write: %d, %v", n, err)
						return
					}
					for i := range 100 {
						if _, err := src.Write([]byte{byte(i)}); err != nil {
							written <- err
							return
						}
					}
					written <- src.Close()
				}()
				// Let the bounded receive queue fill before the application reads.
				time.Sleep(30 * time.Millisecond)
				got, err := io.ReadAll(dst)
				if err != nil {
					t.Fatal(err)
				}
				for i := range 100 {
					payload = append(payload, byte(i))
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("data mismatch: got %d bytes, want %d", len(got), len(payload))
				}
				if err := <-written; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestTransportShutdown(t *testing.T) {
	for name, factory := range transportFactories {
		for _, trigger := range []string{"cancel", "eof", "close"} {
			t.Run(name+"/"+trigger, func(t *testing.T) {
				a, b := net.Pipe()
				defer b.Close()
				p := factory(a)
				defer p.close()
				c := p.conn(1)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan struct{}, 2)
				go func() { p.run(ctx); done <- struct{}{} }()
				go func() { c.Read(make([]byte, 1)); done <- struct{}{} }()
				switch trigger {
				case "cancel":
					cancel()
				case "eof":
					b.Close()
				case "close":
					p.close()
				}
				for range 2 {
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Fatal("shutdown left Run or Read blocked")
					}
				}
			})
		}
	}
}
