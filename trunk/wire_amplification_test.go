package trunk

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// countConn wraps a net.Conn and counts bytes actually written to / read from the wire.
type countConn struct {
	net.Conn
	w *atomic.Int64
	r *atomic.Int64
}

func (c *countConn) Write(bs []byte) (int, error) {
	n, err := c.Conn.Write(bs)
	c.w.Add(int64(n))
	return n, err
}
func (c *countConn) Read(bs []byte) (int, error) {
	n, err := c.Conn.Read(bs)
	c.r.Add(int64(n))
	return n, err
}

// TestWireAmplification pumps payload through one virtual trunk conn over N real
// TCP loopback conns and compares bytes-on-the-wire vs payload bytes.
func TestWireAmplification(t *testing.T) {
	for _, nConn := range []int{1, 4, 8, 32} {
		t.Run(fmt.Sprintf("conns=%d", nConn), func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()

			var cliWire, svcWire atomic.Int64 // write-side byte counters
			var cliRws, svcRws []io.ReadWriteCloser

			doneAccept := make(chan struct{})
			go func() {
				defer close(doneAccept)
				for range nConn {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					svcRws = append(svcRws, &countConn{Conn: c, w: &svcWire, r: &atomic.Int64{}})
				}
			}()

			for range nConn {
				c, err := net.Dial("tcp", ln.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				cliRws = append(cliRws, &countConn{Conn: c, w: &cliWire, r: &atomic.Int64{}})
			}
			<-doneAccept

			cliTrunk := NewTrunk(cliRws...)
			svcTrunk := NewTrunk(svcRws...)
			ctx := context.Background()
			go cliTrunk.Run(ctx)
			go svcTrunk.Run(ctx)

			cliV := cliTrunk.GetConn(1)
			svcV := svcTrunk.GetConn(1)

			const total = 64 << 20 // 64 MiB
			payload := make([]byte, 1<<20)
			_, _ = rand.Read(payload)

			// server -> client (download direction)
			var recv int64
			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 8192) // same size as socket.Copy pool
				for recv < total {
					n, err := cliV.Read(buf)
					recv += int64(n)
					if err != nil {
						return
					}
				}
			}()

			start := time.Now()
			go func() {
				for sent := 0; sent < total; {
					n, _ := svcV.Write(payload)
					sent += n
				}
			}()
			<-done
			elapsed := time.Since(start)

			// give counters a moment to settle, then measure
			time.Sleep(200 * time.Millisecond)
			wire := svcWire.Load()
			ratio := float64(wire) / float64(total)
			t.Logf("payload=%d wire=%d ratio=%.4f goodput=%.1f MB/s",
				total, wire, ratio, float64(total)/(1<<20)/elapsed.Seconds())
			if ratio > 1.05 {
				t.Errorf("wire amplification: ratio %.2fx", ratio)
			}

			cliTrunk.Close()
			svcTrunk.Close()
		})
	}
}
