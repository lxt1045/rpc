package trunk_kcp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	mrand "math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// wireStats counts KCP segments actually written to one direction of the wire.
type wireStats struct {
	bytes  atomic.Int64
	segs   atomic.Int64
	acks   atomic.Int64
	dups   atomic.Int64
	mu     sync.Mutex
	seenSn map[uint32]struct{}
}

func newWireStats() *wireStats { return &wireStats{seenSn: make(map[uint32]struct{})} }

// countKCPConn wraps a net.Conn; each Write carries exactly one KCP packet
// (sendLoop uses writeFull per packet), so we can parse and account every segment.
type countKCPConn struct {
	net.Conn
	tx *wireStats
}

func (c *countKCPConn) Write(bs []byte) (int, error) {
	if len(bs) >= kcpHeaderSize {
		cmd := bs[4]
		sn := binary.LittleEndian.Uint32(bs[12:16])
		c.tx.bytes.Add(int64(len(bs)))
		c.tx.segs.Add(1)
		if cmd == 0x52 { // IKCP_CMD_ACK
			c.tx.acks.Add(1)
		} else if cmd == 0x51 { // IKCP_CMD_PUSH
			c.tx.mu.Lock()
			if _, ok := c.tx.seenSn[sn]; ok {
				c.tx.dups.Add(1)
			} else {
				c.tx.seenSn[sn] = struct{}{}
			}
			c.tx.mu.Unlock()
		}
	}
	return c.Conn.Write(bs)
}

// delayLink forwards traffic between a and b with fixed delay + uniform jitter,
// emulating a WAN path. lossPct randomly drops packets (like a QoS policer);
// TCP underneath would retransmit those, KCP sees them as loss too.
func delayLink(a, b net.Conn, delay, jitter time.Duration, lossPct int) {
	forward := func(dst, src net.Conn) {
		type pkt struct {
			b   []byte
			due time.Time
		}
		ch := make(chan pkt, 8192)
		go func() {
			for p := range ch {
				if d := time.Until(p.due); d > 0 {
					time.Sleep(d)
				}
				if _, err := dst.Write(p.b); err != nil {
					return
				}
			}
		}()
		buf := make([]byte, 64*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if lossPct > 0 && mrand.Intn(100) < lossPct {
					continue // policer drop
				}
				j := time.Duration(0)
				if jitter > 0 {
					j = time.Duration(mrand.Int63n(int64(jitter)))
				}
				ch <- pkt{append([]byte(nil), buf[:n]...), time.Now().Add(delay + j)}
			}
			if err != nil {
				close(ch)
				return
			}
		}
	}
	go forward(a, b)
	go forward(b, a)
}

func splice(a, b net.Conn) {
	go io.Copy(a, b) //nolint
	go io.Copy(b, a) //nolint
}

// TestWireAmplificationKCP measures bytes-on-the-wire vs payload for trunk_kcp,
// counting duplicate KCP segments (retransmissions) explicitly.
func TestWireAmplificationKCP(t *testing.T) {
	scenarios := []struct {
		name          string
		delay, jitter time.Duration
		lossPct       int
		nodelay       []int // optional NoDelay(nodelay, interval, resend, nc) override
	}{
		{"loopback", 0, 0, 0, nil},
		{"wan50ms_jitter40ms", 50 * time.Millisecond, 40 * time.Millisecond, 0, nil},
		{"wan50ms_jitter80ms_loss2pct", 50 * time.Millisecond, 80 * time.Millisecond, 2, nil},
		// same harsh path, retuned KCP parameters:
		{"wan50ms_jitter80ms_loss2pct/minrto100_nc_off", 50 * time.Millisecond, 80 * time.Millisecond, 2, []int{0, 20, 0, 1}},
		{"wan50ms_jitter80ms_loss2pct/minrto100_i40_nc_off", 50 * time.Millisecond, 80 * time.Millisecond, 2, []int{0, 40, 0, 1}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			const nConn = 4
			const conv = 0x5a0b0001

			cliTx, svcTx := newWireStats(), newWireStats()
			var cliRws, svcRws []io.ReadWriteCloser

			for range nConn {
				cliEnd, fwdA := net.Pipe()
				fwdB, svcEnd := net.Pipe()
				if sc.delay > 0 || sc.jitter > 0 {
					delayLink(fwdA, fwdB, sc.delay, sc.jitter, sc.lossPct)
				} else {
					splice(fwdA, fwdB)
				}
				cliRws = append(cliRws, &countKCPConn{Conn: cliEnd, tx: cliTx})
				svcRws = append(svcRws, &countKCPConn{Conn: svcEnd, tx: svcTx})
			}

			cliTrunk := NewTrunkKCP(conv, nil, cliRws...)
			svcTrunk := NewTrunkKCP(conv, nil, svcRws...)
			if len(sc.nodelay) == 4 {
				for _, tr := range []*TrunkKCP{cliTrunk, svcTrunk} {
					tr.kcpLock.Lock()
					tr.kcp.NoDelay(sc.nodelay[0], sc.nodelay[1], sc.nodelay[2], sc.nodelay[3])
					tr.kcpLock.Unlock()
				}
			}
			ctx := context.Background()
			go cliTrunk.Run(ctx)
			go svcTrunk.Run(ctx)

			cliV := cliTrunk.GetConn(1)
			svcV := svcTrunk.GetConn(1)

			const total = 16 << 20
			payload := make([]byte, 8192)
			_, _ = rand.Read(payload)

			var recv int64
			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 8192)
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
					n, err := svcV.Write(payload)
					if err != nil {
						return
					}
					sent += n
				}
			}()
			<-done
			elapsed := time.Since(start)
			time.Sleep(300 * time.Millisecond)

			wireDown := svcTx.bytes.Load() // server -> client (download direction)
			wireUp := cliTx.bytes.Load()   // client -> server (mostly ACKs)
			ratio := float64(wireDown+wireUp) / float64(total)
			t.Logf("payload=%d wireDown=%d wireUp=%d ratio=%.3f goodput=%.2f MB/s",
				total, wireDown, wireUp, ratio, float64(total)/(1<<20)/elapsed.Seconds())
			t.Logf("down: segs=%d dupRetrans=%d (%.1f%% of segs)  up: segs=%d acks=%d",
				svcTx.segs.Load(), svcTx.dups.Load(),
				100*float64(svcTx.dups.Load())/float64(max(svcTx.segs.Load(), 1)),
				cliTx.segs.Load(), cliTx.acks.Load())

			cliTrunk.Close()
			svcTrunk.Close()
		})
	}
}
