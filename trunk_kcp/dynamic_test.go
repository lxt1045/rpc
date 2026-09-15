package trunk_kcp

import (
	"context"
	"testing"
	"time"

	"github.com/lxt1045/rpc"
)

func TestTrunkKCP_DynamicRemoveAdd(t *testing.T) {
	ctx := context.Background()
	s0, c0 := rpc.NewFakeConnPipe()
	s1, c1 := rpc.NewFakeConnPipe()

	p1 := NewTrunkKCP(0x60000001, nil, s0, s1)
	p2 := NewTrunkKCP(0x60000001, nil, c0, c1)
	defer p1.Close()
	defer p2.Close()

	go p1.Run(ctx)
	go p2.Run(ctx)

	waitCount := func(p *TrunkKCP, want int) {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if p.ConnCount() == want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("ConnCount = %d, want %d", p.ConnCount(), want)
	}
	waitCount(p1, 2)
	waitCount(p2, 2)

	// Graceful remove on both sides.
	if err := p1.RemoveConn(0); err != nil {
		t.Fatal(err)
	}
	if err := p2.RemoveConn(0); err != nil {
		t.Fatal(err)
	}
	waitCount(p1, 1)
	waitCount(p2, 1)

	// Add a replacement physical connection on both sides.
	s2, c2 := rpc.NewFakeConnPipe()
	if _, err := p1.AddConn(s2); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.AddConn(c2); err != nil {
		t.Fatal(err)
	}
	waitCount(p1, 2)
	waitCount(p2, 2)
}
