package trunk_kcp

import (
	"sync"
	"testing"
)

func TestVirtualConnReuseAfterClose(t *testing.T) {
	trunk := NewTrunkKCP(1, nil)
	defer trunk.Close()
	old := trunk.GetConn(1)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	// In-flight data must stay with the old connection until the peer closes it.
	trunk.demuxData(Header{ConnID: 1, Len: 3}, []byte("old"))
	if got := trunk.GetConn(1); got != old {
		t.Fatal("ID reused before the remote close")
	}
	trunk.demuxData(Header{ConnID: 1, Cmd: CmdCloseConn}, nil)
	next := trunk.GetConn(1)
	if next == old {
		t.Fatal("closed virtual connection was not recycled")
	}
	trunk.demuxData(Header{ConnID: 1, Len: 3}, []byte("new"))
	buf := make([]byte, 3)
	if n, err := next.Read(buf); err != nil || n != 3 || string(buf) != "new" {
		t.Fatalf("new connection read = %q, %v", buf[:n], err)
	}
}

func TestOpenConnReservesIDs(t *testing.T) {
	trunk := NewTrunkKCP(1, nil)
	defer trunk.Close()
	const limit = 8
	var wg sync.WaitGroup
	opened := make(chan *VirtualConn, limit*2)
	for i := 0; i < limit*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := trunk.OpenConn(limit)
			if err == nil {
				opened <- conn
			}
		}()
	}
	wg.Wait()
	close(opened)
	ids := make(map[uint16]bool)
	for conn := range opened {
		if ids[conn.ConnID()] {
			t.Fatalf("allocated active ID %d twice", conn.ConnID())
		}
		ids[conn.ConnID()] = true
	}
	if len(ids) != limit {
		t.Fatalf("allocated %d connections, want %d", len(ids), limit)
	}
	old := trunk.GetConn(1)
	_ = old.Close()
	if _, err := trunk.OpenConn(limit); err == nil {
		t.Fatal("reused ID before receiving close acknowledgment")
	}
	trunk.demuxData(Header{ConnID: 1, Cmd: CmdCloseConn}, nil)
	conn, err := trunk.OpenConn(limit)
	if err != nil || conn == old || conn.ConnID() != 1 {
		t.Fatalf("did not reuse the only free ID: %v", err)
	}
}

func TestRemoteCloseBeforeData(t *testing.T) {
	trunk := NewTrunkKCP(1, func(*VirtualConn) {
		t.Error("close frame invoked the new stream callback")
	})
	defer trunk.Close()
	trunk.demuxData(Header{ConnID: 1, Cmd: CmdCloseConn}, nil)
	conn := trunk.conns[1]
	if !conn.reusable() || trunk.VirtualConnCount() != 0 {
		t.Fatal("unused stream close was not acknowledged")
	}
}

func TestVirtualConnWriteDuringTrunkClose(t *testing.T) {
	for i := 0; i < 20; i++ {
		trunk := NewTrunkKCP(1, nil)
		conn := trunk.GetConn(1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = conn.Write(make([]byte, 1<<20))
		}()
		trunk.Close()
		wg.Wait()
	}
}
