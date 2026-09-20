package trunk_kcp

import (
	"context"
	"net"
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

// TestSetNoDelay 是行为级冒烟测试：kcp-go 的 KCP 字段不可导出，无法直接断言
// 内部状态，这里验证各种参数组合（含 -1 保持）调用不 panic，且配置后链路
// 仍能正常收发。
func TestSetNoDelay(t *testing.T) {
	trunk := NewTrunkKCP(1, nil)
	defer trunk.Close()
	for _, p := range [][4]int{
		{0, 20, 0, 1},   // TCP/TLS 底层推荐值（minRTO=100ms）
		{-1, -1, -1, -1}, // 全部保持
		{1, 10, 32, 1},  // 库默认值
		{0, 40, 2, 0},
	} {
		trunk.SetNoDelay(p[0], p[1], p[2], p[3])
	}
}

// TestSetNoDelayDataPath 配置参数后虚拟连接仍能正常传输。
func TestSetNoDelayDataPath(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	cli := NewTrunkKCP(1, nil, a)
	svc := NewTrunkKCP(1, nil, b)
	for _, tr := range []*TrunkKCP{cli, svc} {
		tr.SetNoDelay(0, 20, 0, 1)
	}
	ctx := context.Background()
	go cli.Run(ctx)
	go svc.Run(ctx)
	defer cli.Close()
	defer svc.Close()

	cliV := cli.GetConn(1)
	svcV := svc.GetConn(1)

	want := []byte("hello kcp param")
	go cliV.Write(want) //nolint

	buf := make([]byte, 64)
	n, err := svcV.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != string(want) {
		t.Fatalf("got %q, want %q", buf[:n], want)
	}
}
