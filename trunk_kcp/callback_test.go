package trunk_kcp

import (
	"context"
	"sync"
	"testing"
	"time"

	rpc "github.com/lxt1045/rpc"
)

// TestOnNewConnCallback 测试新连接回调功能
func TestOnNewConnCallback(t *testing.T) {
	ctx := context.Background()

	// 创建管道连接
	s0, c0 := rpc.NewFakeConnPipe()

	// 用于记录回调调用
	var mu sync.Mutex
	callbackInvoked := make(map[uint16]bool)

	// 创建回调函数
	onNewConn := func(conn *VirtualConn) {
		mu.Lock()
		defer mu.Unlock()
		callbackInvoked[conn.connID] = true
		t.Logf("onNewConn called for ConnID: %d", conn.connID)
	}

	// 创建两个 TrunkKCP，peer2 设置回调
	peer1 := NewTrunkKCP(0x77777777, nil, s0)
	peer2 := NewTrunkKCP(0x77777777, onNewConn, c0)

	defer peer1.Close()
	defer peer2.Close()

	// 在 goroutine 中运行 peer1 和 peer2
	go func() {
		if err := peer1.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("peer1.Run failed: %v", err)
		}
	}()
	go func() {
		if err := peer2.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("peer2.Run failed: %v", err)
		}
	}()

	// 等待 trunk 启动
	time.Sleep(50 * time.Millisecond)

	// peer1 发送数据到不同的虚拟连接
	testData := []struct {
		connID uint16
		data   string
	}{
		{0, "Hello from conn 0"},
		{1, "Hello from conn 1"},
		{2, "Hello from conn 2"},
	}

	for _, td := range testData {
		conn1 := peer1.GetConn(td.connID)
		if conn1 == nil {
			t.Fatalf("peer1.GetConn(%d) returned nil", td.connID)
		}

		// 写入数据
		_, err := conn1.Write([]byte(td.data))
		if err != nil {
			t.Fatalf("conn1.Write failed: %v", err)
		}
		t.Logf("Sent to ConnID %d: %s", td.connID, td.data)
	}

	// 等待数据传输和回调触发
	time.Sleep(100 * time.Millisecond)

	// 验证回调被调用
	mu.Lock()
	defer mu.Unlock()

	for _, td := range testData {
		if !callbackInvoked[td.connID] {
			t.Errorf("Callback not invoked for ConnID %d", td.connID)
		}
	}

	// 验证可以读取数据
	for _, td := range testData {
		conn2 := peer2.GetConn(td.connID)
		if conn2 == nil {
			t.Fatalf("peer2.GetConn(%d) returned nil", td.connID)
		}

		buf := make([]byte, 1024)
		n, err := conn2.Read(buf)
		if err != nil {
			t.Errorf("conn2.Read failed for ConnID %d: %v", td.connID, err)
			continue
		}

		received := string(buf[:n])
		if received != td.data {
			t.Errorf("ConnID %d: expected %q, got %q", td.connID, td.data, received)
		} else {
			t.Logf("ConnID %d received correctly: %s", td.connID, received)
		}
	}
}

// TestOnNewConnCallbackNil 测试回调为 nil 时不会崩溃
func TestOnNewConnCallbackNil(t *testing.T) {
	ctx := context.Background()

	s0, c0 := rpc.NewFakeConnPipe()

	// 创建两个 TrunkKCP，都不设置回调
	peer1 := NewTrunkKCP(0x88888888, nil, s0)
	peer2 := NewTrunkKCP(0x88888888, nil, c0)

	defer peer1.Close()
	defer peer2.Close()

	// 在 goroutine 中运行 peer1 和 peer2
	go func() {
		if err := peer1.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("peer1.Run failed: %v", err)
		}
	}()
	go func() {
		if err := peer2.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("peer2.Run failed: %v", err)
		}
	}()

	// 等待 trunk 启动
	time.Sleep(50 * time.Millisecond)

	// peer1 发送数据
	conn1 := peer1.GetConn(0)
	testData := "Test without callback"
	_, err := conn1.Write([]byte(testData))
	if err != nil {
		t.Fatalf("conn1.Write failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// peer2 读取数据
	conn2 := peer2.GetConn(0)
	buf := make([]byte, 1024)
	n, err := conn2.Read(buf)
	if err != nil {
		t.Fatalf("conn2.Read failed: %v", err)
	}

	received := string(buf[:n])
	if received != testData {
		t.Errorf("expected %q, got %q", testData, received)
	}

	t.Log("Nil callback test passed")
}
