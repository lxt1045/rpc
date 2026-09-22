package socks_faux_kcp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/trunk_kcp"
)

// TestOnDemandVirtualConn 测试按需创建虚拟连接的功能
func TestOnDemandVirtualConn(t *testing.T) {
	ctx := context.Background()

	// 创建管道连接
	s0, c0 := rpc.NewFakeConnPipe()

	var callbackCount atomic.Int32

	// 创建服务端的回调函数
	onNewConn := func(vconn *trunk_kcp.VirtualConn) {
		count := callbackCount.Add(1)
		t.Logf("onNewConn called for ConnID: %d (total callbacks: %d)", vconn.ConnID(), count)
	}

	// 创建 TrunkKCP
	serverTrunk := trunk_kcp.NewTrunkKCP(0x12345678, onNewConn, s0)
	clientTrunk := trunk_kcp.NewTrunkKCP(0x12345678, nil, c0)

	defer serverTrunk.Close()
	defer clientTrunk.Close()

	// 启动 trunk
	go func() {
		if err := serverTrunk.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("serverTrunk.Run failed: %v", err)
		}
	}()
	go func() {
		if err := clientTrunk.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("clientTrunk.Run failed: %v", err)
		}
	}()

	// 等待 trunk 启动
	time.Sleep(50 * time.Millisecond)

	// 测试：只使用部分虚拟连接 ID
	usedConnIDs := []uint16{1, 5, 10, 20, 50}
	var wg sync.WaitGroup

	for _, connID := range usedConnIDs {
		wg.Add(1)
		go func(id uint16) {
			defer wg.Done()

			// 客户端获取虚拟连接并发送数据
			vconn := clientTrunk.GetConn(id)
			if vconn == nil {
				t.Errorf("clientTrunk.GetConn(%d) returned nil", id)
				return
			}

			testData := []byte("Test data for conn " + string(rune('0'+id)))
			if _, err := vconn.Write(testData); err != nil {
				t.Errorf("vconn.Write failed for ConnID %d: %v", id, err)
				return
			}
			t.Logf("Client sent data to ConnID %d", id)
		}(connID)
	}

	wg.Wait()

	// 等待服务端接收和回调
	time.Sleep(200 * time.Millisecond)

	// 验证回调次数应该等于使用的虚拟连接数
	count := callbackCount.Load()
	if count != int32(len(usedConnIDs)) {
		t.Errorf("Expected %d callbacks, got %d", len(usedConnIDs), count)
	} else {
		t.Logf("Callback invoked correct number of times: %d", count)
	}

	// 验证：只有被使用的虚拟连接 ID 才应该触发回调
	t.Log("On-demand virtual connection creation test passed")
}

// TestNoCallbackBeforeUse 测试在虚拟连接被使用之前不应该创建
func TestNoCallbackBeforeUse(t *testing.T) {
	ctx := context.Background()

	s0, c0 := rpc.NewFakeConnPipe()

	var callbackCount atomic.Int32

	onNewConn := func(vconn *trunk_kcp.VirtualConn) {
		callbackCount.Add(1)
		t.Logf("onNewConn called for ConnID: %d", vconn.ConnID())
	}

	serverTrunk := trunk_kcp.NewTrunkKCP(0x87654321, onNewConn, s0)
	clientTrunk := trunk_kcp.NewTrunkKCP(0x87654321, nil, c0)

	defer serverTrunk.Close()
	defer clientTrunk.Close()

	go func() {
		if err := serverTrunk.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("serverTrunk.Run failed: %v", err)
		}
	}()
	go func() {
		if err := clientTrunk.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("clientTrunk.Run failed: %v", err)
		}
	}()

	// 等待 trunk 启动
	time.Sleep(50 * time.Millisecond)

	// 初始状态：不应该有任何回调
	initialCount := callbackCount.Load()
	if initialCount != 0 {
		t.Errorf("Expected 0 callbacks initially, got %d", initialCount)
	}

	// 等待一段时间，确认没有预先创建的连接
	time.Sleep(200 * time.Millisecond)

	finalCount := callbackCount.Load()
	if finalCount != 0 {
		t.Errorf("Expected 0 callbacks after waiting, got %d", finalCount)
	} else {
		t.Log("No callbacks invoked before virtual connections are used - correct behavior")
	}
}
