package socks_kcp

import (
	"context"
	"testing"
	"time"
)

// TestRefreshConn 测试连接刷新功能
// 这个测试需要server端运行才能工作
func TestRefreshConn(t *testing.T) {
	t.Skip("需要手动运行server端，取消skip来测试")

	ctx := context.Background()
	// log.InitLog(ctx, "debug", "console", "test_refresh.log")

	// 这里需要配置client连接到实际的server
	// 示例代码，实际使用时需要配置正确的参数
	client := &SocksCli{
		Name:     "test-client",
		PeerAddr: "localhost:8080",
		// TlsConf: ... 需要配置TLS
		Token:  "test-token",
		ChPeer: make(chan *Peer, 10),
		TrunkCfg: TrunkKCPConfig{
			Conv:     12345,
			MinConns: 3,
			MaxConns: 3,
		},
	}

	// 初始化trunk
	if err := client.InitTrunk(ctx); err != nil {
		t.Fatalf("failed to init trunk: %v", err)
	}

	// 等待足够长的时间，观察连接刷新（至少30秒才能看到3次刷新）
	time.Sleep(35 * time.Second)

	// 检查连接信息
	client.mu.Lock()
	connCount := len(client.connInfos)
	t.Logf("Current connection count: %d", connCount)
	for i, info := range client.connInfos {
		age := time.Since(info.createdAt)
		t.Logf("Connection %d: ID=%d, Age=%v", i, info.id, age)
	}
	client.mu.Unlock()

	// 清理
	if client.trunk != nil {
		_ = client.trunk.Close()
	}
}
