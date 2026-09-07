package trunk_kcp

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/lxt1045/rpc"
	"golang.org/x/sync/errgroup"
)

// TestTrunkKCP_SingleConn 测试单个物理连接的情况
func TestTrunkKCP_SingleConn(t *testing.T) {
	ctx := context.Background()

	// 创建一对 FakeConnPipe
	s0, c0 := rpc.NewFakeConnPipe()

	// 创建两个 TrunkKCP 实例（使用不同的 conv）
	peer1 := NewTrunkKCP(0x11111111, s0)
	peer2 := NewTrunkKCP(0x11111111, c0)

	go peer1.Run(ctx)
	go peer2.Run(ctx)

	// 获取虚拟连接
	vconn1 := peer1.GetConn(1)
	vconn2 := peer2.GetConn(1)

	// 测试数据传输
	testData := []byte("Hello, TrunkKCP!")
	n, err := vconn1.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Write returned %d, expected %d", n, len(testData))
	}

	// 读取数据
	buf := make([]byte, 1024)
	n, err = vconn2.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Read returned %d, expected %d", n, len(testData))
	}
	if string(buf[:n]) != string(testData) {
		t.Fatalf("Data mismatch: got %q, expected %q", buf[:n], testData)
	}

	t.Logf("Single conn test passed: sent and received %q", testData)

	// 关闭连接
	vconn1.Close()
	vconn2.Close()
	peer1.Close()
	peer2.Close()
}

// TestTrunkKCP_MultiConn 测试多个物理连接的聚合
func TestTrunkKCP_MultiConn(t *testing.T) {
	ctx := context.Background()

	// 创建 3 对 FakeConnPipe
	s0, c0 := rpc.NewFakeConnPipe()
	s1, c1 := rpc.NewFakeConnPipe()
	s2, c2 := rpc.NewFakeConnPipe()

	// 创建两个 TrunkKCP 实例
	peer1 := NewTrunkKCP(0x22222222, s0, s1, s2)
	peer2 := NewTrunkKCP(0x22222222, c0, c1, c2)

	go peer1.Run(ctx)
	go peer2.Run(ctx)

	// 获取虚拟连接
	vconn1 := peer1.GetConn(1)
	vconn2 := peer2.GetConn(1)

	// 并发读写测试
	g := errgroup.Group{}
	const numMessages = 100

	g.Go(func() error {
		for i := 0; i < numMessages; i++ {
			msg := fmt.Sprintf("Message %d", i)
			_, err := vconn1.Write([]byte(msg))
			if err != nil {
				return err
			}
			// 添加小延迟,避免发送速度超过 KCP 的处理能力
			time.Sleep(time.Millisecond)
		}
		// 等待一段时间确保所有数据都被 KCP 处理完毕
		time.Sleep(200 * time.Millisecond)
		vconn1.Close()
		return nil
	})

	g.Go(func() error {
		buf := make([]byte, 1024)
		receivedCount := 0
		for receivedCount < numMessages {
			n, err := vconn2.Read(buf)
			if err != nil {
				t.Logf("Read ended after %d messages: %v", receivedCount, err)
				if receivedCount != numMessages {
					return fmt.Errorf("expected %d messages, got %d before error", numMessages, receivedCount)
				}
				break
			}
			receivedCount++
			t.Logf("Received: %s", buf[:n])
		}
		return nil
	})

	err := g.Wait()
	if err != nil {
		t.Fatalf("Multi conn test failed: %v", err)
	}

	t.Logf("Multi conn test passed: sent and received %d messages", numMessages)

	peer1.Close()
	peer2.Close()
}

// TestTrunkKCP_VirtualConn 测试多个虚拟连接
func TestTrunkKCP_VirtualConn(t *testing.T) {
	ctx := context.Background()

	s0, c0 := rpc.NewFakeConnPipe()
	peer1 := NewTrunkKCP(0x33333333, s0)
	peer2 := NewTrunkKCP(0x33333333, c0)

	go peer1.Run(ctx)
	go peer2.Run(ctx)

	// 创建多个虚拟连接
	vconn11 := peer1.GetConn(1)
	vconn12 := peer2.GetConn(1)
	vconn21 := peer1.GetConn(2)
	vconn22 := peer2.GetConn(2)

	g := errgroup.Group{}

	// 虚拟连接 1 的通信
	g.Go(func() error {
		msg := "Hello from vconn1"
		_, err := vconn11.Write([]byte(msg))
		if err != nil {
			return err
		}
		vconn11.Close()
		return nil
	})

	g.Go(func() error {
		buf := make([]byte, 1024)
		n, err := vconn12.Read(buf)
		if err != nil {
			return err
		}
		t.Logf("vconn12 received: %s", buf[:n])
		return nil
	})

	// 虚拟连接 2 的通信
	g.Go(func() error {
		msg := "Hello from vconn2"
		_, err := vconn21.Write([]byte(msg))
		if err != nil {
			return err
		}
		vconn21.Close()
		return nil
	})

	g.Go(func() error {
		buf := make([]byte, 1024)
		n, err := vconn22.Read(buf)
		if err != nil {
			return err
		}
		t.Logf("vconn22 received: %s", buf[:n])
		return nil
	})

	err := g.Wait()
	if err != nil {
		t.Fatalf("VirtualConn test failed: %v", err)
	}

	t.Log("VirtualConn test passed: multiple virtual connections working independently")

	peer1.Close()
	peer2.Close()
}

// TestTrunkKCP_LargeData 测试大数据传输
func TestTrunkKCP_LargeData(t *testing.T) {
	ctx := context.Background()

	t.Logf("time:%s", time.Now())
	defer func() {
		t.Logf("time:%s", time.Now())
	}()

	s0, c0 := rpc.NewFakeConnPipe()
	peer1 := NewTrunkKCP(0x44444444, s0)
	peer2 := NewTrunkKCP(0x44444444, c0)

	// s0, c0 := rpc.NewFakeConnPipe()
	// s1, c1 := rpc.NewFakeConnPipe()
	// s2, c2 := rpc.NewFakeConnPipe()

	// // 创建两个 TrunkKCP 实例
	// peer1 := NewTrunkKCP(0x22222222, s0, s1, s2)
	// peer2 := NewTrunkKCP(0x22222222, c0, c1, c2)

	go peer1.Run(ctx)
	go peer2.Run(ctx)

	// 给一点时间让所有 goroutine 启动
	time.Sleep(50 * time.Millisecond)

	vconn1 := peer1.GetConn(1)
	vconn2 := peer2.GetConn(1)

	// 发送 1MB 数据
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	g := errgroup.Group{}

	g.Go(func() error {
		n, err := vconn1.Write(largeData)
		if err != nil {
			return err
		}
		t.Logf("Wrote %d bytes", n)

		// 等待数据传输完成（对于 1MB 数据，需要较长时间）
		// 基于实际测试，1MB 需要约 30-40 秒传输
		// KCP preserves send order, so close follows all data frames.

		vconn1.Close()
		return nil
	})

	g.Go(func() error {
		received := make([]byte, 0, len(largeData))
		buf := make([]byte, math.MaxUint16)

		for len(received) < len(largeData) {
			n, err := vconn2.Read(buf)
			if err != nil {
				t.Logf("Read error after %d bytes: %v", len(received), err)
				break
			}
			received = append(received, buf[:n]...)
		}

		t.Logf("Received %d bytes", len(received))

		if len(received) != len(largeData) {
			return fmt.Errorf("size mismatch: sent %d, received %d", len(largeData), len(received))
		}

		// 验证数据完整性
		for i := range received {
			if received[i] != largeData[i] {
				return fmt.Errorf("data mismatch at byte %d: got %d, expected %d", i, received[i], largeData[i])
			}
		}

		return nil
	})

	err := g.Wait()
	if err != nil {
		t.Fatalf("LargeData test failed: %v", err)
	}

	t.Log("LargeData test passed: 1MB data transmitted correctly")

	peer1.Close()
	peer2.Close()
}

// TestTrunkKCP_CloseHandling 测试关闭处理
func TestTrunkKCP_CloseHandling(t *testing.T) {
	ctx := context.Background()

	s0, c0 := rpc.NewFakeConnPipe()
	peer1 := NewTrunkKCP(0x55555555, s0)
	peer2 := NewTrunkKCP(0x55555555, c0)

	go peer1.Run(ctx)
	go peer2.Run(ctx)

	// 给一点时间让所有 goroutine 启动
	time.Sleep(50 * time.Millisecond)

	vconn1 := peer1.GetConn(1)
	vconn2 := peer2.GetConn(1)

	// 写入一些数据
	testData := []byte("Test data before close")
	_, err := vconn1.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 等待数据传输
	time.Sleep(100 * time.Millisecond)

	// 关闭发送端
	err = vconn1.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 给足够时间让关闭命令传输到接收端并被处理
	time.Sleep(200 * time.Millisecond)

	// 读取端应该能读到数据，然后收到 EOF
	buf := make([]byte, 1024)
	n, err := vconn2.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(buf[:n]) != string(testData) {
		t.Fatalf("Data mismatch: got %q, expected %q", buf[:n], testData)
	}

	// 第二次读应该返回 EOF（带超时）
	done := make(chan error, 1)
	go func() {
		_, err := vconn2.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Expected EOF, got nil")
		}
		// 收到 EOF 或其他错误，测试通过
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Read timeout waiting for EOF - close command may not have been received")
	}

	t.Log("CloseHandling test passed: connection closed properly")

	peer1.Close()
	peer2.Close()
}

// BenchmarkTrunkKCP_Write 写入性能基准测试
func BenchmarkTrunkKCP_Write(b *testing.B) {
	ctx := context.Background()

	s0, c0 := rpc.NewFakeConnPipe()
	peer1 := NewTrunkKCP(0x66666666, s0)
	peer2 := NewTrunkKCP(0x66666666, c0)

	go peer1.Run(ctx)
	go peer2.Run(ctx)

	// 给一点时间让所有 goroutine 启动
	time.Sleep(50 * time.Millisecond)

	vconn1 := peer1.GetConn(1)
	vconn2 := peer2.GetConn(1)

	// 启动接收端
	go func() {
		buf := make([]byte, 1024)
		for {
			_, err := vconn2.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	testData := []byte("Benchmark test data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vconn1.Write(testData)
	}
	b.StopTimer()

	vconn1.Close()
	peer1.Close()
	peer2.Close()
}
