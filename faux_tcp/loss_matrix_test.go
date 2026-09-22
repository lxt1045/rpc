package faux_tcp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lxt1045/rpc/trunk_kcp"
)

// ---------------------------------------------------------------------------
// 丢包率 × 带宽利用率 矩阵测试
// ---------------------------------------------------------------------------

// kcpCountConn 包装 net.Conn，统计 trunk_kcp 写到物理连接的 KCP 段：
// 字节数、段数、重复段数（sn 去重）。
type kcpCountConn struct {
	net.Conn
	bytes atomic.Int64
	segs  atomic.Int64
	dups  atomic.Int64
	mu    sync.Mutex
	seen  map[uint32]struct{}
}

func newKCPCountConn(c net.Conn) *kcpCountConn {
	return &kcpCountConn{Conn: c, seen: make(map[uint32]struct{})}
}

func (c *kcpCountConn) Write(bs []byte) (int, error) {
	// trunk_kcp 的 sendLoop 每次 Write 恰好一个 KCP 包
	if len(bs) >= 24 && bs[4] == 0x51 { // IKCP_CMD_PUSH
		sn := binary.LittleEndian.Uint32(bs[12:16])
		c.segs.Add(1)
		c.bytes.Add(int64(len(bs)))
		c.mu.Lock()
		if _, ok := c.seen[sn]; ok {
			c.dups.Add(1)
		} else {
			c.seen[sn] = struct{}{}
		}
		c.mu.Unlock()
	}
	return c.Conn.Write(bs)
}

// lossHook 用散列化的包序号做确定性伪随机丢包（线程安全、可复现、无模式偏差）
func lossHook(rate int) func(src, dst [4]byte, bs []byte) bool {
	var count atomic.Int64
	return func(src, dst [4]byte, bs []byte) bool {
		p, err := parsePacket(bs)
		if err != nil || len(p.Payload) == 0 {
			return true
		}
		if rate <= 0 {
			return true
		}
		x := count.Add(1)
		return (x*2654435761)%100 >= int64(rate) // Knuth 散列
	}
}

// TestLossMatrix 测量不同丢包率下的：
//   - 裸 faux_tcp 有效交付率（delivered/sent，验证"丢包=丢整条报文"语义）
//   - KCP over faux_tcp 的有效吞吐（goodput）与线上放大率（线上字节/载荷）
//
// 注：内存链路无真实 NIC/RTT，数值反映相对趋势而非绝对性能。
func TestLossMatrix(t *testing.T) {
	rates := []int{0, 1, 5, 10, 20, 30}

	fmt.Printf("\n| 丢包率 | 裸faux_tcp交付率 | KCP有效吞吐 | KCP线上放大率 | KCP重传段占比 |\n")
	fmt.Printf("| --- | --- | --- | --- | --- |\n")

	for _, rate := range rates {
		kcp := runLossCase(t, rate)
		raw := runRawCase(t, rate)
		fmt.Printf("| %d%% | %.1f%% | %.1f MB/s | %.3f | %.1f%% |\n",
			rate, raw, kcp.goodput, kcp.ratio, kcp.dupPct)
	}
}

type kcpResult struct {
	goodput float64 // MB/s
	ratio   float64 // 线上字节/载荷
	dupPct  float64 // 重传段占比
}

// runLossCase 在注入 rate% 丢包的链路上跑 KCP over faux_tcp
func runLossCase(t *testing.T, rate int) kcpResult {
	t.Helper()
	cli, srv, _, _ := testPair(t, nil, lossHook(rate))
	defer cli.Close()
	defer srv.Close()

	// 用计数器包装两条 faux_tcp 连接（trunk_kcp 的物理连接）
	cliCnt := newKCPCountConn(cli)
	srvCnt := newKCPCountConn(srv)

	const conv = 0x5a0b0003
	cliTrunk := trunk_kcp.NewTrunkKCP(conv, nil, cliCnt)
	srvTrunk := trunk_kcp.NewTrunkKCP(conv, nil, srvCnt)
	ctx := context.Background()
	go cliTrunk.Run(ctx)
	go srvTrunk.Run(ctx)
	defer cliTrunk.Close()
	defer srvTrunk.Close()

	cliV := cliTrunk.GetConn(1)
	srvV := srvTrunk.GetConn(1)

	const totalBytes = 4 << 20 // 4MB（加大样本降低内存链路调度抖动噪声）
	payload := make([]byte, 1400)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var recv atomic.Int64
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for recv.Load() < totalBytes {
			n, err := srvV.Read(buf)
			if err != nil {
				return
			}
			recv.Add(int64(n))
		}
	}()

	start := time.Now()
	for sent := 0; sent < totalBytes; {
		n, err := cliV.Write(payload)
		if err != nil {
			t.Fatal(err)
		}
		sent += n
	}
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("rate %d%%: transfer timeout, received %d/%d", rate, recv.Load(), totalBytes)
	}
	elapsed := time.Since(start)

	wire := cliCnt.bytes.Load() // 数据方向是 client→server，发送侧计数在 cliCnt
	ratio := float64(wire) / float64(totalBytes)
	goodput := float64(totalBytes) / (1 << 20) / elapsed.Seconds()
	dupPct := 100 * float64(cliCnt.dups.Load()) / float64(max(cliCnt.segs.Load(), 1))
	return kcpResult{goodput: goodput, ratio: ratio, dupPct: dupPct}
}

// runRawCase 裸 faux_tcp：并发读取下发送 500 个报文，统计交付率
// （顺序写再读会让接收队列（cap 256）溢出，混淆注入丢包）
func runRawCase(t *testing.T, rate int) float64 {
	t.Helper()
	cli, srv, _, _ := testPair(t, nil, lossHook(rate))
	defer cli.Close()
	defer srv.Close()

	var got atomic.Int64
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		buf := make([]byte, 256)
		drain := func(deadline time.Duration) {
			srv.SetReadDeadline(time.Now().Add(deadline))
			for {
				n, err := srv.Read(buf)
				if n > 0 {
					got.Add(1)
				}
				if err != nil {
					return
				}
			}
		}
		for {
			// 滚动 50ms 超时：到期醒来看 stop 是否关闭
			srv.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			n, err := srv.Read(buf)
			if n > 0 {
				got.Add(1)
			}
			if err != nil {
				select {
				case <-stop:
					drain(300 * time.Millisecond) // 主流程写完，静默收尾
					return
				default:
				}
			}
		}
	}()

	const total = 500
	for i := 0; i < total; i++ {
		if _, err := cli.Write([]byte(fmt.Sprintf("pkt-%04d", i))); err != nil {
			t.Fatal(err)
		}
		// 轻微限速（50µs/包 ≈ 20k pps），保证读取方始终跟得上，
		// 避免 chData（cap 256）溢出造成的额外丢包混淆注入丢包
		time.Sleep(50 * time.Microsecond)
	}
	close(stop)
	<-readerDone
	return 100 * float64(got.Load()) / float64(total)
}
