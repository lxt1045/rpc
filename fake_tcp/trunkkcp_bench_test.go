package fake_tcp

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lxt1045/rpc/trunk_kcp"
)

// trunkkcp_bench_test.go：fake_tcp + trunk_kcp 组合在"丢包率 → 带宽利用率"上的量化测试。
//
// 拓扑：trunk_kcp(虚拟连接) ── fake_tcp.Conn（RawTCP 语义，内存管道）── lossyLink（定距丢包）
//
// 丢包注入在 fake_tcp 的 LinkIO 层（服务端→客户端方向，定距：每 N 个数据段丢 1 个）。
// fake_tcp 本身不重传；上层 trunk_kcp 的 KCP 负责可靠性与重传——正是生产组合形态。
// 测量口径：
//   - 交付率  = 应用层收到的字节 / 应用层发送的字节
//   - 线上字节 = fake_tcp 链路层尝试发出的应用载荷总字节（含 KCP 重传；不含每段
//     恒定的 fake_tcp 私有头 12B + TCP/IP 头 52B，约 +4.5% 固定开销）
//   - 带宽利用率 = 应用字节 / 线上字节；带宽放大 = 线上字节 / 应用字节
//
// 帧对齐前提：fake_tcp MTU=1500 → MaxPayload=1436 ≥ KCP 最大段 1400，
// 保证一个 KCP 段恰好一个 fake_tcp 段（trunk_kcp recvLoop 按流解析，丢段即"整帧丢失"，
// 不会半帧错位——见 plan.md M4 与 trunk_kcp/README.md）。

// lossyLink 定距丢包的 LinkIO 包装（确定性，便于复现）
type lossyLink struct {
	LinkIO
	dropEvery uint64 // 每 dropEvery 个数据段丢 1 个；0 = 不丢

	seenData    atomic.Int64 // 经过的数据段数
	droppedData atomic.Int64 // 被丢的数据段数
	wireBytes   atomic.Int64 // 尝试发出的应用载荷字节数（含被丢的）
}

func (l *lossyLink) WriteSegment(seg *Segment) error {
	if len(seg.Payload) > 0 {
		l.seenData.Add(1)
		l.wireBytes.Add(int64(len(seg.Payload)))
		if l.dropEvery > 0 && uint64(l.seenData.Load())%l.dropEvery == 0 {
			l.droppedData.Add(1)
			return nil // 丢包：不交给底层
		}
	}
	return l.LinkIO.WriteSegment(seg)
}

// benchEnv 一组带丢包的 fake_tcp 管道连接
func benchEnv(t *testing.T, dropEvery uint64, datagram bool) (cli *Conn, srv *Conn, lossy *lossyLink, cleanup func()) {
	t.Helper()
	cfg := Config{
		MTU:              1500, // MaxPayload=1436 ≥ KCP 段 1400，帧对齐
		DatagramOnly:     datagram,
		Keepalive:        time.Hour,
		HandshakeRetries: 3,
		RecvQueue:        65536,
	}
	srvLink, cliLink := pipePair()
	// 帧对齐关键：MaxPayload 必须 ≥ KCP 最大段（1400），保证一个 KCP 段一个 fake_tcp 段。
	// 生产对应：RawTCP 模式 MTU ≥ 1464（MaxPayload=MTU-64）。
	srvLink.maxPayload = 1436
	cliLink.maxPayload = 1436
	lossy = &lossyLink{LinkIO: srvLink, dropEvery: dropEvery} // 服务端→客户端方向丢包

	ctx, cancel := context.WithCancel(context.Background())
	srvPeer := PeerAddr{IP: netip.MustParseAddr("10.9.9.1"), Port: 8443}
	cliPeer := PeerAddr{IP: netip.MustParseAddr("10.9.9.2"), Port: 54321}
	l := listenLink(ctx, cfg, lossy, srvPeer, nil)
	conn, err := dialLink(ctx, cfg, cliLink, cliPeer, srvPeer, nil)
	if err != nil {
		cancel()
		t.Fatalf("dialLink: %v", err)
	}
	sc, err := l.Accept()
	if err != nil {
		cancel()
		t.Fatalf("Accept: %v", err)
	}
	return conn, sc.(*Conn), lossy, func() {
		_ = l.Close()
		_ = cliLink.Close()
		_ = srvLink.Close()
		cancel()
	}
}

// benchRow 一行测量结果
type benchRow struct {
	Loss       float64       // 标称丢包率
	Duration   time.Duration // 传输耗时
	AppBytes   int64         // 应用层交付字节
	WireBytes  int64         // 线上字节（fake_tcp 载荷口径）
	Dropped    int64         // 被丢段数
	DeliveryOK bool          // 交付完整性（哈希一致）
}

func (r benchRow) goodputMbps() float64 {
	return float64(r.AppBytes) * 8 / r.Duration.Seconds() / 1e6
}
func (r benchRow) wireAmp() float64 { return float64(r.WireBytes) / float64(r.AppBytes) }
func (r benchRow) util() float64    { return float64(r.AppBytes) / float64(r.WireBytes) }

// benchTrunkKCP  trunk_kcp over fake_tcp：交付率恒 100%（KCP 重传），看线上放大与吞吐
func benchTrunkKCP(t *testing.T, dropEvery uint64, nbytes int64) benchRow {
	t.Helper()
	cli, srv, lossy, cleanup := benchEnv(t, dropEvery, true) // 数据报模式：帧原子性由构造保证
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const conv = 0x1234abcd
	trunkS := trunk_kcp.NewTrunkKCP(conv, nil, srv)
	trunkC := trunk_kcp.NewTrunkKCP(conv, nil, cli)
	defer trunkS.Close()
	defer trunkC.Close()
	go trunkS.Run(ctx)
	go trunkC.Run(ctx)

	vs := trunkS.GetConn(1)
	vc := trunkC.GetConn(1)

	// 确定性数据（可哈希校验）
	data := make([]byte, nbytes)
	for i := range data {
		data[i] = byte(i*31 + i>>13)
	}
	want := sha256.Sum256(data)

	recvDone := make(chan error, 1)
	var recvBytes int64
	go func() {
		got := make([]byte, 0, nbytes)
		buf := make([]byte, 64*1024)
		for int64(len(got)) < nbytes {
			n, err := vc.Read(buf)
			if err != nil {
				recvDone <- err
				return
			}
			got = append(got, buf[:n]...)
		}
		atomic.StoreInt64(&recvBytes, int64(len(got)))
		if sha256.Sum256(got) != want {
			recvDone <- fmt.Errorf("数据哈希不符")
			return
		}
		recvDone <- nil
	}()

	start := time.Now()
	go func() { // 服务端全速发
		for sent := int64(0); sent < nbytes; {
			m := int64(32 * 1024)
			if sent+m > nbytes {
				m = nbytes - sent
			}
			n, err := vs.Write(data[sent : sent+m])
			if err != nil {
				return
			}
			sent += int64(n)
		}
	}()

	select {
	case err := <-recvDone:
		dur := time.Since(start)
		if err != nil {
			t.Fatalf("丢包率 1/%d 收数据失败: %v", dropEvery, err)
		}
		return benchRow{
			Duration:   dur,
			AppBytes:   recvBytes,
			WireBytes:  lossy.wireBytes.Load(),
			Dropped:    lossy.droppedData.Load(),
			DeliveryOK: true,
		}
	case <-time.After(90 * time.Second):
		t.Fatalf("丢包率 1/%d 传输超时（已交付 %d/%d 字节）", dropEvery, atomic.LoadInt64(&recvBytes), nbytes)
		return benchRow{}
	}
}

// benchRawFakeTCP 裸 fake_tcp（无 KCP）：不重传，交付率即 (1-丢包率)
func benchRawFakeTCP(t *testing.T, dropEvery uint64, nbytes int64) (delivery float64, dur time.Duration) {
	t.Helper()
	cli, srv, _, cleanup := benchEnv(t, dropEvery, false) // 流式模式：大 Write 内部切片
	defer cleanup()
	_ = cli

	data := make([]byte, nbytes)
	for i := range data {
		data[i] = byte(i*31 + i>>13)
	}

	start := time.Now()
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for sent := int64(0); sent < nbytes; {
			m := int64(64 * 1024)
			if sent+m > nbytes {
				m = nbytes - sent
			}
			n, err := srv.Write(data[sent : sent+m])
			if err != nil {
				return
			}
			sent += int64(n)
		}
	}()

	// 写到完后等 200ms 排空，统计实际交付
	var got int64
	buf := make([]byte, 64*1024)
	for {
		n, err := cli.Read(buf)
		got += int64(n)
		if err != nil {
			break
		}
		select {
		case <-writeDone:
			// 写完后用短超时判定排空
			_ = cli.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		default:
		}
	}
	return float64(got) / float64(nbytes), time.Since(start)
}

// TestTrunkKCPLossBench 丢包率 → 带宽利用率关系（结果写入 fake_tcp/README.md）
func TestTrunkKCPLossBench(t *testing.T) {
	const nbytes = 4 << 20 // 每行 4MB
	rates := []struct {
		name      string
		dropEvery uint64
	}{
		{"0%", 0},
		{"0.5%", 200},
		{"1%", 100},
		{"2%", 50},
		{"5%", 20},
		{"10%", 10},
		{"20%", 5},
		{"33%", 3},
	}

	t.Logf("| 丢包率 | 裸fake_tcp交付率 | trunk_kcp交付率 | trunk_kcp有效吞吐 | 带宽放大 | 带宽利用率 |")
	t.Logf("|---|---|---|---|---|---|")
	for _, r := range rates {
		// 裸 fake_tcp 基线
		delivery, _ := benchRawFakeTCP(t, r.dropEvery, nbytes)
		// trunk_kcp 组合
		row := benchTrunkKCP(t, r.dropEvery, nbytes)
		row.Loss = 0
		if r.dropEvery > 0 {
			row.Loss = 100.0 / float64(r.dropEvery)
		}
		t.Logf("| %s | %.1f%% | %.0f%%（%d段丢失已重传） | %.1f Mbps | %.3fx | %.1f%% |",
			r.name,
			delivery*100,
			float64(row.AppBytes)/float64(nbytes)*100,
			row.Dropped,
			row.goodputMbps(),
			row.wireAmp(),
			row.util()*100,
		)
	}
}
