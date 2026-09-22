package faux_tcp

import (
	"context"
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lxt1045/rpc/trunk_kcp"
)

// TestTrunkKCPOverFauxTCP 全链路集成：trunk_kcp（KCP ARQ）跑在 faux_tcp 之上。
// 在 10% 丢包的链路上：
//   - 裸 faux_tcp 会丢数据（TestNoRetransmitUnderLoss 已验证这是特性）
//   - 叠加 KCP 后全部数据完整到达（KCP 负责重传，职责分层正确）
func TestTrunkKCPOverFauxTCP(t *testing.T) {
	var dropped, total atomic.Int64
	hook := func(src, dst [4]byte, bs []byte) bool {
		p, err := parsePacket(bs)
		if err != nil || len(p.Payload) == 0 {
			return true // 不动握手/纯 ACK
		}
		if total.Add(1)%10 == 0 {
			dropped.Add(1)
			return false // 10% 丢包
		}
		return true
	}

	cli, srv, _, _ := testPair(t, nil, hook)
	defer cli.Close()
	defer srv.Close()

	const conv = 0x5a0b0002
	cliTrunk := trunk_kcp.NewTrunkKCP(conv, nil, cli)
	srvTrunk := trunk_kcp.NewTrunkKCP(conv, nil, srv)
	ctx := context.Background()
	go cliTrunk.Run(ctx)
	go srvTrunk.Run(ctx)
	defer cliTrunk.Close()
	defer srvTrunk.Close()

	cliV := cliTrunk.GetConn(1)
	srvV := srvTrunk.GetConn(1)

	const totalBytes = 4 << 20 // 4MB
	payload := make([]byte, 8192)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}

	// 校验和累加器（简单完整性校验：字节求和 + 计数）
	var recvSum, recvCnt atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 8192)
		for recvCnt.Load() < totalBytes {
			n, err := srvV.Read(buf)
			if err != nil {
				return
			}
			for _, b := range buf[:n] {
				recvSum.Add(int64(b))
			}
			recvCnt.Add(int64(n))
		}
	}()

	var sendSum int64
	start := time.Now()
	for sent := 0; sent < totalBytes; {
		n, err := cliV.Write(payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range payload[:n] {
			sendSum += int64(b)
		}
		sent += n
	}

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("transfer timeout: received %d/%d bytes", recvCnt.Load(), totalBytes)
	}

	if recvCnt.Load() != totalBytes || recvSum.Load() != sendSum {
		t.Fatalf("integrity: received %d bytes sum %d, want %d bytes sum %d",
			recvCnt.Load(), recvSum.Load(), totalBytes, sendSum)
	}
	elapsed := time.Since(start)
	t.Logf("KCP over faux_tcp: %d bytes in %v (%.1f MB/s), link dropped %d/%d packets, all recovered by KCP",
		totalBytes, elapsed, float64(totalBytes)/(1<<20)/elapsed.Seconds(), dropped.Load(), total.Load())
}
