//go:build linux

package fake_tcp

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// raw_linux_test.go：RawTCP 模式的 lo 回环测试。
// 需要 root（AF_PACKET + raw socket + iptables/nft），用环境变量门控：
//
//	sudo FAKE_TCP_RAW_TEST=1 go test -run '^TestRawTCP' -v ./fake_tcp
//
// tcpdump 验收清单见 plan.md M3：sudo tcpdump -i lo -nn 'tcp port <port>' -vv

func rawGate(t *testing.T) {
	t.Helper()
	if os.Getenv("FAKE_TCP_RAW_TEST") != "1" {
		t.Skip("设置 FAKE_TCP_RAW_TEST=1 且以 root 运行时启用")
	}
	if os.Geteuid() != 0 {
		t.Skip("需要 root（AF_PACKET/raw socket/防火墙规则）")
	}
}

// TestRawTCPLoopback lo 回环全链路：握手 → 双向传输 → 关闭
func TestRawTCPLoopback(t *testing.T) {
	rawGate(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Mode:             ModeRawTCP,
		LocalAddr:        "127.0.0.1:0", // 端口 0：自动挑选空闲端口
		Magic:            0x66aa55cc,
		Keepalive:        time.Hour,
		HandshakeRetries: 3,
		AutoFirewall:     true,
	}
	l, err := Listen(ctx, cfg)
	if err != nil {
		t.Skipf("RawTCP 不可用（%v），跳过", err) // 无 iptables/权限不全时降级为 Skip
	}
	defer l.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", l.Addr().Port)

	ccfg := cfg
	ccfg.LocalAddr = "127.0.0.1:0"
	ccfg.RemoteAddr = addr

	cli, err := Dial(ctx, ccfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()
	srv, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	// 双向传输：序号块校验（RawTCP 到达即交；lo 上应近似零丢失零乱序）
	const chunk = 1336 // MaxPayload = MTU(1400) - 64
	const nChunks = 512

	type result struct {
		st  seqStats
		err error
	}
	wDone := make(chan error, 2)
	rDone := make(chan result, 2)

	go func() { wDone <- writeSeqChunks(cli, chunk, nChunks, 0) }()
	go func() { wDone <- writeSeqChunks(srv, chunk, nChunks, 1<<20) }()
	go func() { st, err := checkSeqChunks(srv, chunk, nChunks, 0); rDone <- result{st, err} }()
	go func() { st, err := checkSeqChunks(cli, chunk, nChunks, 1<<20); rDone <- result{st, err} }()

	if err := <-wDone; err != nil {
		t.Fatalf("写: %v", err)
	}
	if err := <-wDone; err != nil {
		t.Fatalf("写: %v", err)
	}
	r1 := <-rDone
	r2 := <-rDone
	for name, r := range map[string]result{"c→s": r1, "s→c": r2} {
		t.Logf("%s: %+v err=%v", name, r.st, r.err)
		if r.err != nil {
			t.Fatalf("%s 读: %v", name, r.err)
		}
		if r.st.dup != 0 || r.st.corrupt != 0 {
			t.Fatalf("%s 重复/损坏: %+v", name, r.st)
		}
		if r.st.loss > nChunks/20 || r.st.reorder > nChunks/20 {
			t.Fatalf("%s 丢失/乱序过多: %+v", name, r.st)
		}
	}

	// 关闭：对端读 EOF
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = srv.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, err := srv.Read(make([]byte, 1))
		if err != nil {
			break
		}
	}
}

// TestRawTCPKernelRSTSuppressed 验证内核 RST 抑制生效：
// 握手后内核若回 RST，中间盒语义下连接即死——这里直接观察会话是否被误回收
func TestRawTCPKernelRSTSuppressed(t *testing.T) {
	rawGate(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Mode:         ModeRawTCP,
		LocalAddr:    "127.0.0.1:0",
		Magic:        0x66aa55cc,
		Keepalive:    200 * time.Millisecond,
		AutoFirewall: true,
	}
	l, err := Listen(ctx, cfg)
	if err != nil {
		t.Skipf("RawTCP 不可用（%v），跳过", err)
	}
	defer l.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", l.Addr().Port)

	ccfg := cfg
	ccfg.LocalAddr = "127.0.0.1:0"
	ccfg.RemoteAddr = addr
	cli, err := Dial(ctx, ccfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()
	srv, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	// 空闲 1s（跨越多个保活周期）：若内核 RST 未被抑制，会话早被重置
	time.Sleep(time.Second)
	if cli.sess.isClosed() || srv.sess.isClosed() {
		t.Fatalf("会话被意外关闭（内核 RST 抑制失效？）")
	}
	if _, err := cli.Write([]byte("still alive")); err != nil {
		t.Fatalf("Write: %v", err)
	}
}
