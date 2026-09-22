//go:build linux

package faux_tcp

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestFauxTCPLoopback 真实链路集成测试（loopback + raw socket）。
// 需要 root/CAP_NET_RAW；内核 RST 抑制规则由 Listen/Dial 自动安装
// （Config.ManualFirewall=true 时需手工配置，见 README）。
// 用 FAUXTCP_E2E=1 显式开启：
//
//	FAUXTCP_E2E=1 sudo -E go test -run TestFauxTCPLoopback -v ./faux_tcp/
func TestFauxTCPLoopback(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("need root/CAP_NET_RAW")
	}
	if os.Getenv("FAUXTCP_E2E") == "" {
		t.Skip("set FAUXTCP_E2E=1 to run (needs iptables RST suppression, see comment)")
	}

	cfg := Config{}
	cfg.defaults()

	ctx := context.Background()
	ln, err := Listen(ctx, cfg, "127.0.0.1:18099")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		// echo
		buf := make([]byte, 2048)
		for {
			n, err := c.Read(buf)
			if err != nil {
				return
			}
			if _, err := c.Write(buf[:n]); err != nil {
				return
			}
		}
	}()

	cli, err := Dial(ctx, cfg, "127.0.0.1:40999", "127.0.0.1:18099")
	if err != nil {
		t.Fatalf("dial: %v (check iptables RST suppression)", err)
	}
	defer cli.Close()

	msg := []byte("hello loopback faux tcp")
	if _, err := cli.Write(msg); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	cli.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := cli.Read(buf)
	if err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf[:n]) != string(msg) {
		t.Fatalf("echo mismatch: %q", buf[:n])
	}
	t.Logf("loopback echo ok: %q", buf[:n])

	// 用 tcpdump 验证线上特征：
	//   sudo tcpdump -i lo -nn 'tcp port 18099' -w faux.pcap
	// 应看到：SYN/SYN+ACK/ACK 握手、PSH+ACK 数据段、FIN 挥手、每个 seq 只出现一次。
}
