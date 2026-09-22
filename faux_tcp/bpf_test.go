//go:build linux

package faux_tcp

import (
	"encoding/binary"
	"testing"

	"golang.org/x/net/bpf"
)

// TestBPFFilterCookedOffsets 用 x/net/bpf 的用户态 VM 校验 cooked（SOCK_DGRAM）
// 模式的 cBPF 过滤器：缓冲从 IPv4 头开始（链路层头已被内核剥掉），
// 只放行 "IP proto == TCP && TCP dst port == 目标端口" 的报文。
//
// 这个测试很关键：新版本从 SOCK_RAW 换成 cooked SOCK_DGRAM 后，过滤器偏移从
// "以太网 14 + IP 9 / 14+X+16" 改成 "IP 9 / X+2"。如果偏移写错，真实链路上
// 会表现为"一个包都收不到、握手超时"，而内存链路单测（memLink 不过滤）不会
// 暴露；所以这里用 VM 直接跑过滤器。
func TestBPFFilterCookedOffsets(t *testing.T) {
	const localPort = 49152
	// NewVM 直接吃 Instruction 列表（Assemble 是给内核用的 RawInstruction）
	vm, err := bpf.NewVM(bpfTCPDstPortProgram(localPort))
	if err != nil {
		t.Fatal(err)
	}
	accept := func(name string, pkt []byte, want bool) {
		t.Helper()
		n, err := vm.Run(pkt)
		if err != nil {
			t.Fatalf("%s: vm run: %v", name, err)
		}
		if got := n != 0; got != want {
			t.Fatalf("%s: accept=%v want=%v (ret=%d)", name, got, want, n)
		}
	}

	cfg := Config{}
	cfg.defaults()
	src := testEndpoint(testClientIP, 40000)
	dst := testEndpoint(testServerIP, localPort)

	// 1) TCP + 目的端口匹配 → 放行（注意：buildPacket 输出正是 cooked 模式的
	//    缓冲形态：从 IPv4 头开始，无以太网头）
	pkt := buildPacket(&cfg, src, dst, 1000, 0, flagSYN, 0, 0, nil, 1)
	accept("tcp dst-port match", pkt, true)

	// 2) 目的端口不匹配 → 丢弃
	pkt = buildPacket(&cfg, src, testEndpoint(testServerIP, localPort+1), 1000, 0, flagSYN, 0, 0, nil, 1)
	accept("tcp dst-port mismatch", pkt, false)

	// 3) 非 TCP（UDP=17）→ 丢弃
	pkt = buildPacket(&cfg, src, dst, 1000, 0, flagSYN, 0, 0, nil, 1)
	pkt[9] = 17
	accept("non-tcp proto", pkt, false)

	// 4) 带 IP 选项（IHL=6，24 字节 IP 头）→ LoadMemShift/LoadIndirect 必须自适应：
	//    把 TCP 头整体后移 4 字节，并修正 IP 头长度/IHL/校验和。
	pkt = buildPacket(&cfg, src, dst, 1000, 0, flagSYN, 0, 0, nil, 1)
	withOpts := make([]byte, 0, len(pkt)+4)
	withOpts = append(withOpts, pkt[:20]...)            // IP 头
	withOpts = append(withOpts, 0x01, 0x01, 0x01, 0x00) // 4 字节选项（NOP*3+EOL）
	withOpts = append(withOpts, pkt[20:]...)            // TCP 头+载荷
	withOpts[0] = 0x46                                  // Ver=4, IHL=6
	binary.BigEndian.PutUint16(withOpts[2:4], uint16(len(withOpts)))
	binary.BigEndian.PutUint16(withOpts[10:12], 0) // IP 校验和先清零重算
	binary.BigEndian.PutUint16(withOpts[10:12], checksum(withOpts[:24]))
	accept("ipv4 with options (IHL=6)", withOpts, true)

	// 5) 带 IP 选项但目的端口不匹配 → 丢弃（确保 X 偏移真的生效，而不是恰好命中）
	pkt = buildPacket(&cfg, src, testEndpoint(testServerIP, 9999), 1000, 0, flagSYN, 0, 0, nil, 1)
	withOpts2 := make([]byte, 0, len(pkt)+4)
	withOpts2 = append(withOpts2, pkt[:20]...)
	withOpts2 = append(withOpts2, 0x01, 0x01, 0x01, 0x00)
	withOpts2 = append(withOpts2, pkt[20:]...)
	withOpts2[0] = 0x46
	binary.BigEndian.PutUint16(withOpts2[2:4], uint16(len(withOpts2)))
	binary.BigEndian.PutUint16(withOpts2[10:12], 0)
	binary.BigEndian.PutUint16(withOpts2[10:12], checksum(withOpts2[:24]))
	accept("ipv4 with options, wrong port", withOpts2, false)
}

// TestMatchTCPDstPortUserspace 调试模式的用户态过滤器与 cBPF 同语义。
func TestMatchTCPDstPortUserspace(t *testing.T) {
	const localPort = 49152
	cfg := Config{}
	cfg.defaults()
	src := testEndpoint(testClientIP, 40000)
	dst := testEndpoint(testServerIP, localPort)

	pkt := buildPacket(&cfg, src, dst, 1000, 0, flagSYN, 0, 0, nil, 1)
	if !matchTCPDstPort(pkt, localPort) {
		t.Fatal("should match dst port")
	}
	if matchTCPDstPort(pkt, localPort+1) {
		t.Fatal("should not match other port")
	}
	udp := append([]byte(nil), pkt...)
	udp[9] = 17
	if matchTCPDstPort(udp, localPort) {
		t.Fatal("should not match non-TCP")
	}
	if matchTCPDstPort(pkt[:10], localPort) {
		t.Fatal("truncated packet should not match")
	}

	// IP 选项（IHL=6）：TCP 头后移 4 字节，仍应命中
	withOpts := make([]byte, 0, len(pkt)+4)
	withOpts = append(withOpts, pkt[:20]...)
	withOpts = append(withOpts, 0x01, 0x01, 0x01, 0x00)
	withOpts = append(withOpts, pkt[20:]...)
	withOpts[0] = 0x46
	binary.BigEndian.PutUint16(withOpts[2:4], uint16(len(withOpts)))
	if !matchTCPDstPort(withOpts, localPort) {
		t.Fatal("IHL=6 packet should match")
	}
}
