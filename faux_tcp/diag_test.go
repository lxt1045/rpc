package faux_tcp

import (
	"context"
	stderr "errors"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lxt1045/errors"
)

// failSendLink 包装链路：WritePacket 恒失败，用于验证握手超时诊断能区分
// "本机发不出去" 与 "对端没回"。
type failSendLink struct {
	Link
	sendErr error
}

func (l *failSendLink) WritePacket(bs []byte) error { return l.sendErr }

// TestHandshakeTimeoutDiagnostics 握手超时错误必须能自诊断：
//   - 对端无回包（常见于服务端未运行 / 云安全组未放行）→ 报"收到 0 个报文"
//   - 本机 raw socket 发包失败 → 报发送失败次数与最后错误
func TestHandshakeTimeoutDiagnostics(t *testing.T) {
	newCfg := func() Config {
		cfg := Config{HandshakeTimeout: 20 * time.Millisecond, HandshakeRetries: 2}
		cfg.defaults()
		return cfg
	}
	dial := func(l Link) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := DialWithLink(ctx, newCfg(), l,
			testEndpoint(testClientIP, 40000), testEndpoint(testServerIP, 9999))
		return err
	}

	// 1) 静默对端：没有任何回包
	err := dial(newMemNet().link(testClientIP))
	if err == nil {
		t.Fatal("want handshake timeout")
	}
	if got := stderr.Unwrap(err); got != nil {
		err = got
	}
	msg := err.Error()
	if !strings.Contains(msg, "收到 0 个报文") {
		t.Fatalf("silent peer: want 收到 0 个报文 in %q", msg)
	}
	if !strings.Contains(msg, "安全组") {
		t.Fatalf("silent peer: want actionable hint in %q", msg)
	}
	if code := errors.AsCode(err); code == nil || code.Code() != ErrHandshakeTimeout.Code() {
		t.Fatalf("want ErrHandshakeTimeout code, got %v", err)
	}
	if !strings.Contains(msg, "握手超时") {
		t.Fatalf("want 握手超时 in %q", msg)
	}

	// 2) 本机发包失败：必须暴露 send 错误，而不是伪装成普通超时
	err = dial(&failSendLink{Link: newMemNet().link(testClientIP), sendErr: stderr.New("mock raw send failure")})
	if err == nil {
		t.Fatal("want handshake timeout")
	}
	if got := stderr.Unwrap(err); got != nil {
		err = got
	}
	msg = err.Error()
	if !strings.Contains(msg, "mock raw send failure") {
		t.Fatalf("send failure: want underlying error in %q", msg)
	}
	if !strings.Contains(msg, "发送失败") {
		t.Fatalf("send failure: want 发送失败 count in %q", msg)
	}
	if !strings.Contains(msg, "raw socket") {
		t.Fatalf("send failure: want actionable hint in %q", msg)
	}
}

// TestReplySrc 无状态 DNAT 环境（Docker 端口映射）下回包源地址必须改写为对外地址。
// 判据：客户端 telnet 该端口（内核 TCP）能通、faux_tcp 握手超时。
func TestReplySrc(t *testing.T) {
	cfg := Config{ReplySrc: netip.MustParseAddr("203.0.113.7")}
	cfg.defaults()

	net0 := newMemNet()
	rec := &recorder{}
	net0.hook = func(src, dst [4]byte, bs []byte) bool {
		if p, err := parsePacket(bs); err == nil {
			rec.add(p)
		}
		return true
	}
	// 监听在 0.0.0.0（DNAT 后目的地址是容器内网地址），链路归属 testServerIP
	ln := listenWithLink(cfg, net0.link(testServerIP),
		Endpoint{IP: netip.IPv4Unspecified(), Port: 8080})
	defer ln.Close()

	syn := buildPacket(&cfg, testEndpoint(testClientIP, 40000),
		testEndpoint(testServerIP, 8080), 1000, 0, flagSYN, clockMS(), 0, nil, 1)
	net0.deliver(testClientIP, testServerIP, syn)

	waitFor(t, time.Second, func() bool { return len(rec.snapshot()) >= 2 }, "synack")
	for _, p := range rec.snapshot() {
		if p.Has(flagSYN) && p.Has(flagACK) {
			if got := p.Src.IP; got != cfg.ReplySrc {
				t.Fatalf("SYN+ACK src=%s, want reply_src=%s", got, cfg.ReplySrc)
			}
			if p.Dst.IP != netip.AddrFrom4(testClientIP) {
				t.Fatalf("SYN+ACK dst=%s, want client ip", p.Dst.IP)
			}
			return
		}
	}
	t.Fatal("no SYN+ACK captured")
}

// TestReplySrcDefaultUsesPacketDst 未配置 reply_src 时，回包源地址必须取"报文的
// 目的 IP"——这正是云主机 1:1 NAT 场景所需要的：源地址是网卡内网地址，由平台 NAT
// 转成公网，与内核 TCP 同路。（填公网 IP 反而会被平台源地址校验丢弃。）
func TestReplySrcDefaultUsesPacketDst(t *testing.T) {
	var cfg Config
	cfg.defaults()

	net0 := newMemNet()
	rec := &recorder{}
	net0.hook = func(src, dst [4]byte, bs []byte) bool {
		if p, err := parsePacket(bs); err == nil {
			rec.add(p)
		}
		return true
	}
	// 监听 0.0.0.0，SYN 的目的是 testServerIP → 回包源地址应为 testServerIP
	ln := listenWithLink(cfg, net0.link(testServerIP),
		Endpoint{IP: netip.IPv4Unspecified(), Port: 8080})
	defer ln.Close()

	syn := buildPacket(&cfg, testEndpoint(testClientIP, 40000),
		testEndpoint(testServerIP, 8080), 1000, 0, flagSYN, clockMS(), 0, nil, 1)
	net0.deliver(testClientIP, testServerIP, syn)

	waitFor(t, time.Second, func() bool { return len(rec.snapshot()) >= 2 }, "synack")
	for _, p := range rec.snapshot() {
		if p.Has(flagSYN) && p.Has(flagACK) {
			if got := p.Src.IP; got != netip.AddrFrom4(testServerIP) {
				t.Fatalf("SYN+ACK src=%s, want 报文目的 IP %s", got, netip.AddrFrom4(testServerIP))
			}
			return
		}
	}
	t.Fatal("no SYN+ACK captured")
}

// TestIsLocalAddr 钉住 listen 启动告警的判据：本机网卡地址（回环）为真，
// 公网/保留地址为假——假的那一类正是云平台源地址校验会丢弃的源地址。
func TestIsLocalAddr(t *testing.T) {
	if !isLocalAddr(netip.MustParseAddr("127.0.0.1")) {
		t.Fatal("127.0.0.1 should be local")
	}
	// TEST-NET-3 / TEST-NET-1，不可能配在本机
	for _, s := range []string{"203.0.113.7", "192.0.2.1"} {
		if isLocalAddr(netip.MustParseAddr(s)) {
			t.Fatalf("%s should not be reported as local", s)
		}
	}
	if isLocalAddr(netip.Addr{}) {
		t.Fatal("invalid addr must not be local")
	}
}

// TestPickLocalPort 钉住源端口选择：必须落在本机 ephemeral 范围内
// （/proc/sys/net/ipv4/ip_local_port_range），并且是可以 bind 的空闲端口。
// 真机踩过：硬编码 49152-65535 而本机范围是 44620-48715，源端口落在范围外时
// 回程 SYN+ACK 被上游设备丢弃（同端口内核 TCP 正常）。
func TestPickLocalPort(t *testing.T) {
	lo, hi := 32768, 60999
	if b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range"); err == nil {
		var a, c int
		if _, err := fmt.Sscanf(string(b), "%d %d", &a, &c); err == nil && a >= 1024 && c > a && c <= 65535 {
			lo, hi = a, c
		}
	}
	for i := 0; i < 16; i++ {
		p, err := pickLocalPort()
		if err != nil {
			t.Fatalf("pickLocalPort: %v", err)
		}
		if int(p) < lo || int(p) > hi {
			t.Fatalf("端口 %d 不在 ephemeral 范围 %d-%d", p, lo, hi)
		}
		if !localPortFree(int(p)) {
			t.Fatalf("端口 %d 被选出来了但已被占用", p)
		}
		t.Logf("ephemeral 范围 %d-%d，选中源端口 %d", lo, hi, p)
	}
}

// TestAdvMSSAdvertised 钉住"MSS（本端发多大）"与"AdvMSS（对外通告多大）"分离：
// 路径 MTU 受限把 MSS 压到 1240 之后，SYN/SYN+ACK 里仍要能通告 1460，
// 否则 MSS-clamp 中间盒不改写、不建流状态，回程 SYN+ACK 会被丢弃。
func TestAdvMSSAdvertised(t *testing.T) {
	advertised := func(cfg Config) uint16 {
		cfg.defaults()
		synack := buildPacket(&cfg, testEndpoint(testServerIP, 8080),
			testEndpoint(testClientIP, 40000), 1000, 2000, flagSYN|flagACK, clockMS(), 0, nil, 1)
		p, err := parsePacket(synack)
		if err != nil {
			t.Fatalf("parsePacket: %v", err)
		}
		return p.MSS
	}

	// 默认：通告值 = MSS
	if got := advertised(Config{MSS: 1240}); got != 1240 {
		t.Fatalf("默认通告 MSS=%d, want 1240（= MSS）", got)
	}
	// 显式分离：MSS 压到 1240，仍通告 1460
	if got := advertised(Config{MSS: 1240, AdvMSS: 1460}); got != 1460 {
		t.Fatalf("通告 MSS=%d, want 1460（AdvMSS）", got)
	}
	// 非法 AdvMSS：validate 必须报错（defaults 之后调用）
	cfg := Config{AdvMSS: 70000}
	cfg.defaults()
	if err := cfg.validate(); err == nil {
		t.Fatal("want error for AdvMSS > 65495")
	}
}
