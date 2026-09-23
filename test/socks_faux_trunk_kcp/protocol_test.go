package socks_faux_kcp

import (
	"bytes"
	"testing"
	"time"

	"github.com/lxt1045/rpc/trunk_kcp"
)

func TestWriteReadOpenHeader(t *testing.T) {
	var buf bytes.Buffer
	addr := "example.com:443"
	head := []byte("HEAD")
	if err := WriteOpenHeader(&buf, addr, head); err != nil {
		t.Fatal(err)
	}
	msg, err := ReadOpenHeader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Addr != addr {
		t.Fatalf("addr = %q, want %q", msg.Addr, addr)
	}
	if string(msg.Body) != string(head) {
		t.Fatalf("head = %q, want %q", msg.Body, head)
	}
}

func TestACL(t *testing.T) {
	if !CheckACL("1.2.3.4:80") {
		t.Fatalf("ACL disabled should allow")
	}
	if err := ConfigureACL(ACLConfig{Enabled: true, AllowNetworks: []string{"10.0.0.0/8"}, DenyHosts: []string{"internal.example.com"}}); err != nil {
		t.Fatal(err)
	}
	if !CheckACL("10.1.2.3:443") {
		t.Fatalf("allowed network rejected")
	}
	if CheckACL("8.8.8.8:443") {
		t.Fatalf("ip outside allow network accepted")
	}
	ConfigureACL(ACLConfig{})
}

func TestConfigDefaults(t *testing.T) {
	c := ClientConfig{Token: "token", ClientConn: ConnConfig{Addr: "127.0.0.1:18099"}}
	if err := ValidateClientConfig(&c); err != nil {
		t.Fatal(err)
	}
	if c.Trunk.Conv == 0 || c.Trunk.MaxConns == 0 || c.Trunk.MaxVirtualConn == 0 {
		t.Fatalf("defaults not applied: %+v", c.Trunk)
	}

	// 缺少对端地址：faux_tcp 无法拨号，必须显式报错
	if err := ValidateClientConfig(&ClientConfig{Token: "token"}); err == nil {
		t.Fatal("missing client-conn.addr should be rejected")
	}

	// MSS 小于 KCP 线上包：数据报边界硬约束，必须显式报错
	bad := ClientConfig{
		Token:      "token",
		ClientConn: ConnConfig{Addr: "127.0.0.1:18099"},
		FauxTCP:    FauxTCPConfig{MSS: 1000},
	}
	if err := ValidateClientConfig(&bad); err == nil {
		t.Fatal("MSS < KCP packet size should be rejected")
	}

	srvBad := ServerConfig{Token: "token", Conn: ConnConfig{Addr: ":18099"}, FauxTCP: FauxTCPConfig{MSS: 1000}}
	if err := ValidateServerConfig(&srvBad); err == nil {
		t.Fatal("server: MSS < KCP packet size should be rejected")
	}
}

func TestNoDelayParam(t *testing.T) {
	// 未配置：全部 -1，ApplyKCPParam 不动库默认值
	var empty *TrunkKCPConfig
	n, nc := empty.NoDelayParam()
	if n != -1 || nc != -1 {
		t.Fatalf("nil config should yield -1/-1, got %d %d", n, nc)
	}
	cfg := &TrunkKCPConfig{}
	n, nc = cfg.NoDelayParam()
	if n != -1 || nc != -1 {
		t.Fatalf("empty config should yield -1/-1, got %d %d", n, nc)
	}
	empty.ApplyKCPParam(nil) // must not panic
	cfg.ApplyKCPParam(nil)   // must not panic

	// 部分配置：未配置项保持 -1
	zero, one := 0, 1
	cfg = &TrunkKCPConfig{KCPNoDelay: &zero, KCPNc: &one}
	n, nc = cfg.NoDelayParam()
	if n != 0 || nc != 1 {
		t.Fatalf("partial config mapping wrong, got %d %d", n, nc)
	}
}

// TestKCPWindowConfig 钉住窗口配置语义：
//   - 默认**不覆盖**库窗口（保持 1024/1024），由使用者按链路显式设置；
//   - 显式 kcp_sndwnd/kcp_rcvwnd → 固定窗口，并且实际生效值要从 Stats 里读得到。
//
// 背景（真机）：库默认 1024 段在 30Mbps 限速出口上相当于 8×BDP，带宽被重传吃掉
// （实测放大 3.2~3.8x）；但把收发窗口一起写死成 128 段又把长 RTT 链路饿死
// （实测只占 7Mbps、下载 300kB/s）——所以窗口必须按链路设，不能设完不检查。
func TestKCPWindowConfig(t *testing.T) {
	var zero TrunkKCPConfig
	NormalizeTrunkKCPConfig(&zero)
	if zero.KCPSndWnd != 0 || zero.KCPRcvWnd != 0 {
		t.Fatalf("默认不应覆盖库窗口，得到 snd=%d rcv=%d", zero.KCPSndWnd, zero.KCPRcvWnd)
	}
	cfg := TrunkKCPConfig{KCPSndWnd: 256, KCPRcvWnd: 1024}
	NormalizeTrunkKCPConfig(&cfg)
	if cfg.KCPSndWnd != 256 || cfg.KCPRcvWnd != 1024 {
		t.Fatalf("显式窗口被覆盖: snd=%d rcv=%d", cfg.KCPSndWnd, cfg.KCPRcvWnd)
	}
	cfg.ApplyKCPParam(nil) // must not panic

	tr := trunk_kcp.NewTrunkKCP(0x5a0b0009, nil)
	(&TrunkKCPConfig{KCPSndWnd: 256, KCPRcvWnd: 1024}).ApplyKCPParam(tr)
	if st := tr.Stats(); st.SndWnd != 256 || st.RcvWnd != 1024 {
		t.Fatalf("固定窗口未生效: snd=%d rcv=%d", st.SndWnd, st.RcvWnd)
	}
}

func TestHandshakeBudget(t *testing.T) {
	// 零值：faux_tcp 默认 1s × (3+1)
	if got := (FauxTCPConfig{}).HandshakeBudget(); got != 4*time.Second {
		t.Fatalf("default budget = %v, want 4s", got)
	}
	// 配置文件里的 3s × (3+1)
	cfg := FauxTCPConfig{HandshakeTimeoutMS: 3000, HandshakeRetries: 3}
	if got := cfg.HandshakeBudget(); got != 12*time.Second {
		t.Fatalf("3s budget = %v, want 12s", got)
	}
	// 单次超时不小于零值时用默认
	if got := (FauxTCPConfig{HandshakeRetries: 1}).HandshakeBudget(); got != 2*time.Second {
		t.Fatalf("1 retry budget = %v, want 2s", got)
	}
}
