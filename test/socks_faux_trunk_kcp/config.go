package socks_faux_kcp

import (
	"crypto/tls"
	"embed"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/lxt1045/rpc/faux_tcp"
	"github.com/lxt1045/rpc/trunk_kcp"
	"github.com/lxt1045/utils/config"
)

// TrunkKCPConfig 控制底层 trunk_kcp 链路参数。
type TrunkKCPConfig struct {
	Conv           uint32 `yaml:"conv"`
	MinConns       int    `yaml:"min_conns"`
	MaxConns       int    `yaml:"max_conns"`
	MaxVirtualConn int    `yaml:"max_virtual_conn"`

	// KCPMtu KCP 线上包 MTU（含 24B KCP 头），默认 1400。
	// 路径 MTU 受限时（VPN/隧道出口改写 MSS）必须调小：kcp_mtu ≤ faux_tcp.mss。
	KCPMtu int `yaml:"kcp_mtu"`

	// KCP NoDelay 参数（语义同 kcp.KCP.NoDelay），nil 表示使用库默认值 (1,10,32,1)。
	// 客户端与服务端必须配置成相同的值。
	//
	// 注意：本示例的物理连接是 faux_tcp（**不可靠**、语义等价 UDP 的数据报），
	// 丢包是真实链路丢包，所以适合库默认的快速模式 (1,10,32,1)；
	// socks_trunk_kcp 里"KCP over TCP/TLS 伪重传"那套保守参数（nodelay=0,
	// interval=20~40, resend=0）不适用于本示例。
	KCPNoDelay  *int `yaml:"kcp_nodelay"`
	KCPInterval *int `yaml:"kcp_interval"`
	KCPResend   *int `yaml:"kcp_resend"`
	KCPNc       *int `yaml:"kcp_nc"`
}

// NoDelayParam 返回配置的 KCP NoDelay 四元组；未配置项为 -1（保持库当前值）。
func (c *TrunkKCPConfig) NoDelayParam() (nodelay, interval, resend, nc int) {
	nodelay, interval, resend, nc = -1, -1, -1, -1
	if c == nil {
		return
	}
	if c.KCPNoDelay != nil {
		nodelay = *c.KCPNoDelay
	}
	if c.KCPInterval != nil {
		interval = *c.KCPInterval
	}
	if c.KCPResend != nil {
		resend = *c.KCPResend
	}
	if c.KCPNc != nil {
		nc = *c.KCPNc
	}
	return
}

// ApplyKCPParam 将配置的 KCP NoDelay 参数应用到 trunk（未配置项保持库默认）。
func (c *TrunkKCPConfig) ApplyKCPParam(t *trunk_kcp.TrunkKCP) {
	if c == nil || t == nil {
		return
	}
	if c.KCPMtu > 0 {
		t.SetMtu(c.KCPMtu)
	}
	nodelay, interval, resend, nc := c.NoDelayParam()
	if nodelay < 0 && interval < 0 && resend < 0 && nc < 0 {
		return
	}
	t.SetNoDelay(nodelay, interval, resend, nc)
}

// FauxTCPConfig 伪装 TCP 底层参数；零值即 faux_tcp 的推荐默认值。
type FauxTCPConfig struct {
	// MSS 单个 TCP 段最大载荷，默认 1448。必须 ≥ KCP 线上包（默认 1400），
	// 否则上层（trunk_kcp 按长度流式重组）会因半帧丢失永久错位。
	MSS int `yaml:"mss"`
	// AdvMSS 本端 SYN/SYN+ACK 里通告的 MSS，默认 = MSS。MSS 管"本端发多大"，
	// AdvMSS 管"对端发多大"；两者分开是为了对付路径上的 MSS-clamp 中间盒：
	// 它只在"需要改写 MSS"时才为该流建会话状态，本端把 MSS 调到 1240（≤ 它的
	// 改写目标）后它不改写也不建状态，回程 SYN+ACK 会被丢弃（内核 TCP 通告 1460
	// 被改写成 1280，所以内核一切正常）。这种情况把 adv_mss 显式设成 1460。
	AdvMSS int `yaml:"adv_mss"`
	// Window 通告的接收窗口，默认 65535（faux_tcp 默认）。
	Window int `yaml:"window"`
	// TTL IPv4 TTL，默认 64（仿 Linux）。
	TTL int `yaml:"ttl"`
	// KeepAliveSeconds 空闲保活间隔（标准 TCP keepalive 探测包），默认 25。
	KeepAliveSeconds int `yaml:"keepalive_seconds"`
	// HandshakeTimeoutMS 握手单次超时，默认 1000ms。
	HandshakeTimeoutMS int `yaml:"handshake_timeout_ms"`
	// HandshakeRetries SYN 重试次数，默认 3。
	HandshakeRetries int `yaml:"handshake_retries"`
	// CloseGraceMS 挥手宽限，默认 500ms。
	CloseGraceMS int `yaml:"close_grace_ms"`
	// HealDelayMS 丢包后 ack "虚拟重传愈合"等待，默认 200ms。
	HealDelayMS int `yaml:"heal_delay_ms"`
	// ReplySrc 出包源地址，**云主机默认留空**：留空=按报文目的 IP 作为源（网卡内网
	// 地址），由平台 NAT 转成公网，与内核 TCP 同路。仅"无状态 DNAT/端口映射"
	// （Docker 桥接、K8s NodePort）回程不通时填对外服务地址。
	// 云主机 1:1 NAT/弹性公网 IP 场景填公网 IP 会被平台源地址校验静默丢弃
	// （内核 TCP 能通、faux_tcp 握手超时），faux_tcp 启动时会对非本机地址打 WARN。
	ReplySrc string `yaml:"reply_src"`
	// DebugPackets 收包调试（不挂 cBPF + 用户态过滤计数），排查"收不到包"时开。
	DebugPackets bool `yaml:"debug_packets"`
	// ManualFirewall true 表示 RST 抑制规则由运维手工维护（faux_tcp 默认自动装拆）。
	ManualFirewall bool `yaml:"manual_firewall"`
}

// ToFauxTCP 转换为 faux_tcp.Config（零值字段交给 faux_tcp 填默认）。
func (c FauxTCPConfig) ToFauxTCP() faux_tcp.Config {
	win := 0 // 0 = 用 faux_tcp 默认窗口
	if c.Window > 0 && c.Window <= 65535 {
		win = c.Window
	}
	cfg := faux_tcp.Config{
		MSS:              c.MSS,
		AdvMSS:           c.AdvMSS,
		Window:           uint16(win),
		TTL:              uint8(c.TTL),
		HandshakeRetries: c.HandshakeRetries,
		DebugPackets:     c.DebugPackets,
		ManualFirewall:   c.ManualFirewall,
	}
	if c.KeepAliveSeconds > 0 {
		cfg.KeepAlive = time.Duration(c.KeepAliveSeconds) * time.Second
	}
	if c.HandshakeTimeoutMS > 0 {
		cfg.HandshakeTimeout = time.Duration(c.HandshakeTimeoutMS) * time.Millisecond
	}
	if c.CloseGraceMS > 0 {
		cfg.CloseGrace = time.Duration(c.CloseGraceMS) * time.Millisecond
	}
	if c.HealDelayMS > 0 {
		cfg.HealDelay = time.Duration(c.HealDelayMS) * time.Millisecond
	}
	if c.ReplySrc != "" {
		if ip, err := netip.ParseAddr(c.ReplySrc); err == nil && ip.Is4() {
			cfg.ReplySrc = ip
		}
	}
	return cfg
}

// HandshakeBudget 估算一次 faux_tcp 拨号最长耗时（单次超时 × (重试+1)），
// 零值字段按 faux_tcp 默认（1s/3 次）计算。客户端拨号 ctx 超时必须大于它，
// 否则超时会先被 ctx 吃掉，握手失败的自诊断信息就看不到了。
func (c FauxTCPConfig) HandshakeBudget() time.Duration {
	timeout := time.Duration(c.HandshakeTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = time.Second // faux_tcp 默认 HandshakeTimeout
	}
	retries := c.HandshakeRetries
	if retries <= 0 {
		retries = 3 // faux_tcp 默认 HandshakeRetries
	}
	return timeout * time.Duration(retries+1)
}

// ConnConfig 服务端监听 / 客户端拨号地址。
type ConnConfig struct {
	Addr string `yaml:"addr"`
	// LocalAddr 客户端可选：指定本地 IP:端口（默认按对端路由自动选择 IP + 随机端口）
	LocalAddr string `yaml:"local_addr"`
}

// TLSConfig 控制端到端 TLS。TLS 跑在 trunk_kcp VirtualConn（可靠有序流）之上——
// faux_tcp 是不可靠数据报语义，不能直接承载 TLS（记录 >MSS 会报错、丢包不补）。
// 控制通道与每条代理数据连接各自一次 TLS 握手；open header（含目标地址）也在
// TLS 之内传输。证书模型与仓库其它示例一致：CA 签发的 server/client 双向证书
// （mTLS，见 utils/config.LoadTLSConfig）。
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Host       string `yaml:"host"` // 客户端校验的 ServerName
	CACert     string `yaml:"ca-cert"`
	ServerCert string `yaml:"server-cert"`
	ServerKey  string `yaml:"server-key"`
	ClientCert string `yaml:"client-cert"`
	ClientKey  string `yaml:"client-key"`
}

// ServerTLS 构建服务端 *tls.Config（mTLS：要求并校验客户端证书）。
// Enabled=false 返回 nil（明文模式，仅供调试/兼容）。
func (c TLSConfig) ServerTLS(fsys embed.FS) (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	cfg, err := config.LoadTLSConfig(fsys, c.ServerCert, c.ServerKey, c.CACert)
	if err != nil {
		return nil, fmt.Errorf("load server tls config: %w", err)
	}
	return cfg, nil
}

// ClientTLS 构建客户端 *tls.Config（携带客户端证书，校验服务端证书）。
// Enabled=false 返回 nil（明文模式）。
func (c TLSConfig) ClientTLS(fsys embed.FS) (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	cfg, err := config.LoadTLSConfig(fsys, c.ClientCert, c.ClientKey, c.CACert)
	if err != nil {
		return nil, fmt.Errorf("load client tls config: %w", err)
	}
	cfg.ServerName = c.Host
	return cfg, nil
}

// ServerConfig 是服务端运行时配置。
type ServerConfig struct {
	Token      string         `yaml:"token"`
	MaxClients int            `yaml:"max_clients"`
	Conn       ConnConfig     `yaml:"conn"`
	Trunk      TrunkKCPConfig `yaml:"trunk_kcp"`
	FauxTCP    FauxTCPConfig  `yaml:"faux_tcp"`
	TLS        TLSConfig      `yaml:"tls"`
	ACL        ACLConfig      `yaml:"acl"`
}

// ClientConfig 是客户端运行时配置。
type ClientConfig struct {
	Token      string         `yaml:"token"`
	ClientConn ConnConfig     `yaml:"client_conn"`
	Trunk      TrunkKCPConfig `yaml:"trunk_kcp"`
	FauxTCP    FauxTCPConfig  `yaml:"faux_tcp"`
	TLS        TLSConfig      `yaml:"tls"`
}

// ACLConfig 简单访问控制。
type ACLConfig struct {
	Enabled       bool     `yaml:"enabled"`
	AllowNetworks []string `yaml:"allow_networks"`
	DenyHosts     []string `yaml:"deny_hosts"`
}

func (c *TrunkKCPConfig) defaults() {
	if c.Conv == 0 {
		c.Conv = 0x5a0b0001
	}
	if c.MinConns <= 0 {
		c.MinConns = 1
	}
	if c.MaxConns < c.MinConns {
		c.MaxConns = 4
	}
	if c.MaxVirtualConn <= 0 {
		c.MaxVirtualConn = 256
	}
}

// NormalizeTrunkKCPConfig 补全 trunk_kcp 默认值。
func NormalizeTrunkKCPConfig(c *TrunkKCPConfig) {
	c.defaults()
}

// ValidateMSS 校验 faux_tcp MSS 与 KCP 线上包的兼容性（数据报边界硬约束）。
func (c *FauxTCPConfig) ValidateMSS(kcpMTU int) error {
	mss := c.MSS
	if mss <= 0 {
		mss = 1448 // faux_tcp 默认
	}
	if kcpMTU <= 0 {
		kcpMTU = 1400 // trunk_kcp 默认 KCP 线上包
	}
	if mss < kcpMTU {
		return fmt.Errorf("faux_tcp MSS(%d) 小于 KCP 线上包(%d)：丢包会造成半帧错位，"+
			"请调大 MSS；若路径 MTU 受限（如 VPN 把 MSS 夹到 1280），请同时调小 "+
			"faux_tcp.mss 与 trunk_kcp.kcp_mtu（保持 kcp_mtu ≤ mss）", mss, kcpMTU)
	}
	return nil
}

func (c *ServerConfig) defaults() {
	c.Trunk.defaults()
	if c.MaxClients <= 0 {
		c.MaxClients = 1024
	}
}

func (c *ClientConfig) defaults() {
	c.Trunk.defaults()
}

// ValidateClientConfig 校验客户端配置。
func ValidateClientConfig(c *ClientConfig) error {
	c.defaults()
	if c.Token == "" {
		return fmt.Errorf("client token is required")
	}
	if c.ClientConn.Addr == "" {
		return fmt.Errorf("client-conn.addr is required")
	}
	if err := c.FauxTCP.ValidateMSS(c.Trunk.KCPMtu); err != nil {
		return err
	}
	return nil
}

// ValidateServerConfig 校验服务端配置。
func ValidateServerConfig(c *ServerConfig) error {
	c.defaults()
	if c.Token == "" {
		return fmt.Errorf("server token is required")
	}
	if c.Conn.Addr == "" {
		return fmt.Errorf("conn.addr is required")
	}
	if err := c.FauxTCP.ValidateMSS(c.Trunk.KCPMtu); err != nil {
		return err
	}
	return nil
}

var acl = struct {
	sync.RWMutex
	cfg      ACLConfig
	networks []netip.Prefix
}{}

// ConfigureACL 设置 ACL。
func ConfigureACL(c ACLConfig) error {
	var networks []netip.Prefix
	for _, s := range c.AllowNetworks {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return fmt.Errorf("invalid allow network %q: %w", s, err)
		}
		networks = append(networks, p)
	}
	acl.Lock()
	acl.cfg = c
	acl.networks = networks
	acl.Unlock()
	return nil
}

// CheckACL 判断目标是否被允许。
func CheckACL(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	host = strings.Trim(host, "[]")
	host = strings.ToLower(host)

	acl.RLock()
	defer acl.RUnlock()
	cfg := acl.cfg
	if !cfg.Enabled {
		return true
	}
	for _, deny := range cfg.DenyHosts {
		d := strings.TrimSuffix(strings.ToLower(deny), ".")
		h := strings.TrimSuffix(host, ".")
		if h == d || strings.HasSuffix(h, "."+d) {
			return false
		}
	}
	addr, perr := netip.ParseAddr(host)
	if perr == nil {
		if len(cfg.AllowNetworks) > 0 {
			for _, p := range acl.networks {
				if p.Contains(addr) {
					return true
				}
			}
			return false
		}
		return true
	}
	return len(cfg.AllowNetworks) == 0
}
