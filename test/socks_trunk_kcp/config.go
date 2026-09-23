package socks_kcp

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"

	"github.com/lxt1045/rpc/trunk_kcp"
)

// TrunkKCPConfig 控制底层 trunk_kcp 链路参数。
type TrunkKCPConfig struct {
	Conv           uint32 `yaml:"conv" mapstructure:"conv"`
	MinConns       int    `yaml:"min_conns" mapstructure:"min_conns"`
	MaxConns       int    `yaml:"max_conns" mapstructure:"max_conns"`
	MaxVirtualConn int    `yaml:"max_virtual_conn" mapstructure:"max_virtual_conn"`

	// KCPNoDelay / KCPNc 透传给 kcp.KCP.NoDelay 的第 1、4 个参数，nil = 用库默认值
	// （库默认 NoDelay(0,10,88,0)，即 minRTO=100ms、拥塞控制开启）。两端必须一致。
	//
	//   nodelay: 0 → minRTO 100ms；1 → minRTO 30ms。物理连接是 TCP/TLS 等可靠流时
	//            建议 0：KCP 的 ARQ 与 TCP 重叠，30ms 会因延迟抖动产生大量伪重传，
	//            线上流量被放大 2~3 倍（详见 README "线上流量放大"）。
	//   nc:      1 → 关闭 kcp-go 拥塞控制，窗口是唯一的在途上限（配合 kcp_sndwnd 用）。
	//
	// 第 2、3 个参数（interval、resend 快速重传阈值）**不开放**：实测对吞吐/重传率
	// 没有可测影响，固定用库默认值即可。
	KCPNoDelay *int `yaml:"kcp_nodelay" mapstructure:"kcp_nodelay"`
	KCPNc      *int `yaml:"kcp_nc" mapstructure:"kcp_nc"`

	// KCPSndWnd / KCPRcvWnd KCP 发送/接收窗口（段；一段载荷 = 1400-24 = 1376B）。
	// **必须按 BDP 设**：sndwnd ≈ 链路速率(B/s) × RTT(s) / 1376。
	// 库默认 1024 段（≈1.4MB）在限速出口上会把瓶颈队列灌爆、带宽被重传吃掉；
	// 实测（trunk_kcp/ratelimit_test.go，24Mbps/RTT40ms）：1024 段放大 3.2~3.8x、
	// 128 段（≈BDP）放大 1.2x 且链路打满。两端必须一致，0 表示不覆盖。
	KCPSndWnd int `yaml:"kcp_sndwnd" mapstructure:"kcp_sndwnd"`
	KCPRcvWnd int `yaml:"kcp_rcvwnd" mapstructure:"kcp_rcvwnd"`
}

// NoDelayParam 返回配置的 nodelay/nc；未配置项为 -1（保持库当前值）。
func (c *TrunkKCPConfig) NoDelayParam() (nodelay, nc int) {
	nodelay, nc = -1, -1
	if c == nil {
		return
	}
	if c.KCPNoDelay != nil {
		nodelay = *c.KCPNoDelay
	}
	if c.KCPNc != nil {
		nc = *c.KCPNc
	}
	return
}

// ApplyKCPParam 将配置的 KCP 参数应用到 trunk（未配置项保持库默认）。
func (c *TrunkKCPConfig) ApplyKCPParam(t *trunk_kcp.TrunkKCP) {
	if c == nil || t == nil {
		return
	}
	if c.KCPSndWnd > 0 || c.KCPRcvWnd > 0 {
		t.SetWindowSize(c.KCPSndWnd, c.KCPRcvWnd)
	}
	nodelay, nc := c.NoDelayParam()
	if nodelay < 0 && nc < 0 {
		return
	}
	// interval/resend 传 -1 = 保持库默认值。
	t.SetNoDelay(nodelay, -1, -1, nc)
}

// ServerConfig 是服务端运行时配置。
type ServerConfig struct {
	Token      string         `yaml:"token" mapstructure:"token"`
	MaxClients int            `yaml:"max_clients" mapstructure:"max_clients"`
	Trunk      TrunkKCPConfig `yaml:"trunk_kcp" mapstructure:"trunk_kcp"`
	ACL        ACLConfig      `yaml:"acl" mapstructure:"acl"`
}

// ClientConfig 是客户端运行时配置。
type ClientConfig struct {
	Token string         `yaml:"token" mapstructure:"token"`
	Trunk TrunkKCPConfig `yaml:"trunk_kcp" mapstructure:"trunk_kcp"`
}

// ACLConfig 简单访问控制。
type ACLConfig struct {
	Enabled       bool     `yaml:"enabled" mapstructure:"enabled"`
	AllowNetworks []string `yaml:"allow_networks" mapstructure:"allow_networks"`
	DenyHosts     []string `yaml:"deny_hosts" mapstructure:"deny_hosts"`
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
	// 窗口默认不覆盖库值（1024/1024）。限速出口上应按 BDP 显式设置 kcp_sndwnd；
	// 接收窗口只做缓冲，不要跟着一起调小。
}

// NormalizeTrunkKCPConfig 补全 trunk_kcp 默认值。
func NormalizeTrunkKCPConfig(c *TrunkKCPConfig) {
	c.defaults()
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
	return nil
}

// ValidateServerConfig 校验服务端配置。
func ValidateServerConfig(c *ServerConfig) error {
	c.defaults()
	if c.Token == "" {
		return fmt.Errorf("server token is required")
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
