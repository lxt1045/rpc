package socks_kcp

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
)

// TrunkKCPConfig 控制底层 trunk_kcp 链路参数。
type TrunkKCPConfig struct {
	Conv           uint32 `yaml:"conv" mapstructure:"conv"`
	MinConns       int    `yaml:"min_conns" mapstructure:"min_conns"`
	MaxConns       int    `yaml:"max_conns" mapstructure:"max_conns"`
	MaxVirtualConn int    `yaml:"max_virtual_conn" mapstructure:"max_virtual_conn"`
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
