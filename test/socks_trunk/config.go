package socks

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
)

var acl = struct {
	sync.RWMutex
	cfg      ACLConfig
	networks []netip.Prefix
}{}

// TrunkConfig 控制客户端底层 Trunk 链路的建立与恢复。
type TrunkConfig struct {
	MinConns       int `yaml:"min_conns" mapstructure:"min_conns"`
	MaxConns       int `yaml:"max_conns" mapstructure:"max_conns"`
	ReconnectSec   int `yaml:"reconnect_sec" mapstructure:"reconnect_sec"`
	HealthCheckSec int `yaml:"health_check_sec" mapstructure:"health_check_sec"`
}

// ACLConfig 服务端对目标地址的简单访问控制。
type ACLConfig struct {
	Enabled       bool     `yaml:"enabled" mapstructure:"enabled"`
	AllowNetworks []string `yaml:"allow_networks" mapstructure:"allow_networks"`
	DenyHosts     []string `yaml:"deny_hosts" mapstructure:"deny_hosts"`
}

// ServerConfig 是服务端运行所需的非连接类参数。
type ServerConfig struct {
	Token             string    `yaml:"token" mapstructure:"token"`
	MaxClients        int       `yaml:"max_clients" mapstructure:"max_clients"`
	MaxConnsPerClient int       `yaml:"max_conns_per_client" mapstructure:"max_conns_per_client"`
	DialTimeoutSec    int       `yaml:"dial_timeout_sec" mapstructure:"dial_timeout_sec"`
	ACL               ACLConfig `yaml:"acl" mapstructure:"acl"`
}

// ClientConfig 是客户端运行所需的非连接类参数。
type ClientConfig struct {
	Token       string      `yaml:"token" mapstructure:"token"`
	Trunk       TrunkConfig `yaml:"trunk" mapstructure:"trunk"`
	EnableSocks bool        `yaml:"enable_socks" mapstructure:"enable_socks"`
	EnableHTTP  bool        `yaml:"enable_http" mapstructure:"enable_http"`
}

// Defaults returns safe defaults before user config is applied.
func (c *TrunkConfig) defaults() {
	if c.MinConns <= 0 {
		c.MinConns = 4
	}
	if c.MaxConns < c.MinConns {
		c.MaxConns = 32
	}
	if c.ReconnectSec <= 0 {
		c.ReconnectSec = 3
	}
	if c.HealthCheckSec <= 0 {
		c.HealthCheckSec = 10
	}
}
func (c *ServerConfig) defaults() {
	if c.MaxClients <= 0 {
		c.MaxClients = 1024
	}
	if c.MaxConnsPerClient <= 0 {
		c.MaxConnsPerClient = 128
	}
	if c.DialTimeoutSec <= 0 {
		c.DialTimeoutSec = 30
	}
}

// ValidateServerConfig applies defaults and checks required security fields.
func ValidateServerConfig(c *ServerConfig) error {
	c.defaults()
	if c.Token == "" {
		return fmt.Errorf("server token must not be empty")
	}
	if len(c.Token) < 8 {
		return fmt.Errorf("server token is too short")
	}
	return nil
}

// ValidateClientConfig applies defaults and checks required fields.
func ValidateClientConfig(c *ClientConfig) error {
	c.Trunk.defaults()
	if c.Token == "" {
		return fmt.Errorf("client token must not be empty")
	}
	return nil
}

// ConfigureACL parses allowed networks and stores the deny host list.
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

// CheckACL returns true if a target host:port is allowed by the configured ACL.
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
	// Explicit deny host/domain suffix check.
	for _, deny := range cfg.DenyHosts {
		d := strings.TrimSuffix(strings.ToLower(deny), ".")
		h := strings.TrimSuffix(host, ".")
		if h == d || strings.HasSuffix(h, "."+d) {
			return false
		}
	}
	// If an IP/CIDR allow list exists, an IP target must be inside it.
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
	// Domain targets are allowed unless denied above. For stricter control,
	// add domain allow lists in a future iteration.
	return len(cfg.AllowNetworks) == 0
}
