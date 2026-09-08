package socks

import (
	"testing"
)

func TestTrunkConfigDefaults(t *testing.T) {
	c := TrunkConfig{}
	c.defaults()
	if c.MinConns != 4 {
		t.Fatalf("MinConns default = %d, want 4", c.MinConns)
	}
	if c.MaxConns < c.MinConns {
		t.Fatalf("MaxConns default = %d, want >= MinConns", c.MaxConns)
	}
	if c.ReconnectSec <= 0 || c.HealthCheckSec <= 0 {
		t.Fatalf("reconnect/health defaults not applied")
	}
}

func TestValidateServerConfig(t *testing.T) {
	if err := ValidateServerConfig(&ServerConfig{Token: "12345678"}); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := ValidateServerConfig(&ServerConfig{}); err == nil {
		t.Fatalf("empty token should be rejected")
	}
}

func TestValidateClientConfig(t *testing.T) {
	if err := ValidateClientConfig(&ClientConfig{Token: "12345678"}); err != nil {
		t.Fatalf("valid client config rejected: %v", err)
	}
	if err := ValidateClientConfig(&ClientConfig{}); err == nil {
		t.Fatalf("empty token should be rejected")
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
	if CheckACL("internal.example.com:443") {
		t.Fatalf("deny host accepted")
	}
	if CheckACL("www.example.com:443") {
		t.Fatalf("domain accepted in IP allow-list mode")
	}
	ConfigureACL(ACLConfig{})
}
