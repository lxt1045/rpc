package socks_kcp

import (
	"bytes"
	"testing"
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
	c := ClientConfig{Token: "token"}
	if err := ValidateClientConfig(&c); err != nil {
		t.Fatal(err)
	}
	if c.Trunk.Conv == 0 || c.Trunk.MaxConns == 0 || c.Trunk.MaxVirtualConn == 0 {
		t.Fatalf("defaults not applied: %+v", c.Trunk)
	}
}
