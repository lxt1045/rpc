package socks

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	usocks "github.com/lxt1045/utils/socks"
)

func TestSocks5ConnectHandshake(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	type result struct {
		addr usocks.Addr
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		addr, err := usocks.Handshake(server)
		resCh <- result{addr: addr, err: err}
	}()

	// Send greeting: VER=5, NMETHODS=1, METHOD=0.
	if _, err := client.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	// Read method selection.
	reply := make([]byte, 2)
	if _, err := io.ReadFull(client, reply); err != nil {
		t.Fatal(err)
	}
	if reply[0] != 5 || reply[1] != 0 {
		t.Fatalf("unexpected method reply: %v", reply)
	}

	// Send CONNECT to example.com:443 (domain addr).
	req := []byte{5, 1, 0, 3, byte(len("example.com"))}
	req = append(req, []byte("example.com")...)
	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], 443)
	req = append(req, portBytes[:]...)
	if _, err := client.Write(req); err != nil {
		t.Fatal(err)
	}

	// Read success reply: VER=5, REP=0, RSV=0, ATYP=1, BND.ADDR=0.0.0.0, BND.PORT=0.
	socksReply := make([]byte, 10)
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(client, socksReply); err != nil {
		t.Fatal(err)
	}
	if socksReply[1] != 0 {
		t.Fatalf("socks connect failed, reply=%v", socksReply)
	}

	res := <-resCh
	if res.err != nil {
		t.Fatalf("Handshake error: %v", res.err)
	}
	if got := res.addr.String(); got != "example.com:443" {
		t.Fatalf("target addr = %q, want example.com:443", got)
	}
}

func TestSocks5UDPCommandRejectedByDefault(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		_, err := usocks.Handshake(server)
		done <- err
	}()

	if _, err := client.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(client, reply); err != nil {
		t.Fatal(err)
	}

	req := []byte{5, 3, 0, 1, 127, 0, 0, 1, 0, 80}
	if _, err := client.Write(req); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatalf("UDP command should not be enabled by default")
	}
}
