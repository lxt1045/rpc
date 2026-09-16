package socks_kcp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lxt1045/rpc/trunk_kcp"
)

func TestHTTPProxyRepeatedConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	go func() {
		for {
			conn, err := target.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	a, b := net.Pipe()
	svc := &SocksSvc{}
	server := trunk_kcp.NewTrunkKCP(1, func(conn *trunk_kcp.VirtualConn) {
		svc.serveVirtualConn(ctx, conn)
	}, a)
	client := trunk_kcp.NewTrunkKCP(1, nil, b)
	var wg sync.WaitGroup
	for _, trunk := range []*trunk_kcp.TrunkKCP{server, client} {
		wg.Add(1)
		go func() { defer wg.Done(); _ = trunk.Run(ctx) }()
	}
	defer func() { client.Close(); server.Close(); wg.Wait() }()
	cli := &SocksCli{trunk: client, TrunkCfg: TrunkKCPConfig{MaxVirtualConn: 256}}
	// Keep ID 1 busy across wraparound; new requests must never share it.
	busy := client.GetConn(1)
	defer busy.Close()
	for i := 0; i < 300; i++ {
		if i > 0 && i%50 == 0 {
			// Rotate the transport while keeping the trunk and ID pool alive.
			nextClient, nextServer := net.Pipe()
			if _, err := server.AddConn(nextServer); err != nil {
				t.Fatal(err)
			}
			if _, err := client.AddConn(nextClient); err != nil {
				t.Fatal(err)
			}
			if err := cli.RemoveTrunkConn(ctx, i/50-1); err != nil {
				t.Fatal(err)
			}
		}
		browser, local := net.Pipe()
		done := make(chan struct{})
		go func() { cli.handleHTTP(ctx, local); close(done) }()
		_ = browser.SetDeadline(time.Now().Add(3 * time.Second))
		_, err := fmt.Fprintf(browser, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target.Addr(), target.Addr())
		if err != nil {
			browser.Close()
			t.Fatal(err)
		}
		r := bufio.NewReader(browser)
		rsp, err := http.ReadResponse(r, &http.Request{Method: http.MethodConnect})
		if err != nil || rsp.StatusCode != http.StatusOK {
			browser.Close()
			t.Fatalf("request %d CONNECT failed: %v", i, err)
		}
		payload := fmt.Sprintf("request-%03d", i)
		_, err = io.WriteString(browser, payload)
		if err != nil {
			browser.Close()
			t.Fatalf("request %d write: %v", i, err)
		}
		buf := make([]byte, len(payload))
		_, err = io.ReadFull(r, buf)
		browser.Close()
		if err != nil || string(buf) != payload {
			t.Fatalf("request %d echo = %q, %v", i, buf, err)
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("request %d handler leaked", i)
		}
	}
	if client.GetConn(1) != busy {
		t.Fatal("active connection was replaced during wraparound")
	}
}

func TestRemovePhysicalConnWithDifferentRemoteID(t *testing.T) {
	client := trunk_kcp.NewTrunkKCP(1, nil)
	server := trunk_kcp.NewTrunkKCP(1, nil)
	defer client.Close()
	defer server.Close()
	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	_, _ = client.AddConn(a0)
	_, _ = client.AddConn(a1)
	_, _ = server.AddConn(b1)
	_, _ = server.AddConn(b0)
	cli := &SocksCli{trunk: client}
	if err := cli.RemoveTrunkConn(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for server.ConnCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if client.ConnCount() != 1 || server.ConnCount() != 1 {
		t.Fatalf("remaining connections: client %d, server %d", client.ConnCount(), server.ConnCount())
	}
	// Server ID 0 belongs to the surviving connection, not the removed one.
	if err := server.RemoveConn(0); err != nil {
		t.Fatalf("wrong remote physical connection removed: %v", err)
	}
}
