package socks_faux_kcp

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestRelayClosesBothEnds(t *testing.T) {
	for _, cancelContext := range []bool{false, true} {
		t.Run(map[bool]string{false: "browser_close", true: "context_cancel"}[cancelContext], func(t *testing.T) {
			browser, local := net.Pipe()
			remote, target := net.Pipe()
			defer browser.Close()
			defer local.Close()
			defer remote.Close()
			defer target.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				relay(ctx, local, remote)
				close(done)
			}()
			if cancelContext {
				cancel()
			} else {
				browser.Close()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("relay stayed blocked after its input closed")
			}
			_ = target.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := target.Read(make([]byte, 1)); err == nil {
				t.Fatal("target connection is still open")
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("target did not receive connection close")
			}
		})
	}
}
