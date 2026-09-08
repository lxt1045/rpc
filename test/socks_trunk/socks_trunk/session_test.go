package socks

import (
	"testing"
)

func TestSessionManagerAuthenticateAndCleanup(t *testing.T) {
	mgr := NewSessionManager()
	mgr.Configure("test-token", 8, 8)

	svc1 := &SocksSvc{}
	svc2 := &SocksSvc{}
	if err := mgr.Authenticate(svc1, "client-a", "test-token"); err != nil {
		t.Fatalf("auth svc1 failed: %v", err)
	}
	if err := mgr.Authenticate(svc2, "client-a", "test-token"); err != nil {
		t.Fatalf("auth svc2 failed: %v", err)
	}

	if got := mgr.Count(); got != 1 {
		t.Fatalf("session count = %d, want 1", got)
	}
	svc1.mu.Lock()
	ok1 := svc1.authorized
	svc1.mu.Unlock()
	if !ok1 {
		t.Fatalf("svc1 not authorized")
	}

	mgr.UnregisterSvc(svc1)
	if got := mgr.Count(); got != 1 {
		t.Fatalf("session removed too early, count = %d", got)
	}

	mgr.UnregisterSvc(svc2)
	if got := mgr.Count(); got != 0 {
		t.Fatalf("session count = %d after last unregister, want 0", got)
	}
}

func TestSessionManagerRejectsBadToken(t *testing.T) {
	mgr := NewSessionManager()
	mgr.Configure("test-token", 2, 4)
	if err := mgr.Authenticate(&SocksSvc{}, "client", "bad"); err == nil {
		t.Fatalf("bad token should be rejected")
	}
}
