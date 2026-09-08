package codec

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

type testReadWriteCloser struct{}

func (testReadWriteCloser) Read([]byte) (int, error)    { return 0, io.EOF }
func (testReadWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (testReadWriteCloser) Close() error                { return nil }

func TestUpgradeWaitReadyAndClose(t *testing.T) {
	u := newUpgrade(nil, 0, 0)
	ready := make(chan error, 1)
	go func() { ready <- u.WaitReady(context.Background()) }()

	u.markReady()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("WaitReady returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady did not unblock")
	}

	if err := u.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if _, err := u.Write([]byte("x")); err == nil || !strings.Contains(err.Error(), ErrUpgradeClosed.Msg()) {
		t.Fatalf("Write after Close = %v, want ErrUpgradeClosed", err)
	}
}

func TestUpgradeCloseUnblocksWaitReady(t *testing.T) {
	u := newUpgrade(nil, 0, 0)
	done := make(chan error, 1)
	go func() { done <- u.WaitReady(context.Background()) }()
	_ = u.Close()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), ErrUpgradeClosed.Msg()) {
			t.Fatalf("WaitReady after Close = %v, want ErrUpgradeClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady remained blocked after Close")
	}
}
