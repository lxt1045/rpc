package socks

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/lxt1045/rpc/socket"
)

type writeCloser struct {
	bytes.Buffer
}

func (w *writeCloser) Close() error { return nil }

func TestCopyFromClosedSource(t *testing.T) {
	pr, pw := io.Pipe()
	dst := &writeCloser{}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = socket.Copy(context.Background(), dst, pr)
	}()

	if _, err := pw.Write([]byte("hello trunk copy")); err != nil {
		t.Fatal(err)
	}
	_ = pw.Close()
	<-done

	if got := dst.String(); got != "hello trunk copy" {
		t.Fatalf("copied data = %q", got)
	}
}
