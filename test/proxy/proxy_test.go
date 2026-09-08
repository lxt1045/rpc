package socks

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/lxt1045/rpc"
	"github.com/lxt1045/rpc/test/proxy/pb"
)

type bufferCloser struct{ bytes.Buffer }

func (*bufferCloser) Close() error { return nil }

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
func (errorReader) Close() error               { return nil }

func TestCopy(t *testing.T) {
	payload := bytes.Repeat([]byte("proxy-data"), 20_000)
	dst := &bufferCloser{}
	written, err := Copy(context.Background(), dst, io.NopCloser(bytes.NewReader(payload)))
	if err != nil {
		t.Fatalf("Copy returned error: %v", err)
	}
	if written != int64(len(payload)) || !bytes.Equal(dst.Bytes(), payload) {
		t.Fatalf("Copy wrote %d bytes, want %d", written, len(payload))
	}
}

func TestCopyCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Copy(ctx, &bufferCloser{}, errorReader{err: errors.New("read should not run")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy error = %v, want context.Canceled", err)
	}
}

func TestCopyReadError(t *testing.T) {
	want := errors.New("read failed")
	_, err := Copy(context.Background(), &bufferCloser{}, errorReader{err: want})
	if !errors.Is(err, want) {
		t.Fatalf("Copy error = %v, want %v", err, want)
	}
}

func TestWriteInitialShortWrite(t *testing.T) {
	if err := writeInitial(shortWriter{}, []byte("body")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeInitial error = %v, want io.ErrShortWrite", err)
	}
}

func TestGetPeerStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cli := &SocksCli{ChPeer: make(chan *Peer)}
	if peer := cli.getPeer(ctx); peer != nil {
		t.Fatalf("getPeer returned %v after cancellation", peer)
	}
}

func TestCloseEmptyPool(t *testing.T) {
	cli := &SocksCli{ChPeer: make(chan *Peer)}
	if err := cli.close(context.Background()); err != nil {
		t.Fatalf("close returned error: %v", err)
	}
}

func TestPeerRegistration(t *testing.T) {
	if _, err := rpc.NewPeer(context.Background(), &SocksSvc{}, pb.NewSocksCliClient, pb.RegisterSocksSvcServer); err != nil {
		t.Fatalf("NewPeer returned error: %v", err)
	}
}
