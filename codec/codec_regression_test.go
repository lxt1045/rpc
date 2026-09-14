package codec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type limitedWriter struct {
	bytes.Buffer
	limit int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		p = p[:w.limit]
	}
	return w.Buffer.Write(p)
}
func (w *limitedWriter) Read(p []byte) (n int, err error) {
	return
}
func (w *limitedWriter) Close() error {
	return nil
}

func TestWriteFullHandlesShortWrites(t *testing.T) {
	w := &limitedWriter{limit: 3}
	payload := []byte("complete frame")
	c := Codec{rwc: w}
	n, err := c.writeFull(payload)
	if err != nil {
		t.Fatalf("writeFull returned error: %v", err)
	}
	if n != len(payload) || !bytes.Equal(w.Bytes(), payload) {
		t.Fatalf("writeFull wrote %d bytes %q, want %d bytes %q", n, w.Bytes(), len(payload), payload)
	}
}

func TestWriteFullRejectsZeroProgress(t *testing.T) {
	c := Codec{rwc: &zeroWriter{}}
	n, err := c.writeFull([]byte("frame"))
	if !errors.Is(err, io.ErrShortWrite) || n != 0 {
		t.Fatalf("writeFull = (%d, %v), want (0, io.ErrShortWrite)", n, err)
	}
}

type zeroWriter struct{}

func (w *zeroWriter) Read(p []byte) (n int, err error) {
	return
}
func (w *zeroWriter) Close() error {
	return nil
}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestCodecCloseDoesNotBlockOnCloseFrame(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	c, err := NewCodec(context.Background(), local, nil, nil, false)
	if err != nil {
		t.Fatalf("NewCodec returned error: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close remained blocked writing the close frame")
	}
}
