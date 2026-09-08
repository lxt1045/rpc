package trunk_kcp

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestReviewHeaderID(t *testing.T) {
	for _, connID := range []uint16{0, 127, 128, 129, 32767} {
		for _, cmd := range []uint16{0, CmdCloseConn} {
			h := Header{ConnID: connID, Cmd: cmd, Len: HeaderSize + 1}
			got, _ := ParseHeader(h.Format(make([]byte, CmdHeaderSize)))
			if got.ConnID != h.ConnID || got.Cmd != h.Cmd {
				t.Fatalf("header changed: ConnID %d -> %d, Cmd %d -> %d", h.ConnID, got.ConnID, h.Cmd, got.Cmd)
			}
		}
	}
}

func TestReviewReadPack(t *testing.T) {
	for _, tc := range []Header{
		{Len: 3, ConnID: 129},
		{Len: 3, ConnID: 32767, Cmd: CmdEventReq},
	} {
		header := tc.Format(make([]byte, CmdHeaderSize))
		frame := append(append([]byte(nil), header...), []byte("abc")...)
		got, body, err := ReadPack(io.NopCloser(bytes.NewReader(frame)), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc || string(body) != "abc" {
			t.Fatalf("got header %+v and body %q, want header %+v and body abc", got, body, tc)
		}
	}
}

func TestReviewReceiveOverflow(t *testing.T) {
	p := NewTrunkKCP(1)
	c := p.GetConn(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 65 {
			p.demuxData(Header{ConnID: 1, Len: 1}, []byte{byte(i)})
		}
		p.demuxData(Header{ConnID: 1, Cmd: CmdCloseConn}, nil)
	}()
	var count int
	for {
		n, err := c.Read(make([]byte, 1))
		count += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	<-done
	if count != 65 {
		t.Fatalf("dispatched 65 bytes, received %d bytes before EOF", count)
	}
}

func TestReviewLocalCloseUnblocksRead(t *testing.T) {
	p := NewTrunkKCP(1)
	c := p.GetConn(1)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Even an explicit remote close cannot unblock the local reader.
	p.demuxData(Header{ConnID: 1, Cmd: CmdCloseConn}, nil)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Read succeeded after Close")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Read remains blocked after local, remote, and trunk Close")
	}
}

func TestReviewCancelRun(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	p := NewTrunkKCP(1, a)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		a.Close()
		<-done
		t.Fatal("Run does not return on context cancellation while transport Read is blocked")
	}
}

func TestReviewRunAfterEOF(t *testing.T) {
	a, b := net.Pipe()
	p := NewTrunkKCP(1, a)
	defer p.Close()
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()
	b.Close()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		p.Close()
		<-done
		t.Fatal("Run remains alive after its only physical connection reaches EOF")
	}
}

func TestReviewMaxConnID(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			t.Fatalf("GetConn(65535) panicked: %v", e)
		}
	}()
	NewTrunkKCP(1).GetConn(65535)
}
