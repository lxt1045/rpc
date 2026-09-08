package trunk

import (
	"bytes"
	"io"
	"testing"
	"time"
)

type reviewBuffer struct{ bytes.Buffer }

func (*reviewBuffer) Close() error { return nil }

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

func TestReviewLargeWrite(t *testing.T) {
	rw := &reviewBuffer{}
	c := NewTrunk(rw).GetConn(1)
	data := bytes.Repeat([]byte{'x'}, 65530)
	n, err := c.Write(data)
	if err != nil {
		return // Rejecting oversized writes is safe.
	}
	if n != len(data) || ParseHeaderLen(rw.Bytes()) < HeaderSize {
		t.Fatalf("Write returned (%d, nil), wire length=%d", n, ParseHeaderLen(rw.Bytes()))
	}
}

func TestReviewTrunkClose(t *testing.T) {
	p := NewTrunk(&reviewBuffer{})
	p.GetConn(1)
	done := make(chan error, 1)
	go func() { done <- p.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Trunk.Close blocked with a nonblocking transport")
	}
}

func TestReviewReceiveBacklog(t *testing.T) {
	p := NewTrunk(&reviewBuffer{})
	c := p.GetConn(1)
	input := make(chan Package, 65)
	for i := range 65 {
		input <- Package{Header: Header{ConnID: 1, Idx: uint16(i), Len: 7}, Body: []byte{byte(i)}}
	}
	close(input)
	done := make(chan error, 1)
	go func() { done <- p.SavePackLoop(input) }()
	for i := range 65 {
		buf := make([]byte, 1)
		if _, err := c.Read(buf); err != nil {
			t.Fatal(err)
		}
		if buf[0] != byte(i) {
			t.Fatalf("frame %d: got %d", i, buf[0])
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReviewMalformedLength(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			t.Fatalf("invalid wire length panicked instead of returning an error: %v", e)
		}
	}()
	_, _, err := ReadPack(io.NopCloser(bytes.NewReader([]byte{0, 0})), make([]byte, 65535))
	if err == nil {
		t.Fatal("invalid length accepted")
	}
}
