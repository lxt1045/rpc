package dcconn

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
)

// msgPipe 模拟 DataChannel 的数据报语义: 每次 Read 返回一条完整消息,
// 缓冲区装不下时丢弃剩余部分(与 SCTP reassembly_queue 的行为一致)。
type msgPipe struct {
	mu     sync.Mutex
	cond   *sync.Cond
	msgs   [][]byte
	closed bool
}

func newMsgPipe() *msgPipe {
	p := &msgPipe{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *msgPipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	p.msgs = append(p.msgs, bytes.Clone(b))
	p.cond.Signal()
	return len(b), nil
}

func (p *msgPipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.msgs) == 0 && !p.closed {
		p.cond.Wait()
	}
	if len(p.msgs) == 0 {
		return 0, io.EOF
	}
	msg := p.msgs[0]
	p.msgs = p.msgs[1:]
	n := copy(b, msg)
	if n < len(msg) {
		// 数据报语义: 剩余部分被丢弃
		return n, io.ErrShortBuffer
	}
	return n, nil
}

func (p *msgPipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cond.Broadcast()
	return nil
}

// 一条大消息可以被多次小 Read 逐段读出, 不丢字节。
// 这正是 codec 先读 2 字节长度前缀、再读剩余部分的访问模式。
func TestReadSmallChunks(t *testing.T) {
	p := newMsgPipe()
	c := New(p)

	want := make([]byte, 5000)
	for i := range want {
		want[i] = byte(i)
	}
	if _, err := p.Write(want); err != nil {
		t.Fatal(err)
	}

	// 先取 2 字节, 模拟长度前缀
	head := make([]byte, 2)
	if _, err := io.ReadFull(c, head); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(head, want[:2]) {
		t.Fatalf("head = %v, want %v", head, want[:2])
	}

	// 再把剩余部分读完
	rest := make([]byte, len(want)-2)
	if _, err := io.ReadFull(c, rest); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rest, want[2:]) {
		t.Fatal("rest mismatch: 消息剩余部分被丢弃了")
	}
}

// 连续多条消息读出来不串味, 且不会把两条消息拼进一次 Read。
func TestReadMultipleMessages(t *testing.T) {
	p := newMsgPipe()
	c := New(p)

	msgs := [][]byte{
		[]byte("first"),
		[]byte("second-longer"),
		bytes.Repeat([]byte("x"), 3000),
	}
	for _, m := range msgs {
		if _, err := p.Write(m); err != nil {
			t.Fatal(err)
		}
	}

	for i, want := range msgs {
		got := make([]byte, len(want))
		if _, err := io.ReadFull(c, got); err != nil {
			t.Fatalf("msg %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("msg %d: got %q, want %q", i, got, want)
		}
	}
}

// 超过 MaxMsgSize 的 Write 要分片, 每片都不超过上限,
// 否则对端读缓冲会触发短缓冲丢弃。
func TestWriteSplitsLargePayload(t *testing.T) {
	p := newMsgPipe()
	c := New(p)

	payload := bytes.Repeat([]byte("y"), MaxMsgSize*2+123)
	n, err := c.Write(payload)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(payload) {
		t.Fatalf("n = %d, want %d", n, len(payload))
	}

	p.mu.Lock()
	msgs := p.msgs
	p.mu.Unlock()

	if len(msgs) != 3 {
		t.Fatalf("分片数 = %d, want 3", len(msgs))
	}
	var total int
	for i, m := range msgs {
		if len(m) > MaxMsgSize {
			t.Fatalf("分片 %d 长度 %d 超过 %d", i, len(m), MaxMsgSize)
		}
		total += len(m)
	}
	if total != len(payload) {
		t.Fatalf("分片总长 = %d, want %d", total, len(payload))
	}
}

// 用 net.Pipe 做一次往返, 确认 Read/Write 组合起来是通的字节流。
func TestRoundTripOverNetPipe(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	ca, cb := New(a), New(b)

	want := bytes.Repeat([]byte("hello dcconn "), 100)
	go func() {
		if _, err := ca.Write(want); err != nil {
			t.Error(err)
		}
	}()

	got := make([]byte, len(want))
	if _, err := io.ReadFull(cb, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("round trip mismatch")
	}
}

// Close 要传导到底层。
func TestCloseUnderlying(t *testing.T) {
	p := newMsgPipe()
	c := New(p)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write([]byte("x")); err != io.ErrClosedPipe {
		t.Fatalf("err = %v, want ErrClosedPipe", err)
	}
}
