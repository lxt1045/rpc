// Package dcconn 把消息式的 WebRTC DataChannel 适配成字节流式的
// io.ReadWriteCloser, 以便交给 rpc 的 codec 使用。
//
// 为什么需要这一层: codec 的 ReadPack 先读 2 字节长度前缀, 再读剩余部分
// (codec/codec.go 的 io.ReadFull(r, buf[:2]))。而 DataChannel 底层是 SCTP,
// 数据报语义: 缓冲区装不下整条消息时返回 io.ErrShortBuffer 并丢弃该消息的
// 剩余部分。直接把 Detach 出来的 DataChannel 交给 codec, 第一次只读 2 字节
// 就会导致整帧剩余内容被丢弃, 连接立刻错乱。
//
// 这里的做法是: 每次从 DataChannel 整条读入内部缓冲, 再按调用方要求的字节数
// 逐次吐出。
package dcconn

import (
	"io"
	"sync"
)

// MaxMsgSize 是单条消息的上限。codec 的帧长度前缀是 uint16, 所以单帧不超过
// 65535 字节, 64 KiB 的缓冲足以容纳任意单帧, 不会触发 SCTP 的短缓冲丢弃。
const MaxMsgSize = 64 * 1024

// Conn 是 DataChannel 之上的字节流视图, 实现 io.ReadWriteCloser。
//
// Read 不可并发调用(codec 的 ReadLoop 是单 goroutine 读, 满足这个约束);
// Write 可以并发调用, 内部有锁。
type Conn struct {
	dc io.ReadWriteCloser

	rbuf []byte
	rn   int // rbuf 中有效数据的长度
	ri   int // rbuf 中已被消费的位置

	wmu sync.Mutex
}

// New 用 dc 构造一个字节流适配。
//
// 形参取 io.ReadWriteCloser 而非 datachannel.ReadWriteCloser: 后者是前者的
// 超集, 收窄之后单测可以直接用 net.Pipe, 不必引入 pion 类型。
func New(dc io.ReadWriteCloser) *Conn {
	return &Conn{
		dc:   dc,
		rbuf: make([]byte, MaxMsgSize),
	}
}

// Read 先从内部缓冲取数据, 缓冲耗尽才去读下一条消息。
// 这样 io.ReadFull(r, buf[:2]) 取走 2 字节之后, 同一条消息的剩余部分仍在
// 缓冲里等待下次 Read, 不会被 SCTP 丢弃。
func (c *Conn) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	for c.ri >= c.rn {
		// 缓冲已空, 读入下一条完整消息。空消息要跳过, 否则会被上层误判为 EOF。
		c.rn, err = c.dc.Read(c.rbuf)
		c.ri = 0
		if err != nil {
			c.rn = 0
			return 0, err
		}
	}
	n = copy(p, c.rbuf[c.ri:c.rn])
	c.ri += n
	return n, nil
}

// Write 把 p 作为消息写入 DataChannel。超过 MaxMsgSize 时按 MaxMsgSize 分片。
//
// codec 单帧本就不超过 65535 字节, 正常路径下一次写完; 但 io.Writer 语义上
// 允许更大的切片, 所以仍要循环切分。
//
// pion 的 DataChannel 并发写不安全, 这里加锁保护。
func (c *Conn) Write(p []byte) (n int, err error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	for n < len(p) {
		chunk := min(MaxMsgSize, len(p)-n)
		nn, err := c.dc.Write(p[n : n+chunk])
		n += nn
		if err != nil {
			return n, err
		}
		if nn < chunk {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

// Close 关闭底层 DataChannel。
func (c *Conn) Close() error {
	return c.dc.Close()
}
