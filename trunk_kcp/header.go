package trunk_kcp

import (
	"encoding/binary"
	"io"
)

// Command 命令应该用上级的 rpc 接口来处理，这一级应该保留简单性？
const (
	CmdCloseConn = 1
	CmdAddConn   = 2
	CmdEventReq  = 3
	CmdEventRes  = 4
)

const (
	HeaderSize    = 4
	CmdHeaderSize = 6
	kcpHeaderSize = 24
)

type Header struct {
	Len    uint16 // 数据体大小
	ConnID uint16 // conn的序号; 0x8000 位为命令标志，低 15 位为连接编号

	// 拓展部分 ConnID 高位bit: 1 时才有
	Cmd uint16
	TS  uint16 // 1min 内的 ms 数: 60*1000= 60 000 < 2^16= 65 532 刚好够用; 用于统计延时，也可以增加一个cmd来传 TS
}

func ParseHeaderLen(bs []byte) (l uint16) {
	l = binary.LittleEndian.Uint16(bs[0:])
	return
}

func ParseHeader1(bs []byte) (h Header, l int) {
	_ = bs[HeaderSize-1]
	h.Len = binary.LittleEndian.Uint16(bs[0:])
	h.ConnID = binary.LittleEndian.Uint16(bs[2:])
	l = HeaderSize
	if h.ConnID&0x8000 != 0 {
		h.ConnID &= 0x7fff
		h.Cmd = binary.LittleEndian.Uint16(bs[4:])
		l = CmdHeaderSize
	}
	return
}

// 6.985 ns/op -> 4.593 ns/op
func ParseHeader(bs []byte) (h Header, l int) {
	_ = bs[CmdHeaderSize-1]
	h.Len = binary.LittleEndian.Uint16(bs[0:])
	h.ConnID = binary.LittleEndian.Uint16(bs[2:])

	l = HeaderSize + int(h.ConnID>>15)<<1
	h.ConnID &= 0x7fff
	h.Cmd = binary.LittleEndian.Uint16(bs[4:])
	return
}

// 21.255 ns/op -> 16.947 ns/op
func (h *Header) Format(bs []byte) (out []byte) {
	_ = bs[CmdHeaderSize-1]
	binary.LittleEndian.PutUint16(bs[0:], h.Len)

	ConnIDMask := uint16(0x8000 << (0x8000 << h.Cmd))
	binary.LittleEndian.PutUint16(bs[2:], h.ConnID|ConnIDMask)
	binary.LittleEndian.PutUint16(bs[4:], h.Cmd)
	return bs[:HeaderSize+(ConnIDMask>>15)<<1]
}

func (h *Header) Format1(bs []byte) (out []byte) {
	_ = bs[HeaderSize-1]
	binary.LittleEndian.PutUint16(bs[0:], h.Len)
	if h.Cmd > 0 {
		binary.LittleEndian.PutUint16(bs[2:], h.ConnID|0x8000)
		binary.LittleEndian.PutUint16(bs[4:], h.Cmd)
		return bs[:CmdHeaderSize]
	}
	binary.LittleEndian.PutUint16(bs[2:], h.ConnID&0x7fff)
	return bs[:HeaderSize]
}

// ReadPack 读一个裸消息
func ReadPack(r io.ReadCloser, buf []byte) (header Header, bsBody []byte, err error) {
	if cap(buf) < HeaderSize {
		buf = make([]byte, HeaderSize)
	}
	buf = buf[:HeaderSize]
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	headerSize := HeaderSize
	if binary.LittleEndian.Uint16(buf[2:4])&0x8000 != 0 {
		headerSize = CmdHeaderSize
	}
	frameSize := headerSize + int(ParseHeaderLen(buf))
	if cap(buf) < frameSize {
		next := make([]byte, frameSize)
		copy(next, buf)
		buf = next
	} else {
		buf = buf[:frameSize]
	}
	if _, err = io.ReadFull(r, buf[HeaderSize:]); err != nil {
		return
	}
	header, headerLen := ParseHeader(buf)
	return header, buf[headerLen:], nil
}
