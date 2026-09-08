package trunk

import (
	"encoding/binary"
	"io"

	"github.com/lxt1045/errors"
)

// Command 命令应该用上级的 rpc 接口来处理，这一级应该保留简单性？
const (
	CmdCloseConn = 1
	CmdAddConn   = 2
	CmdEventReq  = 3
	CmdEventRes  = 4
)

const (
	HeaderSize    = 6
	CmdHeaderSize = 8
)

type Header struct {
	Len    uint16 // 包大小
	Idx    uint16 // 包的序号
	ConnID uint16 // conn的序号; 0x8000 位为命令标志，低 15 位为连接编号

	// 拓展部分 ConnID 高位bit: 1 时才有
	Cmd uint16
	TS  uint16 // 1min 内的 ms 数: 60*1000= 60 000 < 2^16= 65 532 刚好够用; 用于统计延时，也可以增加一个cmd来传 TS
}

func ParseHeaderLen(bs []byte) (l uint16) {
	l = binary.LittleEndian.Uint16(bs[0:])
	return
}

func ParseHeader(bs []byte) (h Header, l int) {
	_ = bs[HeaderSize-1]
	h.Len = binary.LittleEndian.Uint16(bs[0:])
	h.Idx = binary.LittleEndian.Uint16(bs[2:])
	h.ConnID = binary.LittleEndian.Uint16(bs[4:])
	l = HeaderSize
	if h.ConnID&0x8000 != 0 {
		h.ConnID &= 0x7fff
		h.Cmd = binary.LittleEndian.Uint16(bs[6:])
		l = CmdHeaderSize
	}
	return
}

func (h *Header) Format(bs []byte) (out []byte) {
	_ = bs[HeaderSize-1]
	binary.LittleEndian.PutUint16(bs[0:], h.Len)
	binary.LittleEndian.PutUint16(bs[2:], h.Idx)
	if h.Cmd > 0 {
		binary.LittleEndian.PutUint16(bs[4:], h.ConnID|0x8000)
		binary.LittleEndian.PutUint16(bs[6:], h.Cmd)
		return bs[:CmdHeaderSize]
	}
	binary.LittleEndian.PutUint16(bs[4:], h.ConnID&0x7fff)
	return bs[:HeaderSize]
}

// ReadPack 读一个裸消息
func ReadPack(r io.ReadCloser, buf []byte) (header Header, bsBody []byte, err error) {
	if cap(buf) < 2 {
		buf = make([]byte, 2)
	}
	buf = buf[:2]
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	length := int(ParseHeaderLen(buf))
	if length < HeaderSize {
		return header, nil, errors.Errorf("invalid packet length: %d", length)
	}
	if cap(buf) < length {
		next := make([]byte, length)
		copy(next, buf)
		buf = next
	} else {
		buf = buf[:length]
	}
	if _, err = io.ReadFull(r, buf[2:]); err != nil {
		return
	}
	if binary.LittleEndian.Uint16(buf[4:])&0x8000 != 0 && length < CmdHeaderSize {
		return header, nil, errors.Errorf("invalid command packet length: %d", length)
	}
	header, headerLen := ParseHeader(buf)
	return header, buf[headerLen:], nil
}
