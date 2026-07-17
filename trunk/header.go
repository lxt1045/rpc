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
	ConnID uint16 // conn的序号; 最高位位类型, 0: ConnID(数据包), 1: 命令类型(命令数据包)

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
	if h.ConnID&0x80 > 0 {
		h.ConnID &= 0x7f
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
		binary.LittleEndian.PutUint16(bs[4:], h.ConnID|0x80)
		binary.LittleEndian.PutUint16(bs[6:], h.Cmd)
		return bs[:CmdHeaderSize]
	}
	binary.LittleEndian.PutUint16(bs[4:], h.ConnID&0x7f)
	return bs[:HeaderSize]
}

// ReadPack 读一个裸消息
func ReadPack(r io.ReadCloser, buf []byte) (header Header, bsBody []byte, err error) {
	n, err := io.ReadFull(r, buf[:2]) // 先读长度
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}
		err = errors.Errorf("%d: %s", n, err.Error())
		return
	}
	if n != 2 {
		err = errors.Errorf("n != 2,%d", n)
		return
	}
	lenNeed := ParseHeaderLen(buf[:2])
	if cap(buf) < int(lenNeed) {
		bufnew := make([]byte, lenNeed)
		bufnew[0], bufnew[1] = buf[0], buf[1] // copy(bufnew, bsHeader[:2])
		buf = bufnew
	}
	n, err = io.ReadFull(r, buf[2:lenNeed]) // 读取剩下的部分
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	if n != int(lenNeed-2) {
		err = errors.Errorf("n != 2,%d", n)
		return
	}

	header, lHeader := ParseHeader(buf[:CmdHeaderSize])
	bsBody = buf[lHeader:lenNeed]
	return
}
