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
	CmdAppData   = 3
)

const (
	HeaderSize = 6
)

type Header struct {
	Len    uint16 // 包大小
	Idx    uint16 // 包的序号
	ConnID uint16 // conn的序号; 最高位位类型, 0: ConnID(数据包), 1: 命令类型(命令数据包)
	TS     uint16 // 1min 内的 ms 数: 60*1000= 60 000 < 2^16= 65 532 刚好够用; 用于统计延时，也可以增加一个cmd来传 TS
}

func ParseHeaderLen(bs []byte) (l uint16) {
	l = binary.LittleEndian.Uint16(bs[0:])
	return
}

func ParseHeader(bs []byte) (h Header) {
	_ = bs[HeaderSize-1]
	h.Len = binary.LittleEndian.Uint16(bs[0:])
	h.Idx = binary.LittleEndian.Uint16(bs[2:])
	h.ConnID = binary.LittleEndian.Uint16(bs[4:])
	return
}

func (h *Header) Format(bs []byte) (out []byte) {
	_ = bs[HeaderSize-1]
	binary.LittleEndian.PutUint16(bs[0:], h.Len)
	binary.LittleEndian.PutUint16(bs[2:], h.Idx)
	binary.LittleEndian.PutUint16(bs[4:], h.ConnID)
	return bs[:HeaderSize]
}

const CmdSize = 2

type Cmd struct {
	Cmd  uint16
	Data []byte
}

func ParseCmd(bs []byte) (h Cmd) {
	_ = bs[CmdSize-1]
	h.Cmd = binary.LittleEndian.Uint16(bs[0:])
	return
}

func (h *Cmd) Format(bs []byte) (out []byte) {
	_ = bs[CmdSize-1]
	binary.LittleEndian.PutUint16(bs[0:], h.Cmd)
	return bs[:CmdSize]
}

// ReadPack 读一个裸消息
func ReadPack(r io.ReadCloser, buf []byte) (header Header, bsBody []byte, err error) {
	n, err := io.ReadFull(r, buf[:HeaderSize]) // 先读长度
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}
		err = errors.Errorf("%d: %s", n, err.Error())
		return
	}
	if n != HeaderSize {
		err = errors.Errorf("n != 2,%d", n)
		return
	}
	header = ParseHeader(buf[:HeaderSize])

	n, err = io.ReadFull(r, buf[0:header.Len]) // 读取剩下的部分
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	if n != int(header.Len) {
		err = errors.Errorf("n != header.Len,%d, %d", n, header.Len)
		return
	}
	bsBody = buf[0:header.Len]
	return
}
