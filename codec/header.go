package codec

import (
	"encoding/binary"
	"math"
)

const (
	VerHeartbeat      = 0
	VerRaw            = 1
	VerCallReq        = 2
	VerCallResp       = 3
	VerClose          = 4
	VerCmdReq         = 5 // 发送控制命令
	VerCmdResp        = 6 // 响应控制命令
	VerStreamReq      = 7
	VerStreamResp     = 8
	VerUpgradeReq     = 9
	VerUpgradeResp    = 10
	VerCallErrResp    = 11
	VerUpgradeErrResp = 12
)
const (
	HeaderSizeRaw = 6
	HeaderSize    = 16
	MaxBuffSize   = HeaderSize + math.MaxUint16
)

type Header struct {
	Len    uint16 // 版本号, 0:raw bytes, 1:CallReq, 2:CallResp; size: 2
	CtxLen uint16 // Ctx透传 body 长度, ctx 最长  65532
	Ver    uint16 // 消息长度, size: 2

	// Len != 0 才需要
	SegmentCount uint16 // 大包拆包总数量, size: 2
	SegmentIdx   uint16 // 大包拆包编号, size: 2
	CallID       uint16 // 调用ID, size: 4; 连接建立后，将 caller func name 与id映射一次
	CallSN       uint32 // 调用序列号, size: 4; stream 的 callSN 和 Call 要保持独立, 因为stream会有可能会碰撞
}

func ParseHeaderLen(bs []byte) (l uint16) {
	l = binary.LittleEndian.Uint16(bs[0:])
	return
}

func ParseHeader(bs []byte) (h Header, l int) {
	_ = bs[HeaderSize-1]
	h.Len = binary.LittleEndian.Uint16(bs[0:])
	h.CtxLen = binary.LittleEndian.Uint16(bs[2:])
	h.Ver = binary.LittleEndian.Uint16(bs[4:])
	if h.Len == 0 {
		l = 6
		return
	}
	h.SegmentCount = binary.LittleEndian.Uint16(bs[6:])
	h.SegmentIdx = binary.LittleEndian.Uint16(bs[8:])
	h.CallID = binary.LittleEndian.Uint16(bs[10:])
	h.CallSN = binary.LittleEndian.Uint32(bs[12:])
	l = 16
	return
}

func (h *Header) Format(bs []byte) (out []byte) {
	_ = bs[HeaderSize-1]
	binary.LittleEndian.PutUint16(bs[0:], h.Len)
	binary.LittleEndian.PutUint16(bs[2:], h.CtxLen)
	binary.LittleEndian.PutUint16(bs[4:], h.Ver)
	if h.Len == 0 {
		return bs[:6]
	}
	binary.LittleEndian.PutUint16(bs[6:], h.SegmentCount)
	binary.LittleEndian.PutUint16(bs[8:], h.SegmentIdx)
	binary.LittleEndian.PutUint16(bs[10:], h.CallID)
	binary.LittleEndian.PutUint32(bs[12:], h.CallSN)
	return bs[:16]
}

func (h *Header) FormatRaw(bs []byte) (out []byte) {
	_ = bs[HeaderSizeRaw-1]
	binary.LittleEndian.PutUint16(bs[0:], h.Len)
	binary.LittleEndian.PutUint16(bs[2:], h.CtxLen)
	binary.LittleEndian.PutUint16(bs[4:], h.Ver)
	return bs[:6]
}

func (h *Header) FormatCall(bs []byte) (out []byte) {
	_ = bs[HeaderSize-1] // 去除接下来的边界检查
	binary.LittleEndian.PutUint16(bs[0:], h.Len)
	binary.LittleEndian.PutUint16(bs[2:], h.CtxLen)
	binary.LittleEndian.PutUint16(bs[4:], h.Ver)
	binary.LittleEndian.PutUint16(bs[6:], h.SegmentCount)
	binary.LittleEndian.PutUint16(bs[8:], h.SegmentIdx)
	binary.LittleEndian.PutUint16(bs[10:], h.CallID)
	binary.LittleEndian.PutUint32(bs[12:], h.CallSN)
	return bs[:16]
}
