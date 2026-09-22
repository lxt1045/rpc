package fake_tcp

import (
	"net/netip"
)

// packetio.go：session 层与底层报文通道之间的抽象。
// 两种模式（RawTCP / UDP）各自实现 LinkIO，session 层只面对统一的 Segment。

// PeerAddr 对端地址（四元组中的一半；本地一半在监听/拨号时确定）
type PeerAddr struct {
	IP   netip.Addr
	Port uint16
}

// sessKey 会话路由键：RawTCP 模式 ConnID 恒为 0（四元组即唯一键）；
// UDP 模式 ConnID 由客户端生成，叠加对端地址防止碰撞。
type sessKey struct {
	Peer   PeerAddr
	ConnID uint64
}

// Segment 一次"报文事件"的统一抽象。
// RawTCP 模式：由 TCP 头解析而来 / 序列化为 TCP 段（Flags/Seq/Ack/TS 有效）；
// UDP 模式：由私有控制头映射而来（仅 Flags 语义与 Payload 有效）。
type Segment struct {
	Peer   PeerAddr // 读：报文来源；写：报文目的
	ConnID uint64

	Flags    uint8 // FlagSYN/FlagACK/FlagFIN/FlagRST/FlagPSH 语义
	Seq      uint32
	Ack      uint32
	TSval    uint32
	TSecr    uint32
	Win      uint16
	SACK     [][2]uint32 // RawTCP 模式：随 ACK 附带的外观 SACK block（plan.md §4.3）
	Payload  []byte      // 读侧：生命周期到下次 ReadSegment 前；写侧：调用方持有
}

// LinkIO 底层报文通道。实现负责报文编解码与收发，必须满足：
//   - ReadSegment 阻塞直到有报文、出错或 Close；
//   - WriteSegment 可并发调用；
//   - Close 后 ReadSegment 立即返回错误，WriteSegment 返回 ErrConnClosed。
type LinkIO interface {
	ReadSegment() (*Segment, error)
	WriteSegment(seg *Segment) error
	// MaxPayload 单个报文可携带的最大应用字节数（Conn.Write 按此切片）
	MaxPayload() int
	Close() error
}
