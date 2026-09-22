package fake_tcp

import (
	"encoding/binary"
	"net"
	"net/netip"
	"sync/atomic"

	"github.com/lxt1045/errors"
)

// udp_packetio.go：ModeUDP 兜底模式的 LinkIO 实现。
// 线格式（plan.md §4.6/§4.7）：
//
//	[Magic   uint32 LE]
//	[ConnID  uint64 LE]
//	[Type    uint8     ]
//	[payload ...]      // 仅 Type=Data 时存在
//
// 控制消息（SYN/SYNACK/FIN/FINACK/RST/Keepalive）无 payload，
// 与 TCP flags 的映射见 udpTypeToFlags / udpFlagsToType。

const udpHdrLen = 4 + 8 + 1

const (
	udpTypeData uint8 = iota
	udpTypeSYN
	udpTypeSYNACK
	udpTypeFIN
	udpTypeFINACK
	udpTypeRST
	udpTypeKeepalive
	udpTypeKeepaliveAck // 保活应答（探针应答应答不再回复，避免乒乓）
)

func udpTypeToFlags(t uint8) (uint8, bool) {
	switch t {
	case udpTypeData:
		return FlagPSH | FlagACK, true
	case udpTypeSYN:
		return FlagSYN, true
	case udpTypeSYNACK:
		return FlagSYN | FlagACK, true
	case udpTypeFIN:
		return FlagFIN, true
	case udpTypeFINACK:
		return FlagFIN | FlagACK, true
	case udpTypeRST:
		return FlagRST, true
	case udpTypeKeepalive:
		return FlagACK, true
	case udpTypeKeepaliveAck:
		return FlagACK | FlagURG, true
	}
	return 0, false
}

// udpFlagsToType 与 udpTypeToFlags 互逆；Data 优先（payload 非空即 Data）
func udpFlagsToType(flags uint8, hasPayload bool) uint8 {
	if hasPayload {
		return udpTypeData
	}
	switch {
	case flags&FlagRST != 0:
		return udpTypeRST
	case flags&FlagSYN != 0 && flags&FlagACK != 0:
		return udpTypeSYNACK
	case flags&FlagSYN != 0:
		return udpTypeSYN
	case flags&FlagFIN != 0 && flags&FlagACK != 0:
		return udpTypeFINACK
	case flags&FlagFIN != 0:
		return udpTypeFIN
	case flags&FlagURG != 0 && flags&FlagACK != 0:
		return udpTypeKeepaliveAck
	default: // 纯 ACK → 保活探针
		return udpTypeKeepalive
	}
}

// udpLinkIO 基于 net.UDPConn 的 LinkIO。
// 客户端与服务端同构：均用非 connected socket，WriteSegment 走 WriteToUDP(seg.Peer)。
type udpLinkIO struct {
	conn  *net.UDPConn
	magic uint32
	mtu   int

	rBuf   []byte // 读缓冲复用（ReadSegment 与 handle 同 goroutine，安全）
	closed atomic.Bool
}

func newUDPLinkIO(conn *net.UDPConn, magic uint32, mtu int) *udpLinkIO {
	// 尽力放大内核缓冲区，减少突发丢包（超上限会被内核静默截断，忽略错误）
	_ = conn.SetReadBuffer(16 << 20)
	_ = conn.SetWriteBuffer(16 << 20)
	return &udpLinkIO{
		conn:  conn,
		magic: magic,
		mtu:   mtu,
		rBuf:  make([]byte, 64*1024),
	}
}

func (l *udpLinkIO) MaxPayload() int { return l.mtu - udpHdrLen }

// LocalAddr 返回本地 UDP 地址（取实际端口用）
func (l *udpLinkIO) LocalAddr() *net.UDPAddr { return l.conn.LocalAddr().(*net.UDPAddr) }

func (l *udpLinkIO) ReadSegment() (*Segment, error) {
	for {
		n, addr, err := l.conn.ReadFromUDP(l.rBuf)
		if err != nil {
			if l.closed.Load() {
				return nil, ErrConnClosed.New()
			}
			return nil, errors.WithErr(err)
		}
		if n < udpHdrLen {
			continue // 静默丢弃垃圾报文（plan.md §4.2）
		}
		b := l.rBuf[:n]
		if binary.LittleEndian.Uint32(b[0:4]) != l.magic {
			continue // Magic 不符，静默丢弃
		}
		connID := binary.LittleEndian.Uint64(b[4:12])
		flags, ok := udpTypeToFlags(b[12])
		if !ok {
			continue
		}
		ip, _ := netip.AddrFromSlice(addr.IP)
		return &Segment{
			Peer:    PeerAddr{IP: ip, Port: uint16(addr.Port)},
			ConnID:  connID,
			Flags:   flags,
			Payload: b[udpHdrLen:n],
		}, nil
	}
}

func (l *udpLinkIO) WriteSegment(seg *Segment) error {
	if l.closed.Load() {
		return ErrConnClosed.New()
	}
	t := udpFlagsToType(seg.Flags, len(seg.Payload) > 0)
	buf := make([]byte, udpHdrLen+len(seg.Payload))
	binary.LittleEndian.PutUint32(buf[0:4], l.magic)
	binary.LittleEndian.PutUint64(buf[4:12], seg.ConnID)
	buf[12] = t
	copy(buf[udpHdrLen:], seg.Payload)
	udpAddr := &net.UDPAddr{IP: seg.Peer.IP.AsSlice(), Port: int(seg.Peer.Port)}
	if _, err := l.conn.WriteToUDP(buf, udpAddr); err != nil {
		if l.closed.Load() {
			return ErrConnClosed.New()
		}
		return errors.WithErr(err)
	}
	return nil
}

func (l *udpLinkIO) Close() error {
	if l.closed.CompareAndSwap(false, true) {
		return l.conn.Close()
	}
	return nil
}
