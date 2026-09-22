package fake_tcp

import (
	"encoding/binary"
	"net/netip"
)

// header.go：IPv4/TCP 头编解码与校验和，纯函数、零状态。
// 所有多字节字段在线路上均为大端（网络字节序），结构体字段为主机序。

const (
	IPv4MinHeaderLen = 20
	TCPMinHeaderLen  = 20
	ProtocolTCP      = 6

	// DefaultTTL 模拟 Linux 的默认 TTL，两端保持一致（避免中间盒用 TTL 差异嗅探）
	DefaultTTL = 64
)

// TCP flags（TCP 头第 13 字节）
const (
	FlagFIN uint8 = 0x01
	FlagSYN uint8 = 0x02
	FlagRST uint8 = 0x04
	FlagPSH uint8 = 0x08
	FlagACK uint8 = 0x10
	FlagURG uint8 = 0x20
)

// TCP option kinds
const (
	optKindEnd       = 0
	optKindNOP       = 1
	optKindMSS       = 2
	optKindWscale    = 3
	optKindSACKPerm  = 4
	optKindSACK      = 5
	optKindTimestamp = 8
)

// TCPOption 解析出的单个 TCP 选项
type TCPOption struct {
	Kind uint8
	Data []byte // 不含 Kind/Len 的选项体；EOL/NOP 为 nil
}

// TCPSegment 一个 TCP 段的全部字段（本模块关心的子集）
type TCPSegment struct {
	SrcPort, DstPort uint16
	Seq, Ack         uint32
	Flags            uint8
	Window           uint16
	Urgent           uint16
	Options          []TCPOption
	Payload          []byte // 引用调用方缓冲，勿长期持有
}

// IPv4Fields IPv4 头字段（无选项，IHL 恒为 5）
type IPv4Fields struct {
	Src, Dst  netip.Addr // 仅支持 IPv4
	ID        uint16
	TTL       uint8
	Protocol  uint8
	DontFrag  bool
	HeaderLen int // 解析时填充（恒为 20，无选项）
}

// Checksum 计算 16 位反码和（RFC 1071），用于 IP 头与 TCP 校验和
func Checksum(b []byte) uint16 {
	return checksumFinish(checksum(0, b))
}

// checksum 在初始值 sum（非反码）基础上累加 b，返回反码
func checksum(sum uint32, b []byte) uint32 {
	for len(b) >= 2 {
		sum += uint32(binary.BigEndian.Uint16(b))
		b = b[2:]
	}
	if len(b) == 1 { // 奇数字节补零
		sum += uint32(b[0]) << 8
	}
	return sum
}

// checksumFinish 进位折叠并取反
func checksumFinish(sum uint32) uint16 {
	for sum > 0xffff {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return ^uint16(sum)
}

// TCPChecksum 计算 TCP 校验和（伪头 + TCP 段）。segment 中的校验和字段必须已置零或已填入；
// 调用方负责在填入校验和前调用本函数。
func TCPChecksum(src, dst netip.Addr, segment []byte) uint16 {
	var pseudo [12]byte
	s4 := src.As4()
	d4 := dst.As4()
	copy(pseudo[0:4], s4[:])
	copy(pseudo[4:8], d4[:])
	pseudo[8] = 0
	pseudo[9] = ProtocolTCP
	binary.BigEndian.PutUint16(pseudo[10:12], uint16(len(segment)))
	sum := checksum(0, pseudo[:])
	sum = checksum(sum, segment)
	return checksumFinish(sum)
}

// VerifyTCPChecksum 校验收到的 TCP 段（校验和字段已填入时，整体反码和应为 0）
func VerifyTCPChecksum(src, dst netip.Addr, segment []byte) bool {
	return TCPChecksum(src, dst, segment) == 0
}

// MarshalIPv4 序列化 IPv4 头（20 字节，无选项，DF=1，TTL=64），返回追加后的缓冲
func MarshalIPv4(dst []byte, f IPv4Fields, payloadLen int) []byte {
	base := len(dst)
	dst = append(dst, make([]byte, IPv4MinHeaderLen)...)
	b := dst[base:]
	b[0] = 0x45 // Ver=4, IHL=5
	b[1] = 0    // TOS
	binary.BigEndian.PutUint16(b[2:4], uint16(IPv4MinHeaderLen+payloadLen))
	binary.BigEndian.PutUint16(b[4:6], f.ID)
	if f.DontFrag {
		binary.BigEndian.PutUint16(b[6:8], 0x4000) // DF=1, FragOff=0
	} else {
		binary.BigEndian.PutUint16(b[6:8], 0)
	}
	b[8] = f.TTL
	b[9] = f.Protocol
	// b[10:12] 校验和先置零
	s4 := f.Src.As4()
	d4 := f.Dst.As4()
	copy(b[12:16], s4[:])
	copy(b[16:20], d4[:])
	binary.BigEndian.PutUint16(b[10:12], Checksum(b))
	return dst
}

// ParseIPv4 解析 IPv4 头，返回字段、载荷（剩余字节）与错误
func ParseIPv4(b []byte) (f IPv4Fields, payload []byte, err error) {
	if len(b) < IPv4MinHeaderLen {
		return f, nil, ErrInvalidPacket.Newf("IPv4 头过短: %d", len(b))
	}
	if b[0]>>4 != 4 {
		return f, nil, ErrInvalidPacket.Newf("非 IPv4: ver=%d", b[0]>>4)
	}
	ihl := int(b[0]&0x0f) * 4
	if ihl < IPv4MinHeaderLen || len(b) < ihl {
		return f, nil, ErrInvalidPacket.Newf("IPv4 IHL 非法: %d", ihl)
	}
	total := int(binary.BigEndian.Uint16(b[2:4]))
	if total < ihl || total > len(b) {
		return f, nil, ErrInvalidPacket.Newf("IPv4 TotalLen 非法: %d(实际 %d)", total, len(b))
	}
	f.ID = binary.BigEndian.Uint16(b[4:6])
	frag := binary.BigEndian.Uint16(b[6:8])
	f.DontFrag = frag&0x4000 != 0
	if frag&0x3fff != 0 { // MF 或偏移非零：分片包，本模块不处理
		return f, nil, ErrInvalidPacket.New("IPv4 分片包不支持")
	}
	f.TTL = b[8]
	f.Protocol = b[9]
	f.Src = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
	f.Dst = netip.AddrFrom4([4]byte{b[16], b[17], b[18], b[19]})
	f.HeaderLen = ihl
	return f, b[ihl:total], nil
}

// MarshalTCP 序列化 TCP 段（含选项与校验和），src/dst 用于伪头校验和。
// 选项编码顺序固定模拟 Linux 5.x：SYN 包 MSS|SACK-Permitted|TS|NOP|WS；其余包 NOP|NOP|TS。
func MarshalTCP(dst []byte, src, dstIP netip.Addr, seg *TCPSegment) ([]byte, error) {
	base := len(dst)
	dst = append(dst, make([]byte, TCPMinHeaderLen)...)
	h := dst[base:]

	binary.BigEndian.PutUint16(h[0:2], seg.SrcPort)
	binary.BigEndian.PutUint16(h[2:4], seg.DstPort)
	binary.BigEndian.PutUint32(h[4:8], seg.Seq)
	binary.BigEndian.PutUint32(h[8:12], seg.Ack)
	h[12] = 0 // DataOff 在选项写完后回填
	h[13] = seg.Flags
	binary.BigEndian.PutUint16(h[14:16], seg.Window)
	// h[16:18] 校验和最后回填
	binary.BigEndian.PutUint16(h[18:20], seg.Urgent)

	// 选项
	for _, opt := range seg.Options {
		switch opt.Kind {
		case optKindEnd:
			dst = append(dst, optKindEnd)
		case optKindNOP:
			dst = append(dst, optKindNOP)
		default:
			dst = append(dst, opt.Kind, byte(len(opt.Data)+2))
			dst = append(dst, opt.Data...)
		}
	}
	// 4 字节对齐（EOL 填充）
	for (len(dst)-base)%4 != 0 {
		dst = append(dst, optKindEnd)
	}
	dst = append(dst, seg.Payload...)

	// 注意：append 可能导致底层数组迁移，此处必须重新取切片再回填
	h = dst[base:]
	// DataOff = （固定头 + 选项对齐后长度） / 4
	headLen := len(dst) - base - len(seg.Payload)
	h[12] = byte(headLen / 4 << 4)

	// 校验和（伪头 + 整个 TCP 段）
	c := TCPChecksum(src, dstIP, dst[base:])
	binary.BigEndian.PutUint16(h[16:18], c)
	return dst, nil
}

// ParseTCP 解析 TCP 段（不含 IP 头）
func ParseTCP(b []byte) (seg *TCPSegment, err error) {
	if len(b) < TCPMinHeaderLen {
		return nil, ErrInvalidPacket.Newf("TCP 头过短: %d", len(b))
	}
	seg = &TCPSegment{
		SrcPort: binary.BigEndian.Uint16(b[0:2]),
		DstPort: binary.BigEndian.Uint16(b[2:4]),
		Seq:     binary.BigEndian.Uint32(b[4:8]),
		Ack:     binary.BigEndian.Uint32(b[8:12]),
		Flags:   b[13],
		Window:  binary.BigEndian.Uint16(b[14:16]),
		Urgent:  binary.BigEndian.Uint16(b[18:20]),
	}
	dataOff := int(b[12]>>4) * 4
	if dataOff < TCPMinHeaderLen || dataOff > len(b) {
		return nil, ErrInvalidPacket.Newf("TCP DataOff 非法: %d", dataOff)
	}
	// 选项解析：遇 EOL 停止；NOP 保留到 Options（保证解析→序列化字节级往返一致）
	opts := b[TCPMinHeaderLen:dataOff]
	for i := 0; i < len(opts); {
		kind := opts[i]
		if kind == optKindEnd {
			break
		}
		if kind == optKindNOP {
			seg.Options = append(seg.Options, TCPOption{Kind: optKindNOP})
			i++
			continue
		}
		if i+1 >= len(opts) {
			return nil, ErrInvalidPacket.New("TCP 选项截断")
		}
		l := int(opts[i+1])
		if l < 2 || i+l > len(opts) {
			return nil, ErrInvalidPacket.Newf("TCP 选项长度非法: kind=%d len=%d", kind, l)
		}
		seg.Options = append(seg.Options, TCPOption{Kind: kind, Data: opts[i+2 : i+l]})
		i += l
	}
	seg.Payload = b[dataOff:]
	return seg, nil
}

// ---- 选项便捷构造/读取 ----

// OptMSS 构造 MSS 选项
func OptMSS(mss uint16) TCPOption {
	var d [2]byte
	binary.BigEndian.PutUint16(d[:], mss)
	return TCPOption{Kind: optKindMSS, Data: d[:]}
}

// OptSACKPermitted 构造 SACK-Permitted 选项（仅握手）
func OptSACKPermitted() TCPOption {
	return TCPOption{Kind: optKindSACKPerm, Data: []byte{}}
}

// OptWscale 构造 Window Scale 选项（仅握手）
func OptWscale(shift uint8) TCPOption {
	return TCPOption{Kind: optKindWscale, Data: []byte{shift}}
}

// OptTimestamp 构造 Timestamp 选项
func OptTimestamp(tsval, tsecr uint32) TCPOption {
	var d [8]byte
	binary.BigEndian.PutUint32(d[0:4], tsval)
	binary.BigEndian.PutUint32(d[4:8], tsecr)
	return TCPOption{Kind: optKindTimestamp, Data: d[:]}
}

// OptSACK 构造 SACK 选项（1~4 个 block，block 为左/右沿 seq 对）
func OptSACK(blocks [][2]uint32) TCPOption {
	d := make([]byte, 0, len(blocks)*8)
	for _, blk := range blocks {
		var b [8]byte
		binary.BigEndian.PutUint32(b[0:4], blk[0])
		binary.BigEndian.PutUint32(b[4:8], blk[1])
		d = append(d, b[:]...)
	}
	return TCPOption{Kind: optKindSACK, Data: d}
}

// OptionMSS 读取 MSS；ok=false 表示无此选项
func (s *TCPSegment) OptionMSS() (mss uint16, ok bool) {
	for _, o := range s.Options {
		if o.Kind == optKindMSS && len(o.Data) == 2 {
			return binary.BigEndian.Uint16(o.Data), true
		}
	}
	return 0, false
}

// OptionTimestamp 读取 TS 选项
func (s *TCPSegment) OptionTimestamp() (tsval, tsecr uint32, ok bool) {
	for _, o := range s.Options {
		if o.Kind == optKindTimestamp && len(o.Data) == 8 {
			return binary.BigEndian.Uint32(o.Data[0:4]), binary.BigEndian.Uint32(o.Data[4:8]), true
		}
	}
	return 0, 0, false
}

// OptionWscale 读取 Window Scale 选项
func (s *TCPSegment) OptionWscale() (shift uint8, ok bool) {
	for _, o := range s.Options {
		if o.Kind == optKindWscale && len(o.Data) == 1 {
			return o.Data[0], true
		}
	}
	return 0, false
}

// OptionSACKPermitted 是否带 SACK-Permitted
func (s *TCPSegment) OptionSACKPermitted() bool {
	for _, o := range s.Options {
		if o.Kind == optKindSACKPerm {
			return true
		}
	}
	return false
}

// OptionSACK 读取 SACK blocks
func (s *TCPSegment) OptionSACK() (blocks [][2]uint32, ok bool) {
	for _, o := range s.Options {
		if o.Kind == optKindSACK && len(o.Data)%8 == 0 && len(o.Data) > 0 {
			for i := 0; i+8 <= len(o.Data); i += 8 {
				blocks = append(blocks, [2]uint32{
					binary.BigEndian.Uint32(o.Data[i : i+4]),
					binary.BigEndian.Uint32(o.Data[i+4 : i+8]),
				})
			}
			return blocks, true
		}
	}
	return nil, false
}

// synOptions 构造握手包选项（模拟 Linux 5.x：MSS|SACKP|TS|NOP|WS）
func synOptions(mss uint16, wscale uint8, tsval, tsecr uint32) []TCPOption {
	return []TCPOption{
		OptMSS(mss),
		OptSACKPermitted(),
		OptTimestamp(tsval, tsecr),
		{Kind: optKindNOP},
		OptWscale(wscale),
	}
}

// dataOptions 构造数据/ACK 包选项（NOP|NOP|TS）
func dataOptions(tsval, tsecr uint32) []TCPOption {
	return []TCPOption{
		{Kind: optKindNOP},
		{Kind: optKindNOP},
		OptTimestamp(tsval, tsecr),
	}
}
