package faux_tcp

import (
	"encoding/binary"
	"net/netip"

	"github.com/lxt1045/errors"
)

// TCP flags
const (
	flagFIN = 0x01
	flagSYN = 0x02
	flagRST = 0x04
	flagPSH = 0x08
	flagACK = 0x10
)

const (
	ipv4HeaderLen = 20
	tcpHeaderLen  = 20

	protoTCP = 6

	// ipv4 DF 位（禁止分片）
	ipv4FlagDF = 0x4000
)

// Endpoint 一个 TCP 端点（IPv4 + 端口）
type Endpoint struct {
	IP   netip.Addr // 仅支持 IPv4
	Port uint16
}

func (e Endpoint) String() string {
	return netip.AddrPortFrom(e.IP, e.Port).String()
}

// Packet 一个解码后的 TCP 段（仅我们关心的字段）
type Packet struct {
	Src, Dst Endpoint

	Seq, Ack uint32
	Flags    uint8 // flagFIN|flagSYN|flagRST|flagPSH|flagACK
	Window   uint16

	TSval, TSecr uint32 // 时间戳选项（0 表示不存在）
	Payload      []byte // 引用原始缓冲区，调用方需要时自行拷贝
}

// Has 判断标志位
func (p *Packet) Has(f uint8) bool { return p.Flags&f != 0 }

// ---------------------------------------------------------------------------
// 构包
// ---------------------------------------------------------------------------

// tcpOptionsSyn 仿 Linux 的 SYN 选项布局：
// MSS(4) + SACK-perm(2) + TS(10) + NOP(1) + WS(3) = 20 字节
func tcpOptionsSyn(mss int, wscale uint8, tsval uint32) []byte {
	opts := make([]byte, 20)
	opts[0], opts[1] = 2, 4 // MSS
	binary.BigEndian.PutUint16(opts[2:4], uint16(mss))
	opts[4], opts[5] = 4, 2 // SACK permitted
	opts[6], opts[7] = 8, 10 // TS
	binary.BigEndian.PutUint32(opts[8:12], tsval)
	binary.BigEndian.PutUint32(opts[12:16], 0) // TSecr=0
	opts[16] = 1                               // NOP
	opts[17], opts[18], opts[19] = 3, 3, wscale
	return opts
}

// tcpOptionsData 仿 Linux 的数据段选项布局：NOP(1) + NOP(1) + TS(10) = 12 字节
func tcpOptionsData(tsval, tsecr uint32) []byte {
	opts := make([]byte, 12)
	opts[0], opts[1] = 1, 1 // NOP NOP
	opts[2], opts[3] = 8, 10
	binary.BigEndian.PutUint32(opts[4:8], tsval)
	binary.BigEndian.PutUint32(opts[8:12], tsecr)
	return opts
}

// buildPacket 构造完整的 IPv4+TCP 报文（含两个校验和）。
// 发送侧不计算校验和的硬件 offload 场景不存在于 raw socket，故始终软件计算。
func buildPacket(cfg *Config, src, dst Endpoint, seq, ack uint32, flags uint8,
	tsval, tsecr uint32, payload []byte, ipID uint16) []byte {

	var opts []byte
	if flags&flagSYN != 0 {
		opts = tcpOptionsSyn(cfg.MSS, cfg.WScale, tsval)
	} else {
		opts = tcpOptionsData(tsval, tsecr)
	}

	tcpLen := tcpHeaderLen + len(opts) + len(payload)
	totalLen := ipv4HeaderLen + tcpLen
	bs := make([]byte, totalLen)

	// IPv4 头
	bs[0] = 0x45 // version=4, IHL=5
	bs[1] = 0    // TOS
	binary.BigEndian.PutUint16(bs[2:4], uint16(totalLen))
	binary.BigEndian.PutUint16(bs[4:6], ipID)
	binary.BigEndian.PutUint16(bs[6:8], ipv4FlagDF)
	bs[8] = cfg.TTL
	bs[9] = protoTCP
	// 校验和先留 0
	src4 := src.IP.As4()
	dst4 := dst.IP.As4()
	copy(bs[12:16], src4[:])
	copy(bs[16:20], dst4[:])
	binary.BigEndian.PutUint16(bs[10:12], checksum(bs[:ipv4HeaderLen]))

	// TCP 头
	tcp := bs[ipv4HeaderLen:]
	binary.BigEndian.PutUint16(tcp[0:2], src.Port)
	binary.BigEndian.PutUint16(tcp[2:4], dst.Port)
	binary.BigEndian.PutUint32(tcp[4:8], seq)
	binary.BigEndian.PutUint32(tcp[8:12], ack)
	tcp[12] = uint8((tcpHeaderLen + len(opts)) / 4 << 4)
	tcp[13] = flags
	binary.BigEndian.PutUint16(tcp[14:16], cfg.Window)
	// checksum 先留 0，urgent=0
	copy(tcp[tcpHeaderLen:], opts)
	copy(tcp[tcpHeaderLen+len(opts):], payload)

	// TCP 校验和：伪首部 + TCP 头 + 载荷
	sum := uint32(0)
	sum += uint32(binary.BigEndian.Uint16(src4[0:2])) + uint32(binary.BigEndian.Uint16(src4[2:4]))
	sum += uint32(binary.BigEndian.Uint16(dst4[0:2])) + uint32(binary.BigEndian.Uint16(dst4[2:4]))
	sum += uint32(protoTCP) + uint32(uint16(tcpLen))
	binary.BigEndian.PutUint16(tcp[16:18], checksumContinue(sum, tcp))
	return bs
}

// checksum 计算标准 Internet 校验和（RFC 1071）
func checksum(bs []byte) uint16 {
	return checksumContinue(0, bs)
}

func checksumContinue(sum uint32, bs []byte) uint16 {
	for len(bs) >= 2 {
		sum += uint32(binary.BigEndian.Uint16(bs[:2]))
		bs = bs[2:]
	}
	if len(bs) == 1 {
		sum += uint32(bs[0]) << 8
	}
	for sum > 0xffff {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return ^uint16(sum)
}

// ---------------------------------------------------------------------------
// 解包
// ---------------------------------------------------------------------------

// parsePacket 解析一个 IPv4 报文（不含以太网头，link 层负责剥离）。
// 校验 IPv4 头校验和与 TCP 校验和；非 TCP/校验失败返回错误。
func parsePacket(bs []byte) (p *Packet, err error) {
	if len(bs) < ipv4HeaderLen+tcpHeaderLen {
		return nil, errors.Errorf("packet too short: %d", len(bs))
	}
	if bs[0]>>4 != 4 {
		return nil, errors.Errorf("not IPv4")
	}
	ihl := int(bs[0]&0x0f) * 4
	if ihl < ipv4HeaderLen || len(bs) < ihl+tcpHeaderLen {
		return nil, errors.Errorf("bad IHL: %d", ihl)
	}
	totalLen := int(binary.BigEndian.Uint16(bs[2:4]))
	if totalLen < ihl+tcpHeaderLen || len(bs) < totalLen {
		return nil, errors.Errorf("bad total length: %d", totalLen)
	}
	if bs[9] != protoTCP {
		return nil, errors.Errorf("not TCP: %d", bs[9])
	}
	if checksum(bs[:ihl]) != 0 {
		return nil, errors.Errorf("bad IPv4 checksum")
	}
	// 分片包不支持（我们发送侧始终 DF，收到的分片包直接丢弃）
	frag := binary.BigEndian.Uint16(bs[6:8])
	if frag&0x3fff != 0 || frag&0x2000 != 0 {
		return nil, errors.Errorf("fragmented packet")
	}

	srcIP, _ := netip.AddrFromSlice(bs[12:16])
	dstIP, _ := netip.AddrFromSlice(bs[16:20])
	tcp := bs[ihl:totalLen]

	p = &Packet{
		Src:    Endpoint{IP: srcIP, Port: binary.BigEndian.Uint16(tcp[0:2])},
		Dst:    Endpoint{IP: dstIP, Port: binary.BigEndian.Uint16(tcp[2:4])},
		Seq:    binary.BigEndian.Uint32(tcp[4:8]),
		Ack:    binary.BigEndian.Uint32(tcp[8:12]),
		Flags:  tcp[13],
		Window: binary.BigEndian.Uint16(tcp[14:16]),
	}
	dataOffset := int(tcp[12]>>4) * 4
	if dataOffset < tcpHeaderLen || len(tcp) < dataOffset {
		return nil, errors.Errorf("bad TCP data offset: %d", dataOffset)
	}

	// TCP 校验和
	sum := uint32(0)
	sum += uint32(binary.BigEndian.Uint16(bs[12:14])) + uint32(binary.BigEndian.Uint16(bs[14:16]))
	sum += uint32(binary.BigEndian.Uint16(bs[16:18])) + uint32(binary.BigEndian.Uint16(bs[18:20]))
	sum += uint32(protoTCP) + uint32(uint16(len(tcp)))
	if checksumContinue(sum, tcp) != 0 {
		return nil, errors.Errorf("bad TCP checksum")
	}

	// 解析选项（只关心 TS）
	parseOptions(tcp[tcpHeaderLen:dataOffset], p)

	p.Payload = tcp[dataOffset:]
	return p, nil
}

func parseOptions(opts []byte, p *Packet) {
	for i := 0; i < len(opts); {
		kind := opts[i]
		if kind == 0 { // EOL
			return
		}
		if kind == 1 { // NOP
			i++
			continue
		}
		if i+1 >= len(opts) {
			return
		}
		l := int(opts[i+1])
		if l < 2 || i+l > len(opts) {
			return
		}
		if kind == 8 && l == 10 { // TS
			p.TSval = binary.BigEndian.Uint32(opts[i+2 : i+6])
			p.TSecr = binary.BigEndian.Uint32(opts[i+6 : i+10])
		}
		i += l
	}
}

// stripEthernet 剥离以太网头；非 IPv4 以太帧返回 nil。
// 返回的切片引用入参。
func stripEthernet(frame []byte) []byte {
	const ethLen = 14
	if len(frame) < ethLen {
		return nil
	}
	if binary.BigEndian.Uint16(frame[12:14]) != 0x0800 {
		return nil
	}
	return frame[ethLen:]
}
