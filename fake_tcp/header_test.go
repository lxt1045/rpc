package fake_tcp

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"testing"
)

// TestChecksumRFC1071 RFC 1071 附录的校验和示例：
// 数据 00 01 f2 03 f4 f5 f6 f7，反码和为 0xddf2，校验和为 0x220d
func TestChecksumRFC1071(t *testing.T) {
	data := []byte{0x00, 0x01, 0xf2, 0x03, 0xf4, 0xf5, 0xf6, 0xf7}
	if c := Checksum(data); c != 0x220d {
		t.Fatalf("RFC1071 向量校验和错误: got %#04x want 0x220d", c)
	}
	// 奇数长度：末尾补零参与计算
	if c := Checksum(data[:7]); c == 0 || c == 0xffff {
		t.Fatalf("奇数长度校验和异常: %#04x", c)
	}
}

// goldenSYN 手工构造的 Linux 风格 SYN 报文（IP+TCP，60 字节，无 payload）：
// IP: 45 00 00 3c ...；TCP: 20 字节头 + MSS(1460)|SACKP|TS|NOP|WS(7) 共 20 字节选项
var goldenSYN = func() []byte {
	src := netip.MustParseAddr("192.168.1.100")
	dst := netip.MustParseAddr("10.0.0.1")
	seg := &TCPSegment{
		SrcPort: 54321,
		DstPort: 8443,
		Seq:     0x12345678,
		Ack:     0,
		Flags:   FlagSYN,
		Window:  64240,
		Options: synOptions(1460, 7, 0xa1b2c3d4, 0),
	}
	pkt, err := MarshalTCP(nil, src, dst, seg)
	if err != nil {
		panic(err)
	}
	ip := MarshalIPv4(nil, IPv4Fields{Src: src, Dst: dst, ID: 0x1a2b, TTL: DefaultTTL, Protocol: ProtocolTCP, DontFrag: true}, len(pkt))
	return append(ip, pkt...)
}()

// TestGoldenSYN 用真实形状的 SYN 字节串验证解析 + 往返序列化一致
func TestGoldenSYN(t *testing.T) {
	ipf, ipPayload, err := ParseIPv4(goldenSYN)
	if err != nil {
		t.Fatalf("ParseIPv4: %v", err)
	}
	if ipf.Protocol != ProtocolTCP || ipf.TTL != DefaultTTL || !ipf.DontFrag {
		t.Fatalf("IP 字段不符: %+v", ipf)
	}
	if ipf.Src.String() != "192.168.1.100" || ipf.Dst.String() != "10.0.0.1" {
		t.Fatalf("IP 地址不符: %s -> %s", ipf.Src, ipf.Dst)
	}
	if ipf.ID != 0x1a2b {
		t.Fatalf("IP ID 不符: %#x", ipf.ID)
	}
	// IP 头校验和自洽
	if Checksum(goldenSYN[:IPv4MinHeaderLen]) != 0 {
		t.Fatalf("IP 头校验和自洽失败")
	}
	if len(ipPayload) != 40 {
		t.Fatalf("TCP 段长度应为 40，实际 %d", len(ipPayload))
	}

	seg, err := ParseTCP(ipPayload)
	if err != nil {
		t.Fatalf("ParseTCP: %v", err)
	}
	if seg.SrcPort != 54321 || seg.DstPort != 8443 {
		t.Fatalf("端口不符: %d -> %d", seg.SrcPort, seg.DstPort)
	}
	if seg.Seq != 0x12345678 || seg.Ack != 0 || seg.Flags != FlagSYN {
		t.Fatalf("TCP 字段不符: seq=%#x ack=%#x flags=%#x", seg.Seq, seg.Ack, seg.Flags)
	}
	if mss, ok := seg.OptionMSS(); !ok || mss != 1460 {
		t.Fatalf("MSS 选项不符: %v %v", mss, ok)
	}
	if !seg.OptionSACKPermitted() {
		t.Fatalf("缺少 SACK-Permitted")
	}
	if tsval, tsecr, ok := seg.OptionTimestamp(); !ok || tsval != 0xa1b2c3d4 || tsecr != 0 {
		t.Fatalf("TS 选项不符: %v %v %v", tsval, tsecr, ok)
	}
	if ws, ok := seg.OptionWscale(); !ok || ws != 7 {
		t.Fatalf("WS 选项不符: %v %v", ws, ok)
	}
	// TCP 校验和自洽
	if !VerifyTCPChecksum(ipf.Src, ipf.Dst, ipPayload) {
		t.Fatalf("TCP 校验和自洽失败")
	}

	// 往返：解析结果重新序列化应得到相同字节
	// （解析时 options 不含对齐填充的 EOL，重新序列化会补回对齐，故比较 TCP 段整体）
	reseg, err := MarshalTCP(nil, ipf.Src, ipf.Dst, seg)
	if err != nil {
		t.Fatalf("MarshalTCP: %v", err)
	}
	if !bytes.Equal(reseg, ipPayload) {
		t.Fatalf("TCP 往返不一致:\n got %x\nwant %x", reseg, ipPayload)
	}
}

// TestDataPacketRoundTrip 数据包（PSH+ACK + NOP|NOP|TS 选项 + payload）编解码往返
func TestDataPacketRoundTrip(t *testing.T) {
	src := netip.MustParseAddr("10.1.2.3")
	dst := netip.MustParseAddr("10.4.5.6")
	payload := []byte("hello fake tcp payload")
	seg := &TCPSegment{
		SrcPort: 12345,
		DstPort: 8443,
		Seq:     1000,
		Ack:     2000,
		Flags:   FlagPSH | FlagACK,
		Window:  defaultWindow,
		Options: dataOptions(111, 222),
		Payload: payload,
	}
	b, err := MarshalTCP(nil, src, dst, seg)
	if err != nil {
		t.Fatalf("MarshalTCP: %v", err)
	}
	got, err := ParseTCP(b)
	if err != nil {
		t.Fatalf("ParseTCP: %v", err)
	}
	if got.Seq != 1000 || got.Ack != 2000 || got.Flags != FlagPSH|FlagACK || got.Window != defaultWindow {
		t.Fatalf("字段不符: %+v", got)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("payload 不符: %q", got.Payload)
	}
	if v, e, ok := got.OptionTimestamp(); !ok || v != 111 || e != 222 {
		t.Fatalf("TS 不符: %v %v %v", v, e, ok)
	}
	if !VerifyTCPChecksum(src, dst, b) {
		t.Fatalf("校验和自洽失败")
	}
}

// TestSACKOption SACK block 编解码
func TestSACKOption(t *testing.T) {
	blocks := [][2]uint32{{100, 200}, {300, 400}}
	src := netip.MustParseAddr("10.0.0.1")
	dst := netip.MustParseAddr("10.0.0.2")
	seg := &TCPSegment{
		SrcPort: 1, DstPort: 2, Flags: FlagACK, Window: defaultWindow,
		Options: append(dataOptions(1, 2), OptSACK(blocks)),
	}
	b, err := MarshalTCP(nil, src, dst, seg)
	if err != nil {
		t.Fatalf("MarshalTCP: %v", err)
	}
	got, err := ParseTCP(b)
	if err != nil {
		t.Fatalf("ParseTCP: %v", err)
	}
	gb, ok := got.OptionSACK()
	if !ok || len(gb) != 2 || gb[0] != blocks[0] || gb[1] != blocks[1] {
		t.Fatalf("SACK 不符: %v %v", gb, ok)
	}
}

// TestParseBadPacket 畸形报文必须报错而不是 panic
func TestParseBadPacket(t *testing.T) {
	cases := [][]byte{
		nil,                                   // 空
		{0x45},                                // 过短
		{0x65, 0, 0, 20},                      // 非法版本 6
		append([]byte{0x45, 0, 0, 10}, make([]byte, 16)...), // TotalLen < IHL
	}
	for i, c := range cases {
		if _, _, err := ParseIPv4(c); err == nil {
			t.Fatalf("case %d 应报错", i)
		}
	}
	// TCP 过短 / 非法 DataOff
	if _, err := ParseTCP([]byte{1, 2, 3}); err == nil {
		t.Fatalf("TCP 过短应报错")
	}
	bad := make([]byte, TCPMinHeaderLen)
	bad[12] = 0xf0 // DataOff=60 > len
	if _, err := ParseTCP(bad); err == nil {
		t.Fatalf("非法 DataOff 应报错")
	}
	// 选项截断
	trunc := make([]byte, TCPMinHeaderLen+4)
	trunc[12] = 0x60 // DataOff=24，选项 4 字节
	trunc[TCPMinHeaderLen] = optKindMSS
	trunc[TCPMinHeaderLen+1] = 10 // 声明 10 字节但实际只有 4
	if _, err := ParseTCP(trunc); err == nil {
		t.Fatalf("选项截断应报错")
	}
}

// TestMarshalIPv4Fields 逐字段核对 IP 头字节布局
func TestMarshalIPv4Fields(t *testing.T) {
	src := netip.MustParseAddr("1.2.3.4")
	dst := netip.MustParseAddr("5.6.7.8")
	b := MarshalIPv4(nil, IPv4Fields{Src: src, Dst: dst, ID: 0xbeef, TTL: 64, Protocol: ProtocolTCP, DontFrag: true}, 40)
	if len(b) != 20 {
		t.Fatalf("IP 头长度 %d", len(b))
	}
	if b[0] != 0x45 || b[8] != 64 || b[9] != 6 {
		t.Fatalf("字段不符: %x", b)
	}
	if binary.BigEndian.Uint16(b[2:4]) != 60 {
		t.Fatalf("TotalLen 不符: %d", binary.BigEndian.Uint16(b[2:4]))
	}
	if binary.BigEndian.Uint16(b[6:8]) != 0x4000 {
		t.Fatalf("DF 不符: %#x", binary.BigEndian.Uint16(b[6:8]))
	}
	if !bytes.Equal(b[12:16], []byte{1, 2, 3, 4}) || !bytes.Equal(b[16:20], []byte{5, 6, 7, 8}) {
		t.Fatalf("地址不符: %x", b)
	}
	if Checksum(b) != 0 {
		t.Fatalf("IP 校验和自洽失败")
	}
}
