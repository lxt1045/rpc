//go:build linux

package fake_tcp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"hash/fnv"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/lxt1045/errors"
	"golang.org/x/net/bpf"
	"golang.org/x/sys/unix"
)

// raw_packetio_linux.go：ModeRawTCP 的 LinkIO 实现（plan.md §5）。
//   - 收包：AF_PACKET(SOCK_RAW) + cBPF（过滤 "IPv4 TCP 且 dst port == 本地端口"），
//     拿到完整 L2 帧自行解析，内核协议栈不参与；
//   - 发包：AF_INET/SOCK_RAW/IPPROTO_RAW（自带 IP 头，L2 交给内核路由，免去手工 ARP）；
//   - 内核 RST 抑制：见 firewall_linux.go（Listen/Dial 入口处安装）。

// rawLinkIO 原始套接字报文通道
type rawLinkIO struct {
	cfg   Config
	local PeerAddr  // 本地 IP+Port（IP 可为 0.0.0.0：发送时按对端路由选源地址）
	peer  *PeerAddr // 客户端：固定对端（收包校验）；服务端 nil

	packetFD int // AF_PACKET 收
	sendFD   int // raw IP 发

	ipID     atomic.Uint32 // IP ID 单调递增（模拟 per-flow 计数器）
	srcMu    sync.Mutex
	srcCache map[netip.Addr]netip.Addr // 0.0.0.0 时按对端缓存源地址选择结果

	rBuf   []byte
	closed atomic.Bool
}

func htons(v uint16) uint16 { return v<<8 | v>>8 }

// newRawLinkIO 创建 RawTCP 报文通道。local.Port 为 0（客户端）时自行挑选空闲端口并回填。
func newRawLinkIO(cfg Config, local PeerAddr, peer *PeerAddr) (_ *rawLinkIO, err error) {
	if local.IP.IsValid() && !local.IP.Is4() && !local.IP.IsUnspecified() {
		return nil, ErrInvalidConfig.New("RawTCP 模式仅支持 IPv4")
	}
	if local.Port == 0 {
		local.Port, err = pickFreePort()
		if err != nil {
			return nil, err
		}
	}

	// 收包 socket：AF_PACKET。无权限时 EPERM → ErrNeedRoot
	packetFD, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW, int(htons(unix.ETH_P_ALL)))
	if err != nil {
		return nil, errnoToNeedRoot(err, "AF_PACKET")
	}
	defer func() {
		if err != nil {
			_ = unix.Close(packetFD)
		}
	}()

	// 指定了具体本地 IP 时绑定到对应网卡；0.0.0.0 则不绑（收所有接口）
	if local.IP.IsValid() && !local.IP.IsUnspecified() {
		ifi, ierr := ifaceIndexOf(local.IP)
		if ierr != nil {
			return nil, ierr
		}
		if err = unix.Bind(packetFD, &unix.SockaddrLinklayer{
			Protocol: htons(unix.ETH_P_ALL),
			Ifindex:  ifi,
		}); err != nil {
			return nil, wrapSysErr("AF_PACKET Bind", err)
		}
	}

	// cBPF：只收"发给我们端口"的 TCP（同时天然滤掉自己发出的包——其 dst port 是对端端口）
	prog, err := bpfFilterTCPDstPort(local.Port)
	if err != nil {
		return nil, err
	}
	if err = unix.SetsockoptSockFprog(packetFD, unix.SOL_SOCKET, unix.SO_ATTACH_FILTER, prog); err != nil {
		return nil, wrapSysErr("SO_ATTACH_FILTER", err)
	}

	// 发包 socket：IPPROTO_RAW 隐含 IP_HDRINCL，IP 头协议字段按我们填的 6 发出
	sendFD, err := unix.Socket(unix.AF_INET, unix.SOCK_RAW, unix.IPPROTO_RAW)
	if err != nil {
		return nil, errnoToNeedRoot(err, "raw IP socket")
	}

	return &rawLinkIO{
		cfg:      cfg,
		local:    local,
		peer:     peer,
		packetFD: packetFD,
		sendFD:   sendFD,
		srcCache: make(map[netip.Addr]netip.Addr),
		rBuf:     make([]byte, 64*1024),
	}, nil
}

// bpfFilterTCPDstPort 组装 cBPF：IPv4 && proto==6 && tcp dst port == port
// （以太网 + IPv4 无分片；IHL 可变用 LoadMemShift/LoadIndirect 处理）
func bpfFilterTCPDstPort(port uint16) (*unix.SockFprog, error) {
	raw, err := bpf.Assemble([]bpf.Instruction{
		bpf.LoadAbsolute{Off: 12, Size: 2}, // A = ethertype
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: 0x0800, SkipTrue: 0, SkipFalse: 5},
		bpf.LoadAbsolute{Off: 23, Size: 1}, // A = IP proto（IPv4 头固定偏移 14+9）
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: ProtocolTCP, SkipTrue: 0, SkipFalse: 3},
		bpf.LoadMemShift{Off: 14},          // X = 4*(ip[0]&0xf)（IHL）
		bpf.LoadIndirect{Off: 16, Size: 2}, // A = [X+16]：TCP dst port（14+2）
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: uint32(port), SkipTrue: 1, SkipFalse: 0},
		bpf.RetConstant{Val: 0},
		bpf.RetConstant{Val: 1 << 18},
	})
	if err != nil {
		return nil, ErrInvalidConfig.Newf("cBPF 组装失败: %v", err)
	}
	filters := make([]unix.SockFilter, len(raw))
	for i, r := range raw {
		filters[i] = unix.SockFilter{Code: r.Op, Jt: r.Jt, Jf: r.Jf, K: r.K}
	}
	return &unix.SockFprog{
		Len:    uint16(len(filters)),
		Filter: (*unix.SockFilter)(unsafe.Pointer(&filters[0])),
	}, nil
}

// ifaceIndexOf 按本地 IP 找网卡索引
func ifaceIndexOf(ip netip.Addr) (int, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return 0, err
	}
	for _, ifi := range ifaces {
		addrs, err := ifi.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ipnet *net.IPNet
			switch v := a.(type) {
			case *net.IPNet:
				ipnet = v
			case *net.IPAddr:
				ipnet = &net.IPNet{IP: v.IP, Mask: net.CIDRMask(32, 32)}
			default:
				continue
			}
			na, ok := netip.AddrFromSlice(ipnet.IP)
			if ok && na == ip {
				return ifi.Index, nil
			}
		}
	}
	return 0, ErrInvalidConfig.Newf("本地 IP %s 不属于任何网卡", ip)
}

// pickFreePort 挑选空闲端口：优先从真实 TCP 客户端的临时端口段（49152~65535）
// 随机选取并验证可用；失败则退回让内核分配
func pickFreePort() (uint16, error) {
	for i := 0; i < 16; i++ {
		var b [2]byte
		if _, err := rand.Read(b[:]); err != nil {
			break
		}
		port := 49152 + binary.BigEndian.Uint16(b[:])%(65535-49152)
		ln, err := net.ListenTCP("tcp4", &net.TCPAddr{Port: int(port)})
		if err != nil {
			continue // 被占用，换下一个
		}
		_ = ln.Close()
		return port, nil
	}
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{Port: 0})
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return uint16(port), nil
}

// connIDOf 四元组的交换性哈希（plan.md §4.6：RawTCP 模式 ConnID 填四元组哈希）
func connIDOf(a, b PeerAddr) uint64 {
	h := func(p PeerAddr) uint64 {
		ha := fnv.New64a()
		ha.Write(p.IP.AsSlice())
		var pb [2]byte
		binary.BigEndian.PutUint16(pb[:], p.Port)
		ha.Write(pb[:])
		return ha.Sum64()
	}
	return h(a) ^ h(b) // XOR 保证交换性（两端算出的值一致）
}

// ReadSegment 收一个 TCP 段：AF_PACKET 读 L2 帧 → 解析 IP/TCP → 校验 → 剥离私有头
func (l *rawLinkIO) ReadSegment() (*Segment, error) {
	for {
		n, err := unix.Read(l.packetFD, l.rBuf)
		if err != nil {
			if l.closed.Load() {
				return nil, ErrConnClosed.New()
			}
			if err == unix.EINTR {
				continue
			}
			return nil, wrapSysErr("AF_PACKET Read", err)
		}
		seg, ok := l.parseFrame(l.rBuf[:n])
		if !ok {
			continue
		}
		return seg, nil
	}
}

// parseFrame 解析 L2 帧为 Segment；false 表示跳过（非目标报文/校验失败/Magic 不符）
func (l *rawLinkIO) parseFrame(frame []byte) (*Segment, bool) {
	// 以太网头（lo 同为 14 字节伪以太网头）；兼容 802.1Q
	if len(frame) < 14 {
		return nil, false
	}
	ethType := binary.BigEndian.Uint16(frame[12:14])
	off := 14
	if ethType == 0x8100 { // VLAN tag
		if len(frame) < 18 {
			return nil, false
		}
		ethType = binary.BigEndian.Uint16(frame[16:18])
		off = 18
	}
	if ethType != 0x0800 {
		return nil, false
	}
	ipf, ipPayload, err := ParseIPv4(frame[off:])
	if err != nil || ipf.Protocol != ProtocolTCP {
		return nil, false
	}
	if l.peer != nil && ipf.Src != l.peer.IP { // 客户端只认固定对端
		return nil, false
	}
	if !VerifyTCPChecksum(ipf.Src, ipf.Dst, ipPayload) {
		return nil, false
	}
	tcp, err := ParseTCP(ipPayload)
	if err != nil {
		return nil, false
	}

	seg := &Segment{
		Peer:  PeerAddr{IP: ipf.Src, Port: tcp.SrcPort},
		Flags: tcp.Flags,
		Seq:   tcp.Seq,
		Ack:   tcp.Ack,
		Win:   tcp.Window,
	}
	seg.ConnID = connIDOf(seg.Peer, PeerAddr{IP: ipf.Dst, Port: tcp.DstPort})
	if v, e, ok := tcp.OptionTimestamp(); ok {
		seg.TSval, seg.TSecr = v, e
	}
	if blocks, ok := tcp.OptionSACK(); ok {
		seg.SACK = blocks
	}

	// 数据段剥离私有头 [Magic][ConnID]（plan.md §4.6）；Magic 不符静默丢弃
	if len(tcp.Payload) > 0 {
		if len(tcp.Payload) < 12 || binary.LittleEndian.Uint32(tcp.Payload[0:4]) != l.cfg.Magic {
			return nil, false
		}
		seg.Payload = tcp.Payload[12:]
	}
	return seg, true
}

// WriteSegment 发送一个段：数据段加私有头，构造 TCP/IP 头，raw IP socket 发出
func (l *rawLinkIO) WriteSegment(seg *Segment) error {
	if l.closed.Load() {
		return ErrConnClosed.New()
	}
	srcIP := l.local.IP
	if !srcIP.IsValid() || srcIP.IsUnspecified() {
		srcIP = l.srcIPFor(seg.Peer.IP)
	}
	dst := seg.Peer

	// payload 加私有头
	var payload []byte
	if len(seg.Payload) > 0 {
		payload = make([]byte, 12+len(seg.Payload))
		binary.LittleEndian.PutUint32(payload[0:4], l.cfg.Magic)
		binary.LittleEndian.PutUint64(payload[4:12], connIDOf(
			PeerAddr{IP: srcIP, Port: l.local.Port}, dst))
		copy(payload[12:], seg.Payload)
	}

	// 选项：SYN 包带 MSS|SACKP|TS|NOP|WS，其余 NOP|NOP|TS（+可选 SACK）
	var opts []TCPOption
	if seg.Flags&FlagSYN != 0 {
		opts = synOptions(uint16(l.cfg.MTU-40), 7, seg.TSval, seg.TSecr)
	} else {
		opts = dataOptions(seg.TSval, seg.TSecr)
		if len(seg.SACK) > 0 {
			opts = append(opts, OptSACK(seg.SACK))
		}
	}

	tcpSeg := &TCPSegment{
		SrcPort: l.local.Port,
		DstPort: dst.Port,
		Seq:     seg.Seq,
		Ack:     seg.Ack,
		Flags:   seg.Flags,
		Window:  seg.Win,
		Options: opts,
		Payload: payload,
	}
	tcpBytes, err := MarshalTCP(nil, srcIP, dst.IP, tcpSeg)
	if err != nil {
		return err
	}
	// IP ID：优先用会话的每连接计数器（仿 Linux per-flow 行为），无则链路级自增
	ipID := seg.IPID
	if ipID == 0 {
		ipID = uint16(l.ipID.Add(1))
	}
	pkt := MarshalIPv4(nil, IPv4Fields{
		Src:      srcIP,
		Dst:      dst.IP,
		ID:       ipID,
		TTL:      DefaultTTL,
		Protocol: ProtocolTCP,
		DontFrag: true,
	}, len(tcpBytes))
	pkt = append(pkt, tcpBytes...)

	d4 := dst.IP.As4()
	sa := &unix.SockaddrInet4{Addr: d4}
	for {
		err = unix.Sendto(l.sendFD, pkt, 0, sa)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return wrapSysErr("raw Sendto", err)
		}
		return nil
	}
}

// srcIPFor 本地地址未指定时，按对端路由选源地址（UDP connect 探测，结果缓存）
func (l *rawLinkIO) srcIPFor(peer netip.Addr) netip.Addr {
	l.srcMu.Lock()
	defer l.srcMu.Unlock()
	if ip, ok := l.srcCache[peer]; ok {
		return ip
	}
	c, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: peer.AsSlice(), Port: 9})
	if err != nil {
		return l.local.IP // 探测失败退回原值（可能发包失败，但比 panic 强）
	}
	ip, _ := netip.AddrFromSlice(c.LocalAddr().(*net.UDPAddr).IP)
	_ = c.Close()
	l.srcCache[peer] = ip
	return ip
}

func (l *rawLinkIO) MaxPayload() int {
	// MTU - IP(20) - TCP(20) - TS选项(12) - 私有头(12)
	return l.cfg.MTU - 64
}

func (l *rawLinkIO) Close() error {
	if l.closed.CompareAndSwap(false, true) {
		_ = unix.Close(l.packetFD)
		_ = unix.Close(l.sendFD)
	}
	return nil
}

// errnoToNeedRoot EPERM/EACCES 归类为权限错误，其它包装为带操作名的错误
func errnoToNeedRoot(err error, what string) error {
	if err == unix.EPERM || err == unix.EACCES {
		return ErrNeedRoot.Clonef("%s: %v", what, err)
	}
	return wrapSysErr(what, err)
}

// wrapSysErr 包装系统调用错误（带操作名）
func wrapSysErr(what string, err error) error {
	return errors.Wrap(err, "fake_tcp: %s", what)
}

// ---- Listen/Dial 入口 ----

// listenRawTCP RawTCP 模式服务端
func listenRawTCP(ctx context.Context, cfg Config) (*Listener, error) {
	ap, err := netip.ParseAddrPort(cfg.LocalAddr)
	if err != nil {
		return nil, ErrInvalidConfig.Newf("LocalAddr 需为 IP:Port 字面量: %v", err)
	}
	local := PeerAddr{IP: ap.Addr(), Port: ap.Port()}
	link, err := newRawLinkIO(cfg, local, nil)
	if err != nil {
		return nil, err
	}
	local.Port = link.local.Port // 回填（端口 0 时）

	var fwCleanup func()
	if cfg.AutoFirewall {
		fwCleanup, err = installRSTDrop(ctx, local.Port)
		if err != nil {
			_ = link.Close()
			return nil, err
		}
	}
	return listenLink(ctx, cfg, link, local, fwCleanup), nil
}

// dialRawTCP RawTCP 模式客户端
func dialRawTCP(ctx context.Context, cfg Config) (*Conn, error) {
	rap, err := netip.ParseAddrPort(cfg.RemoteAddr)
	if err != nil {
		return nil, ErrInvalidConfig.Newf("RemoteAddr 需为 IP:Port 字面量: %v", err)
	}
	var local PeerAddr
	if cfg.LocalAddr != "" {
		lap, err := netip.ParseAddrPort(cfg.LocalAddr)
		if err != nil {
			return nil, ErrInvalidConfig.Newf("LocalAddr 需为 IP:Port 字面量: %v", err)
		}
		local = PeerAddr{IP: lap.Addr(), Port: lap.Port()}
	}
	remote := PeerAddr{IP: rap.Addr(), Port: rap.Port()}
	link, err := newRawLinkIO(cfg, local, &remote)
	if err != nil {
		return nil, err
	}
	local = link.local // 回填挑选的端口

	var fwCleanup func()
	if cfg.AutoFirewall {
		fwCleanup, err = installRSTDrop(ctx, local.Port)
		if err != nil {
			_ = link.Close()
			return nil, err
		}
	}
	return dialLink(ctx, cfg, link, local, remote, fwCleanup)
}
