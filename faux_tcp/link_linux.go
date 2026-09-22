//go:build linux

package faux_tcp

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync/atomic"
	"unsafe"

	"golang.org/x/net/bpf"
	"golang.org/x/sys/unix"
)

// rawLink Linux 原始链路：
//   - 发送：raw IP socket（IPPROTO_RAW 隐含 IP_HDRINCL，内核按我们给的 IP 头原样发出）
//   - 接收：AF_PACKET 旁路内核协议栈，附 cBPF 只收"目的端口=本端口"的 TCP 帧
//     （内核仍会对该端口回 RST——部署时由 installRSTDrop 抑制，默认自动安装）
//
// 需要 root 或 CAP_NET_RAW。
type rawLink struct {
	recvFd int
	sendFd int
	local  netip.Addr
	port   uint16 // 本地端口（调试模式的用户态过滤用）

	ifaceName string // 本端 IP 所属网卡名（仅诊断展示；收包不绑定网卡）
	rBuf      []byte // 读缓冲复用（仅 demux 读循环一个消费者）

	// 调试模式（Config.DebugPackets）计数：跳过 cBPF，用户态过滤并统计，
	// 用于区分"网卡侧一个包都没收到"与"收到了但被过滤掉"。
	// rx* 只统计**入向**报文（PACKET_OUTGOING 单独计入 txSeen）——否则本端自己
	// 发出的 SYN 会因为"目的端口=对端端口"而被算进 rx_dropped，读数会误导。
	debug     bool
	rxTotal   atomic.Int64 // 入向报文总数
	rxHit     atomic.Int64 // 入向且目的端口匹配（会被交付）
	rxDropped atomic.Int64 // 入向但不匹配（被丢弃）
	txSeen    atomic.Int64 // 本端发出的报文（AF_PACKET 回环可见，仅诊断）
}

// Describe 供错误信息展示本端链路细节（实现可选诊断接口 LinkDescriber）。
func (l *rawLink) Describe() string {
	desc := fmt.Sprintf("iface=%s local=%s:%d cooked(AF_PACKET/SOCK_DGRAM)", l.ifaceName, l.local, l.port)
	if l.debug {
		desc += fmt.Sprintf("; debug(未挂 cBPF) rx_total=%d rx_match=%d rx_dropped=%d tx_seen=%d",
			l.rxTotal.Load(), l.rxHit.Load(), l.rxDropped.Load(), l.txSeen.Load())
	}
	return desc
}

// htons 主机序转网络序（AF_PACKET socket 的 protocol 参数需要网络序）
func htons(i uint16) int { return int(i<<8 | i>>8) }

// newRawLink 创建原始链路。local 为 0.0.0.0 时监听所有接口（监听侧）。
// localPort 用于 cBPF 过滤（只收目的端口匹配的 TCP 帧）。
func newRawLink(cfg Config, local netip.Addr, localPort uint16) (Link, error) {
	if os.Geteuid() != 0 {
		return nil, ErrNeedRoot.New()
	}

	// 发送 socket：raw IP
	sendFd, err := unix.Socket(unix.AF_INET, unix.SOCK_RAW, unix.IPPROTO_RAW)
	if err != nil {
		return nil, errnoToNeedRoot(err, "raw send socket")
	}

	// 接收 socket：AF_PACKET + **SOCK_DGRAM（cooked）**，只收 IPv4。
	//
	// 为什么不用 SOCK_RAW：SOCK_RAW 把链路层头一起交上来，而链路层头并非总是
	// 14 字节以太网头——tun/wireguard/ppp 等点对点接口（VPN、WSL2 的某些模式）
	// 根本没有以太网头，按以太网偏移解析会把所有包丢掉，表现为"本机一个包都
	// 收不到、握手超时"（telnet 走内核 TCP 则完全正常）。cooked 模式由内核
	// 统一剥掉链路层头（以太网 14B / 环回伪以太网 / tun 无头），用户态始终拿到
	// IPv4 报文。
	recvFd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_DGRAM, htons(unix.ETH_P_IP))
	if err != nil {
		unix.Close(sendFd)
		return nil, errnoToNeedRoot(err, "af_packet socket")
	}

	// cBPF：cooked 模式下缓冲从 IP 头开始，只收"TCP && dst port == localPort"
	// 的报文，无关流量不再进用户态（繁忙网卡上省 CPU；自己发出的包 dst 是对端
	// 端口，天然滤掉）
	prog, err := bpfFilterTCPDstPort(localPort)
	if err != nil {
		unix.Close(sendFd)
		unix.Close(recvFd)
		return nil, err
	}
	if !cfg.DebugPackets {
		if err := unix.SetsockoptSockFprog(recvFd, unix.SOL_SOCKET, unix.SO_ATTACH_FILTER, prog); err != nil {
			unix.Close(sendFd)
			unix.Close(recvFd)
			return nil, ErrRawSocket.Newf("attach cBPF: %s", err)
		}
	}

	// 注意：**不把收包 socket 绑定到某张网卡**。多网卡/策略路由的主机上，
	// 去程走 eth1、回程可能从 eth0 回来（非对称路由），绑定单网卡会一个
	// 回包都收不到；不绑定则所有网卡的报文都会进来，再由 cBPF/用户态按
	// "目的端口"过滤，开销可控且天然兼容非对称路由。
	// 这里只查一次本端 IP 所属网卡名，纯粹用于错误信息展示。
	ifaceName := "all"
	if !local.IsUnspecified() {
		if ifi, err := ifaceByIP(local); err == nil {
			ifaceName = ifi.Name
		}
	}

	return &rawLink{
		recvFd:    recvFd,
		sendFd:    sendFd,
		local:     local,
		port:      localPort,
		ifaceName: ifaceName,
		rBuf:      make([]byte, 64*1024),
		debug:     cfg.DebugPackets,
	}, nil
}

// bpfFilterTCPDstPort 组装 cBPF：proto==TCP && tcp dst port == port。
// 偏移按 cooked（SOCK_DGRAM）模式：缓冲从 IPv4 头开始（链路层头已被内核剥掉），
// 非 IPv4 流量由 socket 的 ETH_P_IP 协议过滤，故这里只需看协议号与目的端口；
// IHL 可变用 LoadMemShift/LoadIndirect 处理。
func bpfFilterTCPDstPort(port uint16) (*unix.SockFprog, error) {
	raw, err := bpf.Assemble(bpfTCPDstPortProgram(port))
	if err != nil {
		return nil, ErrRawSocket.Newf("assemble cBPF: %s", err)
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

// bpfTCPDstPortProgram cBPF 指令序列（独立出来便于用用户态 VM 做单测）。
func bpfTCPDstPortProgram(port uint16) []bpf.Instruction {
	return []bpf.Instruction{
		bpf.LoadAbsolute{Off: 9, Size: 1}, // A = IP proto
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: protoTCP, SkipTrue: 0, SkipFalse: 3},
		bpf.LoadMemShift{Off: 0},          // X = 4*(ip[0]&0xf)（IHL）
		bpf.LoadIndirect{Off: 2, Size: 2}, // A = [X + 2]：TCP dst port
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: uint32(port), SkipTrue: 1, SkipFalse: 0},
		bpf.RetConstant{Val: 0},
		bpf.RetConstant{Val: 1 << 18},
	}
}

// matchTCPDstPort 用户态判断 cooked 缓冲（IPv4 报文）是否为 "TCP 且目的端口匹配"，
// 语义与 cBPF 过滤器一致（DebugPackets 模式下使用；也被单测直接覆盖）。
func matchTCPDstPort(b []byte, port uint16) bool {
	if len(b) < ipv4HeaderLen || b[0]>>4 != 4 {
		return false
	}
	ihl := int(b[0]&0x0f) * 4
	if ihl < ipv4HeaderLen || len(b) < ihl+tcpHeaderLen {
		return false
	}
	if b[9] != protoTCP {
		return false
	}
	return binary.BigEndian.Uint16(b[ihl+2:ihl+4]) == port
}

// ifaceByIP 找到携带指定 IPv4 地址的网卡
func ifaceByIP(ip netip.Addr) (*net.Interface, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, ifi := range ifaces {
		addrs, err := ifi.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			pfx, err := netip.ParsePrefix(a.String())
			if err != nil {
				continue
			}
			if pfx.Addr() == ip {
				return &ifi, nil
			}
		}
	}
	return nil, ErrInvalidAddr.Newf("no interface has ip %s", ip)
}

// errnoToNeedRoot EPERM/EACCES 归类为权限错误（ErrNeedRoot），其余归 ErrRawSocket。
func errnoToNeedRoot(err error, what string) error {
	if err == unix.EPERM || err == unix.EACCES {
		return ErrNeedRoot.Clonef("%s: %v", what, err)
	}
	return ErrRawSocket.Clonef("%s: %v", what, err)
}

// WritePacket 发送一个完整 IPv4 报文（含 IP 头）
func (l *rawLink) WritePacket(bs []byte) error {
	var dst [4]byte
	copy(dst[:], bs[16:20])
	addr := &unix.SockaddrInet4{Addr: dst}
	return unix.Sendto(l.sendFd, bs, 0, addr)
}

// ReadPacket 阻塞接收一个 IPv4 报文（cooked 模式：内核已剥掉链路层头）。
// 返回的切片引用复用的读缓冲，仅在下次 ReadPacket 前有效——
// parsePacket 会为 Payload 独立分配，字段均为值拷贝，故调用侧安全。
func (l *rawLink) ReadPacket() ([]byte, error) {
	for {
		n, sa, err := unix.Recvfrom(l.recvFd, l.rBuf, 0)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return nil, err
		}
		if !l.debug {
			// 校验 IPv4 版本即可（cooked 模式应该总是 IPv4；坏包直接跳过）
			if n >= ipv4HeaderLen && l.rBuf[0]>>4 == 4 {
				return l.rBuf[:n], nil
			}
			continue
		}
		// 调试模式：cBPF 未挂，这里在用户态过滤并统计，便于判断
		// "socket 一个包都没收到" 还是 "收到了但不匹配被丢弃"。
		// AF_PACKET 也会把本端发出的报文回环给 socket（tcpdump 的 Out 行同源），
		// 单独计 txSeen，不计入 rx*。
		if ll, ok := sa.(*unix.SockaddrLinklayer); ok && ll.Pkttype == unix.PACKET_OUTGOING {
			l.txSeen.Add(1)
			continue
		}
		l.rxTotal.Add(1)
		if matchTCPDstPort(l.rBuf[:n], l.port) {
			l.rxHit.Add(1)
			return l.rBuf[:n], nil
		}
		l.rxDropped.Add(1)
	}
}

// Close 关闭链路
func (l *rawLink) Close() error {
	unix.Close(l.recvFd)
	unix.Close(l.sendFd)
	return nil
}
