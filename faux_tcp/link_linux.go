//go:build linux

package faux_tcp

import (
	"net"
	"net/netip"
	"os"
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

	rBuf []byte // 读缓冲复用（仅 demux 读循环一个消费者）
}

// htons 主机序转网络序（AF_PACKET socket 的 protocol 参数需要网络序）
func htons(i uint16) int { return int(i<<8 | i>>8) }

// newRawLink 创建原始链路。local 为 0.0.0.0 时监听所有接口（监听侧）。
// localPort 用于 cBPF 过滤（只收目的端口匹配的 TCP 帧）。
func newRawLink(local netip.Addr, localPort uint16) (Link, error) {
	if os.Geteuid() != 0 {
		return nil, ErrNeedRoot.New()
	}

	// 发送 socket：raw IP
	sendFd, err := unix.Socket(unix.AF_INET, unix.SOCK_RAW, unix.IPPROTO_RAW)
	if err != nil {
		return nil, errnoToNeedRoot(err, "raw send socket")
	}

	// 接收 socket：AF_PACKET，只收 IPv4
	recvFd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW, htons(unix.ETH_P_IP))
	if err != nil {
		unix.Close(sendFd)
		return nil, errnoToNeedRoot(err, "af_packet socket")
	}

	// cBPF：只收"IPv4 && TCP && dst port == localPort"的帧，
	// 无关流量不再进用户态（繁忙网卡上省 CPU；自己发出的包 dst 是对端端口，天然滤掉）
	prog, err := bpfFilterTCPDstPort(localPort)
	if err != nil {
		unix.Close(sendFd)
		unix.Close(recvFd)
		return nil, err
	}
	if err := unix.SetsockoptSockFprog(recvFd, unix.SOL_SOCKET, unix.SO_ATTACH_FILTER, prog); err != nil {
		unix.Close(sendFd)
		unix.Close(recvFd)
		return nil, ErrRawSocket.Newf("attach cBPF: %s", err)
	}

	// 绑定到指定 IP 所在网卡；0.0.0.0 绑定所有接口
	if !local.IsUnspecified() {
		ifi, err := ifaceByIP(local)
		if err != nil {
			unix.Close(sendFd)
			unix.Close(recvFd)
			return nil, err
		}
		addr := &unix.SockaddrLinklayer{
			Protocol: uint16(htons(unix.ETH_P_IP)),
			Ifindex:  ifi.Index,
		}
		if err := unix.Bind(recvFd, addr); err != nil {
			unix.Close(sendFd)
			unix.Close(recvFd)
			return nil, errnoToNeedRoot(err, "af_packet bind "+ifi.Name)
		}
	}

	return &rawLink{
		recvFd: recvFd,
		sendFd: sendFd,
		local:  local,
		rBuf:   make([]byte, 64*1024),
	}, nil
}

// bpfFilterTCPDstPort 组装 cBPF：IPv4 && proto==6 && tcp dst port == port
// （以太网头 14 字节；IHL 可变用 LoadMemShift/LoadIndirect 处理；
// 注意 802.1Q VLAN 帧的 ethertype 是 0x8100，会被滤掉——不适用于 trunk 链路）
func bpfFilterTCPDstPort(port uint16) (*unix.SockFprog, error) {
	raw, err := bpf.Assemble([]bpf.Instruction{
		bpf.LoadAbsolute{Off: 12, Size: 2}, // A = ethertype
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: 0x0800, SkipTrue: 0, SkipFalse: 5},
		bpf.LoadAbsolute{Off: 23, Size: 1}, // A = IP proto（以太网 14 + IP 头内偏移 9）
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: protoTCP, SkipTrue: 0, SkipFalse: 3},
		bpf.LoadMemShift{Off: 14},          // X = 4*(ip[0]&0xf)（IHL）
		bpf.LoadIndirect{Off: 16, Size: 2}, // A = [14 + X + 16]：TCP dst port
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: uint32(port), SkipTrue: 1, SkipFalse: 0},
		bpf.RetConstant{Val: 0},
		bpf.RetConstant{Val: 1 << 18},
	})
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

// ReadPacket 阻塞接收一个 IPv4 报文（剥离以太网头；非 IPv4 帧跳过）。
// 返回的切片引用复用的读缓冲，仅在下次 ReadPacket 前有效——
// parsePacket 会为 Payload 独立分配，字段均为值拷贝，故调用侧安全。
func (l *rawLink) ReadPacket() ([]byte, error) {
	for {
		n, _, err := unix.Recvfrom(l.recvFd, l.rBuf, 0)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return nil, err
		}
		if ip := stripEthernet(l.rBuf[:n]); ip != nil {
			return ip, nil
		}
	}
}

// Close 关闭链路
func (l *rawLink) Close() error {
	unix.Close(l.recvFd)
	unix.Close(l.sendFd)
	return nil
}
