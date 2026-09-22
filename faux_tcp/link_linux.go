//go:build linux

package faux_tcp

import (
	"net"
	"net/netip"
	"os"

	"github.com/lxt1045/errors"
	"golang.org/x/sys/unix"
)

// rawLink Linux 原始链路：
//   - 发送：raw IP socket（IPPROTO_RAW 隐含 IP_HDRINCL，内核按我们给的 IP 头原样发出）
//   - 接收：AF_PACKET 旁路内核协议栈（内核仍会收到副本并对"未知连接"回 RST，
//     部署时必须按 README 用 iptables 抑制出站 RST）
//
// 需要 root 或 CAP_NET_RAW。
type rawLink struct {
	recvFd int
	sendFd int
	local  netip.Addr
}

// htons 主机序转网络序（AF_PACKET socket 的 protocol 参数需要网络序）
func htons(i uint16) int { return int(i<<8 | i>>8) }

// newRawLink 创建原始链路。local 为 0.0.0.0 时监听所有接口（监听侧）。
func newRawLink(local netip.Addr) (Link, error) {
	if os.Geteuid() != 0 {
		return nil, errors.New("fauxtcp requires root/CAP_NET_RAW (or use mode: udp)")
	}

	// 发送 socket：raw IP
	sendFd, err := unix.Socket(unix.AF_INET, unix.SOCK_RAW, unix.IPPROTO_RAW)
	if err != nil {
		return nil, errors.Errorf("raw send socket: %s", err)
	}

	// 接收 socket：AF_PACKET，只收 IPv4
	recvFd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW, htons(unix.ETH_P_IP))
	if err != nil {
		unix.Close(sendFd)
		return nil, errors.Errorf("af_packet socket: %s (need root)", err)
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
			return nil, errors.Errorf("af_packet bind %s: %s", ifi.Name, err)
		}
	}

	return &rawLink{recvFd: recvFd, sendFd: sendFd, local: local}, nil
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
	return nil, errors.Errorf("no interface has ip %s", ip)
}

// WritePacket 发送一个完整 IPv4 报文（含 IP 头）
func (l *rawLink) WritePacket(bs []byte) error {
	var dst [4]byte
	copy(dst[:], bs[16:20])
	addr := &unix.SockaddrInet4{Addr: dst}
	// 构造在具体监听 IP 上；0.0.0.0 监听时 outbound 报文的源 IP 已由构包层填好
	return unix.Sendto(l.sendFd, bs, 0, addr)
}

// ReadPacket 阻塞接收一个 IPv4 报文（剥离以太网头；非 IPv4 帧跳过）
func (l *rawLink) ReadPacket() ([]byte, error) {
	buf := make([]byte, 65535)
	for {
		n, _, err := unix.Recvfrom(l.recvFd, buf, 0)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return nil, err
		}
		if ip := stripEthernet(buf[:n]); ip != nil {
			return append([]byte(nil), ip...), nil
		}
	}
}

// Close 关闭链路
func (l *rawLink) Close() error {
	unix.Close(l.recvFd)
	unix.Close(l.sendFd)
	return nil
}
