package socks_faux_kcp

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/lxt1045/rpc/faux_tcp"
)

// ConnDialer 底层连接拨号抽象：控制面 RPC 连接与数据面物理连接都经它建立。
// 默认实现是 FauxDialer（faux_tcp 伪装 TCP）；测试可注入内存链路实现，
// 从而在没有 root / 不碰内核协议栈的情况下跑通全链路。
type ConnDialer interface {
	Dial(ctx context.Context) (net.Conn, error)
}

// FauxDialer 基于 faux_tcp 的拨号器：每次 Dial 建立一条独立的伪装 TCP 连接
// （独立四元组，服务端按四元组分流）。
type FauxDialer struct {
	Config  FauxTCPConfig // 伪装 TCP 参数（零值用 faux_tcp 默认）
	Local   string        // 可选本地 "ip:port"；留空则自动选 IP + 随机端口
	Remote  string        // 对端 "ip:port"
	Timeout time.Duration
}

// Dial 实现 ConnDialer。
func (d *FauxDialer) Dial(ctx context.Context) (net.Conn, error) {
	cfg := d.Config.ToFauxTCP()
	if d.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.Timeout)
		defer cancel()
	}
	conn, err := faux_tcp.Dial(ctx, cfg, d.Local, d.Remote)
	if err != nil {
		return nil, fmt.Errorf("faux_tcp dial %s: %w "+
			"(本机需要 Linux + root/CAP_NET_RAW；跨机需确认服务端已运行、"+
			"云安全组/防火墙放行入站该 TCP 端口、两端都做了 RST 抑制；"+
			"不想自动装 iptables 规则可设 faux_tcp.manual_firewall 并按 README 手工配置)", d.Remote, err)
	}
	return conn, nil
}
