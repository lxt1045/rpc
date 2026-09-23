package socks_faux_kcp

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/lxt1045/rpc/faux_tcp"
	"github.com/lxt1045/utils/log"
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
	if cn, ok := conn.(*faux_tcp.Conn); ok {
		go logPacketCounters(cn)
	}
	return conn, nil
}

// logPacketCounters 每 30s 打一条"报文级"计数（诊断丢包发生在哪一段）。
//
// 为什么要它：KCP 层的自诊断（线上/已确认/重传）只能说明"发送端认为丢了多少"，
// 无法区分"公网路径丢了"与"本端用户态没交上来"。把两端的 faux_tcp 报文计数对齐：
//
//	对端 sent ≈ 本端 recv → 路径没丢，是本端用户态的问题
//	对端 sent ≫ 本端 recv → 路径（或本端内核 socket 缓冲）在丢
//
// 再用本端上层（trunk_kcp 的"收线/收段"）对比 recv，就能看出"收到但没交给上层"
// （faux_tcp 接收队列满时丢包）占多少。
func logPacketCounters(cn *faux_tcp.Conn) {
	const every = 30 * time.Second
	tk := time.NewTicker(every)
	defer tk.Stop()
	var lastSent, lastRecv int64
	sent0, recv0 := cn.PacketCounters()
	begin := time.Now()
	for {
		select {
		case <-cn.Done():
			return
		case <-tk.C:
			sent, recv := cn.PacketCounters()
			secs := time.Since(begin).Seconds()
			if secs <= 0 {
				secs = 1
			}
			log.Ctx(context.Background()).Info().
				Str("local", cn.LocalAddr().String()).
				Str("remote", cn.RemoteAddr().String()).
				Int64("sent_pkts", sent-lastSent).
				Int64("recv_pkts", recv-lastRecv).
				Float64("sent_pps", float64(sent-sent0)/secs).
				Float64("recv_pps", float64(recv-recv0)/secs).
				Msg("faux_tcp 报文计数（本连接；与对端对比即可定位丢包段）")
			lastSent, lastRecv = sent, recv
		}
	}
}
