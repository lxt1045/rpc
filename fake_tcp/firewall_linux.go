//go:build linux

package fake_tcp

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/lxt1045/utils/log"
)

// firewall_linux.go：内核 RST 抑制（plan.md §5）。
// 收到目的端口无内核 socket 的 TCP 报文时内核会自动回 RST，中间盒看到即判定连接已断；
// 这里在 OUTPUT 链丢弃本模块端口的 RST。规则启动时装、进程退出时卸，幂等。

const fwCmdTimeout = 5 * time.Second

// installRSTDrop 安装"丢弃本端口发出的 RST"规则，返回清理函数（幂等，可重复调用）。
// 优先 iptables（含 iptables-nft 兼容层），其次 nft。
func installRSTDrop(ctx context.Context, port uint16) (cleanup func(), err error) {
	if path, e := exec.LookPath("iptables"); e == nil {
		return iptablesRSTDrop(ctx, path, port)
	}
	if path, e := exec.LookPath("nft"); e == nil {
		return nftRSTDrop(ctx, path, port)
	}
	return nil, ErrFirewall.New("未找到 iptables/nft，请手工配置 RST 抑制或设置 AutoFirewall=false")
}

func fwRun(ctx context.Context, name string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, fwCmdTimeout)
	defer cancel()
	out, err := exec.CommandContext(cctx, name, args...).CombinedOutput()
	return string(out), err
}

// iptablesRSTDrop iptables 实现：
//
//	iptables -A OUTPUT -p tcp --sport <port> --tcp-flags RST RST -j DROP
func iptablesRSTDrop(ctx context.Context, iptables string, port uint16) (func(), error) {
	rule := []string{"-p", "tcp", "--sport", fmt.Sprint(port), "--tcp-flags", "RST", "RST", "-j", "DROP"}

	// -C 检查幂等：已存在（可能运维手工配过）则不重复添加，清理时也不删别人的规则
	check := append([]string{"-C", "OUTPUT"}, rule...)
	if _, err := fwRun(ctx, iptables, check...); err == nil {
		log.Ctx(ctx).Info().Caller().Msgf("fake_tcp: RST 抑制规则已存在（非本进程安装）, port=%d", port)
		return func() {}, nil
	}
	add := append([]string{"-A", "OUTPUT"}, rule...)
	if out, err := fwRun(ctx, iptables, add...); err != nil {
		return nil, ErrFirewall.Clonef("iptables -A 失败: %v, %s", err, strings.TrimSpace(out))
	}
	log.Ctx(ctx).Info().Caller().Msgf("fake_tcp: 已安装 RST 抑制规则, port=%d", port)

	var once bool = true
	return func() {
		if !once {
			return
		}
		once = false
		del := append([]string{"-D", "OUTPUT"}, rule...)
		if out, err := fwRun(context.Background(), iptables, del...); err != nil {
			log.Ctx(context.Background()).Warn().Caller().Msgf("fake_tcp: RST 抑制规则卸载失败: %v, %s", err, strings.TrimSpace(out))
		}
	}, nil
}

// nftRSTDrop nftables 实现：独立表 inet fake_tcp_rst，便于整体清理：
//
//	nft add table inet fake_tcp_rst
//	nft add chain inet fake_tcp_rst output '{ type filter hook output priority 0; policy accept; }'
//	nft add rule inet fake_tcp_rst output tcp sport <port> tcp flags rst counter drop
func nftRSTDrop(ctx context.Context, nft string, port uint16) (func(), error) {
	const (
		table = "fake_tcp_rst"
		chain = "output"
	)
	// 表不存在则创建（含 chain）；已存在则复用
	if _, err := fwRun(ctx, nft, "list", "table", "inet", table); err != nil {
		if out, err := fwRun(ctx, nft, "add", "table", "inet", table); err != nil {
			return nil, ErrFirewall.Clonef("nft add table 失败: %v, %s", err, strings.TrimSpace(out))
		}
		if out, err := fwRun(ctx, nft, "add", "chain", "inet", table, chain,
			"{", "type", "filter", "hook", "output", "priority", "0", ";", "policy", "accept", ";", "}"); err != nil {
			return nil, ErrFirewall.Clonef("nft add chain 失败: %v, %s", err, strings.TrimSpace(out))
		}
	}
	if out, err := fwRun(ctx, nft, "add", "rule", "inet", table, chain,
		"tcp", "sport", fmt.Sprint(port), "tcp", "flags", "rst", "counter", "drop"); err != nil {
		return nil, ErrFirewall.Clonef("nft add rule 失败: %v, %s", err, strings.TrimSpace(out))
	}
	log.Ctx(ctx).Info().Caller().Msgf("fake_tcp: 已安装 RST 抑制规则(nft), port=%d", port)

	return func() {
		// 用 handle 精确定位并删除本端口规则（其它端口的规则保留）
		out, err := fwRun(context.Background(), nft, "--handle", "list", "chain", "inet", table, chain)
		if err != nil {
			return
		}
		for _, line := range strings.Split(out, "\n") {
			if !strings.Contains(line, fmt.Sprintf("sport %d", port)) {
				continue
			}
			idx := strings.LastIndex(line, "handle ")
			if idx < 0 {
				continue
			}
			h := strings.TrimSpace(line[idx+len("handle "):])
			_, _ = fwRun(context.Background(), nft, "delete", "rule", "inet", table, chain, "handle", h)
		}
	}, nil
}
