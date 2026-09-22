//go:build linux

package faux_tcp

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// firewall_linux.go：内核 RST 抑制的自动装拆。
// 内核收到目的端口无 socket 的 TCP 报文会自动回 RST，中间盒看到即判定连接已断；
// 因此在 OUTPUT 链丢弃本端口的出站 RST。规则幂等：Listen/Dial 安装、Close 卸载；
// 若运维已手工配置同名规则则复用且不卸载（Config.ManualFirewall=true 时完全不动）。
//
// 需要 CAP_NET_ADMIN（root）。优先 iptables（含 iptables-nft 兼容层），其次 nft。

const fwCmdTimeout = 5 * time.Second

// installRSTDrop 安装"丢弃本端口发出的 RST"规则，返回清理函数（幂等，可重复调用）。
func installRSTDrop(port uint16) (cleanup func(), err error) {
	if path, e := exec.LookPath("iptables"); e == nil {
		return iptablesRSTDrop(path, port)
	}
	if path, e := exec.LookPath("nft"); e == nil {
		return nftRSTDrop(path, port)
	}
	return nil, ErrFirewall.New("未找到 iptables/nft，请手工配置 RST 抑制（见 README）或设置 ManualFirewall=true")
}

func fwRun(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fwCmdTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return string(out), err
}

// iptablesRSTDrop iptables 实现：
//
//	iptables -A OUTPUT -p tcp --sport <port> --tcp-flags RST RST -j DROP
func iptablesRSTDrop(iptables string, port uint16) (func(), error) {
	rule := []string{"-p", "tcp", "--sport", fmt.Sprint(port), "--tcp-flags", "RST", "RST", "-j", "DROP"}

	// -C 检查幂等：已存在（运维手工配过）则不重复添加，清理时也不删别人的规则
	check := append([]string{"-C", "OUTPUT"}, rule...)
	if _, err := fwRun(iptables, check...); err == nil {
		return func() {}, nil
	}
	add := append([]string{"-A", "OUTPUT"}, rule...)
	if out, err := fwRun(iptables, add...); err != nil {
		return nil, ErrFirewall.Newf("iptables -A 失败: %v, %s", err, strings.TrimSpace(string(out)))
	}

	var once bool = true
	return func() {
		if !once {
			return
		}
		once = false
		del := append([]string{"-D", "OUTPUT"}, rule...)
		_, _ = fwRun(iptables, del...)
	}, nil
}

// nftRSTDrop nftables 实现：独立表 inet faux_tcp_rst，便于整体清理
func nftRSTDrop(nft string, port uint16) (func(), error) {
	const (
		table = "faux_tcp_rst"
		chain = "output"
	)
	if _, err := fwRun(nft, "list", "table", "inet", table); err != nil {
		if _, err := fwRun(nft, "add", "table", "inet", table); err != nil {
			return nil, ErrFirewall.Newf("nft add table 失败: %v", err)
		}
		if _, err := fwRun(nft, "add", "chain", "inet", table, chain,
			"{", "type", "filter", "hook", "output", "priority", "0", ";", "policy", "accept", ";", "}"); err != nil {
			return nil, ErrFirewall.Newf("nft add chain 失败: %v", err)
		}
	}
	if out, err := fwRun(nft, "add", "rule", "inet", table, chain,
		"tcp", "sport", fmt.Sprint(port), "tcp", "flags", "rst", "counter", "drop"); err != nil {
		return nil, ErrFirewall.Newf("nft add rule 失败: %v, %s", err, strings.TrimSpace(string(out)))
	}

	return func() {
		// 用 handle 精确定位并删除本端口规则（其它端口的规则保留）
		out, err := fwRun(nft, "--handle", "list", "chain", "inet", table, chain)
		if err != nil {
			return
		}
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.Contains(line, fmt.Sprintf("sport %d", port)) {
				continue
			}
			idx := strings.LastIndex(line, "handle ")
			if idx < 0 {
				continue
			}
			h := strings.TrimSpace(line[idx+len("handle "):])
			_, _ = fwRun(nft, "delete", "rule", "inet", table, chain, "handle", h)
		}
	}, nil
}
