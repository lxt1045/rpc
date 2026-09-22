package faux_tcp

import (
	"context"
	"net"
)

// 本文件导出"自定义链路"入口：真实链路（raw socket）见 link_linux.go，
// 内存链路见 link.go（测试用）。
//
// 用途：把 faux_tcp 的完整状态机/编解码跑在调用方提供的 Link 实现上——
//   - 单元/集成测试注入内存链路，无需 root 即可端到端验证上层（如 socks 代理）；
//   - 非 Linux 平台自行接入 WinDivert 等链路实现；
//   - 需要自定义丢包/时延注入的压测。
//
// 注意：与 Dial/Listen 不同，这两个入口**不安装内核 RST 抑制规则**
// （内存/自定义链路不存在内核干扰），也不做权限检查。

// DialWithLink 在指定链路上发起握手并返回连接（相当于 Dial 的链路注入版）。
func DialWithLink(ctx context.Context, cfg Config, link Link, local, remote Endpoint) (net.Conn, error) {
	cfg.defaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return dialWithLink(ctx, cfg, link, local, remote)
}

// ListenWithLink 在指定链路上监听（相当于 Listen 的链路注入版）。
func ListenWithLink(cfg Config, link Link, local Endpoint) net.Listener {
	cfg.defaults()
	return listenWithLink(cfg, link, local)
}
