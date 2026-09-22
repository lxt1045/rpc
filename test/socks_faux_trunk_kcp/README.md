# socks_faux_trunk_kcp

基于 **`faux_tcp`（伪装 TCP 的 UDP 式传输）+ `trunk_kcp`（KCP 链路聚合）** 的 SOCKS5 /
HTTP CONNECT 代理，是 `test/socks_trunk_kcp` 的同构版本，区别只在**物理链路**：

| | socks_trunk_kcp | socks_faux_trunk_kcp（本示例） |
|---|---|---|
| 物理连接 | TLS over TCP（可靠流） | `faux_tcp`（线路上是 TCP 报文，但**不重传/不拥塞控制/不滑窗**，语义等价 UDP） |
| 可靠性来源 | TCP + KCP 双层（易伪重传放大） | 只由 KCP 提供（丢包是真实丢包，无放大） |
| 运营商视角 | 普通 HTTPS 流量 | 普通 TCP 流量（规避 UDP QoS/限速） |
| 加密 | TLS | 暂无（token 明文过线，见「安全说明」） |
| 运行要求 | 普通用户 | Linux + root/CAP_NET_RAW（raw socket） |

代理功能本身（SOCKS5/HTTP CONNECT、认证、ACL、trunk 动态增删连接、空闲/慢连接
替换）与 `socks_trunk_kcp` 完全一致，控制面协议（`pb/`）、虚拟连接首包协议
（`protocol.go`）、中继逻辑（`copy.go`）均相同。

## 结构

```
cmd/socks-faux-trunk-kcp-client   客户端入口（起本地 SOCKS5/HTTP 代理）
cmd/socks-faux-trunk-kcp-server   服务端入口（faux_tcp 监听 + trunk 汇聚）
config.go                         TrunkKCPConfig / FauxTCPConfig / ConnConfig / ACL
fauxconn.go                       ConnDialer 抽象 + 默认 FauxDialer（faux_tcp 拨号）
peer_client.go                    客户端：控制连接 + N 条物理连接（Upgrade 给 trunk）
server.go                         服务端：Accept → Clone peer → Svc
session.go                        服务端会话/trunk 管理（与 socks_trunk_kcp 同构）
protocol.go / copy.go             虚拟连接首包协议 / 双向中继
faux_mem_test.go                  内存 faux_tcp 网络 + 全链路端到端测试
filesystem/static/conf/default.yml 编译期嵌入配置
scripts/local_integration_test.sh 本地集成冒烟（需 root）
```

## 链路模型

```
浏览器/应用 ──SOCKS5/HTTP──▶ 客户端
                               │  控制面：RPC(Auth/TrunkStart/TrunkUpgrade) —— faux_tcp 连接（每条一次 RPC）
                               │  数据面：N 条 faux_tcp 物理连接 ── Upgrade ──▶ trunk_kcp
                               ▼
                        fake-TCP 报文（IP proto=6，握手/PSH+ACK/FIN 齐全）
                               ▼
                             服务端 faux_tcp 监听（AF_PACKET + raw IP socket）
                               │  trunk_kcp 虚拟连接（KCP 重传/排序）
                               ▼
                             拨号目标站点 ── 双向中继
```

- **控制面**：每条 RPC 控制连接也是一条 `faux_tcp` 连接（伪装 TCP）。`faux_tcp`
  不做重传，所以链路丢包时控制面调用可能失败——由客户端重连循环与 `InitTrunk`
  重试兜底；数据面（KCP）不受影响。
- **数据面**：`TrunkUpgrade` 之后该 `faux_tcp` 连接不再承载 RPC 帧，直接作为
  `trunk_kcp` 的物理连接（与 `socks_trunk_kcp` 的 TLS 连接完全同构）。

## 关键约束：数据报边界（MSS ≥ KCP 线上包）

`faux_tcp` 一次 `Write` = 一个 TCP 段，**不做拆分**（超过 MSS 直接报错）。
`trunk_kcp` 的 recvLoop 按长度字段做流式重组，所以必须保证「一个 KCP 段恰好落在
一个 faux_tcp 段里」，否则丢包会造成半帧错位、物理连接被剔除。

- 默认值即安全：KCP 线上包 1400B ≤ `faux_tcp` MSS 1448；
- `config.go` 的 `ValidateMSS()` 会在 `mss < 1400` 时直接报错，避免静默错位；
- 若调小 KCP MTU，需同步保证 `faux_tcp.mss ≥ KCP 线上包`。

## 运行

需要 **Linux + root/CAP_NET_RAW**（AF_PACKET 收包、raw IP socket 发包）；
`faux_tcp` 默认自动安装/卸载内核 RST 抑制规则（iptables 优先，nft 兜底）。

```bash
# 1) 编辑 filesystem/static/conf/default.yml（改 token / 地址 / 连接数）
# 2) 构建并运行（两端都需 root）
go build -o /tmp/socks-faux-server ./test/socks_faux_trunk_kcp/cmd/socks-faux-trunk-kcp-server
go build -o /tmp/socks-faux-client ./test/socks_faux_trunk_kcp/cmd/socks-faux-trunk-kcp-client

# 服务端（本机）
cd test/socks_faux_trunk_kcp && sudo /tmp/socks-faux-server
# 客户端（本机或另一台机器，配置里 client-conn.addr 指向服务端）
sudo /tmp/socks-faux-client
# 浏览器/curl 使用 socks5h://127.0.0.1:11090 或 http://127.0.0.1:11091
```

`faux_tcp.manual_firewall: true` 时不自动改防火墙，需自行维护（客户端每条物理连接
一个本地端口，连接数多时建议按端口段配置）：

```bash
# 服务端：监听端口
sudo iptables -A OUTPUT -p tcp --sport 18099 --tcp-flags RST RST -j DROP
# 客户端：物理连接使用的本地端口段（示例）
sudo iptables -A OUTPUT -p tcp --sport 49152:65535 --tcp-flags RST RST -j DROP
```

## 配置要点

```yaml
conn:            { addr: "0.0.0.0:18099" }   # 服务端 faux_tcp 监听
client-conn:     { addr: "1.2.3.4:18099" }   # 客户端对端
faux_tcp:
  mss: 1448              # ≥ KCP 线上包 1400（启动时校验）
  keepalive_seconds: 25  # 标准 TCP keepalive 探测包
  heal_delay_ms: 200     # 丢包后 ack 愈合等待（详见 faux_tcp/README.md）
  manual_firewall: false # true 则自行维护 RST 抑制规则
trunk_kcp:
  conv: 123456789        # 两端必须一致
  min_conns: 2
  max_conns: 4
  max_virtual_conn: 8096
```

**KCP 参数**：物理层是不可靠的 `faux_tcp`，丢包是真实丢包，因此使用**库默认快速模式
`(1,10,32,1)`** 即可；`socks_trunk_kcp` 中针对「TCP/TLS 底层伪重传」的保守参数
（`nodelay=0, interval=20~40, resend=0`）**不适用**于本示例（那会让恢复变慢）。
需要显式配置时用 `kcp_nodelay / kcp_interval / kcp_resend / kcp_nc`（两端一致）。

## 安全说明（重要）

本示例**没有 TLS**：`faux_tcp` 目前只提供伪装与传输，`Config.PSK`（AEAD）仍是预留
字段（置非空会报错）。因此：

- token 与代理数据以明文承载在伪装 TCP 报文里，只适合**可信链路**或叠加外层加密；
- 需要加密时的路线：在 `faux_tcp` 内实现 PSK/AEAD（`faux_tcp/plan.md` 阶段 4），
  或在 `trunk_kcp` 之上做应用层加密；
- 不要把本示例直接暴露在不可信网络上传输敏感数据。

## 测试

```bash
# 离线全量（无需 root）：协议/ACL/认证/中继/会话 + 内存链路全链路端到端
go test -count=1 ./test/socks_faux_trunk_kcp/

# 端到端用例说明：
#   TestFauxTrunkProxyEndToEnd    内存 faux_tcp 上跑通 openProxy→KCP→目标回显（含并发虚拟连接）
#   TestFauxTrunkProxySurvivesLoss 物理链路注入 ~12% 丢包，回显仍 100% 正确（KCP 兜底）

# 真实链路集成冒烟（需 root；自动装 RST 抑制规则）
sudo ./test/socks_faux_trunk_kcp/scripts/local_integration_test.sh
```

## 排障

| 现象 | 原因/处理 |
|---|---|
| `fauxtcp: 需要 root 或 CAP_NET_RAW...` | 未以 root 运行；或容器缺少 `CAP_NET_RAW`/`CAP_NET_ADMIN` |
| `fauxtcp: 未找到 iptables/nft...` | 无防火墙工具：装 iptables，或 `manual_firewall: true` 自行配置 |
| 连接建立后立即断（RST） | RST 抑制规则未生效：确认 iptables/nft 可用，或手工加规则 |
| `invalid KCP segment length` | `faux_tcp.mss` 小于 KCP 线上包（半帧错位）：调大 MSS（默认 1448 安全） |
| 控制面 RPC 偶发失败 | `faux_tcp` 无重传，链路丢包会打断控制面调用：客户端会自动重连 |
