# Changelog - socks_faux_trunk_kcp

## 2026-09-22 - 新增示例：faux_tcp + trunk_kcp 代理

### 背景

仓库已有两条链路聚合路线：

- `test/socks_trunk`：可靠底层（TCP/TLS/QUIC）+ `trunk`；
- `test/socks_trunk_kcp`：TCP/TLS 之上叠 KCP（可靠性双层，实测存在 KCP 伪重传导致的
  线上流量放大，恶劣链路下放大 2~3 倍）。

本示例补齐第三条：**物理连接用 `faux_tcp`（线路上是完整 TCP 报文，但不重传/不拥塞
控制/不滑窗，语义等价 UDP）**，可靠性只由 `trunk_kcp` 的 KCP 提供。收益：

- 运营商视角是普通 TCP 流量，规避 UDP QoS/限速；
- 没有 TCP 层重传，不存在 KCP over TCP 的伪重传放大；
- 丢包是真实丢包，KCP 的 RTO/快速重传模型可正常工作。

### 实现

- 目录 `test/socks_faux_trunk_kcp/`，以 `test/socks_trunk_kcp` 为模板，代理功能
  （SOCKS5/HTTP CONNECT、Auth、ACL、虚拟连接协议、中继、trunk 动态增删连接、
  空闲/慢连接替换）保持一致。
- 新增 `ConnDialer` 抽象与默认 `FauxDialer`：控制面 RPC 连接与数据面物理连接都由
  `faux_tcp.Dial` 建立；`SocksCli.Dialer` 可注入，测试因此能在内存链路上跑全链路。
- 新增 `Server`（`server.go`）：把 main 里的 accept/Clone/会话回收逻辑收敛到包内，
  main 与测试共用。
- 新增 `FauxTCPConfig`（MSS/TTL/keepalive/握手重试/愈合等待/manual_firewall）与
  `ValidateMSS()`：强制 `MSS ≥ KCP 线上包`（数据报边界硬约束，防止半帧错位）。
- 无 TLS：`faux_tcp` 的 PSK/AEAD 仍是预留字段，token 与数据明文过线，README 已
  明确说明适用场景与后续路线。
- 控制面与数据面共用同一套 faux_tcp 连接模型：`TrunkUpgrade` 之后该连接只跑 KCP 段。

### 配套改动（faux_tcp）

- 导出 `faux_tcp.DialWithLink` / `faux_tcp.ListenWithLink`：允许把完整状态机跑在
  调用方提供的 `Link` 上（内存链路测试、非 Linux 平台自研链路）。不安装防火墙规则、
  不做权限检查。

### 修复（rpc/codec，被本示例的 `-race` 测试暴露）

- `codec.Codec.rwc` 字段存在数据竞争：`Read()`/`ReadLoop` 无锁读取该字段，而
  `Close()` 在 `writeLock` 下将其置 nil。新增 `rwcLock` 与 `getRWC()`/`takeRWC()`
  统一访问（锁序 `writeLock → rwcLock`），`ReadLoop` 取本地快照后再使用，
  消除竞争与潜在 nil 使用。

### 测试

- 离线（无需 root）：
  - 复制并适配 `protocol_test` / `auth_test` / `relay_test` / `session_test` /
    `proxy_lifecycle_test`；
  - 新增 `faux_mem_test.go`：内存 faux_tcp 网络（服务端单链路 + 客户端每连接一条链路，
    按目的端口路由），端到端跑通 `openProxy → KCP 虚拟连接 → 服务端拨号 → 双向中继`；
  - `TestFauxTrunkProxyEndToEnd`：并发虚拟连接 + 物理连接数校验；
  - `TestFauxTrunkProxySurvivesLoss`：建链后注入 ~12% 丢包，回显 100% 正确。
- 真实链路：`scripts/local_integration_test.sh`（root 门控，非 root 自动 SKIP）。
