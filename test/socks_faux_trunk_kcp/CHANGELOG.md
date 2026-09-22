# Changelog - socks_faux_trunk_kcp

## 2026-09-22 - 端到端 TLS（mTLS）支持

### 背景

`faux_tcp` 是不可靠的数据报语义（一次 Write = 一个 TCP 段、丢包不补、
超过 MSS 报错），不能直接承载 TLS（TLS 记录最大 16KB、丢包即校验失败）。
因此 TLS 叠在 `trunk_kcp` VirtualConn（可靠有序流）之上，控制面与数据面都覆盖。

### 架构变更

- **控制通道**：faux_tcp 连接 → 写 1 字节类型标记（`0x01`）→ 迷你 trunk
  （KCP 可靠层，`ctrlConv` 固定）→ 控制 vconn（connID=0）→ TLS → RPC。
  token 与全部控制调用（Auth/TrunkStart/TrunkRemoveConn）都在 TLS 之内，
  且 faux_tcp 丢包不再打断控制面（此前是已知弱点）。
- **物理连接**：类型标记 `0x02` + 裸 RPC（只做 TrunkUpgrade，无秘密）；
  服务端按 `conv` 关联到控制通道里已授权的会话，未授权直接拒绝。
- **数据面**：每条代理 VirtualConn 端到端 TLS，open header（含目标地址）
  也在 TLS 之内（DPI 看不到代理目的地）。
- **建链顺序反转**：客户端先在控制连接上 `TrunkStart`（服务端建 0 连接 trunk
  并登记 conv→会话），再逐条拨物理连接 `TrunkUpgrade`（`AddConn` 加入）。
  `trunk_kcp` 原生支持 0 连接启动 + 动态 `AddConn`。
- 会话授权从 per-RPC-conn 改为 per-session（`session.authorized`），
  `sessionManager` 新增 `byConv` 索引。

### 新增/修改

- `config.go`：`TLSConfig`（enabled/host/证书路径）+ `ServerTLS`/`ClientTLS`
  （复用 `utils/config.LoadTLSConfig`，mTLS：CA 双向校验）。
- `tlsvconn.go`：连接类型标记、`VirtualConn`→`net.Conn` 适配、带超时的 TLS
  握手包装、`ctrlConn`（关闭时连带迷你 trunk，防泄漏）。
- `peer_client.go`：控制通道分层、物理连接免 Auth、`InitTrunk` 顺序反转、
  `openProxy` 数据面 TLS；`SocksCli.TLSCfg` 字段。
- `server.go`：accept 后按类型字节分流；控制通道 TLS + RPC Clone。
- `session.go`：见「架构变更」；`SetServerTLSConfig` 注入。
- `default.yml`：新增 `tls:` 段；`tls.enabled=false` 退回明文（仅可信链路）。

### 测试

- 内存自签 CA（`utils/cert`，无文件依赖）+ 内存 faux_tcp 链路：
  - `TestFauxTrunkProxyEndToEnd` / `TestFauxTrunkProxySurvivesLoss` 改为全 TLS；
  - `TestFauxTrunkProxyPlaintext`（明文兼容）；
  - `TestTrunkUpgradeRequiresAuthorization`（未授权拒绝）；
  - `TestTLSCertsUsable`（证书链自检）。
- 修复（本次重构引入并修复）：`TrunkStart` 在持有 `sess.mu` 时调用
  `maxVirtualConns()`（取同一把锁）导致自死锁——移出临界区。

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
