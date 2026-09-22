# fake_tcp 实现计划 / fake_tcp Implementation Plan

> 本文档是 `fake_tcp` 模块的设计与实施计划，描述**意图**，落地行为以代码为准（同仓库根目录 `plan.md` 的惯例）。

---

## 1. 背景与目标 / Goals

### 1.1 要解决的问题

运营商（尤其是家宽/4G/5G 出口）普遍对 UDP 做严格 QoS：限速、随机丢弃、高峰期压制。
而内核 TCP 协议栈自带的**拥塞控制、丢包重传、滑动窗口流量控制**，在代理/测速/大吞吐场景下
会主动压低速率、放大时延抖动，且这些行为**无法在用户态关闭**。

### 1.2 目标（按优先级）

1. **线路上是 TCP**：c/s 之间通信的每一个 IP 报文都是协议号 6（TCP）的报文，具备真实 TCP
   连接的全部可观测特征——三次握手、选项协商（MSS/SACK/TS/WS）、PSH+ACK 数据、ACK 确认、
   FIN 四次挥手、RST 异常——运营商中间盒/DPI/CONTRACK 看到的就是一个正常的 Linux TCP 连接，
   **不被识别为 UDP**。
2. **无拥塞控制、无重传、无滑动窗口限流**：数据以应用给定的速率直发，丢包不回填、不降速、
   不做背压。这三个机制只保留"外观特征"（窗口通告、ack 字段、SACK 选项照常填写），
   不保留任何实际行为。
3. **可靠性上交给上层**：本模块只做"伪装 + 传输"，顺序/去重/重传由上层按需叠加
   （本仓库已有 `trunk_kcp`，KCP 天然适合跑在本模块之上）；不需要可靠性的场景（测速、
   实时音视频）可直接裸用。
4. **c/s 两端模块**：提供 `Listen`（服务端，多连接接入）与 `Dial`（客户端）两端 API。
5. **可插拔**：产出的连接实现 `io.ReadWriteCloser`，可直接喂给 `trunk_kcp.NewTrunkKCP`、
   `rpc.NewPeer` / `Peer.Clone`，复用现有 RPC、链路聚合与 `test/socks*` 示例生态。

### 1.3 非目标 / Non-Goals

- 不做拥塞控制、丢包重传、滑动窗口限流的任何"真实行为"（这是特性，不是缺陷）。
- 不做加密/认证本身（复用上层 KCP 之后的现有认证链路；本模块只提供 Magic 字段做垃圾报文过滤）。
- 首版只支持 Linux（`AF_PACKET` + raw IP socket）；Windows（WinDivert）列为远期。
- 不追求通过国家级 DPI 的主动探测（如主动注入 RST 探测、重放探测）。目标是"运营商 QoS 分类器
  认为是 TCP"，不是"抗审查"。
- 不修改内核、不加载内核模块、eBPF/XDP 收包加速列为远期优化。

---

## 2. 方案选型 / Approach

### 2.1 候选方案

| 方案 | 线路上是什么 | 能否彻底去掉 CC/重传/滑窗 | 工程量 | 风险 | 结论 |
|---|---|---|---|---|---|
| **A. Raw socket 伪造 TCP**（udp2raw 路线：应用层手写极简 TCP 状态机，raw socket 收发） | 真实 TCP 报文（内核不参与） | ✅ 天然没有——内核协议栈被完全旁路，要几个特征写几个 | 中（自写编解码 + 状态机，均为纯函数，可单测） | 需 root/CAP_NET_RAW；需 iptables 抑制内核 RST | **主路线** |
| B. 魔改成熟 TCP 开源库（gVisor netstack / lwIP 删除 CC、RTO、滑窗） | 取决于出口：仍需 tun/AF_PACKET/raw socket 才能把报文拍上线，与 A 相同 | ⚠️ 难。重传/RTO 深耦合在发送队列、RTT 估计、SACK 记分板里，删除后状态机一致性难保证；netstack 编译体积大；lwIP 需 cgo | 大 | 删不干净留下"半残 TCP"，反而特征异常 | 否决（仅作技术参考） |
| C. 纯 UDP + 私有头 | UDP | ✅（本来就没有） | 小 | 运营商看到 UDP，**达不到目标 1** | **兜底模式**：无 root/非 Linux 环境退化使用 |

### 2.2 结论

- **主路线 A**：参考 `wangyu-/udp2raw` 已验证多年的做法——
  - **收包**：`AF_PACKET`（SOCK_RAW）+ cBPF 过滤器（按端口/对端 IP 过滤），拿到完整 L2 帧，自己解析 IP/TCP 头；
  - **发包**：`AF_INET` + `SOCK_RAW(IPPROTO_TCP)` + `IP_HDRINCL`，自己填 IP 头与 TCP 头、算校验和，
    L2（MAC/ARP）交给内核路由，免去手工 ARP；
  - **TCP 状态机**：应用层自写极简版本（§4），只保留外观特征。
- **兜底模式 C**：与 A 共用会话管理/分包/API，仅报文封装不同（§4.7）。配置项 `mode: raw-tcp | udp`。
- 方案 B 否决原因：gVisor netstack 的重传逻辑分布在 `sender`/`receiver`/`scoreboard`/RTT 估算中，
  "删掉重传"等价于重写半个栈；且出口仍需 A 的 raw socket 通路，总工程量是 A 的两倍以上。
  但若未来需要"更像真实 TCP 的时序特征"，可再评估把 netstack 作为**报文生成参考**（离线对比用）。

---

## 3. 总体架构 / Architecture

```
┌────────────────────────────────────────────────────────────────┐
│  上层：rpc.NewPeer / trunk_kcp.NewTrunkKCP / socks 示例 / 裸应用 │
│         （可靠性由 trunk_kcp 的 KCP 按需叠加）                    │
└──────────────────────────────┬─────────────────────────────────┘
                               │ io.ReadWriteCloser
┌──────────────────────────────▼─────────────────────────────────┐
│ fake_tcp.Conn（每条虚拟连接一个）                                 │
│  - Write: 切片(MTU) → 套 TCP/IP 头 → PacketIO 发出               │
│  - Read : 收包 → 校验 → 去重 → 按序/乱序上交(可配) → 读缓冲        │
└──────────────────────────────┬─────────────────────────────────┘
                               │
┌──────────────────────────────▼─────────────────────────────────┐
│ Session 层（fake_tcp.session）                                   │
│  - 极简 TCP 状态机：握手 / 数据 / 保活 / 关闭                     │
│  - seq/ack/TS 维护、接收位图去重、SACK 通告（只演戏，不限流）       │
│  - 四元组 → session 路由表（服务端多连接复用）                     │
└──────────────────────────────┬─────────────────────────────────┘
                               │ PacketIO 接口（ReadPacket/WritePacket）
              ┌────────────────┴────────────────┐
              ▼                                 ▼
┌─────────────────────────────┐   ┌──────────────────────────────┐
│ RawTCP PacketIO（主，Linux） │   │ UDP PacketIO（兜底）          │
│ 收: AF_PACKET + cBPF        │   │ net.UDPConn + [Magic][ConnID]│
│ 发: raw IP + IP_HDRINCL     │   │ 私有头                        │
│ + iptables/nftables RST 抑制 │   │ 无内核干扰问题                │
└─────────────────────────────┘   └──────────────────────────────┘
```

关键设计点：

- **对称 Peer**：与本仓库整体哲学一致，c/s 两端协议对称，差异只在"谁发 SYN"。
- **会话即四元组**：RawTCP 模式下 `(srcIP, srcPort, dstIP, dstPort)` 唯一标识一条连接；
  服务端用哈希表路由（参考 `conn/udp.go` 的 `consistentHash` / `ToUdpConn` 注册表模式）。
- **可靠性分层**：本层收到乱序/空洞数据**不丢**（除非重复），统统上交；ack/SACK 字段按
  RFC 语义"演"给中间盒看。上层叠 KCP 时，KCP 的重传会以新的 seq 发出——在线路上恰好呈现
  "TCP 快速重传"的外观，是加分项而非破绽。

---

## 4. 协议设计 / Wire Protocol

### 4.1 报文结构（RawTCP 模式）

线路上是**标准 IPv4 + TCP**，无任何私有扩充头（私有 Magic 放 payload 内，见 §4.6）：

```
IPv4 header (20B): Ver/IHL=0x45, TOS=0, TotalLen, ID(递增), Flags=DF, FragOff=0,
                   TTL=64, Proto=6, Checksum(正确计算), SrcIP, DstIP
TCP  header (20B+options):
                   SrcPort, DstPort, Seq, Ack, DataOff, Flags,
                   Window(固定通告，如 64240), Checksum(伪头+正确计算), UrgPtr=0
TCP  options:
  握手包(SYN):     MSS(1400) | SACK-Permitted | TS(tsval, ecr) | NOP | WS(7)   ← 模拟 Linux 5.x
  数据/ACK 包:     NOP | NOP | TS(tsval, ecr=对端最新 tsval)
  丢包演戏(可选):  上述之后追加 SACK block（左/右沿来自接收位图）
```

- **校验和必须正确**：IP 头校验和 + TCP 校验和（含伪头）。这是中间盒最廉价的一致性检查，
  错一个就前功尽弃。实现为纯函数并配 golden 测试。
- **DF=1 + MSS=1400**：避免 PMTU 黑洞与 IP 分片；发送切片上限 `min(MTU 配置, 1400)`。
- **IP ID**：单调递增（模拟未打补丁内核对 TCP 的 per-flow 计数器行为）。
- **TTL=64**（Linux 风格，两端一致，避免中间盒用 TTL 差异嗅探双栈/代理）。

### 4.2 握手（三次，应用层重试）

```
Client                                    Server
  │  SYN  seq=ISN_c, opts(MSS/SACKP/TS/WS)  │
  │ ───────────────────────────────────────► │  收到 SYN：校验 Magic 白名单(可选)、分配 session
  │  SYN+ACK seq=ISN_s, ack=ISN_c+1, opts    │
  │ ◄─────────────────────────────────────── │
  │  ACK  seq=ISN_c+1, ack=ISN_s+1           │
  │ ───────────────────────────────────────► │  状态 → ESTABLISHED，触发 Accept 返回
```

- ISN 用 `crypto/rand`；TS 值用本地单调时钟（ms），SYN 中 ecr=0，之后严格回显。
- 握手允许重试：SYN 每 1s 重发，最多 5 次（此时连接未建立，重发是应用层行为，
  与"数据不重传"原则不冲突；且真实 TCP 也重传 SYN，外观一致）。
- 服务端防半开扫描：session 建立前只认带正确 Magic/端口的 SYN，其余静默丢弃（不 RST，
  避免被扫描器标记端口存活——也可配置回 RST 模拟关闭端口）。

### 4.3 数据传输与 ack 语义（核心：只演戏，不限流）

发送侧维护：

```
snd_nxt  : 下一个要用的 seq，严格按已发字节数递增（线上永远连续）
snd_una  : 对端 ack 到的位置，仅用于日志/统计，不驱动任何行为（不重传、不降速）
```

接收侧维护：

```
rcv_nxt  : 连续接收点（ack 字段填它，单调不减）
rcv_max  : 已收到的最大 seq+len
位图      : (rcv_nxt, rcv_max] 区间内哪些段已收到，用于去重与生成 SACK block
```

收包规则：

| 收到报文 | 行为 |
|---|---|
| `seq == rcv_nxt` | 上交 payload；`rcv_nxt += len`；位图滑动；按 delayed-ack 节奏回 ACK |
| `seq > rcv_nxt`（空洞） | **payload 照常上交**（可靠性是上层的事）；位图记录；回 ACK（ack=rcv_nxt，可选附带 SACK block）——外观 = 真实 TCP 丢包后的 dup-ack/SACK |
| `seq+len <= rcv_nxt` 或位图命中（重复） | 丢弃 payload（可能是对端上层 KCP 重传已送达过的数据）；照常回 ACK |
| seq 落在接收位图窗口之外（过老/过新到离谱） | 丢弃并计日志，不更新任何状态 |

- **ack 频率**：模仿 delayed ACK——每收到 2 个数据包或 40ms 回一次 ACK；空闲按 §4.5 保活。
  ACK 纯按需触发，**收到乱序不产生任何重传请求语义**（SACK block 只是外观，对端不响应它）。
- **窗口通告**：固定值（默认 64240，WS=7），不随接收缓冲变化——"有滑窗字段，无滑窗行为"。
- **payload 上交顺序**：到达即交（配 KCP 使用，KCP 负责排序）。
  ~~可选"按序上交"~~（实现中否决：无重传语义下 seq 空洞永远不会被回填，按序等待必然死锁；
  `Config.Ordered` 保留字段，置 true 返回配置错误）。

### 4.4 关闭与异常

```
主动关闭方: FIN+ACK ──►  对方 ACK ──► 对方 FIN+ACK ──► ACK   （四次挥手齐全）
异常路径  : 直接发 RST（如对端进程消失、session 被强制回收）
```

- 关闭后 session 保留 2×MSL（60s）再回收，防止四元组立即复用串话。
- 对端无响应的半死连接：保活超时（默认 3×Keepalive 间隔无包）→ 本地标记关闭，发 RST。

### 4.5 保活

- 空闲超过 `KeepaliveInterval`（默认 30s，可配）：发一个 `seq = snd_nxt-1`、无 payload 的
  纯 ACK 保活包（外观 = 标准 TCP keepalive），维持 NAT/CGN 与 conntrack 的 TCP 表项。
- 运营商 NAT 对 TCP established 表项超时普遍 ≥ 几分钟，30s 足够；UDP 模式此值复用。

### 4.6 payload 内私有头（两种模式共用）

```
[Magic   uint32 LE]   固定魔数，过滤扫描/垃圾报文；服务端第一包校验，不符静默丢
[ConnID  uint64 LE]   UDP 模式下的虚拟连接标识（RawTCP 模式下保留以统一解析，填四元组哈希）
[payload ...]         上层数据（KCP 段 / RPC 帧 / 裸应用数据）
```

- Magic 只挡"偶发垃圾"，不抗主动攻击；真认证在上层（`test/socks_trunk_kcp` 的 session/auth 可复用）。

### 4.7 UDP 模式封装（兜底）

```
UDP payload = §4.6 私有头 + 数据
```

- 不跑 TCP 状态机（握手/关闭/保活语义保留为**应用层消息**，复用同一份 session 代码：
  SYN/SYN+ACK/FIN 作为 payload 内的控制消息类型，而不是 TCP flags）。
- 会话标识 = ConnID + 对端 UDP 四元组（参考 `conn/udp.go` 的 addr↔conn 注册表）。

### 4.8 两种模式的互通性（重要约束）

- **同一条连接内，两端必须同模式**：RawTCP 线路上是协议号 6 的报文，UDP 模式是协议号 17；
  NAT/conntrack 按五元组中的协议字段分别建表，一端发 TCP、一端发 UDP 在中间盒眼里是两条
  互不相关的流，无法互通——也没有意义，UDP 腿在运营商眼里仍是 UDP，目标 1 落空。
- **应用层完全兼容**：两种模式共用 §4.6 私有头、会话语义与 `Conn` API，上层（KCP/RPC/
  应用）无感；同一进程可同时双栈监听（TCP 端口接 RawTCP 客户端 + UDP 端口接 UDP 客户端），
  各成会话、互不干扰。典型场景：Linux 客户端走 RawTCP，Windows/无权限客户端走 UDP。
- **Windows 客户端也要"线路上是 TCP"的路径**（按推荐度）：
  1. **网关部署**：fake_tcp 客户端跑在网关/路由器（OpenWrt 等 Linux 设备）上，Windows 机器
     在 LAN 内用 UDP/内核 TCP 连网关——运营商只能看到网关↔服务器这一段（udp2raw 用户的
     典型玩法，`test/socks_trunk_kcp` 的合并二进制形态可直接复用）；
  2. **WinDivert 移植**：方案 A 的 Windows 实现路径（见 §11），udp2raw 的 Windows 版即此方案；
  3. 本机 relay：仅开发调试场景。
- **Windows 原生限制备忘**：XP SP2 起 Windows 在系统层禁止 raw socket 发送 TCP，接收仅能
  SIO_RCVALL 嗅探——方案 A 的纯用户态 socket 实现在 Windows 上不可行，必须借助驱动。
  WSL2 是真 Linux 内核，可正常开发调试方案 A（注意其网络是 Hyper-V NAT）。

### 4.9 与 udp2raw 的关系：Windows 支持的三条路线

背景：Windows 无法纯用户态发 TCP（§4.8），udp2raw 在 Windows 上依赖 **WinDivert**。
注意 WinDivert 是独立开源项目（basil00/Divert：签名内核驱动 + 用户态 DLL），
**不是 udp2raw 私有物——我们可以直接用它，不必经过 udp2raw**。因此"Windows 支持"与
"兼容 udp2raw 线协议"是两个独立决策。

| 路线 | 做法 | 工作量 | 优点 | 缺点 |
|---|---|---|---|---|
| **R1 外挂 udp2raw relay**（短期） | Windows 与服务器各跑一个真 udp2raw 进程；本模块两端都用 **UDP 模式**连本机 udp2raw，由 udp2raw↔udp2raw 完成 FakeTCP 段 | ≈0（M2 完成即可用） | 零开发立即可用；驱动/RST 吞掉/重连全部现成 | 每节点多一进程 + 一次本机转发；udp2raw 强制加密信封（CPU + 双重加密）；部署变复杂 |
| R2 线协议兼容 udp2raw | fake_tcp 移植 udp2raw 的 FakeTCP 线格式，Windows 只跑真 udp2raw 客户端直连我们的服务端 | 大 | 服务端单进程；可对接存量 udp2raw 基础设施 | 协议无正式规范，需逆向/移植其 C++ 并**长期跟踪上游**；强制其加密/认证信封（`--cipher-mode none --auth-mode none` 档也有自身帧格式）；seq 窗口/保活/重连行为需逐一对齐 |
| **R3 原生 WinDivert PacketIO**（中期，推荐） | 直接用 WinDivert（Go binding/cgo，候选 `github.com/imgk/divert-go`）实现 Windows 版 PacketIO；线格式仍是我们自己的精简协议 | 中 | 全平台协议统一、信封精简；WinDivert 过滤器捕获内核 RST 后不再注入即吞掉（替代 §5 的 iptables 环节）；不依赖 udp2raw 演进 | 驱动分发/安装（需管理员）；cgo；WinDivert 许可证（GPL 系）对分发方式的约束需实现时确认 |

决策：
- **基线（默认，见 M0）**：直接用 udp2raw 做底层伪装（即 R1 常态化），本仓库现有 UDP+KCP 栈
  零改动架在上面；先实测验证收益，再决定是否投入自研。
- **中期（M6 之后，或 M0 决策门触发自研信号时）**：做 R3，见里程碑 M7。
- **R2 仅在有"必须对接存量 udp2raw 服务端"的需求时才做**，它不是 Windows 支持的必要条件
  （R1/R3 都不需要线协议兼容）。做 R2 时以 udp2raw 源码为准逐字段对齐，并把它锁定的版本号
  记录在案。

---

## 5. 内核干扰抑制（RawTCP 模式的关键配套）

**问题**：收到目的端口无内核 socket 的 TCP 报文，内核协议栈会自动回 RST——中间盒看到 RST
会认为连接已断，NAT 表项随即拆除。

**对策（两端都要做，iptables/nftables 二选一，启动时装、退出时卸）**：

```bash
# 服务端（<port> 为监听端口）
iptables -A OUTPUT -p tcp --sport <port> --tcp-flags RST RST -j DROP
# 客户端（<lport> 为本地端口；Dial 时显式指定或由我们占用后得知）
iptables -A OUTPUT -p tcp --sport <lport> --tcp-flags RST RST -j DROP

# nftables 等价（优先检测，发行版默认越来越多用 nft）
nft add rule inet filter output tcp sport <port> tcp flags rst drop
```

- 规则用 sport(+对端 IP) 收窄，避免误伤本机其它 TCP。
- 安装前先 `-C` 检查幂等；进程退出（含信号处理）负责删除；提供配置 `auto_firewall: bool`
  （默认 true；生产环境若由运维手工管防火墙则置 false 并文档化规则）。
- 备选/远期：XDP/eBPF 在驱动层收包，完全绕过内核协议栈，可免去 RST 抑制（性能也更好），列为 M6 之后。

**权限**：`AF_PACKET` 与 raw IP socket 均需 root 或 `CAP_NET_RAW`（装防火墙规则另需
`CAP_NET_ADMIN`）。部署文档给出 `setcap 'cap_net_raw,cap_net_admin+ep' <binary>` 方案，
避免整体 root 运行。

---

## 6. 模块划分与文件布局

全部代码放 `fake_tcp/` 包内（与 `trunk/`、`trunk_kcp/` 平级），零 cgo：

| 文件 | 职责 |
|---|---|
| `plan.md` | 本文档 |
| `doc.go` | 包注释：模式、权限要求、与 trunk_kcp 的搭配用法 |
| `errno.go` | 错误码，`errors.NewCode(moduleCode+n, ...)`（moduleCode 取 **500000**，实现时先全仓 grep 确认未占用），错误需可过网（兼容 `codec/reply.go` 的 Code/Msg/Stack 约定） |
| `header.go` | IPv4/TCP 头编解码、checksum（含伪头）、options 编解码——**纯函数，零状态** |
| `session.go` | 极简 TCP 状态机：握手/数据/关闭/保活，seq/ack/TS/位图维护；对 PacketIO 抽象编程 |
| `packetio.go` | `PacketIO` 接口（`ReadPacket() (payload, meta, error)` / `WritePacket(pkt, meta) error`）+ meta（四元组、L2 信息） |
| `raw_packetio_linux.go` | RawTCP 实现：AF_PACKET+cBPF 收、raw IP+IP_HDRINCL 发（`//go:build linux`）；BPF 用 `golang.org/x/net/bpf` 纯 Go 组装 |
| `raw_packetio_other.go` | 非 Linux 桩（`//go:build !linux`），构造即返回 errno 不支持 |
| `udp_packetio.go` | UDP 兜底实现：`net.UDPConn` + ConnID 头 |
| `firewall_linux.go` | iptables/nftables RST 抑制规则的安装/幂等检查/卸载 |
| `conn.go` | `Conn`：`io.ReadWriteCloser` + `net.Conn` 语义子集（LocalAddr/RemoteAddr/SetDeadline），发送切片、接收重组、读缓冲 |
| `listener.go` | `Listener`：服务端 session 表（四元组哈希→session，参考 `conn/udp.go` 注册表模式）、`Accept()`、半开连接清理 |
| `dialer.go` | `Dial`：客户端建连（含握手重试、本地端口分配与防火墙联动） |
| `keepalive.go` | 保活/超时扫描协程（所有 session 共用一个 ticker，参考 `conn/kcp.go` 的共享 update 循环） |
| `config.go` | `Config` 结构 + 默认值 + 校验 |
| `header_test.go` | 编解码/checksum golden 测试（内置抓包样本字节） |
| `session_test.go` | 状态机单测：注入报文序列，断言状态迁移与回复报文字段 |
| `fake_tcp_test.go` | **UDP 模式**端到端集成测试（无权限要求，CI 可跑）：c/s 互发、乱序/丢包注入、关闭流程 |
| `raw_linux_test.go` | RawTCP 模式 lo 回环测试（需 root，环境变量 `FAKE_TCP_RAW_TEST=1` 门控） |
| `bench_test.go` | 吞吐/CPU 基准：fake_tcp vs UDP vs 内核 TCP |

依赖：仅新增/复用 `golang.org/x/net`（bpf 子包，纯 Go）；其余沿用仓库现有
（`lxt1045/utils` 日志、`lxt1045/errors`）。

---

## 7. API 设计（草图，以最终代码为准）

```go
package fake_tcp

type Mode int
const (
    ModeRawTCP Mode = iota + 1 // 主模式：线路上是 TCP（Linux + CAP_NET_RAW）
    ModeUDP                    // 兜底：UDP + 私有头
)

type Config struct {
    Mode             Mode
    LocalAddr        string        // "0.0.0.0:8443"（服务端）/ 本地端口（客户端，RST 抑制需要）
    RemoteAddr       string        // 客户端必填
    Magic            uint32        // payload 私有头魔数，默认随机派生
    MTU              int           // payload 切片上限，默认 1400
    Ordered          bool          // 接收是否按序上交（默认 false：到达即交，配 KCP）
    Keepalive        time.Duration // 默认 30s
    HandshakeRetries int           // 默认 5
    AutoFirewall     bool          // 默认 true：自动装/卸 RST 抑制规则
}

// 服务端
func Listen(ctx context.Context, cfg Config) (*Listener, error)
type Listener struct { /* ... */ }
func (l *Listener) Accept() (*Conn, error)
func (l *Listener) Close() error

// 客户端
func Dial(ctx context.Context, cfg Config) (*Conn, error)

// 连接：满足 io.ReadWriteCloser，可直接喂 trunk_kcp.NewTrunkKCP / rpc.NewPeer
type Conn struct { /* ... */ }
func (c *Conn) Read(p []byte) (int, error)
func (c *Conn) Write(p []byte) (int, error)   // 内部按 MTU 切片成多个 TCP 段
func (c *Conn) Close() error                   // 发 FIN，走完四次挥手（超时降级 RST）
```

---

## 8. 与现有仓库的集成

1. **接 RPC**：`Conn` 直接作为 `io.ReadWriteCloser` 传入 `rpc.NewPeer(ctx, conn, ...)`；
   写一个 `fake_tcp` 版的端到端测试（仿 `test_service_base.go` 的 `NewFakeConnPipe` 形状，
   用 UDP 模式 + 真实 fake_tcp.Conn）。
2. **接链路聚合**：多条 fake_tcp.Conn（不同四元组/不同源端口）喂给
   `trunk_kcp.NewTrunkKCP(conv, onNewConn, conns...)`——KCP 在上提供可靠性，fake_tcp 在下
   提供"全是 TCP"的线路外观，这是本模块的**主打组合**。
3. **进 socks 示例**：`test/socks_trunk_kcp` 的配置增加 `transport: fake-tcp` 选项
   （复用其 config/session/auth 链路），验证真实代理场景。
4. **socket 包关系**：`socket.Listen/Dial` 维持不动；fake_tcp 自建 socket，不改动现有包。

---

## 9. 里程碑 / Milestones

### M0 —— 基线：直接以 udp2raw 为底层伪装（零自研，最先做）

**思路**：不自研任何伪装逻辑，把 udp2raw 当作底层转发器；本仓库**现有** UDP+KCP 栈
（`socks_trunk_kcp` / `conn/kcp.go`）原封不动架在其上，今天即可部署。

```
客户端:  socks-client ──UDP(127.0.0.1)──► udp2raw -c ──伪装TCP(线上)──►
服务端:  socks-service ◄──UDP(127.0.0.1)── udp2raw -s ◄───────────────┘
```

- [x] 两端 udp2raw 配置模板：FakeTCP 模式；`--cipher-mode none --auth-mode none`（或 simple 档，
      避免与上层 socks 链路双重加密——以实际版本支持的档位为准）；内层 KCP `mtu` ≤ 1300
      （`deploy/udp2raw/README.md`）
- [x] 进程守护与配置同步方案（`deploy/udp2raw/*.service` systemd 单元）
- [ ] Windows 客户端直接用 udp2raw 官方二进制（自带 WinDivert），验证 §4.9 的 R1 全链路
- [ ] **运营商实测**：同链路分时对比 裸UDP vs udp2raw-FakeTCP 的吞吐/丢包/晚高峰表现，出量化报告

**验收**：全链路跑通 + 实测报告。
**决策门**：
- 若 FakeTCP 相比裸 UDP **无显著收益** → 重新评估整个 fake_tcp 自研的立项（可能只需保留 UDP 模式）；
- 若收益显著 → 继续用 udp2raw 作为生产基线；出现下列任一信号再启动 M3 自研 RawTCP：
  ① 要求单二进制交付；② udp2raw 成为吞吐/CPU 瓶颈；③ 链路聚合需要 N 条路径的精细化控制
  （N 对 udp2raw 进程运维不可接受）；④ 上游维护风险兑现（内核/ISP 行为变化导致失效）。

### M1 —— 报文编解码基座（纯函数，无机权限制）✅ 已完成

- [x] `header.go`：IPv4/TCP 头、options（MSS/SACKP/TS/WS/SACK block）编解码
- [x] checksum（含伪头）实现
- [x] `errno.go` + `config.go` 骨架
- [x] golden 测试：内置 3 组真实抓包字节（SYN/数据/FIN），编解码往返一致、checksum 与抓包一致
      （实现为：RFC1071 向量 + Linux 风格 SYN golden + 解析→序列化字节级往返 + 校验和自洽）

**验收**：`go test -run 'TestHeader|TestChecksum' -count=1 ./fake_tcp` 全绿；`go vet ./fake_tcp` 干净。
（实际测试名：`TestChecksumRFC1071|TestGoldenSYN|TestDataPacketRoundTrip|TestSACKOption|TestParseBadPacket|TestMarshalIPv4Fields`，全绿）

### M2 —— 状态机 + UDP 模式端到端（CI 可跑）✅ 已完成

- [x] `packetio.go` 接口 + `udp_packetio.go`（Segment 统一抽象 + LinkIO；
      UDP 私有头 [Magic][ConnID][Type]，控制消息映射 TCP flags 语义）
- [x] `session.go`：握手（含重试）/数据（§4.3 全部收包规则）/关闭/保活
      （实现中修正两处计划未覆盖的语义：① 保活为探针-应答式，单向探针会在对方失活时
      造成"只收不发被判活"的单向死亡螺旋；② Ordered 配置否决——无重传语义下空洞永不回填，
      按序等待必然死锁，置 true 现在返回配置错误）
- [x] `conn.go` / `listener.go` / `dialer.go`（UDP 模式全链路）
- [x] `session_test.go`：pipe 链路注入报文断言状态迁移/收包规则/关闭挥手/半开回收；
      `fake_tcp_test.go`：UDP 端到端（10MB 双工/8 连接并发/关闭语义/垃圾报文/保活/超时）
- [x] 关键架构修正（实现中发现）：session 发送路径改为出站队列 + 专职 writeLoop，
      读循环零阻塞——否则两个对端的读循环在"handle 内同步写"下互等形成 ABBA 死锁；
      数据段阻塞入队（应用边界本地背压），ACK/控制段非阻塞丢得起

**验收**：`go test -count=1 -timeout 120s ./fake_tcp` 全绿（无需 root，连续两轮通过）；测试遵守仓库纪律
（`-run` 精确指定、不用 `log.Fatal`、动态端口不占固定端口）。
注：UDP 端到端断言"无重复无损坏 + 容忍少量内核丢包/乱序"（本层即不可靠语义）；
字节级精确校验由 pipe 链路的 `TestPipeEndToEnd` 承担。

### M3 —— RawTCP 模式（Linux，需 root 门控）🚧 代码完成，门控测试待跑

- [x] `raw_packetio_linux.go`：AF_PACKET+cBPF 收、raw IP(IPPROTO_RAW，隐含 IP_HDRINCL) 发
- [x] `firewall_linux.go`：RST 抑制规则装/卸/幂等（iptables 与 nft 自动探测）
- [x] `raw_linux_test.go`：lo 回环 c/s 互通（`FAKE_TCP_RAW_TEST=1` + root 门控）——已写好待跑
- [ ] **tcpdump 验收清单**（本机 `tcpdump -i lo -nn 'tcp port <p>' -vvXX` 逐项人工核对）：
  - [ ] 三次握手齐全，SYN 选项 = MSS/SACKP/TS/WS
  - [ ] 数据包 PSH+ACK，seq 连续递增，TS 回显正确
  - [ ] 全程**无内核 RST**、**无重传报文**（同一个 seq 不出现两次）
  - [ ] 关闭为完整四次挥手
  - [ ] `conntrack -L` 能看到该连接处于 ESTABLISHED

**验收**：门控测试通过 + tcpdump 清单全勾；`iptables -L OUTPUT -n` 确认规则进出成对。

### M4 —— 上层集成

- [x] fake_tcp.Conn 喂 `trunk_kcp.NewTrunkKCP`：丢包率→带宽利用率量化测试
      （`trunkkcp_bench_test.go`，结果表见 `README.md`；KCP 在 0~33% 丢包下 100% 交付，
      带宽放大 ≈ 1/(1−p) 与几何重试模型吻合）
- [ ] 裸 Conn 喂 `rpc.NewPeer` 的双向调用测试（对称 Peer 验证）
- [ ] `bench_test.go`：fake_tcp vs UDP vs 内核 TCP 的吞吐/CPU 对比

**⚠️ 集成硬性前提（M4 实测发现）**：trunk_kcp 的 recvLoop 按字节流解析 KCP 头长度字段，
必须保证**一个 KCP 段恰好一个 fake_tcp 段**（整段丢失而非半帧错位）：
RawTCP 模式 MTU ≥ 1464，UDP 模式 MTU ≥ 1413（或把 trunk_kcp 的 KCP MTU 调小）。
违反后果：丢包时 `invalid KCP segment length` 解析错位 → 物理连接被剔除。
后续应在 Conn 层或配置校验中强制该约束（或给 trunk_kcp 加错位重同步）。

**验收**：集成测试全绿；基准数据写入 `fake_tcp/README.md`（目标：单连接吞吐 ≥ UDP 的 70%，
CPU ≤ UDP 的 1.5×）。

### M5 —— 真机跨主机验证

- [ ] 两台主机经家用路由器/NAT 互通，重复 M3 的 tcpdump 清单（两侧同时抓包比对）
- [ ] 打流 30 分钟：无 conntrack 表项丢失、无中间盒 RST 注入导致断流
- [ ] 与内核 TCP/UDP 的吞吐对比（同链路分时打流）
- [ ]（可选，用户环境）运营商实网验证：对比同链路 UDP 被限速 vs fake_tcp 的实际速率

**验收**：真机报告附在 `fake_tcp/README.md`；问题回填本文档 §11。

### M6 —— 加固与文档

- [ ] 错误码梳理（全部走 `errno.go`，错误信息可过网）
- [ ] `fake_tcp/README.md`：用法、权限（setcap）、防火墙规则、与 trunk_kcp 的组合示例
- [ ] `test/socks_trunk_kcp` 增加 `transport: fake-tcp` 配置项并联调
- [ ] CLAUDE.md 增补一段 fake_tcp 架构说明（含 M0 的 udp2raw 基线部署形态）

### M7（可选，Windows 一等支持）—— 原生 WinDivert PacketIO（路线 R3，§4.9）

- [ ] `windivert_packetio_windows.go`：WinDivert 收发；过滤器同时匹配我们的报文与内核 RST，
      捕获 RST 后不再注入（=吞掉），替代防火墙规则
- [ ] 驱动检测/安装流程（WinDivert.dll + 签名 .sys；确认其 GPL 系许可证对分发的约束）
- [ ] Windows 真机验证清单（对齐 M3 的 tcpdump 项，用 Wireshark 核对）
- [ ] Windows↔Linux RawTCP 模式互通测试

**验收**：Windows 客户端不依赖 udp2raw，直接以 RawTCP 模式与 Linux 服务端互通；
Wireshark 清单全勾。

---

## 10. 测试计划 / Testing

| 层 | 内容 | 权限 | 运行方式 |
|---|---|---|---|
| 纯函数 | header 编解码、checksum golden、seq 运算 | 无 | `go test -run 'TestHeader\|TestChecksum' ./fake_tcp` |
| 状态机 | 注入报文序列 → 断言状态迁移/回复报文/ack 值/位图 | 无 | `go test -run 'TestSession' ./fake_tcp` |
| UDP 模式集成 | 端到端收发、乱序/丢包/重复注入、多连接、关闭、保活 | 无 | `go test -count=1 ./fake_tcp`（CI 锚点） |
| RawTCP 集成 | lo 回环全链路 + RST 抑制生效 | root + `FAKE_TCP_RAW_TEST=1` | 手动/实机 |
| 真机 | 跨 NAT tcpdump 清单、长稳打流 | 两台主机 | 手动 |
| 基准 | 吞吐/CPU 三方对比 | 无（UDP 模式）/root（Raw） | `go test -bench . -run '^$' ./fake_tcp` |

纪律：遵循仓库 CLAUDE.md——测试内不 `log.Fatal`、不占固定端口、网络测试必须 `-run` 可单点执行。

---

## 11. 风险与对策 / Risks

| 风险 | 影响 | 对策 |
|---|---|---|
| 无 root/CAP_NET_RAW（容器、受限主机） | RawTCP 不可用 | 自动探测权限，失败降级 ModeUDP 并打日志；文档给 setcap 方案 |
| iptables 与 nftables 并存/缺失 | RST 抑制失败 | 启动时探测后端，双实现；`AutoFirewall=false` 时打印应配规则 |
| conntrack 把"窗口外"包标 INVALID 被防火墙 DROP | 断流 | seq 永远连续、ack 单调；M5 真机覆盖；文档提示 `nf_conntrack_tcp_be_liberal` 兜底 |
| 丢包造成 seq 空洞被高级 DPI 标记 | 被识别异常 | 低概率：民用 QoS 分类器不做此粒度；叠 KCP 后重传会呈现"快速重传"外观反而更真 |
| 运营商对"无拥塞退避的大流量 TCP"做行为分析 | 被限速 | 本模块目标是协议识别层；速率形态由使用方自控（可加限速上层） |
| 同机 c/s 测试四元组冲突/双收 | 测试 flake | 客户端强制独立源端口；AF_PACKET 双 socket 都配严 cBPF；测试用动态端口 |
| 内核升级导致 AF_PACKET 行为变化 | 兼容 | 锁定语义只用 stable ABI（SOCK_RAW+cBPF）；CI 多内核版本跑门控测试（远期） |
| Windows 无 AF_PACKET，且 XP SP2 起禁止 raw socket 发 TCP | 平台缺失 | 三条路线见 §4.9：短期 R1（外挂 udp2raw，零开发）→ 中期 R3（原生 WinDivert，M7）；R2（线协议兼容 udp2raw）仅在需对接存量 udp2raw 设施时做 |
| 误用：把本模块当加密/抗审查工具 | 合规 | 文档明确：只解决"UDP 被 QoS"，payload 明文物种由上层（TLS/已有 socks 链路）负责 |

---

## 12. 参考 / References

- `wangyu-/udp2raw`（FakeTCP 模式）——本方案 A 的原型与可行性证据（raw socket 收发分工、
  RST 抑制、BPF 过滤均来自其成熟实践）
- RFC 793（TCP）、RFC 1323（TS/WS 选项）、RFC 2018（SACK）——演戏要演全的字段依据
- gVisor netstack / lwIP ——方案 B 评估对象（已否决，留档）
- 本仓库：`conn/udp.go`（addr↔conn 注册表）、`conn/kcp.go`（共享 update 循环）、
  `trunk_kcp/`（上层可靠性组合对象）、`codec/reply.go`（错误码过网约定）、
  `test_service_base.go`（端到端测试形状）
