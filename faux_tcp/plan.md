# faux_tcp 实现计划 / faux_tcp Implementation Plan

## 背景与目标 / Background & Goals

**真正的目标（第 4、5 点澄清）：**

> 使用 TCP/IP 网络通信，但避免被运营商识别为 UDP（规避 UDP QoS/限速/歧视），
> 同时避免 TCP 的重传和拥塞控制（丢包恢复由上层 KCP/应用层 ARQ 负责，避免
> TCP 层重传与 KCP 层重传叠加造成的流量放大——见 `test/socks_trunk_kcp/README.md`
> "线上流量放大问题"一节）。

"UDP 伪装成 TCP" 只是实现手段之一，不是目标本身。任何能达到上述目标的技术路线
都允许采用。

**动机：** 现有 `trunk_kcp` 的物理连接是 TLS/TCP（可靠流），KCP 的 ARQ 跑在 TCP 上
会产生大量伪重传（实测线上流量放大 2~3 倍）。如果底层改为 UDP，KCP 行为正常，
但运营商对 UDP 的 QoS/限速往往非常激进。faux_tcp 提供第三条路：
**线上是 TCP 的样子，行为是 UDP 的样子**（不重传、不拥塞控制、不流控）。

---

## 需求拆解 / Requirements

### 必须保留的 TCP 表象（对中间盒/DPI 可见的全部特征）

- [x] 三次握手：SYN / SYN+ACK / ACK，ISN 随机化（RFC 6528）—— `stack.go` + `TestHandshakeAndEcho`
- [x] 序号/确认号单调推进，语义自洽（ack 不超过已"接收"的字节）—— `TestAckSemanticsWithHole`
- [x] 数据段 PSH+ACK，MSS 协商，不分片（DF）—— `packet.go` 构包常量
- [x] 选项指纹模拟主流 OS（Linux: MSS,SACK,TS,NOP,WS；顺序与初值逼真）—— `tcpOptionsSyn/tcpOptionsData`（真机指纹比对待阶段 3）
- [x] 四次挥手 FIN/ACK，RST 处理—— `TestCloseHandshake` / `TestRstToUnknownPort`
- [x] 窗口通告固定大窗口（如 65535 + wscale=7），不出现零窗口—— `Config.Window/WScale`

### 必须去掉的 TCP 机制

- [x] 重传：RTO 定时器、重传队列、快速重传、SACK 驱动重传 —— 不存在（`TestNoRetransmitUnderLoss` 验证）
- [x] 拥塞控制：cwnd/ssthresh/慢启动/拥塞避免/reno/cubic —— 不存在
- [x] 滑动窗口流控：不依据对端通告窗口节流；接收侧不做窗口收紧 —— 不存在
- [x] 发送积压：不维护未确认队列；写出即发出，背压直接抛给上层 —— chData 满即丢包

### 架构需求

- c/s 两端都是我们的栈（**端到端自控**），只需对路径上的中间设备逼真，
  不需要与真实 TCP 端点互操作。
- 传输模式可切换：`udp`（直连对照/无权限回退）与 `fauxtcp`（伪装）。
- 与现有代码集成：实现 `net.Conn` / `io.ReadWriteCloser`，可作为
  `trunk_kcp` / `trunk` 的物理连接（KCP 在上层做 ARQ）。

---

## 方案选型 / Options

### 方案 A（推荐）：Raw Socket + 自研最小 TCP 状态机（udp2raw 路线）

用纯 Go 实现一个"只有表象、没有可靠性"的 TCP 状态机。成熟先例：
`wangyu-/udp2raw`（C++，同目标，生产验证多年）。

- 优点：代码量可控（核心 ~1.5k 行 Go）；对线上每个字节完全可控；
  指纹可精确校准；**纯 Go 无 cgo**（见下"依赖与 cgo"）。
- 缺点：需要 root/CAP_NET_RAW；需处理内核 RST 干扰（iptables 规则）。

### 依赖与 cgo / Dependencies & cgo

本模块**不引入任何 cgo 依赖**（实测确认，gopacket@v1.1.19）：

- **收发包**：`golang.org/x/sys/unix` 直接调 AF_PACKET / raw IP socket
  （`unix.Socket`/`unix.SockaddrLinklayer`/`unix.Sendto`/`unix.Recvfrom`，
  纯 Go；x/sys 已是本仓库间接依赖，零新增）。
- **构包/解包**：用 `gopacket` + `gopacket/layers`（实测纯 Go 无 cgo）。
  ⚠️ gopacket 的 `pcap`/`pfring`/`afpacket` 三个子包带 cgo（afpacket 仅用
  几个 C 常量，x/sys/unix 均有等价物），**禁止使用这三个子包**。
- **零新依赖备选**：构包也可以手搓（IPv4 头 20B + TCP 头 20B + 选项 +
  校验和，约 300 行，风格同 `codec/header.go`），届时连 gopacket 都不引入。
  通过 `packet.go` 的编解码接口隔离，两种实现可随时互换。

### 方案 B（备选）：裁剪成熟用户态 TCP 栈

vendor gVisor **netstack**（纯 Go）或 lwIP（cgo），删除其重传/拥塞控制/流控模块。

- 优点：状态机完备（边界情况已验证）；netstack 的 CC 可插拔。
- 缺点：netstack 代码量大、内部 API 不稳定，删除 RTO 重传涉及核心路径改动；
  lwIP 引入 cgo，破坏本仓库纯 Go 构建。维护负担明显高于方案 A。
- 触发条件：方案 A 在某种中间盒/DPI 下逼真度不足时，再评估切换。

### 方案 C（否决）：内核 TCP + TCP_NODELAY / TCP_CORK

内核 TCP **无法关闭重传和拥塞控制**（无 sysctl 可关 RTO；cwnd 强制存在）。
不满足核心需求，否决。

### 方案 D（过渡/对照）：外挂 udp2raw 进程（sidecar）

直接使用成熟实现 [udp2raw](https://github.com/wangyu-/udp2raw)（或 wire 兼容的
[udp2raw-rs](https://github.com/brianpht/udp2raw-rs)）：它是独立二进制，本机暴露
UDP 端口，我们的 c/s 把 trunk 物理连接指到本机 udp2raw 端口即可，
伪装层完全交给 udp2raw。

```
我们的 client ↔ UDP ↔ udp2raw-client ══ fake TCP ══ udp2raw-server ↔ UDP ↔ 我们的 server
```

- 优点：零开发、立即可用；自带加密/防重放/心跳/iptables 脚本；生产验证多年；
  可立即解除运营商 UDP 限速。
- 缺点：
  - **无库 API，只能外挂进程**（cgo 嵌入不现实：全局状态 + 自带事件循环/线程）；
  - 破坏单二进制交付，部署方要维护额外进程 + root + iptables 规则；
  - 与 trunk 聚合模型不匹配：一个 udp2raw 实例 = 一条 fake TCP 隧道，
    N 条聚合链路需要 N 个实例 + N 组端口/规则；
  - 多一跳本机 UDP 转发与缓冲，时延/CPU 各多一层；FEC 功能需关闭；
  - 主线维护基本停滞（2023-02 后无正式发布）。
- 定位：**阶段 0 的立即可用过渡方案**，以及自研实现的**指纹/行为对照基准（oracle）**；
  不作为最终形态。

### 方案对比 / Comparison

| 维度 | A. 自研最小状态机 | B. 裁剪 netstack/lwIP | C. 内核 TCP | D. 外挂 udp2raw |
| --- | --- | --- | --- | --- |
| 可关重传 | ✅ 天然没有 | ⚠️ 需改核心 | ❌ | ✅ |
| 可关拥塞控制 | ✅ 天然没有 | ⚠️ 可插拔但要删流控 | ❌ | ✅ |
| 线上逼真度 | ✅ 字节级可控 | ✅ | ✅ | ✅（生产验证） |
| 开发量 | 中 | 大 | 小 | ≈0 |
| 单二进制交付 | ✅ | ✅ | ✅ | ❌（额外进程+root+iptables） |
| trunk 多链路聚合 | ✅ 原生 | ✅ | ✅ | ⚠️ 需 N 个实例 |
| 依赖 | x/sys/unix + gopacket/layers（纯 Go） | netstack(大) / lwIP(cgo) | 无 | 外部二进制 |
| 权限 | root | root | 无 | root |

**决策门**：M1（PoC 抓包验证）时评审——若自研栈逼真度达标则走方案 A 主线；
若遇到难以克服的中间盒兼容问题，则以方案 D 落地交付、方案 A 转入长期迭代。

---

## 关键设计 / Key Design

### 线上行为规范（Wire Spec）

```
握手：
  C → S: SYN, seq=ISN_c(随机), 选项: [MSS=1380, SACK, TS, NOP, WS=7]  (仿 Linux)
  S → C: SYN+ACK, seq=ISN_s(随机), ack=ISN_c+1, 选项同上
  C → S: ACK, seq=ISN_c+1, ack=ISN_s+1
  → Established。ISN 用 crypto/rand。

数据（双向对称）：
  每个应用层包 → 一个 TCP 段: PSH+ACK, seq=发送侧已发字节数(单调), ack=最近接收序号
  seq 只增不减，与对端 ack 无关；发送端不保留副本、永不重发。
  接收端：按 seq 维护"最高连续序号"，ack 单调递增；丢包产生空洞时 ack 停在空洞处
  （保持语义自洽），绝不发 dup ACK/SACK（丢包对 DPI 不可见）。

关闭：
  FIN+ACK → ACK → FIN+ACK → ACK（四次挥手完整模拟），超时 TIME_WAIT 省略。
  任一端异常消失：对端靠应用层心跳超时（KCP 心跳/模块自带心跳）清理。

保活：
  无数据时每 N 秒发 ACK 保活段（防 NAT/状态防火墙表项老化，N 默认 25s 可配）。
```

### 丢包语义 / Loss Semantics

- 发送即忘（fire-and-forget）：写出 = 构包发出，不排队、不计时、不重发。
- 接收端允许 seq 空洞；数据直接交给上层（KCP 负责排序/重传）。
- 关键约束：**ack 绝不能超过已收到的最高连续序号**（否则状态跟踪型 DPI
  能识别异常）；seq 可以有空洞（对 DPI 表现为"观察者漏看"，无害）。

### 与内核共存 / Kernel Coexistence

内核协议栈看到不属于任何 socket 的 SYN+ACK/数据段会回 RST，必须抑制：

- Linux：文档化 iptables 规则（udp2raw 同款）：
  `iptables -A OUTPUT -p tcp --sport <listen_port> --tcp-flags RST RST -j DROP`
  （发送侧）；接收经 AF_PACKET 旁路内核，无需额外规则。
- 无 root 权限：自动回退 `udp` 模式并打 warn 日志。
- Windows/macOS：本期不做；预留 `LinkLayer` 接口抽象（后续可接 WinDivert）。

### 安全 / Security

- 载荷即 KCP 段（上层语义不变）；可选 PSK+AEAD 封装头（防注入/防重放，
  也可遮蔽 KCP 头特征，配置开关，默认开）。
- 接入校验：四元组 + ISN 窗口校验 +（可选）首包 HMAC 挑战应答。

### 接口 / API

```go
package faux_tcp

type Mode string // "udp" | "fauxtcp"

type Config struct {
    Mode     Mode
    MSS      int    // 默认 1448（1500 MTU - IP/TCP/TS 头），Write 超限报错
    Window   uint16 // 通告窗口，默认 65535（WS=7）
    KeepAlive time.Duration // 默认 25s
    PSK      []byte // 可选加密/防注入
}

// 与 net.Listener / net.Conn 语义一致，可直接喂给 trunk_kcp.NewTrunkKCP(...)
func Listen(ctx context.Context, cfg Config, addr string) (net.Listener, error)
func Dial(ctx context.Context, cfg Config, laddr, raddr string) (net.Conn, error)
```

- **数据报边界（实现中修正的关键设计）**：一次 `Write` = 一个 TCP 段，超过 MSS
  直接报错（由上层分段）。不能像 TCP 一样拆分大写请求——本层不保序不重传，
  拆分后任一片段丢失都会让上层（trunk_kcp 按长度流式重组）永久错位。
  整段投递使"丢包 = 丢整条报文"，与 UDP 语义一致，KCP 可正确重传。
  KCP 线上包 1400B ≤ MSS 1448，天然兼容。
- 无 root 时 `Listen/Dial` 返回明确错误，由上层（`conn/` 适配器）回退 UDP。

---

## 模块结构 / Layout

```
faux_tcp/
├── plan.md            // 本文档
├── README.md          // 用法与部署（iptables）说明 ✅
├── config.go          // Config/Mode/默认值 ✅
├── conn.go            // net.Conn 适配（Read/Write/Close/Deadline）✅
├── listen.go          // Listen/Accept，会话表（四元组 → conn）✅
├── stack.go           // 最小 TCP 状态机（握手/数据/挥手/保活）✅
├── packet.go          // 手搓 IPv4+TCP 构包/解包（纯 Go 零依赖，已选定）✅
├── link.go            // Link 接口 + 内存网络（测试用）✅
├── link_linux.go      // x/sys/unix AF_PACKET/raw socket 收发 ✅
├── link_stub.go       // 非 Linux 平台桩 ✅
├── crypto.go          // PSK+AEAD 封装（可选，未实现）
├── stack_test.go      // 状态机单测 + 吞吐基准 ✅（race 干净）
├── trunkkcp_test.go   // trunk_kcp 全链路集成（10% 丢包 KCP 全恢复）✅
└── integration_test.go// loopback 双端互通（需 root + iptables，FAUXTCP_E2E 门禁）✅
```

`conn/` 下新增 `fauxtcp.go`：把 `faux_tcp.Dial/Listen` 包成现有 `io.ReadWriteCloser`
工厂，接入 `socks_trunk_kcp` 的 `TrunkConn` 物理连接创建路径（配置项选择 udp/tcp/fauxtcp）。

---

## 阶段划分 / Phases

### 阶段 0：调研与 PoC（验证可行性）

**任务：**
- [x] x/sys/unix AF_PACKET 收包 + raw IP socket（IPPROTO_RAW）发包实现（`link_linux.go`；
      真机抓包验证待有权限环境执行）
- [x] 构包路径定版：**手搓实现**（`packet.go`，纯 Go 零依赖，风格同 `codec/header.go`；
      gopacket/layers 对比后未引入——需求只需 IPv4+TCP 两个头，手搓 ~260 行足够）
- [ ] 复现内核 RST 干扰并用 iptables 规则抑制，写成脚本（规则已写入 README，待真机验证）
- [ ] **部署 udp2raw（方案 D）跑通 trunk_kcp 全链路**，作为立即可用的过渡交付物；
  并抓包留档其指纹/行为，作为自研栈的对照基准（oracle）
- [ ] 手工构造三次握手 + 一个数据段 + 挥手，Wireshark/tcpdump 抓包验证：
  协议树显示为正常 TCP 会话，无 RST/异常（待真机；内存链路单测已覆盖等价断言）
- [ ] 调研 udp2raw 的指纹与规则细节（含其源码中 seq 模拟、心跳、防重放实现），
  整理成清单附在 README

**产出：** `faux_tcp/poc/` 可运行 demo + 抓包截图/pcap 文件 + iptables 脚本
**验收：** Wireshark 中 PoC 流量被完整识别为 TCP 会话；无内核 RST 干扰；
udp2raw 过渡链路可承载 trunk_kcp 流量（下载压测通过）。

### 阶段 1：最小 TCP 状态机 ✅ 已完成

**任务：**
- [x] `stack.go`：syn-sent/syn-rcvd/established/closing/closed 五态 + 事件驱动
- [x] seq/ack 单调性、ack 不超过最高连续序号（`TestAckSemanticsWithHole`）
- [x] 构包指纹模板（Linux 选项顺序 MSS/SACK/TS/NOP/WS、TSval 递增、DF 位、随机 ISN）
- [x] 内存链路层（`link.go` memNet，支持丢包/乱序注入），单测覆盖：握手、数据、
  乱序（`TestReorder`）、丢包空洞、挥手（`TestCloseHandshake`）、保活（`TestKeepalive`）、
  握手重试（`TestHandshakeRetry`）、未知端口 RST（`TestRstToUnknownPort`）、
  握手超时（`TestDialTimeout`）

**产出：** `stack.go` + `stack_test.go` 全绿（含 `-race`）
**验收：** ✅ 单测模拟 20% 丢包下零重发（`TestNoRetransmitUnderLoss`）、ack 自洽。

### 阶段 2：集成进仓库

**任务：**
- [x] `conn.go`/`listen.go`：net.Conn/net.Listener 语义
- [ ] `conn/fauxtcp.go` 适配器 + `socks_trunk_kcp` 配置项 `mode: fauxtcp`
- [x] 以 faux_tcp 为物理连接跑通 `trunk_kcp` 全链路（`TestTrunkKCPOverFauxTCP`：
      10% 丢包下 4MB 数据经 KCP 全部恢复，19.6MB/s @ 内存链路）
- [ ] 集成测试：c/s 各起 trunk_kcp，HTTP 下载压测（待真机/有权限环境）

### 阶段 3：加固与性能

**任务：**
- [ ] netem 丢包（5%/20%）下吞吐对比：faux_tcp vs 直连 UDP vs 内核 TCP
- [ ] 校验和 offload 兼容性（硬件 checksum 下的抓包/发包验证）
- [ ] 指纹校准：与真实 Linux curl 流量并排抓包比对选项/TTL/窗口初值
- [ ] NAT/状态防火墙存活测试（空闲老化、空洞容忍度）
- [ ] 线上流量放大回归：KCP over faux_tcp 在 2% 丢包下放大率 ≤ 1.1x
  （对照 `trunk_kcp/wire_amplification_test.go` 场景）

**产出：** 性能报告 + 指纹比对记录
**验收：** 吞吐 ≥ 直连 UDP 的 90%；放大率达标。

### 阶段 4：文档与发布

- [ ] README（用法、iptables 部署、权限、回退说明、指纹说明）
- [ ] `test/socks_trunk_kcp` 增加 `fauxtcp` 模式示例配置
- [ ] CHANGELOG 条目

---

## 测试计划 / Testing

- **单测**：`go test ./faux_tcp` —— 内存链路注入收包，状态机全覆盖。
- **集成**：`go test -run FauxTCP ./faux_tcp`（root 才跑真链路，否则 Skip）。
- **线上特征 checklist（tcpdump/Wireshark 逐条核对）**：
  - [x] 握手三包、选项顺序与值逼真（`TestHandshakeAndEcho`/`TestPacketRoundTrip` 内存链路断言；真机抓包待验）
  - [x] 数据段 PSH+ACK，seq 单调（`TestHandshakeAndEcho`）
  - [x] 每个 seq 全程只出现一次（无重传）—— 丢包下亦然（`TestNoRetransmitUnderLoss` 20% 丢包断言）
  - [x] ack 不超过最高连续接收序号，无 dup ACK（`TestAckSemanticsWithHole`）
  - [x] 挥手四包完整；无异常 RST（`TestCloseHandshake`/`TestRstToUnknownPort`）
- **性能基准**：`BenchmarkThroughput`（内存链路 736MB/s，已建）；与 UDP 直连对照待真机。
- **回归**：复用 `trunk_kcp/wire_amplification_test.go` 的段计数器测放大率。

---

## 风险与缓解 / Risks

| 风险 | 缓解 |
| --- | --- |
| 内核 RST 打断伪装连接 | iptables 出站 RST 丢弃（udp2raw 验证多年的做法），README 脚本化 |
| 运营商部署状态跟踪型 DPI（校验 ack/重传一致性） | ack 语义严格自洽；预留"模拟重传"开关（检测到空洞时补发空洞长度占位数据） |
| 无 root 权限环境 | 自动回退 `udp` 模式 + 明确告警 |
| 校验和 offload 导致抓包校验和异常 | 发送时固定计算软件校验和；接收不校验（gopacket 可配） |
| NAT 表项老化 | 25s 保活段 + 应用层心跳 |
| IPv6/Windows/macOS | 本期不支持，`link_stub.go` 明确报错，接口预留 |

---

## 里程碑 / Milestones

| 里程碑 | 内容 | 预计 |
| --- | --- | --- |
| M1 | 阶段 0 PoC 完成，抓包验证 | 1~2 天 |
| M2 | 阶段 1 状态机 + 单测 | 2~3 天 |
| M3 | 阶段 2 集成 trunk_kcp 跑通 | 1~2 天 |
| M4 | 阶段 3 性能/加固达标 | 2~3 天 |
| M5 | 阶段 4 文档/示例/发布 | 1 天 |

---

## 参考资料 / References

- udp2raw（同目标成熟实现；方案 D 直接复用，方案 A 的指纹/规则参照）：
  https://github.com/wangyu-/udp2raw
- udp2raw-rs（wire 兼容的 Rust 重写，维护较活跃，方案 D 备选）：
  https://github.com/brianpht/udp2raw-rs
- gopacket（仅用纯 Go 的 layers 构包；pcap/pfring/afpacket 带 cgo 禁用）：
  https://github.com/google/gopacket
- golang.org/x/sys/unix（AF_PACKET/raw socket 纯 Go 封装）
- gVisor netstack（方案 B 备选）：https://github.com/google/gvisor
- RFC 793（TCP）、RFC 7323（TS/WS 选项）、RFC 6528（ISN 随机化）、RFC 2018（SACK）
