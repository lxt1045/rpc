# Changelog - socks_faux_trunk_kcp

## 2026-09-23 - KCP 窗口：从"写死 1024"到"按 BDP 设 / 可自动调"（含一次回退）

**真机两次实测**（服务端出口 30Mbps）：

| 配置 | 现象 |
| --- | --- |
| 库默认窗口 1024 段（≈1.2MB ≈ 8×BDP） | 出口 30Mbps 打满，但下载只有 1.0~1.1 MB/s（放大 ~3.4x）；换成 faux_tcp 承载几乎无改善 → 瓶颈在两者共用的 KCP 窗口 |
| 发送+接收窗口都设成 128 段 | **更差**：出口只占 7Mbps、下载 300kB/s —— 窗口成了唯一限速手段，小于 BDP 就把发送端饿死 |

**自动复现**（新增 `trunk_kcp/ratelimit_test.go`：内存链路 + 共享令牌桶整形器 + 单向延迟，
4 条物理连接，量 goodput/线上放大/重传率/瓶颈丢包/链路利用）：24Mbps、RTT 40ms、
排队上限 50ms 下扫窗口，结论与真机一致——**放大率主因是窗口 ≫ BDP**：

| 发送窗口 | 线上放大 | 重传 | 瓶颈丢包 | goodput | 链路利用 |
| --- | --- | --- | --- | --- | --- |
| 1024（库默认） | 3.2~3.9x | 68% | 62~65% | 2.3 MB/s | 101% |
| 256 | 1.4~1.7x | 25~34% | 12~19% | 2.3 MB/s | 94~101% |
| 128（≈BDP） | 1.17~1.26x | 11~17% | ~0% | 2.4~2.6 MB/s | 98~101% |
| 32（<BDP） | 1.05x | 2~6% | 0% | 1.0 MB/s | 32~36% |
| 1024 + `nc=0` | 1.05x | 2% | 0% | 0.2 MB/s | 5~8% |

### 变更

- `trunk_kcp`：新增 `SetWindowSize(snd,rcv)`（并在 README 给出 `sndwnd ≈ 速率×RTT/(mtu-24)`
  的取法与全部实测数字）；新增 `Stats()`
  （SndWnd/RcvWnd/Backlog/Push/Acked/Retrans/RetransPct/WireBytes/SRTT/Amp）与每 3s 一行
  debug 自调窗日志，运维可直接看到"窗口该多大"的依据。
- `trunk_kcp`：`VirtualConn.Write` 增加**发送队列背压**（kcp-go 的 `Send` 不限制队列长度，
  不限流会让发送端无限缓冲、应用看不到真实速率）。
- `trunk_kcp`：新增实验性 `SetAutoWindow(maxWnd,rcvWnd)`：按线上放大率做 AIMD 调窗
  （放大 ≤1.5 加性增、>1.5 乘性减），40ms 无损限速链路上实测稳定收敛；长 RTT/有损链路
  仍会振荡，**默认关闭**，生产建议先用固定窗口按 BDP 设。
- 修正一处统计错误（也复现/钉死在 `TestStatsAckAccounting`）：KCP 一次 flush 会把多个段
  拼在一个缓冲里、ACK 也是合并发送的（实测 448 段只回 8 个 ACK），因此
  "已发送/重传/已确认"必须按段遍历、且"已送达"要用 ACK 里的累计确认 `una`；
  否则放大率会虚高几十倍并让自动调窗一路收缩（这正是第二次真机故障的放大器）。
- 示例配置：新增 `kcp_sndwnd / kcp_rcvwnd / kcp_auto_wnd`（**默认都不覆盖库值**，
  保持既有行为），yml 注释给出公式、两次真机故障的数字与自查方法。
- 文档：`trunk_kcp/README.md` 与本示例 README 的「KCP 参数」重写为"窗口按 BDP 设、
  接收窗口是缓冲、`nc=0` 不是解、长 RTT 用 `nodelay=0`"。

## 2026-09-23 - 真机吞吐没有再改善的根因：KCP 窗口 ≫ BDP（限速出口被重传打满）

**真机现象**：`socks_faux_trunk_kcp` 与 `socks_trunk_kcp` 打满服务端 30 Mbps 时都只有
约 1.0~1.1 MB/s（≈3.4x 线上放大），换成 faux_tcp 承载几乎没有改善——说明浪费的主因不在
承载层，而在两者共用的 `trunk_kcp`。

**复现（`trunk_kcp/ratelimit_test.go`，自动测试，无需真机）**：内存链路 + 共享令牌桶
整形器（速率 + 排队上限）+ 单向延迟，4 条物理连接，量 goodput / 线上放大 / 重传段占比。
24 Mbps、RTT 40 ms、排队上限 50 ms 时扫窗口：

| 窗口（段） | 线上放大 | 重传段占比 | 瓶颈丢包 | 有效吞吐 | 链路利用 |
| --- | --- | --- | --- | --- | --- |
| 1024（库默认，≈8×BDP） | **3.23~3.84x** | **68%** | 62~65% | 2.3 MB/s | 101% |
| 512 | 2.24x | 50% | 43% | 2.4 MB/s | 101% |
| 256 | 1.58x | 32% | 17% | 2.2 MB/s | 94% |
| **128（≈BDP）** | **1.17~1.26x** | 11~17% | ~0% | **2.4~2.6 MB/s** | **98~101%** |
| 64 | 1.12x | 9% | 0% | 1.7~1.9 MB/s | 65~71% |
| 32 | 1.07x | 4% | 0% | 0.9 MB/s | 32~34% |
| 1024 + `nc=0`（开拥塞控制） | 1.05x | 2% | 0% | 0.1~0.2 MB/s | **5~8%** |

结论：**KCP 窗口就是"在途数据上限"，窗口 ≫ BDP 会把瓶颈队列灌爆 → 丢包 → RTO 重传 →
带宽全变重传；窗口 < BDP 又浪费链路**。kcp-go 自带的拥塞控制（`nc=0`）爬升太慢，在限速
链路上只用到 5~8%，所以不是解。

### 变更

- `trunk_kcp`：新增 `SetWindowSize(sndwnd, rcvwnd)`（薄封装 `kcp.WndSize`，带锁），
  文档给出取值公式 `sndwnd ≈ 速率(B/s) × RTT(s) / (mtu-24)` 与上面的实测表。
- 示例配置新增 `kcp_sndwnd / kcp_rcvwnd`（示例默认 **128/128**，即 30Mbps×40ms 的 BDP
  量级；`NormalizeTrunkKCPConfig` 也会补齐该默认值，老配置即使没写也能生效）。
- 新增 `trunk_kcp/ratelimit_test.go`：把"限速瓶颈 + 真实 RTT"固化成自动测试，断言
  默认窗口必然被压出放大（>2x、重传>30%）、BDP 窗口放大 <1.5x 且链路利用 >90%、
  过小窗口浪费链路——防止以后有人把窗口调回 1024。
- 新增 `TestKCPWindowDefaults`（示例）钉住窗口默认值；`socks_trunk_kcp` 同步新增
  `kcp_sndwnd/kcp_rcvwnd`（默认 100，1376B/段）与同一段说明，保证两者对比公平。
- README（`trunk_kcp`、本示例、`socks_trunk_kcp`）改为"窗口按 BDP 设"的指导，
  更正此前"用库默认快速模式即可"的说法。

## 2026-09-23 - 清理排查期间加入的试验性代码（经验保留在 README）

根因确定后，把排查过程中"为了验证假设"加进去、且已被证伪的开关删掉，避免它们继续
把后来者带偏。保留的都是**有独立价值**的诊断/能力（下面「保留」一节）。

### 删除

- `faux_tcp.Config.ReplySrc` + 示例 `faux_tcp.reply_src` + 示例启动日志里的 `reply_src`
  一行：为"平台只为内核跟踪的流做回程 SNAT"这个后来被证伪的猜想加的（把出包源 IP
  统一改写成对外地址）。真机上它只会被云平台源地址校验丢掉，并把排查方向带偏。
  **回包源地址固定取"收到 SYN 的那个本地地址"**，无需配置；容器场景用 `--network host`。
- `faux_tcp.Listen` 启动时对 `ReplySrc` 的本机地址校验与 WARN、`isLocalAddr` 及其单测
  （随 ReplySrc 一起删除）。
- SYN debug 日志里的 `回包源=<ip>` 字段（源地址恒等于 SYN 的目的地址，日志里的
  `%s -> %s` 已包含该信息）。
- `debug_packets` 的 `tx_seen` 计数与 `PACKET_OUTGOING` 判断：协议限定的 AF_PACKET
  socket 本来就收不到本端出向报文，该计数恒为 0，属噪声。
- 相应测试 `TestReplySrc` / `TestIsLocalAddr`（`TestReplySrcDefaultUsesPacketDst`
  改名为 `TestReplySourceIsPacketDst` 保留，钉住"源地址=报文目的 IP"这一正确行为）。

### 保留（有独立价值，不是一次性调试代码）

- `faux_tcp.debug_packets`：不挂 cBPF + 用户态过滤，`rx_total/rx_match/rx_dropped`
  随握手失败信息打印——这是区分"网卡侧没收到包"与"收到但不匹配"的关键手段（本次靠
  `rx_match=0` 排除了本端收包侧）。
- 握手超时的自诊断信息（已发/收到/发送失败次数、最后发送错误、`iface=... cooked(...)`、
  可执行的分支提示）与 `LinkDescriber`。
- 服务端 `收到 SYN ... 对端通告 MSS=` 日志：一眼判断路径上有没有 MSS-clamp 中间盒。
- RST 抑制规则装在 **raw 表**（conntrack 之前）+ `TestRSTDropRuleUsesRawTable`。
- 源端口按 `ip_local_port_range` 选择 + `TestPickLocalPort`（本次根因修复）。
- `adv_mss`（发送上限与通告值分离）、`window` 可选项（默认仍 65535）。

## 2026-09-23 - 根因：源端口落在本机 ephemeral 范围之外，回程 SYN+ACK 被上游静默丢弃

**决定性 A/B**（同一台客户端、同一个公网端口、同一时间段）：

| 客户端 | 源端口 | 结果 |
| --- | --- | --- |
| `telnet`（内核 TCP，连**我们的 faux 服务端**） | 46650（本机 `ip_local_port_range=44620-48715` 范围内） | 客户端抓到入向 `[S.] mss 1240 win 65535`（正是 faux 发的包）并 `Connected` ✅ |
| `go run ./`（faux 客户端） | 固定 `49152 + rand%16384`，**全部 ≥ 49152**（范围外） | SYN 到达服务端、服务端回了合法 SYN+ACK，客户端一个包收不到（`rx_match=0`） ❌ |

**根因**：客户端上游设备/运营商只放行"源端口落在本机 ephemeral 范围内"的 TCP 会话的
回程报文。faux_tcp 的 `Dial` 此前硬编码用 IANA 高端口区间（49152-65535），与内核选的
范围（`/proc/sys/net/ipv4/ip_local_port_range`）完全不重叠，于是 SYN 一路正常、回程
SYN+ACK 被丢——而同一个端口的内核 TCP 一切正常。这也解释了为什么 `socks_trunk_kcp`
（内核 TCP）一直能用。

排查中排除掉的假设（都无解释力，仅保留相应配置能力）：

- 服务端源地址：`reply_src` 填公网 IP 被云平台源地址校验丢弃（腾讯云 1:1 NAT 实测），
  留空后源地址与内核一致（`10.8.0.2`）**仍然失败** → 不是本故障的原因；
- MSS 通告值：客户端 SYN 从 `mss 1240` 改到 1460 后仍失败 → 不成立；
- 窗口大小：客户端 SYN 改成内核典型值 `win 64240` 后仍失败 → 不成立；
- 服务端 wire 格式/校验和/conntrack：内核客户端连 faux 服务端能通 → 服务端完全正常。

### 变更

- `faux_tcp`：`Dial` 的源端口改为**读本机 `ip_local_port_range`**（读不到时回退
  32768-60999）并避开已占用端口；新增 `TestPickLocalPort`。
- `faux_tcp`：服务端 `收到 SYN` 日志增加 `对端通告 MSS=`（可直接判断路径上是否有
  MSS-clamp 中间盒，无需抓包）。
- `faux_tcp.Config.AdvMSS`（+ 示例 `faux_tcp.adv_mss`）：发送单段上限与通告值分离
  （语义更清晰；**不修上面那个故障**）。示例新增 `faux_tcp.window` 可选项，默认配置
  里给了 `window: 64240`。
- `debug_packets` 的 `rx_total/rx_match/rx_dropped` 只统计入向（协议限定的 AF_PACKET
  socket 本来就收不到自己的出向报文；当时另加的 `tx_seen` 计数已在清理中删除）。
- README：新增「跨机部署 → 排查决策树」（六步，每步带命令与判据）、
  「真机复盘：源端口不在本机 ephemeral 范围」（证据表 + 假设-判据排除表 + 三条教训），
  并修正了两处旧说法：`telnet` 连正在运行的 faux 服务端**会**显示 `Connected`（raw 栈
  本身就是完整的握手应答方，判据是"telnet 能连、faux 客户端连不上"这个差异）；
  `rx_total=0` 不再被解释成"本机收包通道异常"。

## 2026-09-23 - 真机定位：`reply_src` 填公网 IP 被云平台源地址校验丢弃（结论修正）

> 注：本条涉及的 `reply_src` 开关（以及 `回包源=` 日志、`tx_seen` 计数）已在后续
> 「清理排查期间加入的试验性代码」条目中**删除**；"不要伪造出包源地址"的结论保留。

**修正上一节的真机结论**：那份结论（"平台只为内核跟踪的流做回程 SNAT，所以要把
`reply_src` 写成公网 IP"）是**在 Docker 端口映射环境**下成立的，推广到"云主机 +
VPC 1:1 NAT"是错的，而且正好把故障从"回程不通"变成"被平台丢包"。

本次真机证据（腾讯云轻量，网卡 `eth0 10.8.0.2`，公网 `43.155.182.37` 由 VPC NAT）：

- 客户端 `tcpdump`：只有 `Out [S]`，无任何入向包；
- 服务端 `tcpdump`：`In [S]`（222.209.83.111 → 10.8.0.2:18099）与
  `Out [S.]`（**源地址 43.155.182.37**，校验和正确、`TSecr` 正确回显）都正常；
- 服务端 `cat /proc/net/nf_conntrack | grep 18099` 为空 → 本机 conntrack 根本没有
  该流（raw socket 流不建 conntrack），此前"RST 污染 conntrack"的推断在此环境不成立；
- A/B：内核 TCP 监听同端口（`SOCKS_FAUX_PLAIN_LISTEN=1`）客户端 `telnet` 秒连——
  内核回包源地址是 `10.8.0.2`，平台正常 SNAT 成公网；faux_tcp 写死公网源地址后
  不匹配任何 NAT 映射，被平台反欺骗静默丢弃。

### 变更

- 示例默认配置 `faux_tcp.reply_src` 改为 **留空**（云主机正确行为），注释写明
  仅无状态 DNAT（Docker 桥接/K8s NodePort）才填对外地址，并记录判据；
- `faux_tcp.Listen` 启动时校验 `ReplySrc` 是否为本机网卡地址：**不是本机地址则打
  WARN**，说明"云平台源地址校验会丢包"与"无状态 DNAT 需要它"两种判读；
- `faux_tcp` 的 SYN debug 日志增加 `回包源=<ip>`，一眼看出回包会用什么源地址；
- README（`faux_tcp` 与本示例）改为按"服务端 SYN+ACK 源地址"判读的表格式排查流程，
  新增「云主机 1:1 NAT 特有的静默丢包」小节；
- 新增测试：`TestReplySrcDefaultUsesPacketDst`（留空时源地址=报文目的 IP）、
  `TestIsLocalAddr`（本机/非本机地址判据）。

## 2026-09-23 - 路径 MTU 受限支持（KCP MTU 可配 + 对端 MSS 告警）

真机发现客户端出口有设备**改写 MSS（1448 → 1280）**，意味着即使握手成功，KCP 的
1400B 线上包也会因超过路径 MTU 被丢弃（faux_tcp 报文带 DF，不分片）。

- `trunk_kcp`：新增 `SetMtu(mtu)`（此前硬编码 `KcpMtu=1400`），收包长度校验同步改用
  配置值；示例新增 `trunk_kcp.kcp_mtu` 配置项。
- `faux_tcp`：解析对端 SYN/SYN+ACK 的 MSS 选项并在握手时记录；若小于本端 `cfg.MSS`
  打印 WARN（提示同时调小 `faux_tcp.mss` 与 `kcp_mtu`）；新增 `Conn.PeerMSS()`。
- 示例 `ValidateMSS` 改为按配置的 `kcp_mtu` 校验，错误信息给出路径 MTU 受限时的调法。
- README/CHANGELOG 记录判定方法与调参组合（mss ≤ 路径MTU−40，kcp_mtu ≤ mss）。


### 真机结论

A/B 对照（同一容器、同一端口）：内核 TCP 监听（`SOCKS_FAUX_PLAIN_LISTEN=1`）客户端
`telnet` 直接 `Connected`，而 faux_tcp 在同一环境下 `收到 SYN` 正常、SYN+ACK 也发出，
客户端却一个包收不到。即：**端口映射/安全组/回程链路都好，只有 raw socket 的回包
没有被平台做回程 SNAT**（Docker DNAT 只为内核跟踪的流转换）。

### 新增

- `faux_tcp.Config.ReplySrc`（+ 示例配置 `faux_tcp.reply_src`）：本端出包统一使用该
  源 IP，绕过平台回程转换（等价于本端自己做 SNAT，适用于 Docker 桥接/K8s NodePort
  等无状态 DNAT 环境；留空为默认行为）。`TestReplySrc` 钉住 SYN+ACK 源地址改写。
  **（已删除：真机证明云平台会按源地址校验丢弃伪造源地址，且它把排查带偏；容器场景
  改用 `--network host`。）**
- README：在「Docker / 容器部署」中给出该开关的适用判据与用法（更干净方案是
  `--network host`）。


### 真机发现

服务端 tcpdump 显示：客户端 SYN 到达容器、我们**回了 SYN+ACK**（`mss 1448`/`win 65535`
/毫秒时钟 TS val 均为本实现特征），但客户端"收到 0 个报文"——问题在**回程**，不在
握手实现；同时发现 SYN+ACK 的 `TSecr` 恒为 0。

### 修复

- `faux_tcp/packet.go`：`tcpOptionsSyn` 增加 `tsecr` 参数——**SYN+ACK 回显对端 SYN 的
  TSval**（真实 Linux 行为；此前硬编码 0，部分状态化 NAT/防火墙会判为无效丢弃）。
  `TestSynAckEchoesTimestamp` 钉住。
- 服务端 main 新增**内核 TCP 对照模式**：`SOCKS_FAUX_PLAIN_LISTEN=1` 时用 `net.Listen`
  监听同一端口并回显，用于在无法修改 docker 启动参数的容器里做 A/B，判定
  "端口映射/回程" 是否通（README 有判读表）。


- 配置加载改为**磁盘优先**：`LoadConfig()` 先读 `static/conf/default.yml`，读不到再回退
  编译期嵌入的默认值（`configfile.go`），启动日志打印 `config loaded conf=file:|embedded:`。
  容器里挂载配置即可生效，不必重建镜像。
- README 新增「Docker / 容器部署」：`--network host`（推荐）或
  `-p 18099:18099 --userland-proxy=false`、`--cap-add=NET_RAW/NET_ADMIN`，
  以及"云控制台安全组必须放行入站 TCP 18099"的提示与宿主机/容器内 tcpdump 排查表。

## 2026-09-22 - 修复跨机（非本机）拨号失败：收包改 cooked 模式

### 现象

`client-conn.addr` 指向本机正常，指向公网服务端（`43.155.182.37:18099`，`telnet`
可连通）时报握手超时，且诊断显示 **`收到 0 个报文`、`发送失败 0 次`** —— SYN 发出
去了，但本端一个回包都没看到。

### 根因

`faux_tcp` 收包用 `AF_PACKET/SOCK_RAW`，用户态拿到的是**含链路层头**的帧，代码按
"以太网 14 字节头"解析（cBPF 也按以太网偏移过滤）。**tun / wireguard / ppp 等
点对点接口没有以太网头**（VPN、部分 WSL2/容器网络场景），于是所有回包都在 cBPF
或解析阶段被丢弃 → 表现为"0 收包 + 握手超时"。`telnet` 走内核 TCP，不经过该路径，
因此完全正常。cBPF 是内核态过滤，tcpdump 反而能看到包，这也是现场容易误判的原因。

### 修复

- `faux_tcp/listen.go`：收到 SYN 时打 debug 日志（`faux_tcp: 收到 SYN a -> b`）——
  服务端不创建内核监听（`ss` 看不到端口），这条日志是判断"SYN 是否真的到达服务端"
  的直接依据；`stack.go` 发包失败前 3 次也打 debug 日志（服务端"回不出 SYNACK"
  可直接从日志看到）。

- `faux_tcp/link_linux.go`：**收包不再绑定单张网卡**（多网卡/策略路由下
  "去程 eth1、回程 eth0"的非对称路由会导致绑定型 socket 一个回包都收不到），
  统一靠 cBPF 按目的端口过滤；`iface=` 仅作展示。
- `faux_tcp/link_linux.go`：新增 `Config.DebugPackets` 收包调试——不挂 cBPF、
  用户态过滤并统计 `rx_total/rx_match/rx_dropped`（随握手失败信息打印），
  可自行区分"网卡侧没收到包"与"收到但被过滤"（连自己发出的 SYN 都应计入
  `rx_total`，因此 `rx_total=0` 直接指向本机收包通道异常）。
- `faux_tcp/link_linux.go`：收包 socket 改为 **cooked `AF_PACKET/SOCK_DGRAM`**，
  由内核统一剥掉链路层头（以太网 14B / lo 伪以太网 / tun 无头），用户态始终拿到
  IPv4 报文；cBPF 偏移改为 IP 头内偏移（`proto @9`、`dst port @IHL+2`）。
- 删除只适用于以太网的 `stripEthernet`。
- `faux_tcp/bpf_test.go`（新增）：用 `x/net/bpf` 用户态 VM 校验过滤器偏移，
  覆盖端口匹配/不匹配、非 TCP、以及 **IHL=6（带 IP 选项）** 用例。
- 诊断增强：`LinkDescriber`（错误信息追加 `iface=... cooked(...)`）、握手超时提示
  增加"用 tcpdump 对比定位"与"本端收包侧"分支。
- README（两个）补充判定流程：tcpdump 与收包同挂钩点，可区分"回包到了但被过滤"
  与"根本没回包"；并说明 `telnet` 若显示 `Connected` 说明端口被内核服务占用
  （faux_tcp 服务端不创建内核监听），需在服务端 `ss -ltnp | grep 18099` 确认。

## 2026-09-22 - 跨机拨号诊断 + 握手超时配置化

### 背景

`client-conn.addr` 指向本机时正常，指向公网服务端时报 `faux_tcp dial ...: 501003,
fauxtcp: 握手超时`，错误信息无法区分原因（对端没运行 / 云安全组未放行 /
本机 raw socket 发不出去 / 服务端缺 RST 抑制）。

### 改进

- `default.yml`：`handshake_timeout_ms` 显式配置为 **3000**（跨公网/高 RTT 链路），
  并注明总预算 = 单次超时 ×(`handshake_retries`+1)。
- `faux_tcp`：发包路径不再静默吞掉 `WritePacket` 错误——新增 `SendErrors` 计数与
  最近一次发送错误；握手超时错误改为自诊断信息：
  `握手超时：无 SYN+ACK；已发 N 个报文，收到 M 个报文，发送失败 E 次；<可操作提示>`，
  并按 `E>0` / `M==0` / `M>0` 三种情况给出对应排查方向（本机发包失败 / 对端无回包 /
  对端缺 RST 抑制）。
- 客户端拨号 ctx 超时改为按握手预算推导（`HandshakeBudget()+2s`，下限 5s），
  避免 ctx 先超时把诊断信息吃掉（此前固定 5s，配置 3s×3 次重试时会提前取消）。
- 拨号失败提示与 README 补充「跨机部署」排查表（安全组放行入站 TCP、
  两端 root、两端 RST 抑制、WSL2/容器 NAT 注意事项、tcpdump 定位命令）。

### 测试

- `faux_tcp/TestHandshakeTimeoutDiagnostics`：静默对端 → 报「收到 0 个报文」；
  发包失败链路 → 暴露底层错误与发送失败计数。
- `socks_faux_trunk_kcp/TestHandshakeBudget`：预算推导（默认 4s；3s 配置 12s）。

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
