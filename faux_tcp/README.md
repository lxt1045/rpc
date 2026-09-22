# faux_tcp

**线路上是 TCP，行为上是 UDP**：每个 IP 报文都是协议号 6 的合法 TCP 报文
（三次握手、选项协商、PSH+ACK 数据、FIN 四次挥手），但**不做拥塞控制、
丢包重传和滑动窗口限流**——丢包不回填、不降速、不背压。

用途：规避运营商对 UDP 的 QoS/限速，同时避免 TCP 层重传与应用层 ARQ（如 KCP）
叠加造成的流量放大。可靠性由上层按需叠加：本仓库 `trunk_kcp` 的 KCP 是典型搭配
（`faux_tcp/trunkkcp_test.go` 验证了 10% 丢包下 KCP 全恢复）。

## 用法

```go
// 服务端
ln, _ := faux_tcp.Listen(ctx, faux_tcp.Config{}, "0.0.0.0:18099")
conn, _ := ln.Accept()   // 只返回完成三次握手的连接

// 客户端
conn, _ := faux_tcp.Dial(ctx, faux_tcp.Config{}, "", "1.2.3.4:18099")

// 返回的都是 net.Conn，可直接喂给：
trunk := trunk_kcp.NewTrunkKCP(conv, nil, conn1, conn2)
```

## 数据报语义（重要）

**一次 `Write` = 一个 TCP 段**，超过 MSS（默认 1448）直接报错。
本层不保序不重传，所以绝不拆分上层报文——"丢包 = 丢整条报文"，与 UDP 语义一致，
上层 KCP 才能正确重传（拆分写请求会导致流式重组永久错位）。

### MSS 与 AdvMSS：一个管"我发多大"，一个管"对端发多大"

- `MSS`（默认 1448）：本端**发送**单段上限。路径 MTU 受限时调小（如 VPN 把 MSS 夹到
  1280 → 设 1240）。它同时决定上层报文能不能塞进一段（须 ≥ KCP 线上包）。
- `AdvMSS`（默认 = `MSS`）：本端在 SYN/SYN+ACK 里**通告**的值，只影响对端发给我们的
  分段大小。

把两者分开是为了让"发送上限"能按路径 MTU 保守设置，而"通告值"保持 Linux 默认
（1460），不必跟着一起缩——对端的实际分段受上层（如 KCP 的 `kcp_mtu`）约束。
接收侧不按通告值校验（读缓冲按最大段预留，见 `Config.MSS` 注释）。

> 注：曾怀疑"通告值 ≤ 中间盒的 MSS 夹取目标会导致它不建流状态"，实测**不成立**
> （服务端 SYN+ACK 报文与链路都正常，见下节源端口的坑）。这里保留 AdvMSS 是因为
> 语义更清晰，不是因为它能修那个故障。

### 源端口必须落在本机 `ip_local_port_range` 内（否则回程被上游丢弃）

`Dial` 不指定 `laddr` 时，源端口取自 `/proc/sys/net/ipv4/ip_local_port_range`
（内核 ephemeral 范围），并避开本机已占用的端口——真实客户端的源端口就是内核从这里
选的。**不要用固定区间（例如 IANA 的 49152-65535）**：

真机案例：某客户端 `net.ipv4.ip_local_port_range = 44620 48715`，而实现里固定用
`49152 + rand%16384`。结果 SYN 正常到达服务端、服务端也回了合法 SYN+ACK（校验和、
TSecr、源地址都对），但**回程在客户端上游被静默丢弃**，客户端一个包都收不到；同一个
端口用内核 TCP（源端口 46650，落在范围内）一切正常。判据：

```bash
sysctl net.ipv4.ip_local_port_range                    # 本机 ephemeral 范围
sudo tcpdump -ni eth1 'tcp port <服务端端口>'          # 看自己 SYN 的源端口是否在范围外
```

修复即"按范围挑端口"，见 `TestPickLocalPort`。

KCP 搭配说明：`trunk_kcp` 的 KCP MTU 1400（线上包 1400B）≤ faux_tcp MSS 1448，
天然兼容，无需配置。

## 部署（Linux，需要 root/CAP_NET_RAW）

发送走 raw IP socket（`IPPROTO_RAW`），接收走 `AF_PACKET` 旁路内核协议栈，
并附 cBPF 只收目的端口匹配的 TCP 报文（无关流量不进用户态）。

收包 socket **不绑定单张网卡**：多网卡/策略路由主机上"去程 eth1、回程 eth0"
（非对称路由）很常见，绑定源 IP 所在网卡会一个回包都收不到；不绑定后所有网卡的
报文都进来，再由 cBPF 按目的端口过滤。`Config.DebugPackets` 可临时关掉 cBPF、
改为用户态过滤并统计 `rx_total/rx_match/rx_dropped`（随握手失败信息打印），
用于区分"网卡侧没收到包"与"收到但被过滤"。

收包 socket 用 **cooked 模式（`SOCK_DGRAM`）**：由内核统一剥掉链路层头，用户态
始终拿到 IPv4 报文——这样可以同时支持以太网（14B 头）、`lo`（伪以太网头）以及
tun/wireguard/ppp 等**没有以太网头**的点对点接口。早期用 `SOCK_RAW` 时，非以太网
接口上收到的包会因按以太网偏移解析而全部被丢弃，表现为"本机一个包都收不到、
握手超时"（`telnet` 走内核 TCP 不受影响），错误信息里的 `iface=` 字段可用于确认
实际使用的网卡。

内核看到不属于任何 socket 的 TCP 段会回 RST，必须抑制。**默认自动处理**（规则装在
**raw 表**，即 conntrack 之前）：

> 为什么必须是 raw 表：若只在 filter 表丢弃，RST 虽被丢掉（客户端看不到），
> 但 **conntrack 已经先把它记进连接状态**，该流被标成 RST/CLOSED；之后本进程
> 用户态发出的 SYN+ACK 在 conntrack 眼里是 INVALID，NAT（宿主机自身或云平台）
> 不会为它做地址转换 —— 客户端表现为"服务端发了 SYN+ACK 却一个包都收不到"，
> 而同端口的内核 TCP 一切正常（内核不会回 RST）。真机踩过，见
> `TestRSTDropRuleUsesRawTable`。


`Listen`/`Dial` 时自动安装本端口的 OUTPUT 链 RST DROP 规则（iptables 或 nft，
幂等，Close 时卸载；已存在的同名规则视为运维手工配置，不重复安装也不卸载）。
想完全自己维护防火墙时置 `ManualFirewall=true`，手工命令：

```bash
# 以监听端口 18099 为例（两端都要，客户端端口随机则按对端端口过滤）
# 装在 raw 表（conntrack 之前）：避免内核 RST 污染连接跟踪状态
sudo iptables -t raw -A OUTPUT -p tcp --dport 18099 --tcp-flags RST RST -j DROP
sudo iptables -t raw -A OUTPUT -p tcp --sport 18099 --tcp-flags RST RST -j DROP
# 若旧版本装过 filter 表规则，清掉它（留着仍会污染 conntrack）
sudo iptables -D OUTPUT -p tcp --sport 18099 --tcp-flags RST RST -j DROP
```

无 root 权限时 `Listen`/`Dial` 返回明确错误（fail-fast，不静默降级）。

### 出包源地址：取"收到 SYN 的那个本地地址"，**不要伪造**

回包源 IP 没有配置项，固定取**触发报文的目的 IP**——服务端就是 SYN 到达的那个本地
地址。这是唯一正确的选择：客户端就是往这个地址发的，回程必须用同一地址才符合它的
期望。云主机（腾讯云 VPC/轻量、阿里云 ECS 等）上它是**网卡内网地址**，公网 IP 由
平台 1:1 NAT 提供，平台正常 SNAT。

**不要试图把源地址改写成公网 IP**：这类平台对虚拟网卡做出站**源地址校验**（反欺骗），
源 IP 不是网卡地址的报文会被静默丢弃。真机上为此加过 `reply_src` 开关，结果是"内核
TCP 同端口能通、faux_tcp 握手超时、客户端一个包都收不到"——后来证明它只是让故障更
难查，已删除。判据（服务端一条命令）：

```bash
# 服务端 tcpdump：SYN+ACK 的源地址
sudo tcpdump -vni any 'tcp port 18099'
#   源地址=公网 IP（网卡是内网）→ 有人在伪造源地址（或历史配置），必须改回内网地址
#   源地址=网卡内网地址而客户端仍收不到 → 按 test/socks_faux_trunk_kcp/README.md
#   的「排查决策树」继续（源端口范围 / RST 抑制 / 链路）
```

容器/端口映射场景（Docker 桥接、K8s NodePort）**优先用 `--network host`**（等价于
直接用宿主机网卡，没有 DNAT，回包源地址天然正确）；不要靠伪造源地址绕过平台转换。

## 验证

```bash
go test ./faux_tcp/            # 内存链路全量单测（无需 root）
go test -race ./faux_tcp/      # 竞态检测
# 真实链路（需 root，RST 抑制自动安装）：
FAUXTCP_E2E=1 sudo -E go test -run TestFauxTCPLoopback -v ./faux_tcp/
# 抓包核对线上特征：
sudo tcpdump -i lo -nn 'tcp port 18099' -w faux.pcap
# 应看到：SYN/SYN+ACK/ACK 握手、PSH+ACK 数据、delayed ACK、FIN 四次挥手、
# 每个 seq 只出现一次；注入丢包时可见 dup ACK/SACK 与 ack 跳变恢复
```

## 丢包 × 带宽实测（内存链路，趋势参考）

`go test -run '^TestLossMatrix$' -v ./faux_tcp/`（4MB KCP-over-faux + 500 包裸发）：

| 丢包率 | 裸 faux_tcp 交付率 | KCP 有效吞吐 | KCP 线上放大率 | KCP 重传段占比 |
|---|---|---|---|---|
| 0% | 100.0% | 32.2 MB/s | 1.037 | 0.0% |
| 1% | 99.0% | 17.3 MB/s | 1.049 | 0.7% |
| 5% | 95.0% | 13.3 MB/s | 1.135 | 7.3% |
| 10% | 90.0% | 9.7 MB/s | 1.270 | 17.6% |
| 20% | 80.0% | 7.8 MB/s | 1.579 | 33.4% |
| 30% | 70.0% | 4.9 MB/s | 2.185 | 49.8% |

裸 faux_tcp 无损吞吐基准 ~1208 MB/s（`BenchmarkThroughput`，内存链路，CPU 上限）。
吞吐绝对值无代表性（无 NIC/RTT），看趋势：交付率恒等于 100%−丢包率（数据报语义），
放大率 ≈ 1/(1−loss) + 少量协议开销，全部由真实丢包驱动、无伪重传风暴。

## 设计要点

- **零重传**：发送即忘；握手 SYN 允许有限重试（真实 TCP 行为，DPI 可见）。
- **零拥塞控制/零滑窗**：窗口通告固定 65535+WS=7；ack 字段照填但不据此做任何事。
- **丢包外观完整闭环**：空洞段照常投递上层，同时回 dup ACK + SACK（仿真实
  Linux 接收端）；空洞逾 `HealDelay`（默认 200ms，≈一个重传时延）未愈则执行
  "虚拟重传愈合"——累计确认直接越过空洞跳变。线上呈现"丢包 → dup ACK/SACK →
  快速恢复"的完整过程，ack 永不冻结（冻结对 DPI 是显著异常）。
- **delayed ACK**：正序数据每 2 包回一个 ACK，尾巴 40ms 冲刷（精确定时器），
  ACK 流量与真实 Linux 一致（约 1:2）。
- **指纹**：SYN 选项布局仿 Linux（MSS,SACK,TS,NOP,WS）；数据段 NOP,NOP,TS；
  DF 位；ISN 随机；IP ID 按流递增；TTL 64。
- **保活**：空闲 25s 发标准 TCP keepalive 探测包（seq=sndNxt-1 纯 ACK），
  对端按 RFC 规则应答；连续 3 个周期无任何入站报文判定对端死亡并关闭。
- **挥手**：完整四次挥手 + 半关闭（对端 FIN 后本端读到 EOF 仍可写）；
  FIN 不重传，宽限期（默认 500ms）后强制关闭，卡死的关闭流程由周期扫描回收。
- **会话卫生**：Accept 只返回完成三次握手的连接；半开连接（SYN 扫描）
  超时自动回收；客户端最后一条连接关闭后链路/防火墙规则自动释放。
- **异步出站队列**：构包在调用方 goroutine 内完成（保证 seq/IP ID 顺序），发出交给
  每连接 writeLoop 协程——状态机/读循环绝不阻塞在链路写系统调用上（慢链路不会拖垮
  ACK 与挥手）。数据段入队可等待（本地背压 ~1024 包，受 `SetWriteDeadline` 约束，
  超时返回 `ErrWriteTimeout`）；控制段（ACK/FIN/探测）队列满时同步兜底直发，不丢。
- **错误分类**：自定义错误统一在 `errno.go`（501000 段，带 error code），
  调用方用 `errors.AsCode(err).Code()` 与 `ErrHandshakeTimeout` / `ErrConnReset` /
  `ErrPeerTimeout` / `ErrPacketTooBig` 等模板比较；对端正常关闭后 `Read` 返回
  标准 `io.EOF`，权限/地址/防火墙问题各有独立错误码。

详细设计与阶段计划见同目录 `plan.md`。
