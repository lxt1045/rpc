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

KCP 搭配说明：`trunk_kcp` 的 KCP MTU 1400（线上包 1400B）≤ faux_tcp MSS 1448，
天然兼容，无需配置。

## 部署（Linux，需要 root/CAP_NET_RAW）

发送走 raw IP socket（`IPPROTO_RAW`），接收走 `AF_PACKET` 旁路内核协议栈，
并附 cBPF 只收目的端口匹配的 TCP 帧（无关流量不进用户态；注意 802.1Q VLAN
帧会被滤掉，不适用于 trunk 链路）。

内核看到不属于任何 socket 的 TCP 段会回 RST，必须抑制。**默认自动处理**：
`Listen`/`Dial` 时自动安装本端口的 OUTPUT 链 RST DROP 规则（iptables 或 nft，
幂等，Close 时卸载；已存在的同名规则视为运维手工配置，不重复安装也不卸载）。
想完全自己维护防火墙时置 `ManualFirewall=true`，手工命令：

```bash
# 以监听端口 18099 为例（两端都要，客户端端口随机则按对端端口过滤）
sudo iptables -A OUTPUT -p tcp --dport 18099 --tcp-flags RST RST -j DROP
sudo iptables -A OUTPUT -p tcp --sport 18099 --tcp-flags RST RST -j DROP
```

无 root 权限时 `Listen`/`Dial` 返回明确错误（fail-fast，不静默降级）。

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
