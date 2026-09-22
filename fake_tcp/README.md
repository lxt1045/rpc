# fake_tcp

伪装 TCP 传输模块：线路上是完整的 TCP 报文特征（三次握手/选项协商/PSH+ACK/FIN 四次挥手），
但**不做拥塞控制、丢包重传、滑动窗口限流**。设计与实施计划见 [plan.md](plan.md)，
与备选实现 `faux_tcp` 的评审对比及合并决议见 plan.md "v2 修订"一节。

- **RawTCP（唯一模式，v2 起）**：Linux + root/CAP_NET_RAW，AF_PACKET + cBPF 收包、
  raw IP socket(IPPROTO_RAW) 发包，内核协议栈完全旁路；
  内核 RST 抑制规则自动装拆（iptables/nft，`AutoFirewall` 可关）。
- **无权限环境**：本包不提供 UDP 兜底（v2 已删除，理由见 plan.md v2），
  请使用外挂 udp2raw 方案（[deploy/udp2raw/README.md](deploy/udp2raw/README.md)）。
- 产出标准 `net.Conn` / `net.Listener`，可直接喂给 `trunk_kcp.NewTrunkKCP` /
  `rpc.NewPeer`。

## 快速上手

```go
// 服务端
ln, _ := fake_tcp.Listen(ctx, fake_tcp.Config{LocalAddr: "0.0.0.0:8443"})
conn, _ := ln.Accept() // net.Conn

// 客户端（本地端口缺省从临时端口段 49152~65535 随机选取）
conn, _ := fake_tcp.Dial(ctx, fake_tcp.Config{RemoteAddr: "1.2.3.4:8443"})
```

### 叠加 trunk_kcp（典型生产形态）

```go
cfg := fake_tcp.Config{
    LocalAddr: "0.0.0.0:8443",
    MTU:         1500,  // MaxPayload=1436 ≥ KCP 段 1400（帧对齐）
    DatagramOnly: true, // 一次 Write = 一个 TCP 段，杜绝半帧错位
}
```

`DatagramOnly` 下超过 MaxPayload 的 `Write` 直接返回 `ErrPacketTooBig`（由上层分段）。
**这是硬性前提**：trunk_kcp 按字节流解析 KCP 长度字段，流式拆分后任一片段丢失
都会让重组永久错位（`invalid KCP segment length` → 物理连接被剔除）。
RawTCP 模式要求 `MTU ≥ 1464`（MaxPayload = MTU−64 ≥ KCP 段 1400），
或把 trunk_kcp 的 KCP MTU 调小。

## 与 trunk_kcp 组合：丢包率 → 带宽利用率实测

`fake_tcp` 层不保证可靠性（这是特性：不重传、不降速）；需要可靠传输时在上层叠
`trunk_kcp`（KCP 应用层重传/排序）。测试 `TestTrunkKCPLossBench` 量化了两者的关系：

```
trunk_kcp.VirtualConn（可靠字节流）
        │
fake_tcp.Conn（伪装 TCP 外观，DatagramOnly，不重传）   ← 定距丢包注入（服务端→客户端）
        │
   lossyLink / pipe（内存管道，零网络变量）
```

**测量口径**：交付率 = 应用层收到/发送字节；线上字节 = fake_tcp 链路层发出的载荷总字节
（含 KCP 重传，不含每段恒定的 12B 私有头 + 52B TCP/IP 头，约 +4.5% 固定开销）；
带宽利用率 = 应用字节 / 线上字节；带宽放大 = 其倒数。每组传输 4MB。

**实测结果**（WSL2，go1.27.1，`go test -run '^TestTrunkKCPLossBench$' -v ./fake_tcp`；
v3 愈合特性启用后复测，吞吐列受本机 CPU 波动影响，利用率/放大列为稳定物理量）：

| 丢包率 | 裸 fake_tcp 交付率 | trunk_kcp 交付率 | trunk_kcp 有效吞吐 | 带宽放大 | 带宽利用率 |
|---|---|---|---|---|---|
| 0%   | 100.0% | 100% | 431.5 Mbps | 1.019x | 98.1% |
| 0.5% | 99.5%  | 100% | 262.8 Mbps | 1.024x | 97.7% |
| 1%   | 99.0%  | 100% | 283.9 Mbps | 1.029x | 97.2% |
| 2%   | 98.0%  | 100% | 187.6 Mbps | 1.039x | 96.2% |
| 5%   | 95.0%  | 100% | 187.9 Mbps | 1.072x | 93.2% |
| 10%  | 90.1%  | 100% | 161.6 Mbps | 1.132x | 88.3% |
| 20%  | 80.0%  | 100% | 112.3 Mbps | 1.274x | 78.5% |
| 33%  | 66.7%  | 100% | 89.0  Mbps | 1.528x | 65.5% |

**结论**：

1. **KCP 在所有丢包率下保持 100% 交付**——fake_tcp 的"不重传"语义与 KCP 的
   "应用层重传"正交组合成功：丢的包被 KCP 以新 TCP seq 重发，线路上呈现的就是
   TCP 快速重传的外观（对伪装有利）。
2. **带宽放大 ≈ 1/(1−p)**（理论值）：20% 丢包实测 1.274x（理论 1.25x），
   33% 丢包实测 1.528x（理论 1.49x）——重传开销与几何重试模型吻合。
3. **带宽利用率 ≈ (1−p) 减去少量协议开销**：10% 丢包仍能保持 88% 利用率。
4. 有效吞吐的绝对值是本机管道/CPU 上限（无真实带宽瓶颈），仅供相对比较；
   在真实限速链路上，吞吐拐点出现在"重传流量吃满物理带宽"处。
5. 丢包模型是定距均匀丢包；真实链路的突发丢包对 KCP 更不友好（RTO 占比升高），
   可用 `TrunkKCP.SetNoDelay` 调更激进的参数，或用多条物理连接分散丢包。

## v2/v3 变更（faux_tcp 评审合并）

- **删除 UDP 兜底模式**：唯一模式即 RawTCP；无权限场景交给外挂 udp2raw
  （deploy/udp2raw），包内不再维护两套会话语义。
- **seq 回绕修复**：收包规则全部改用 `seqAfter/seqBefore`（int32 差值法），
  4GB/连接回绕点前后的连续/空洞/重复判定正确（`TestSessionSeqWrap` 覆盖）。
- **`Config.DatagramOnly`**：见上"叠加 trunk_kcp"。
- **标准接口**：`Listen` 返回实现 `net.Listener` 的对象，`Accept() net.Conn`。
- **指纹**：每连接 IP ID 计数器（随机起步）；客户端缺省端口从 49152~65535 选取。
- **v3 虚拟重传愈合**（`Config.HealDelay`，默认 200ms）：丢包后先 dup ACK + SACK，
  逾 HealDelay 未愈合则直接越过空洞推进累计确认——线上呈现
  "丢包 → dup ACK/SACK → 快速重传恢复"完整外观，ack 不会永久冻结在空洞处
  （`TestSessionHoleHeal` 覆盖；移植自 faux_tcp healLocked，实现适配为共享扫描
  时间戳检查，无每连接定时器开销）。

## 测试

```bash
go test -count=1 -timeout 120s ./fake_tcp              # 全量（无机权限制的部分）
go test -race -count=1 -timeout 240s ./fake_tcp        # 竞态检测
go test -run '^TestTrunkKCPLossBench$' -v ./fake_tcp   # 本文丢包/带宽表
sudo FAKE_TCP_RAW_TEST=1 go test -run '^TestRawTCP' -v ./fake_tcp  # RawTCP 门控测试（需 root）
```

RawTCP 真机验收（tcpdump 清单）见 plan.md M3。
