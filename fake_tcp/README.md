# fake_tcp

伪装 TCP 传输模块：线路上是完整的 TCP 报文特征（三次握手/选项协商/PSH+ACK/FIN 四次挥手），
但**不做拥塞控制、丢包重传、滑动窗口限流**。设计与实施计划见 [plan.md](plan.md)。

- **ModeRawTCP**（主）：Linux + root/CAP_NET_RAW，raw socket 旁路内核协议栈；
  内核 RST 抑制规则自动装拆（iptables/nft）。
- **ModeUDP**（兜底）：UDP + 私有头，无需特权；也可外挂 udp2raw 完成伪装
  （零自研基线方案见 [deploy/udp2raw/README.md](deploy/udp2raw/README.md)）。
- 产出的 `Conn` 实现 `io.ReadWriteCloser`，可直接喂给 `trunk_kcp.NewTrunkKCP` /
  `rpc.NewPeer`。

## 快速上手

```go
// 服务端
ln, _ := fake_tcp.Listen(ctx, fake_tcp.Config{Mode: fake_tcp.ModeRawTCP, LocalAddr: "0.0.0.0:8443"})
conn, _ := ln.Accept()

// 客户端
conn, _ := fake_tcp.Dial(ctx, fake_tcp.Config{
    Mode: fake_tcp.ModeRawTCP, LocalAddr: ":0", RemoteAddr: "1.2.3.4:8443",
})
```

RawTCP 模式不可用时（无权限/非 Linux）自动降级为 ModeUDP 并打日志。

## 与 trunk_kcp 组合：丢包率 → 带宽利用率实测

`fake_tcp` 层不保证可靠性（这是特性：不重传、不降速）；需要可靠传输时在上层叠
`trunk_kcp`（KCP 应用层重传/排序）。测试 `TestTrunkKCPLossBench` 量化了两者的关系：

```
trunk_kcp.VirtualConn（可靠字节流）
        │
fake_tcp.Conn（伪装 TCP 外观，不重传）   ← 定距丢包注入（服务端→客户端方向）
        │
   lossyLink / pipe（内存管道，零网络变量）
```

**测量口径**：交付率 = 应用层收到/发送字节；线上字节 = fake_tcp 链路层发出的载荷总字节
（含 KCP 重传，不含每段恒定的 12B 私有头 + 52B TCP/IP 头，约 +4.5% 固定开销）；
带宽利用率 = 应用字节 / 线上字节；带宽放大 = 其倒数。每组传输 4MB。

**实测结果**（WSL2，go1.27.1，`go test -run '^TestTrunkKCPLossBench$' -v ./fake_tcp`）：

| 丢包率 | 裸 fake_tcp 交付率 | trunk_kcp 交付率 | trunk_kcp 有效吞吐 | 带宽放大 | 带宽利用率 |
|---|---|---|---|---|---|
| 0%   | 100.0% | 100% | 424.9 Mbps | 1.019x | 98.1% |
| 0.5% | 99.5%  | 100% | 310.9 Mbps | 1.024x | 97.7% |
| 1%   | 99.0%  | 100% | 259.9 Mbps | 1.029x | 97.2% |
| 2%   | 98.0%  | 100% | 242.1 Mbps | 1.039x | 96.2% |
| 5%   | 95.0%  | 100% | 243.1 Mbps | 1.072x | 93.2% |
| 10%  | 90.1%  | 100% | 154.2 Mbps | 1.132x | 88.4% |
| 20%  | 80.0%  | 100% | 102.4 Mbps | 1.273x | 78.5% |
| 33%  | 66.7%  | 100% | 91.4  Mbps | 1.528x | 65.5% |

**结论**：

1. **KCP 在所有丢包率下保持 100% 交付**——fake_tcp 的"不重传"语义与 KCP 的
   "应用层重传"正交组合成功：丢的包被 KCP 以新 TCP seq 重发，线路上呈现的就是
   TCP 快速重传的外观（对伪装有利）。
2. **带宽放大 ≈ 1/(1−p)**（理论值）：20% 丢包实测 1.273x（理论 1.25x），
   33% 丢包实测 1.528x（理论 1.49x）——重传开销与几何重试模型吻合。
3. **带宽利用率 ≈ (1−p) 减去少量协议开销**：10% 丢包仍能保持 88% 利用率。
4. 有效吞吐的绝对值是本机管道/CPU 上限（无真实带宽瓶颈），仅供相对比较；
   在真实限速链路上，吞吐拐点出现在"重传流量吃满物理带宽"处。
5. 丢包模型是定距均匀丢包；真实链路的突发丢包对 KCP 更不友好（RTO 占比升高），
   可用 `TrunkKCP.SetNoDelay` 调更激进的参数，或用多条物理连接分散丢包。

**组合部署的硬性前提（实测发现）**：`trunk_kcp` 的 recvLoop 按字节流解析 KCP 头长度字段，
丢包场景下必须保证**一个 KCP 段恰好落在一个 fake_tcp 段里**（整段丢失，不产生半帧错位）：
- RawTCP 模式：`MTU ≥ 1464`（MaxPayload = MTU−64 ≥ KCP 段 1400）；
- UDP 模式：`MTU ≥ 1413`（MaxPayload = MTU−13 ≥ 1400）；
- 或者反过来把 trunk_kcp 的 KCP MTU 调小到 fake_tcp MaxPayload 以内。
- 违反后果：丢包时 recvLoop 解析错位 → `invalid KCP segment length` → 物理连接被剔除。

## 测试

```bash
go test -count=1 -timeout 120s ./fake_tcp          # 全量（无机权限制的部分）
go test -run '^TestTrunkKCPLossBench$' -v ./fake_tcp   # 本文丢包/带宽表
sudo FAKE_TCP_RAW_TEST=1 go test -run '^TestRawTCP' -v ./fake_tcp  # RawTCP 门控测试（需 root）
```

RawTCP 真机验收（tcpdump 清单）见 plan.md M3。
