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
conn, _ := ln.Accept()

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

发送走 raw IP socket（`IPPROTO_RAW`），接收走 `AF_PACKET` 旁路内核协议栈。
内核看到不属于任何 socket 的 TCP 段会回 RST，**必须抑制**：

```bash
# 以监听端口 18099 为例（两端都要，客户端端口随机则按对端端口过滤）
sudo iptables -A OUTPUT -p tcp --dport 18099 --tcp-flags RST RST -j DROP
sudo iptables -A OUTPUT -p tcp --sport 18099 --tcp-flags RST RST -j DROP
```

无权限时 `Listen`/`Dial` 返回明确错误，上层应回退 UDP 模式。

## 验证

```bash
go test ./faux_tcp/            # 内存链路全量单测（无需 root）
go test -race ./faux_tcp/      # 竞态检测
# 真实链路（需 root + iptables 规则）：
FAUXTCP_E2E=1 sudo -E go test -run TestFauxTCPLoopback -v ./faux_tcp/
# 抓包核对线上特征：
sudo tcpdump -i lo -nn 'tcp port 18099' -w faux.pcap
# 应看到：SYN/SYN+ACK/ACK 握手、PSH+ACK 数据、FIN 挥手、每个 seq 只出现一次
```

## 设计要点

- **零重传**：发送即忘；握手 SYN 允许有限重试（真实 TCP 行为，DPI 可见）。
- **零拥塞控制/零滑窗**：窗口通告固定 65535+WS=7；ack 字段照填但不据此做任何事。
- **ack 语义自洽**：接收端 ack 不超过最高连续接收序号；丢包产生空洞时**不发
  dup ACK**（丢包对 DPI 不可见），空洞数据直接上交（KCP 排序）。
- **指纹**：SYN 选项布局仿 Linux（MSS,SACK,TS,NOP,WS）；数据段带 TS；DF 位；
  ISN 随机；IP ID 递增；TTL 64。
- **保活**：空闲 25s 发裸 ACK（非 TCP keepalive 探测包特征），防 NAT 表项老化。
- **挥手**：FIN 四次挥手完整；FIN 不重传，宽限期（默认 500ms）后强制关闭。

详细设计与阶段计划见同目录 `plan.md`。
