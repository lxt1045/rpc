# socks_faux_trunk_kcp

基于 **`faux_tcp`（伪装 TCP 的 UDP 式传输）+ `trunk_kcp`（KCP 链路聚合）** 的 SOCKS5 /
HTTP CONNECT 代理，是 `test/socks_trunk_kcp` 的同构版本，区别只在**物理链路**：

| | socks_trunk_kcp | socks_faux_trunk_kcp（本示例） |
|---|---|---|
| 物理连接 | TLS over TCP（可靠流） | `faux_tcp`（线路上是 TCP 报文，但**不重传/不拥塞控制/不滑窗**，语义等价 UDP） |
| 可靠性来源 | TCP + KCP 双层（易伪重传放大） | 只由 KCP 提供（丢包是真实丢包，无放大） |
| 运营商视角 | 普通 HTTPS 流量 | 普通 TCP 流量（规避 UDP QoS/限速） |
| 加密 | TLS | TLS over KCP 虚拟连接（mTLS 双向证书，控制/数据端到端） |
| 运行要求 | 普通用户 | Linux + root/CAP_NET_RAW（raw socket） |

代理功能本身（SOCKS5/HTTP CONNECT、认证、ACL、trunk 动态增删连接、空闲/慢连接
替换）与 `socks_trunk_kcp` 完全一致，控制面协议（`pb/`）、虚拟连接首包协议
（`protocol.go`）、中继逻辑（`copy.go`）均相同。

## 结构

```
cmd/socks-faux-trunk-kcp-client   客户端入口（起本地 SOCKS5/HTTP 代理）
cmd/socks-faux-trunk-kcp-server   服务端入口（faux_tcp 监听 + trunk 汇聚）
config.go                         TrunkKCPConfig / FauxTCPConfig / ConnConfig / ACL
fauxconn.go                       ConnDialer 抽象 + 默认 FauxDialer（faux_tcp 拨号）
tlsvconn.go                       VirtualConn→net.Conn 适配、TLS 包装、连接类型标记
peer_client.go                    客户端：控制连接 + N 条物理连接（Upgrade 给 trunk）
server.go                         服务端：Accept → Clone peer → Svc
session.go                        服务端会话/trunk 管理（与 socks_trunk_kcp 同构）
protocol.go / copy.go             虚拟连接首包协议 / 双向中继
faux_mem_test.go                  内存 faux_tcp 网络 + 全链路端到端测试
filesystem/static/conf/default.yml 编译期嵌入配置
scripts/local_integration_test.sh 本地集成冒烟（需 root）
```

## 链路模型

```
浏览器/应用 ──SOCKS5/HTTP──▶ 客户端
                               │ 控制连接（1~2 条）：faux_tcp ──▶ 迷你 trunk(KCP) ──▶ TLS ──▶ RPC
                               │   承载 Auth/TrunkStart/TrunkRemoveConn（token 在 TLS 之内）
                               │ 物理连接（N 条）：faux_tcp ──▶ 裸 RPC TrunkUpgrade ──▶ trunk_kcp
                               ▼
                        fake-TCP 报文（IP proto=6，握手/PSH+ACK/FIN 齐全）
                               ▼
                             服务端 faux_tcp 监听（AF_PACKET + raw IP socket）
                               │  数据面 trunk_kcp 虚拟连接（KCP 重传/排序）
                               │  每条代理会话：TLS（vconn 之上）→ open header → 双向中继
                               ▼
                             拨号目标站点
```

为什么 TLS 叠在 KCP 之上而不是 faux_tcp 之上：`faux_tcp` 是不可靠的数据报语义
（一次 Write = 一个 TCP 段、丢包不补），而 TLS 要求可靠有序流（记录最大 16KB、
丢包即校验失败）。`trunk_kcp` 的 VirtualConn 恰好是可靠有序流，因此：

- **控制通道**：faux_tcp 连接外套一个迷你 trunk（KCP 可靠层）→ 控制 vconn →
  TLS → RPC。token 与控制调用全程加密，且 faux_tcp 丢包不再打断控制面；
- **物理连接**：只做裸 RPC 的 TrunkUpgrade（无秘密），升级后整连接交给数据面
  trunk 跑 KCP；服务端按 conv 关联到控制通道里已授权的会话；
- **数据面**：每条代理 VirtualConn 端到端 TLS——open header（含目标地址）也在
  TLS 之内，DPI 既看不到数据也看不到代理目的地。

## 关键约束：数据报边界（MSS ≥ KCP 线上包）

`faux_tcp` 一次 `Write` = 一个 TCP 段，**不做拆分**（超过 MSS 直接报错）。
`trunk_kcp` 的 recvLoop 按长度字段做流式重组，所以必须保证「一个 KCP 段恰好落在
一个 faux_tcp 段里」，否则丢包会造成半帧错位、物理连接被剔除。

- 默认值即安全：KCP 线上包 1400B ≤ `faux_tcp` MSS 1448；
- `config.go` 的 `ValidateMSS()` 会在 `mss < 1400` 时直接报错，避免静默错位；
- 若调小 KCP MTU，需同步保证 `faux_tcp.mss ≥ KCP 线上包`。

## 运行

需要 **Linux + root/CAP_NET_RAW**（AF_PACKET 收包、raw IP socket 发包）；
`faux_tcp` 默认自动安装/卸载内核 RST 抑制规则（iptables 优先，nft 兜底）。

```bash
# 1) 编辑 filesystem/static/conf/default.yml（改 token / 地址 / 连接数）
# 2) 构建并运行（两端都需 root）
go build -o /tmp/socks-faux-server ./test/socks_faux_trunk_kcp/cmd/socks-faux-trunk-kcp-server
go build -o /tmp/socks-faux-client ./test/socks_faux_trunk_kcp/cmd/socks-faux-trunk-kcp-client

# 服务端（本机）
cd test/socks_faux_trunk_kcp && sudo /tmp/socks-faux-server
# 客户端（本机或另一台机器，配置里 client-conn.addr 指向服务端）
sudo /tmp/socks-faux-client
# 浏览器/curl 使用 socks5h://127.0.0.1:11090 或 http://127.0.0.1:11091
```

### Docker / 容器部署（云服务器常见，务必先看）

faux_tcp 收发都走 raw socket，容器化有两个**硬要求**，否则现象就是客户端
"握手超时、收到 0 个报文"：

1. **报文必须真的到达容器的网络命名空间**：
   - **推荐 `--network host`**：容器直接用宿主机网卡，没有 DNAT，最省事；
   - 若用端口映射（`-p 18099:18099`）必须加 `--userland-proxy=false`：
     docker-proxy 是**用户态 TCP 代理**，它会让宿主机内核先与客户端完成三次握手，
     再另起连接转发到容器——faux_tcp 的握手/伪装就此失效（客户端表现为连到一半被断）。
2. **能力**：`--cap-add=NET_RAW`（AF_PACKET/raw socket）+ `--cap-add=NET_ADMIN`
   （自动装 iptables/nft RST 抑制规则）。

```bash
docker run -d --name socks-faux-server \
  --network host \
  --cap-add=NET_RAW --cap-add=NET_ADMIN \
  -v "$PWD/test/socks_faux_trunk_kcp/filesystem/static/conf/default.yml:/app/static/conf/default.yml:ro" \
  <image>
```

> 容器里若**没有 `iptables`/`nft`**，`faux_tcp` 自动装 RST 抑制规则会失败并直接退出
> （`docker logs` 里能看到 `faux_tcp: 未找到 iptables/nft ...`）。两条路：镜像里装上
> iptables；或设 `faux_tcp.manual_firewall: true` 后手工加规则（`--network host` 时在
> 宿主机加，桥接时在容器内加）。

程序**优先读磁盘** `static/conf/default.yml`（工作目录 `/app` 时即上表挂载路径），
读不到才用编译期嵌入的默认值——所以容器里挂载配置即可生效，不必重建镜像。
启动日志会打印 `config loaded conf=file:... | embedded:...`。

**云服务器还要放行端口**：轻量应用服务器/ECS 的**控制台防火墙（安全组）**默认只开
少数端口。若你之前 18088 能连通，说明当时只放行了 18088——**换成 18099 必须在控制台
新加入站 TCP 18099 放行规则**（这条最容易被忽略，且现象与"服务端没跑"完全一样）。

**回程不通：按"服务端 SYN+ACK 源地址 + 本端源端口"定性**（完整顺序见「跨机部署 →
排查决策树」）：

| 判据 | 含义 | 处理 |
| --- | --- | --- |
| 源地址是**公网 IP**（如 `43.155.182.37`），而网卡是内网地址 | 配置了 `reply_src`，云平台对虚拟网卡做出站源地址校验 → 报文被静默丢弃（客户端一个包收不到；服务端 `nf_conntrack` 里没有该流） | **把 `reply_src` 留空**（启动日志会 WARN）。回包源地址改取报文目的 IP（内网地址），与内核 TCP 同路，由平台 NAT 转公网 |
| 源地址是**网卡内网地址**、客户端仍无入向包，且**本端源端口在 `ip_local_port_range` 之外** | 上游设备/运营商只为"源端口落在本机 ephemeral 范围内"的会话放行回程（**真机根因**） | 用含源端口修复的版本（`faux_tcp` 自动按 `ip_local_port_range` 选）；或手工指定范围内的 `client-conn.local_addr`。见文末「真机复盘」 |
| 源地址是**网卡内网地址**、源端口正常，仍收不到 | 平台只为**内核跟踪的流**做回程 SNAT（无状态 DNAT/端口映射：Docker 桥接、K8s NodePort） | 在服务端配置 `reply_src: "<对外服务地址>"`，等价于本端自己做 SNAT |

```yaml
faux_tcp:
  reply_src: ""   # 云主机（1:1 NAT/弹性公网 IP）留空；仅无状态 DNAT 环境才填对外地址
```

> 判据要点：**云主机 1:1 NAT（腾讯云 VPC/轻量、阿里云 ECS）本身就会为网卡内网地址
> 做 SNAT**，所以独立部署在云主机上时必须留空——填公网 IP 反而触发源地址校验被丢。
> 只有"平台不认识我们的 raw socket 流"的容器端口映射场景才需要 `reply_src`。

**客户端抓包的判据**（决策树第 4 步）：只有 `Out [S]`、没有入向 `[S.]` → 回包死在
"服务端 → 客户端"之间（按上表定性）；看到入向 `[S.]` 却仍超时 → 回包到了本机，问题在
本端收包/后续流程（看 `debug_packets` 的 `rx_match` 与 `iface=`）。

**路径 MTU 受限（VPN/隧道）**：若客户端发出的 SYN 是 `mss 1448`、而服务端 tcpdump
收到的是 `mss 1280`，说明中间有设备改写 MSS——**握手通了之后大数据也会被丢**
（本层报文带 DF，不会被分片）。此时三处一起调小：

```yaml
faux_tcp:
  mss: 1240          # 本端发送单段上限 ≤ 路径MTU-40（MSS 被夹到 1280 时取 1240）
  adv_mss: 1460      # 通告值可与发送上限分开，保持内核默认 1460
trunk_kcp:
  kcp_mtu: 1200      # ≤ faux_tcp.mss，且含 24B KCP 头
```

**源端口**：`faux_tcp` 的源端口取自本机 `/proc/sys/net/ipv4/ip_local_port_range`
（内核 ephemeral 范围）并避开已占用端口——**不要用固定区间**（例如 IANA 的
49152-65535）。真机踩过：某客户端该范围是 `44620 48715`，固定区间选出的端口落在范围
外，回程 SYN+ACK 被上游静默丢弃；同端口的内核 TCP（源端口 46650，在范围内）完全正常。
详见文末「真机复盘」。自查：

```bash
sysctl net.ipv4.ip_local_port_range      # 本机 ephemeral 范围
# 抓包里自己 SYN 的源端口若落在范围外，就是这个坑
```

faux_tcp 在握手时会把对端通告的 MSS 记下来，**发现小于本端 `mss` 会打 WARN 日志**
（`对端/路径通告 MSS=... < 本端 MSS=...`），照它给的数字调即可。

**对照实验（性价比最高的一步）**：客户端用**内核 TCP** 连**正在运行的 faux 服务端**：

```bash
# 服务端保持 faux_tcp 运行，客户端机器上直接：
telnet 43.155.182.37 18099
#   Connected → 服务端 SYN+ACK 与整条回程链路都正常（这个三次握手就是我们的 raw 栈
#               完成的：服务端日志会同步出现 `收到 SYN`，而 `ss -ltnp | grep 18099`
#               仍然是空的）→ 问题只在"客户端 raw 流"这条路径上，继续按第 6 步在
#               客户端侧抠（源端口范围 / 本端收包）
#   一直 Trying ... → 链路/安全组/端口本身不通（映射没生效、云安全组、容器网络）
```

> 判读要点是**差异**，不是"telnet 能不能连"：`telnet` 显示 `Connected` 本身是正常的
> （faux 服务端不创建内核监听，但会像真实栈一样回 SYN+ACK 完成握手，之后 telnet 只是
> 挂在那里）。真正有价值的是"**telnet 能连、`go run ./` 连不上**"这个组合——它把故
> 障一刀切到客户端那条 raw 流上，本次真机故障正是靠它定位的（见文末「真机复盘」）。
>
> 想反过来验证"端口映射/安全组对内核 TCP 也通"时，可停掉 faux 服务端改用
> `SOCKS_FAUX_PLAIN_LISTEN=1 ./socks-faux-trunk-kcp-server`（内核 TCP 监听同端口并回显），
> 两种起法下 `telnet` 都能连，才说明链路与端口映射没问题。

服务端在容器里的排查顺序（客户端正在重试时）：

| 位置 | 命令 | 结论 |
|---|---|---|
| 云主机（宿主机） | `sudo tcpdump -ni any 'tcp port 18099'` | **一个包都没有 → 云安全组/网络 ACL 未放行入站 TCP 18099**（最常见） |
| 容器内 | `docker exec <容器> tcpdump -ni any 'tcp port 18099'` | 宿主机有、容器内没有 → 端口映射/docker-proxy 问题（改 `--network host`） |
| 容器内 | 服务端日志有无 `faux_tcp: 收到 SYN ...` | 有 SYN 但客户端收不到回包 → 回程（容器 NAT/客户端收包侧），看客户端 `debug_packets` 计数 |
| 容器内 | 服务端日志有无 `faux_tcp: 发包失败(N) ...` | 回不出 SYN+ACK：能力（NET_RAW）/路由/源地址问题 |

### 跨机部署（client-conn.addr 指向公网/另一台机器）

本机 127.0.0.1 能通、指向远程就报 `握手超时`（错误码 501003）时，按下面顺序排查。
`faux_tcp` 的握手超时错误现在自带诊断，先读它的后半段：

```
501003, 握手超时：重试 3 次无 SYN+ACK；已发 4 个报文，收到 0 个报文，发送失败 0 次；
     对端无任何回包：确认服务端已运行，且云安全组/防火墙放行入站 TCP 端口
```

| 诊断字段 | 含义 | 对应处理 |
|---|---|---|
| `发送失败 N 次`（N>0） | 本机 raw socket 发包就失败了（无权限/无路由/源地址不对） | 看错误尾部的「最后发送错误」；确认 root、`ip route get <对端IP>` 与选到的源地址 |
| `收到 0 个报文` | SYN 出去了但**没有任何回包** | ① 服务端进程是否在跑（服务端日志应有 `server started (faux_tcp transport)`）；② 云主机**安全组/网络 ACL 放行入站 TCP 18099**；③ 服务端本机防火墙（`iptables -L INPUT -n`）；④ **本端源端口是否落在 `ip_local_port_range` 内**（真机根因，见文末「真机复盘」）；⑤ 中间链路/公司网出口是否拦了；⑥ 若服务端就在同一出口/同一台机器，用公网 IP 会走 **NAT 回环（hairpin）**，很多路由器/云 NAT 不支持 → 改用内网 IP 或 127.0.0.1 |
| `有回包但无 SYN+ACK` | 收到包但不是正常 SYN+ACK（多为内核 RST） | 服务端**没装 RST 抑制规则**：确认服务端是 root 运行且 `faux_tcp.manual_firewall: false`，或按下面手工加规则 |

#### 排查决策树（按顺序做，每步都有明确判据）

| 步 | 机器 | 命令 / 看什么 | 判据与结论 |
|---|---|---|---|
| 0 | 服务端 | `ps -ef \| grep socks-faux-trunk-kcp-server`；启动日志 | 没有 `server started (faux_tcp transport)` → 进程没起或启动就失败（权限/iptables/证书），先解决它 |
| 1 | 服务端 | 日志（`log-level: debug`）有无 `faux_tcp: 收到 SYN <客户端> -> <服务端>, seq=..., 对端通告 MSS=..., 回包源=...` | **完全没有** → SYN 没到服务端：云安全组/网络 ACL 未放行入站 TCP、服务端本机防火墙、链路；有 → 进第 2 步 |
| 2 | 服务端 | 同一行里的 `对端通告 MSS=` / `回包源=` | `MSS` 比客户端配的值小很多 → 路径上有 MSS-clamp 中间盒（只影响大数据分段，见「路径 MTU 受限」）；`回包源` 不是本机网卡地址 → 云平台源地址校验会丢包，`reply_src` 应留空 |
| 3 | 服务端 | `sudo tcpdump -vni any 'tcp port 18099'` | 应看到 `In [S]` 紧跟 `Out [S.]`，且 `[S.]` 的**源地址=网卡内网地址**、`cksum ... (correct)`、`ecr` 回显客户端 TSval；没有 `Out [S.]` → 本机发不出去，看日志里的 `发包失败(N)` |
| 4 | 客户端 | `sudo tcpdump -ni <出口网卡> 'tcp port 18099'` | **只有 `Out [S]`、没有入向 `[S.]`** → 回程被丢，进第 5 步；看到入向 `[S.]` 却仍超时 → 本端收包侧（看 `debug_packets` 的 `rx_match` 与 `iface=`） |
| 5 | 客户端 | **A/B**：`telnet <服务端IP> 18099`（对**正在运行的 faux 服务端**） | `Connected` → 服务端报文与整条回程链路正常（服务端日志同步出现 `收到 SYN`），问题只在客户端 raw 流 → 进第 6 步；一直 `Trying ...` → 链路/安全组/端口本身不通 |
| 6 | 客户端 | ① `sysctl net.ipv4.ip_local_port_range` 对比抓包里**自己 SYN 的源端口**；② `debug_packets` 的计数；③ 换网络（手机热点）复跑 | 源端口落在范围外 → 本次真机根因；`rx_match>0` 仍超时 → 报文到了本机但被判无效；换网络就好 → 客户端所在网络/出口设备的问题 |

> `debug_packets` 三个计数的读法（**只统计入向**；协议限定的 AF_PACKET socket 收不到
> 自己发出的报文，所以 `tx_seen` 为 0 是正常的，不是故障）：
> `rx_total=0` → 本机一个包都没收到；`rx_total>0, rx_match=0` → 收到了但没一个是"目的
> 端口=本连接本地端口"的（回包根本没到本机）；`rx_match>0` 仍超时 → 报文已进本连接但
> 不是有效 SYN+ACK（对端缺 RST 抑制，或端口被别的内核服务占用）。


#### 云主机（1:1 NAT / 弹性公网 IP）特有的静默丢包：`reply_src` 别填公网 IP

> 这是"内核 TCP 能通、faux 不通"的**其中一种**成因，不是通用解释（真机上还踩到过
> 源端口不在 ephemeral 范围，见文末复盘）。判据只看服务端 `Out [S.]` 的源地址。

**症状**：客户端 `tcpdump` 只有 `Out [S]`、一个入向包都没有；服务端 tcpdump 里
`In [S]` 与 `Out [S.]` 都正常且校验和正确；`ss -ltnp | grep 18099` 为空（raw socket
不占监听端口）；TCP 客户端（`telnet`/内核 TCP）却能连上。

**判据（一条命令定性）**——对比服务端 SYN+ACK 的源地址：

```bash
# 服务端（云主机）
sudo tcpdump -vni any 'tcp port 18099'
#   faux_tcp:  Out IP 43.155.182.37.18099 > <客户端>.xxx: Flags [S.]    ← 公网 IP，被平台丢弃
#   内核 TCP:  Out IP 10.8.0.2.18099  > <客户端>.xxx: Flags [S.]        ← 网卡内网地址，平台 NAT 放行
# 服务端上确认没有本地 conntrack 牵连（raw socket 流不建 conntrack）
sudo cat /proc/net/nf_conntrack | grep 18099
```

原因：云主机网卡只有内网地址，公网 IP 由平台 1:1 NAT 提供，并对**虚拟网卡做出站源
地址校验**（反欺骗）。内核 TCP 回包源地址是网卡内网地址，平台正常 SNAT 成公网；
`reply_src` 直接写公网 IP 的 raw 报文不匹配任何 NAT 映射，被静默丢弃。

**处理**：把 `faux_tcp.reply_src` 留空（这也是默认值；启动日志对非本机地址会打 WARN）。
留空后回包源地址取"报文目的 IP"＝网卡内网地址，与内核 TCP 完全同路。

> 反例（**需要** `reply_src` 的场景）：容器端口映射（Docker 桥接、K8s NodePort）等
> 无状态 DNAT —— 平台只为内核跟踪的流做回程转换，raw socket 回包会带着容器私网源
> 地址出去被丢弃。那种环境下把 `reply_src` 填成容器外的对外服务地址才正确。

**收包侧计数**（`faux_tcp.debug_packets: true` 时握手失败信息里会多一段）：

```
本端链路: iface=eth1 local=10.1.1.95:46205 cooked(AF_PACKET/SOCK_DGRAM);
      debug(未挂 cBPF) rx_total=N rx_match=M rx_dropped=K tx_seen=0
```

`rx_*` 只统计入向；`tx_seen` 为 0 属正常（协议限定的 AF_PACKET socket 收不到自己发出的
报文，tcpdump 用的是 `ETH_P_ALL` 才看得到）。三个计数的读法见上面决策树。

**tcpdump 与 faux_tcp 的收包挂在同一个位置**（网卡收包点），所以"tcpdump 看得到回包、
faux_tcp 却报收到 0 个"就说明是**本端收包侧**的问题。历史上真出现过一类：`SOCK_RAW`
会把链路层头一起交上来，而 tun/wireguard/ppp 等点对点接口**没有以太网头**，按以太网
偏移解析会把所有包丢掉——现已改为 cooked `AF_PACKET/SOCK_DGRAM`（内核统一剥链路层头），
并在错误信息里打印 `iface=` 便于确认实际使用的网卡：

```bash
# 客户端：eth0 换成 `ip route get <服务端IP>` 显示的出口网卡
sudo tcpdump -ni eth0 'tcp port 18099 and host <服务端IP>'
#   看得到对端 SYN+ACK → 本端收包侧问题（帧格式/过滤器，先升级到最新版本再看 iface=）
#   只看到自己发的 SYN、没有回包 → 回程被丢（按决策树第 5、6 步）
# 服务端：看 SYN 是否到达、自己有没有回 SYN+ACK
sudo tcpdump -ni any 'tcp port 18099'
```

**关于多网卡/非对称路由**：faux_tcp 的收包 socket **不绑定单张网卡**（只按目的端口
过滤），因此"去程走 eth1、回程从 eth0 回来"也能收到；早期版本绑定源 IP 所在网卡，
在多网卡+策略路由的主机上会一个回包都收不到。

**服务端日志的判读**（`log-level: debug`）：

| 服务端日志 | 含义 | 处理 |
|---|---|---|
| 启动时有 `server started (faux_tcp transport)` | 服务端正常监听 | 继续 |
| `faux_tcp: 收到 SYN <客户端> -> <服务端>, seq=..., 对端通告 MSS=..., 回包源=...` | SYN 到了服务端 | 问题在回程或客户端 raw 流：先看 `对端通告 MSS`（有无 MSS-clamp）、`回包源`（是否网卡地址），再按决策树第 4~6 步 |
| 只有 `faux_tcp: 半开连接超时回收 peer=...（已发 N 个报文未收到 ACK）` | 我们回了 SYN+ACK、对端没 ACK | 与客户端"收不到回包"是同一件事的两面，按第 4~6 步查 |
| 完全没有 `收到 SYN` | SYN 没到服务端 | 云**安全组/网络 ACL 未放行入站 TCP**；或服务端进程没在跑（`ps -ef \| grep socks-faux-trunk-kcp-server`）；或客户端出口没把包发出去 |
| `faux_tcp: 发包失败(N) ...` | 服务端回不出 SYN+ACK | 看错误内容：路由/权限/源地址问题 |

`ss -ltnp | grep 18099` **无输出是 faux_tcp 服务端的正常表现**——它不创建内核监听
socket（收包走 AF_PACKET、发包走 raw IP）。出现输出才说明该端口被别的内核服务占用了。

**`telnet <server> 18099` 的判读**：faux 服务端本身就是完整的 TCP 握手应答方，所以
`telnet` 显示 `Connected` 是**正常**的（连接被 faux 栈接受，之后 telnet 只是挂在那里；
服务端日志会同步出现 `收到 SYN`）。判据是**与 `go run ./` 对比**：telnet 能连而 faux
客户端连不上 → 服务端 wire 与回程链路正常，问题在客户端那条 raw 流（见文末复盘）。

要点：**faux_tcp 的服务端在内核里没有监听 socket**，进出都是 raw socket。所以：

- 云安全组要按 **TCP 18099** 放行入站（faux_tcp 是合法 TCP 报文，安全组按 TCP 处理）；
- **两端都要 root/CAP_NET_RAW**，且**两端都要 RST 抑制**（客户端抑制自己发出的 RST，
  服务端抑制内核因"无监听 socket"而回的 RST）；
- 客户端所在网络若是 **WSL2 / 容器 NAT**：raw socket 需要真正的 `CAP_NET_RAW`，
  且 WSL2 走 Hyper-V NAT，跨公网建议在宿主机或云主机上跑；
- 常规定位手段（两端各跑一条）：
  ```bash
  # 客户端：确认 SYN 是否真的发出去了（以及有没有回包）
  sudo tcpdump -ni any 'tcp port 18099 and host <服务端IP>'
  # 服务端：确认 SYN 是否到达、自己有没有回 SYN+ACK
  sudo tcpdump -ni any 'tcp port 18099'
  ```
- 握手单次超时在配置里是 `handshake_timeout_ms`（默认 3s，× `handshake_retries+1` 为总预算）；
  客户端拨号 ctx 超时会自动取「握手预算 + 2s」，保证失败时能看到上面的诊断而不是被 ctx 提前取消。

`faux_tcp.manual_firewall: true` 时不自动改防火墙，需自行维护（客户端每条物理连接
一个本地端口，连接数多时建议按端口段配置）。**规则要装在 raw 表**（conntrack 之前），
否则内核 RST 会先污染连接跟踪状态，导致我们自己的 SYN+ACK 被判 INVALID、NAT 不做
地址转换——现象就是"服务端发了 SYN+ACK，客户端一个包都收不到"：

```bash
# 服务端：监听端口
sudo iptables -t raw -A OUTPUT -p tcp --sport 18099 --tcp-flags RST RST -j DROP
# 客户端：源端口范围按本机 ephemeral 范围来（faux_tcp 就是从这里选源端口的）
sudo iptables -t raw -A OUTPUT -p tcp \
  --sport "$(sysctl -n net.ipv4.ip_local_port_range | tr ' \t' ':')" \
  --tcp-flags RST RST -j DROP
```

#### 真机复盘：源端口不在本机 ephemeral 范围（2026-09-23）

这是本示例最隐蔽的一次故障：**服务端一切正常、内核 TCP 一切正常、127.0.0.1 也一切
正常，只有 faux 客户端连不上任何远程地址**。

**现象**（客户端 `root@lxt`，本地 `10.1.1.95` → 腾讯云轻量服务端 `43.155.182.37:18099`）：

- `go run ./` 恒报 `501003 握手超时：已发 3 个报文，收到 0 个报文，发送失败 0 次`，
  `debug_packets` 显示 `rx_total>0 rx_match=0`；
- 服务端日志正常出现 `faux_tcp: 收到 SYN 222.209.83.111:xxxxx -> 10.8.0.2:18099`，
  服务端 tcpdump 里 `In [S]` 与紧跟的 `Out [S.]` 都正常、`cksum (correct)`、`ecr` 正确；
- 客户端 tcpdump **只有 `Out [S]`，一个入向包都没有**；
- 服务端 `nf_conntrack` 里没有该流（raw socket 流本来就不建 conntrack）。

**决定性对照（同一时间段、同一端口，只有客户端实现不同）**：

| 客户端 | 源端口 | 服务端看到 | 客户端收到的回包 |
| --- | --- | --- | --- |
| `telnet 43.155.182.37 18099`（内核 TCP，连的就是**我们的 faux 服务端**） | **46650** | `In [S] mss 1280` → `Out [S.] mss 1240` | 入向 `[S.] mss 1240 win 65535`，`Connected` ✅ |
| `go run ./`（faux 客户端） | **≥ 49152**（旧实现硬编码 IANA 高端口区间） | `In [S] mss 1240` → `Out [S.]` | 无 ❌ |

客户端 `sysctl net.ipv4.ip_local_port_range = 44620 48715`：内核选的 **46650 落在范围内**，
而硬编码的 **49152-65535 完全在范围外**。客户端上游设备/运营商只为"源端口落在本机
ephemeral 范围内"的 TCP 会话放行回程报文——SYN（出向）能出去，回程 SYN+ACK 的目的
端口在范围外就被静默丢弃。

**修复**：`faux_tcp.Dial` 的源端口改为读 `/proc/sys/net/ipv4/ip_local_port_range`
（读不到回退 `32768-60999`），并先用一次 bind 探测避开本机已占用端口；`TestPickLocalPort`
钉住"必须落在本机 ephemeral 范围内且可用"。这也更符合伪装 TCP 的本意——内核就是从该
范围挑源端口。

**验证**（客户端侧，一条命令就能确认修复生效）：

```bash
sudo tcpdump -ni eth1 'tcp port 18099'     # eth1 = `ip route get <服务端IP>` 的出口网卡
# 期望：Out [S] 的源端口落在 `sysctl -n net.ipv4.ip_local_port_range` 范围内，
#       紧接着出现 In [S.]，随后是 Out [.] 与业务数据
go test -run '^TestPickLocalPort$' -count=1 -v ./faux_tcp   # 单元测试（无需 root）
```

**下次遇到同类问题**：直接按上面「排查决策树」六步走；本节的假设-判据表可以先扫一眼，
避免重复已经排除过的方向。

**排查中走过、已排除的弯路**（下次可直接跳过，节省几小时）：

| 假设 | 判据 | 结论 |
| --- | --- | --- |
| 云平台按源地址校验丢了伪造的公网源 IP（`reply_src: 43.155.182.37`） | 服务端 `Out [S.]` 的源地址是公网 IP 还是网卡内网地址 | 改成留空（源地址与内核一致 `10.8.0.2`）后**仍然失败** → 不是本次原因。但云主机本就该留空，`reply_src` 只在无状态 DNAT（Docker/K8s）场景需要 |
| 内核 RST 污染 conntrack（规则必须装 raw 表而不是 filter 表） | 服务端 tcpdump 有无出向 `Flags [R]`；`/proc/net/nf_conntrack` 有无该流 | 抑制规则生效（抓包里无 RST）、conntrack 为空 → 也不是本次原因（raw 表规则仍保留，语义更正确） |
| SYN 通告的 MSS ≤ 中间盒的夹取目标 ⇒ 它不建流状态 | 客户端 SYN 的 `mss` vs 服务端抓到的 `mss`；改 `adv_mss: 1460` 后重试 | 改了仍失败 → 不成立（`adv_mss` 作为配置项保留：发送上限与通告值分离，语义更清晰） |
| 初始窗口 `win 65535` 被中间盒按策略拦 | 客户端 SYN 的 `win` vs 内核 SYN 的 `win`；改 `window: 64240` 后重试 | 改成内核同款 64240 后仍失败 → 不成立 |
| 客户端收包通道/帧格式问题 | `debug_packets` 的 `rx_total / rx_match / rx_dropped` | `rx_total>0 rx_match=0` ⇒ socket 工作正常（能收到本机其它流量），只是没有"目的端口=本连接端口"的入向包 → 回包根本没到本机 |
| faux 服务端报文不合法 | 客户端**内核 TCP** 连 faux 服务端能否 `Connected` | 能连上（客户端抓到 `mss 1240 win 65535`，正是 faux 发的包）→ 服务端 wire、校验和、回程链路全部正常 |

**三条教训**：

1. "内核 TCP 能通"只证明**服务端报文 + 回程链路**没问题，不证明客户端那条 raw 流没
   问题。反过来做（`telnet` 连正在运行的 faux 服务端）才是把问题一刀切开的实验；
2. 对照实验要**逐字段比**（源地址、**源端口**、MSS、窗口、时间戳），只比"通/不通"
   会一路猜；本次就是靠"内核选 46650 / faux 选 ≥49152"这一个字段差异定案的；
3. 伪装 TCP 的每个可见字段都该与内核一致：源端口范围、初始窗口、MSS 通告、TS 回显、
   TTL/IP ID 递增方式……这次栽在源端口范围上。

## 配置要点

```yaml
conn:            { addr: "0.0.0.0:18099" }   # 服务端 faux_tcp 监听
client-conn:
  addr: "1.2.3.4:18099"  # 客户端对端
  # local_addr: "10.1.1.95:0"  # 可选：指定本地 IP（端口 0 或留空 = 自动）
  #   自动时源端口取自本机 ip_local_port_range（内核 ephemeral 范围）并避开已占用端口。
  #   不要手工指定范围外的端口：某些上游设备只放行"源端口在 ephemeral 范围内"的会话回程
  #   （真机踩过，详见「真机复盘：源端口不在本机 ephemeral 范围」）。
faux_tcp:
  mss: 1240              # 本端发送单段上限：≥ KCP 线上包，且 ≤ 路径 MTU-40
  adv_mss: 1460          # 对外通告的 MSS（只影响对端发给我们），可与 mss 分开
  keepalive_seconds: 25  # 标准 TCP keepalive 探测包
  heal_delay_ms: 200     # 丢包后 ack 愈合等待（详见 faux_tcp/README.md）
  # reply_src: ""        # 仅无状态 DNAT（Docker 桥接/K8s）需要；云主机必须留空
  manual_firewall: false # true 则自行维护 RST 抑制规则
tls:
  enabled: true          # 端到端 TLS（mTLS）；false 为明文模式（仅可信链路）
  host: "speedtest.cn"   # 客户端校验的 ServerName（与 server-cert SAN 一致）
  ca-cert: "static/ca/root-cert.pem"
  server-cert: "static/ca/server-cert.pem"
  server-key: "static/ca/server-key.pem"
  client-cert: "static/ca/client-cert.pem"
  client-key: "static/ca/client-key.pem"
trunk_kcp:
  conv: 123456789        # 两端必须一致
  min_conns: 2
  max_conns: 4
  max_virtual_conn: 8096
```

**KCP 参数**：物理层是不可靠的 `faux_tcp`，丢包是真实丢包，因此使用**库默认快速模式
`(1,10,32,1)`** 即可；`socks_trunk_kcp` 中针对「TCP/TLS 底层伪重传」的保守参数
（`nodelay=0, interval=20~40, resend=0`）**不适用**于本示例（那会让恢复变慢）。
需要显式配置时用 `kcp_nodelay / kcp_interval / kcp_resend / kcp_nc`（两端一致）。

## 安全说明

**默认启用端到端 TLS（mTLS 双向证书校验）**，TLS 跑在 `trunk_kcp` VirtualConn
（可靠有序流）之上：

- **控制通道**（faux_tcp → 迷你 trunk → TLS → RPC）：token 与控制调用（Auth/
  TrunkStart/TrunkRemoveConn）全程加密；
- **数据面**：每条代理 VirtualConn 各自一次 TLS 握手，open header（含目标地址）
  与应用数据都在 TLS 之内；
- **物理连接**（faux_tcp 上的裸 RPC）只承载 TrunkUpgrade 编排消息（conv/序号，
  无秘密），服务端按 conv 关联到已授权会话，未授权会话的 TrunkUpgrade 直接拒绝
  （`TestTrunkUpgradeRequiresAuthorization` 覆盖）。

证书与 `socks_trunk_kcp` 同一套 CA 模型（`utils/config.LoadTLSConfig`：
CA 签发 server/client 双向证书）。证书不进 git，部署前生成：

```bash
go test -run TestMake -count=1 ./test/cert   # 生成到 test/cert/ca/
mkdir -p test/socks_faux_trunk_kcp/filesystem/static/ca
cp test/cert/ca/{root-cert,server-cert,server-key,client-cert,client-key}.pem \
   test/socks_faux_trunk_kcp/filesystem/static/ca/
```

`tls.enabled: false` 退回明文模式（token 明文过线），仅限可信链路/调试。

## 测试

```bash
# 离线全量（无需 root）：协议/ACL/认证/中继/会话 + 内存链路全链路端到端
go test -count=1 ./test/socks_faux_trunk_kcp/

# 端到端用例说明（内存 faux_tcp 链路 + 内存自签 CA，无需 root/文件）：
#   TestFauxTrunkProxyEndToEnd    全 TLS 跑通 openProxy→KCP→目标回显（含并发虚拟连接）
#   TestFauxTrunkProxySurvivesLoss 全 TLS + 物理链路 ~12% 丢包，回显仍 100% 正确
#   TestFauxTrunkProxyPlaintext   tls.enabled=false 明文兼容路径
#   TestTrunkUpgradeRequiresAuthorization 未授权会话的 TrunkUpgrade 被拒

# 真实链路集成冒烟（需 root；自动装 RST 抑制规则）
sudo ./test/socks_faux_trunk_kcp/scripts/local_integration_test.sh
```

## 排障

| 现象 | 原因/处理 |
|---|---|
| `fauxtcp: 需要 root 或 CAP_NET_RAW...` | 未以 root 运行；或容器缺少 `CAP_NET_RAW`/`CAP_NET_ADMIN` |
| `fauxtcp: 未找到 iptables/nft...` | 无防火墙工具：装 iptables，或 `manual_firewall: true` 自行配置 |
| 连接建立后立即断（RST） | RST 抑制规则未生效：确认 iptables/nft 可用，或手工加规则 |
| 远程地址报 `握手超时`（501003） | 先读错误里的诊断字段：`收到 0 个报文` = 服务端未运行/安全组未放行/**本端源端口在 `ip_local_port_range` 之外**；`发送失败 >0` = 本机 raw socket 发包失败；`有回包但无 SYN+ACK` = 服务端缺 RST 抑制。**按「跨机部署 → 排查决策树」六步走**，并用 `telnet <服务端> 18099` 连正在运行的 faux 服务端做分半实验 |
| 内核 TCP 能通、faux 连不上，且客户端一个入向包都没有 | 见「真机复盘：源端口不在本机 ephemeral 范围」：`sysctl net.ipv4.ip_local_port_range` 对比抓包里自己 SYN 的源端口；其次看服务端 `Out [S.]` 的源地址（`reply_src` 是否误填公网 IP） |
| `invalid KCP segment length` | `faux_tcp.mss` 小于 KCP 线上包（半帧错位）：调大 MSS（默认 1448 安全） |
| 控制面 RPC 偶发失败 | 控制通道已叠 KCP 可靠层；若仍失败多为 faux_tcp 物理层问题（权限/防火墙） |
| `tls handshake timeout` / `certificate` 报错 | 证书未生成或 client/server 证书不属同一 CA：按「安全说明」重新生成并拷贝 |
| `trunk ... not authorized` | 物理连接的 TrunkUpgrade 早于控制通道认证：检查客户端是否先完成 Auth（InitTrunk 顺序） |
