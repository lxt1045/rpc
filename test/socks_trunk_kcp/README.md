# socks_trunk_kcp

基于 `trunk_kcp` 的 SOCKS/HTTP 代理模块，控制面使用 RPC/Proto，数据面使用 `trunk_kcp.VirtualConn`。

## 结构

- `cmd/socks-trunk-kcp-client`：客户端入口
- `cmd/socks-trunk-kcp-server`：服务端入口
- `pb/`：protobuf 定义
- `filesystem/static/conf/default.yml`：编译期嵌入配置
- `scripts/local_integration_test.sh`：本地集成测试

## 运行

修改 `filesystem/static/conf/default.yml` 后重新构建：

```bash
go build ./test/socks_trunk_kcp/cmd/...
./socks-trunk-kcp-server
./socks-trunk-kcp-client
```

## 动态底层连接

- `RemoveTrunkConn(id)`：关闭客户端对应的底层连接，服务端通过 EOF 清理同一条连接；两端的物理连接 ID 不保证一致，不能直接跨端使用
- `trunk_kcp.AddConn(rw)` / `TrunkUpgrade` RPC：自动补充新的底层连接
- client `MaintainTrunk` 会周期性检查底层连接数量并自动补足

## 连接健康检测

客户端实现了两种连接健康检测机制，自动维护高质量的连接池：

### 空闲连接检测

当某条底层连接 60 秒内没有收到任何数据时，自动断开并创建新连接替换。这可以：
- 避免长时间无流量的连接占用资源
- 防止 NAT 超时导致的"僵尸连接"
- 保持连接池活跃

### 慢速连接检测

每 30 秒检测一次所有连接的传输速率，对于创建时间超过 30 分钟的连接，如果其发送或接收速率低于所有连接平均速率的 10%，则自动断开并创建新连接替换。这可以：
- 自动识别并剔除网络质量差的路径
- 保持连接池的高吞吐量
- 避免个别慢速连接影响整体性能

速率统计会每 30 秒打印到日志中，包括每条连接的：
- 连接 ID 和创建时间
- 发送/接收字节数
- 发送/接收速率（字节/秒）
- 所有连接的平均速率

**注意**：这两种检测机制仅在客户端启用，服务端只需被动接受连接。

## 虚拟连接回收

客户端从空闲 ID 中分配虚拟连接，双方交换 `CmdCloseConn` 后才允许复用 ID，避免旧数据进入新请求。`max_virtual_conn` 默认 256，限制同时占用（包括等待关闭确认）的 ID 数量，不限制累计请求数；全部占用时新请求会失败并记录日志。

部署本次连接回收修复时，必须同时更新 client 和 server。帧格式不变，但旧版本不会回复关闭确认，混用版本无法正常回收连接。浏览器断开或转发结束时会关闭两端并回收转发协程。

## 测试

```bash
go test -count=1 ./test/socks_trunk_kcp/... ./trunk_kcp
test/socks_trunk_kcp/scripts/local_integration_test.sh
```

### 单元测试说明

- `TestOnDemandVirtualConn`：验证按需创建功能，只有被使用的虚拟连接 ID 才会触发回调
- `TestNoCallbackBeforeUse`：验证在虚拟连接被使用之前不会预先创建任何 goroutine
- `auth_test.go`：认证流程测试
- `protocol_test.go`：地址协议序列化测试
- `TestHTTPProxyRepeatedConnections`：连续 300 次 HTTP CONNECT，保留活跃 ID 并轮换底层连接，验证回绕后仍可代理
- `TestRelayClosesBothEnds`：验证浏览器断开及取消上下文后释放两端连接
- `TestRemovePhysicalConnWithDifferentRemoteID`：验证两端物理连接编号不同时只删除对应连接

## 代码架构

```text
                     SOCKS5 / HTTP CONNECT 用户
                                  │
                                  ▼
                        ┌─────────────────────┐
                        │   cmd/client main    │
                        └─────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │       peer_client.go       │
                    │ - RunSocks / RunHTTPProxy  │
                    │ - InitTrunk / MaintainTrunk│
                    └─────────────┬─────────────┘
                                  │
                     ┌────────────▼─────────────┐
                     │   RPC 控制面（pb.SocksSvc） │
                     │ Auth / TrunkUpgrade /     │
                     │ TrunkStart / RemoveConn   │
                     └────────────┬─────────────┘
                                  │ 底层连接
                     ┌────────────▼─────────────┐
                     │      trunk_kcp.TrunkKCP   │
                     │  多个 io.ReadWriteCloser  │
                     │  KCP 可靠传输 / 聚合        │
                     └────────────┬─────────────┘
                                  │ VirtualConn
                     ┌────────────▼─────────────┐
                     │      peer/session 服务端  │
                     │ 解析地址头 -> Dial 目标    │
                     └────────────┬─────────────┘
                                  ▼
                           目标 TCP 服务
```

### 文件职责

| 文件 | 职责 |
|------|------|
| `cmd/socks-trunk-kcp-client/main.go` | 客户端入口、启动连接、重试、优雅退出 |
| `cmd/socks-trunk-kcp-server/main.go` | 服务端入口、TLS/RPC 监听、会话清理 |
| `config.go` | 配置结构、默认值、ACL |
| `protocol.go` | VirtualConn 首包协议：长度前缀 + protobuf 地址 |
| `peer_client.go` | 客户端代理逻辑、Trunk 建立/补链 |
| `session.go` | 服务端会话、认证、TrunkUpgrade/Start/Remove |
| `copy.go` | 双向 Copy 与关闭传播 |
| `pb/service.proto` | RPC/Proto 接口定义 |
| `trunk_kcp` | 底层 KCP 链路聚合模块 |

## 核心算法

### 1. 建连流程

1. client 启动 2 条控制 RPC 连接，每条先 `Auth`。
2. client 建立 N 条底层连接，每条先 `Auth`，再调用 `TrunkUpgrade`。
3. 服务端把 Upgrade 得到的原始连接收进当前 session。
4. client 通过控制 RPC 调用 `TrunkStart`。
5. 服务端确认 N 条连接已到齐后：
   - 创建 `trunk_kcp.NewTrunkKCP(conv, onNewConn, rws...)`，传入回调函数
   - `go trunk.Run(ctx)`
   - **按需创建**：只有当客户端真正使用某个虚拟连接 ID 时，回调才会被触发
6. client 也创建同一个 `conv` 的 `TrunkKCP` 并 `Run`（客户端不需要回调）。

### 2. 代理数据流

```text
用户 TCP
   │
   ▼
SOCKS5/HTTP CONNECT 解析得到 addr
   │
   ▼
TrunkKCP.GetConn(virtualID)
   │
   ▼
WriteOpenHeader: [4B len][protobuf addr/head]
   │
   ▼
server serveVirtualConn:
   ReadOpenHeader
   → CheckACL(addr)
   → net.Dial(addr)
   → 若 head 非空先发给目标
   → relay(vconn, remote)
```

### 3. 底层连接动态剔除与补充

剔除：

```text
发起方检测到连接异常
   │
   ├─ trunk.CloseWriteConn(id)   // 发起方先关闭发送
   ├─ RPC TrunkRemoveConn(id)    // 通知对端
   │      └─ 对端 CloseWriteConn(id)
   │      └─ 对端 RemoveConn(id)
   └─ trunk.RemoveConn(id)       // 发起方最终移除
```

补充：

```text
client MaintainTrunk 周期检查
   │
   └─ ConnCount() < target
        │
        ├─ 建立新底层 RPC/Upgrade 连接
        ├─ 服务端 TrunkUpgrade 动态 trunk.AddConn(rw)
        └─ 客户端 trunk.AddConn(raw)
```

### 4. 并发模型

- 每个用户连接独占一个 `VirtualConn`，互不共享读写锁。
- `VirtualConn.Read` / `Write` 各自有锁，避免同连接并发读写竞态。
- **按需创建**：server 通过回调机制，仅在客户端首次使用某个虚拟连接 ID 时才创建对应的处理 goroutine，而非预先为所有可能的连接（如 1-256）创建协程。这大幅减少了资源占用。
- 物理连接层每个 `activeConn` 有独立 send/recv loop；单条断开只影响该条。
- 所有 RPC handler 与 listener 都带有 recover / 关闭保护。

### 5. 优雅退出

```text
SIGTERM/SIGINT
   │
   ├─ cancel context
   ├─ 关闭 listener
   ├─ 关闭 TrunkKCP（虚拟连接、物理连接）
   ├─ CloseAllSessions()
   └─ errgroup.Wait()
```

## trunk_kcp 动态能力

### 目标

在 `TrunkKCP` 运行期间，底层物理连接可以：

- 动态加入：`AddConn(rw)`
- 动态剔除：`RemoveConn(id)`
- 优雅半关闭：`CloseWriteConn(id)`
- 查询当前数量：`ConnCount()`

这样上层代理在 N 条底层连接中某一条断开时，不需要重启整个 Trunk。

### 内部状态

```text
TrunkKCP
├── kcp             // 全局唯一 KCP 实例，所有物理连接共享
├── sendChan        // KCP output -> 物理连接发送协程
├── recvChan        // 物理连接接收协程 -> KCP input
├── active          // map[int]*activeConn
│   └── activeConn  // 每条底层连接的运行时状态
│       ├── id      // 在本 TrunkKCP 内的编号
│       ├── rw      // 原始 RPC/Upgrade 连接
│       └── stop    // 停止该连接 send/recv loop 的信号
└── connMu          // 保护 active map
```

### 动态加入算法

```text
AddConn(rw)
  ├─ 加锁检查 Trunk 未关闭
  ├─ 分配新 id
  ├─ 创建 activeConn
  ├─ 写入 active map
  ├─ 解锁
  ├─ go sendLoop(ac)   // 从 sendChan 取数据并写 rw
  └─ go recvLoop(ac)   // 从 rw 读数据并喂 recvChan
```

### 动态剔除算法

```text
RemoveConn(id)
  ├─ 加锁
  ├─ 从 active map 删除
  ├─ close(ac.stop)     // 通知该连接的 send/recv loop 退出
  ├─ 解锁
  └─ ac.rw.Close()
```

### 优雅剔除流程

```text
发起方
  │
  ├─ trunk.CloseWriteConn(id)   // 先关闭本地发送方向
  ├─ RPC TrunkRemoveConn(id)    // 通知对端
  │    └─ 对端 trunk.CloseWriteConn(id)
  │    └─ 对端 trunk.RemoveConn(id)
  └─ trunk.RemoveConn(id)       // 最后本地剔除
```

### 断线自动剔除

```text
sendLoop / recvLoop
  ├─ 读或写返回错误
  ├─ 记录 warn 日志
  └─ trunk.RemoveConn(id)
```

单条物理连接出错时，只会移除该 `activeConn`，不会关闭整个 `TrunkKCP`。

### 自动补足

```text
client MaintainTrunk
  │
  └─ 每 1 秒检查
      │
      └─ ConnCount() < target
          │
          ├─ 新建 N 条 RPC/Upgrade 连接
          ├─ 服务端 TrunkUpgrade 收到后 trunk.AddConn(rw)
          └─ 客户端 trunk.AddConn(raw)
```

### 运行退出条件

```text
TrunkKCP.Run 在以下情况退出：
  ├─ ctx 被取消
  ├─ Trunk.Close() 被调用
  └─ ConnCount() == 0   // 所有底层连接都丢失
```

只要还有至少 1 条活跃底层连接，`Run` 就会保持运行，并允许继续 AddConn / RemoveConn。

## 线上流量放大问题（KCP over TCP 的伪重传）

### 现象

c/s 建立连接后，只有一个浏览器在下载：Windows 任务管理器显示网卡占用 ~30Mbps，
但浏览器下载速度只有 ~1.3MB/s（≈10.4Mbps）。线上字节约为有效载荷的 2.9 倍。

### 实测数据

在 `trunk_kcp/wire_amplification_test.go` 中用字节计数器包住物理连接，
逐段解析 KCP 包（16MiB 下载，4 条物理连接，pipe 模拟链路）：

| 链路场景 | 线上/载荷 | KCP 重复重发段占比 | 有效吞吐 |
| --- | --- | --- | --- |
| 回环（无延迟抖动） | 1.02x | 0% | 88 MB/s |
| 50ms 延迟 + 40ms 抖动，**零丢包** | 1.36~1.52x | 25~32% | 6.3 MB/s |
| 50ms + 80ms 抖动 + 2% 丢包 | 1.88x | **45%** | 2.4 MB/s |

注意第二行：链路完全不丢包，KCP 依然重发了约 1/3 的段——纯伪重传。
链路越差放大越大，真实国际链路（更大抖动 + QoS 丢包）达到 2.9x 与观察吻合。
重复段到达网卡照样计数（任务管理器虚高），KCP 接收端丢弃重复段（浏览器只算有效数据）。

### 根因

本模块的物理连接是 TLS/TCP（可靠、不丢包的流），而 KCP 是为 UDP/有损链路
设计的 ARQ 协议。库默认参数 `NoDelay(1, 10, 32, 1)`（见 `trunk_kcp.go`）：

1. **RTO 太敏感**：`nodelay=1` 使 KCP 最小 RTO = 30ms（kcp-go `IKCP_RTO_NDL`）。
   trunk 把 KCP 包经 `sendChan` 轮询分发到多条 TCP 连接，每条连接排队延迟不同，
   ACK 回程也要排队，尾部延迟频繁超过 RTO，KCP 于是重发"其实没丢"的段。
2. **`nc=1` 关闭拥塞控制**：发送方始终保持 1024 段（~1.4MB）窗口打满 →
   排队更久 → RTO 误判更多 → 重发段又占用瓶颈带宽 → 正反馈，吞吐被压垮。
3. KCP 重发在应用层，底层 TCP 对它再做一次可靠性传输，双重保险、双倍浪费。

**2026-09-23 补充：窗口本身才是主要杠杆（有定量实验）**。在限速出口（24 Mbps、
RTT 40ms、排队上限 50ms）上用 `trunk_kcp/ratelimit_test.go` 扫窗口：

| 窗口（段） | 线上放大 | 重传段占比 | 有效吞吐 | 链路利用 |
| --- | --- | --- | --- | --- |
| 1024（库默认，≈8×BDP） | 3.23~3.84x | 68% | 2.3 MB/s | 101% |
| 128（≈BDP） | 1.17~1.26x | 11~17% | 2.4~2.6 MB/s | 98~101% |
| 32（<BDP） | 1.07x | 4% | 0.9 MB/s | 32% |
| 1024 + `nc=0`（开拥塞控制） | 1.05x | 2% | 0.1~0.2 MB/s | 5~8% |

即：**窗口 ≫ BDP 时排队溢出→重传放大，窗口 ≪ BDP 时浪费链路；kcp-go 自带的拥塞控制
爬升太慢、填不满链路**。所以限速出口上应"关拥塞控制 + 按 BDP 设窗口"：

```yaml
trunk_kcp:
  # 按 BDP 显式设置发送窗口（两端一致）；不设则用库默认 1024 段（可能偏大）
  kcp_sndwnd: 100   # ≈ 30Mbps × 40ms / 1376B ≈ 100 段
  # 接收窗口只做缓冲，**不要跟着一起调小**：真机把收发都设成 128 段后，
  # 应用一停顿就关窗、发送端被饿死（只占 7Mbps、下载 300kB/s）
  kcp_rcvwnd: 1024
  # 长 RTT 链路建议 minRTO 100ms，减少伪重传：
  # kcp_nodelay: 0
```


调参对比（同一恶劣链路：50ms + 80ms 抖动 + 2% 丢包）：

| 参数 | 线上/载荷 | 吞吐 | 评价 |
| --- | --- | --- | --- |
| `NoDelay(1,10,32,1)`（库默认） | 1.88x | 2.6 MB/s | — |
| `NoDelay(0,40,0,1)`（minRTO=100ms） | 1.54x | 2.3 MB/s | 有缓解 |
| `NoDelay(1,10,32,0)`（开拥塞控制） | 1.50x | 0.2 MB/s | 吞吐崩溃 |
| `NoDelay(0,40,0,0)`（普通模式） | — | 卡死 | KCP cwnd 在持续丢包下饿死 |

调参只能缓解，不能根治——ARQ 层与 TCP 层的可靠性语义本质冲突。

### 缓解：KCP 参数配置化

`TrunkKCPConfig` 新增 4 个可选项（对应 `kcp.KCP.NoDelay` 的四个参数），
库侧通过 `TrunkKCP.SetNoDelay(nodelay, interval, resend, nc)` 应用，
`default.yml` 的 `trunk_kcp` 节下配置，**客户端与服务端必须一致**：

```yaml
trunk_kcp:
  conv: 123456789
  min_conns: 8
  max_conns: 16
  max_virtual_conn: 8096
  kcp_nodelay: 0   # 0: 普通模式 minRTO=100ms；1: nodelay 模式 minRTO=30ms（库默认，仅建议 UDP 底层）
  kcp_interval: 20 # KCP 内部时钟 ms，>=10
  kcp_resend: 0    # 快速重传阈值，0 关闭
  kcp_nc: 1        # 1: 关闭 KCP 拥塞控制；0: 开启
```

不配置（或某项不配置）时保持库默认值 `(1, 10, 32, 1)`。

### 建议（按优先级）

1. **底层是 TCP/TLS 时优先使用 `test/socks_trunk`（TCP 版 trunk）**；
   `trunk_kcp` 的设计前提是 UDP/有损底层，跑在 TCP 上没有收益还会引入伪重传病理。
2. 必须使用本模块时（例如计划切 UDP 底层），启用上面这组 `kcp_*` 配置
   （minRTO 回到 100ms，可显著减少伪重传）。
3. 物理连接数不宜过多（更多连接 = 更大的分发抖动）；服务端开启 BBR。
4. 排查手段：Windows `netstat -s -p tcp` 看 TCP 重传；
   `trunk_kcp/wire_amplification_test.go` 里的 KCP 段计数器可直接复用做线上验证。
