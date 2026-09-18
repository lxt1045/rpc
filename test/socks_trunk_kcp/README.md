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
