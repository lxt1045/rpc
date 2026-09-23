# trunk_kcp

基于 KCP 协议的链路聚合模块 / KCP-based Link Aggregation Module

## 概述 / Overview

`trunk_kcp` 是一个基于 KCP 协议的链路聚合实现，将多个网络连接聚合成一个逻辑连接，通过 KCP 提供可靠传输保障。与 `trunk` 模块不同，`trunk_kcp` 使用 KCP 协议在应用层提供可靠性保证，适用于需要在不可靠网络上进行链路聚合的场景。

`trunk_kcp` is a KCP-based link aggregation implementation that merges multiple network connections into one logical connection with reliable transmission guaranteed by KCP protocol. Unlike the `trunk` module, `trunk_kcp` provides reliability at the application layer using KCP, suitable for scenarios requiring link aggregation over unreliable networks.

## 特性 / Features

- **多连接聚合** / **Multi-Connection Aggregation**: 将多个物理连接聚合成一个逻辑连接
- **KCP 可靠传输** / **KCP Reliable Transmission**: 使用 KCP 协议保证数据包顺序和可靠传输
- **虚拟连接** / **Virtual Connections**: 支持在同一个 trunk 上复用多个虚拟连接
- **轮询发送** / **Round-Robin Sending**: 在多个物理连接上轮询发送数据包，实现带宽聚合
- **并发接收** / **Concurrent Receiving**: 每个物理连接独立接收数据，提高吞吐量
- **完整分包接收** / **Complete Packet Reception**: 接收端根据 KCP 头部的长度字段拆分半包和粘包，只把完整 KCP 段送入 KCP

## 架构 / Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Application                          │
│                    (Read/Write Interface)                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                       TrunkKCP                               │
│  ┌─────────────────┐         ┌─────────────────┐           │
│  │  Sender KCP     │         │  Receiver KCP   │           │
│  │  (Encode)       │         │  (Decode)       │           │
│  └────────┬────────┘         └────────▲────────┘           │
│           │                            │                     │
│           ▼                            │                     │
│  ┌─────────────────┐         ┌─────────────────┐           │
│  │   sendChan      │         │   recvChan      │           │
│  │ (KCP packets)   │         │ (KCP packets)   │           │
│  └────────┬────────┘         └────────▲────────┘           │
└───────────┼──────────────────────────────┼──────────────────┘
            │                              │
            ▼                              │
   ┌────────────────┐           ┌────────────────┐
   │  Conn Writer   │           │  Conn Reader   │
   │  Goroutines    │           │  Goroutines    │
   │  (Round-robin) │           │  (Collect all) │
   └────────┬───────┘           └────────▲───────┘
            │                            │
            ▼                            │
   ┌────────────────────────────────────────────┐
   │    Multiple Network Connections            │
   │    (TCP / UDP / QUIC / KCP / ...)         │
   └────────────────────────────────────────────┘
```

### 工作原理 / How It Works

1. **发送路径** / **Send Path**:
   - 应用数据通过 `VirtualConn.Write()` 写入
   - 添加 ConnID 头部后发送到发送端 KCP
   - KCP 将数据分片并调用 output 回调
   - output 回调将 KCP 数据包写入 `sendChan`
   - 发送协程轮询从 `sendChan` 读取并发送到物理连接

2. **接收路径** / **Receive Path**:
   - 每个物理连接的接收协程读取字节流
   - 根据 KCP 头部 `20~24` 字节中的小端长度字段拆出完整 KCP 段
   - 只有完整 KCP 段才会写入 `recvChan`，半包会留在接收缓存中，粘包会被拆成多个段
   - KCP 输入协程从 `recvChan` 读取并喂给接收端 KCP
   - 从 KCP 读取完整数据，解析 ConnID 并分发到虚拟连接
   - 应用通过 `VirtualConn.Read()` 读取数据

3. **KCP 更新** / **KCP Update**:
   - 独立的更新协程每 10ms 调用一次 `KCP.Update()`
   - 同一个 KCP 实例负责双向数据的发送和接收

## 业务帧格式 / Application Frame Format

KCP 负责可靠传输、重传和顺序恢复，因此 `trunk_kcp` 的业务头不包含 `Idx`。业务数据在 KCP 数据中使用以下格式：

```text
普通数据帧: [Len uint16][ConnID uint16][Body Len 字节]
命令数据帧: [Len uint16][ConnID|0x8000 uint16][Cmd uint16][Body Len 字节]
```

字段使用小端序。`Len` 表示业务体 `Body` 的字节数；普通帧头为 4 字节，命令帧头为 6 字节。`ConnID` 的最高位 `0x8000` 是命令标志，低 15 位保存虚拟连接编号，因此有效 `ConnID` 范围为 `0` 到 `32767`。

物理连接承载的是 KCP 段。KCP 段头固定为 24 字节，段长度位于偏移 `20` 到 `24` 字节，接收协程会先依据该长度组装完整 KCP 段，再交给 KCP 输入协程。

该业务帧格式与 `trunk` 不兼容；使用 `trunk_kcp` 的两端必须运行相同的头部格式。

## 使用示例 / Usage Example

```go
package main

import (
    "context"
    "fmt"
    "net"
    
    "github.com/lxt1045/rpc/trunk_kcp"
)

func main() {
    ctx := context.Background()
    
    // 服务端：创建多个监听器
    ln1, _ := net.Listen("tcp", ":8001")
    ln2, _ := net.Listen("tcp", ":8002")
    ln3, _ := net.Listen("tcp", ":8003")
    
    // 接受连接
    conn1, _ := ln1.Accept()
    conn2, _ := ln2.Accept()
    conn3, _ := ln3.Accept()
    
    // 创建 TrunkKCP（conv 必须两端一致）
    trunk := trunk_kcp.NewTrunkKCP(0x12345678, conn1, conn2, conn3)
    go trunk.Run(ctx)
    
    // 获取虚拟连接（ConnID = 1）
    vconn := trunk.GetConn(1)
    
    // 使用虚拟连接进行读写
    buf := make([]byte, 1024)
    n, err := vconn.Read(buf)
    if err != nil {
        fmt.Println("Read error:", err)
        return
    }
    
    fmt.Printf("Received: %s\n", buf[:n])
    
    // 写回响应
    vconn.Write([]byte("Hello from server"))
    
    // 关闭连接
    vconn.Close()
    trunk.Close()
}
```

### 客户端示例 / Client Example

```go
package main

import (
    "context"
    "fmt"
    "net"
    
    "github.com/lxt1045/rpc/trunk_kcp"
)

func main() {
    ctx := context.Background()
    
    // 连接到服务端的多个端口
    conn1, _ := net.Dial("tcp", "server:8001")
    conn2, _ := net.Dial("tcp", "server:8002")
    conn3, _ := net.Dial("tcp", "server:8003")
    
    // 创建 TrunkKCP（conv 必须与服务端一致）
    trunk := trunk_kcp.NewTrunkKCP(0x12345678, conn1, conn2, conn3)
    go trunk.Run(ctx)
    
    // 获取虚拟连接
    vconn := trunk.GetConn(1)
    
    // 发送数据
    vconn.Write([]byte("Hello from client"))
    
    // 接收响应
    buf := make([]byte, 1024)
    n, _ := vconn.Read(buf)
    fmt.Printf("Received: %s\n", buf[:n])
    
    vconn.Close()
    trunk.Close()
}
```

## API 文档 / API Documentation

### TrunkKCP

#### `NewTrunkKCP(conv uint32, rws ...io.ReadWriteCloser) *TrunkKCP`

创建一个新的 TrunkKCP 实例。

**参数** / **Parameters**:
- `conv`: KCP conversation ID，必须在两端保持一致
- `rws`: 物理连接列表，至少需要一个连接

**返回** / **Returns**: TrunkKCP 实例

#### `Run(ctx context.Context) error`

启动 TrunkKCP 的所有协程，包括发送、接收、KCP 更新等。此方法会阻塞直到 context 取消、物理连接关闭或发生错误；退出时会关闭关联的物理连接和虚拟连接。

#### `GetConn(connID uint16) *VirtualConn`

获取或创建指定 ConnID 的虚拟连接。

**参数** / **Parameters**:
- `connID`: 虚拟连接的 ID，取值范围 0-32767

**返回** / **Returns**: VirtualConn 实例

#### `OpenConn(maxConns int) (*VirtualConn, error)`

在 `1..maxConns` 中原子分配空闲 ID，跳过活跃连接及等待关闭确认的连接；没有空闲 ID 时返回错误。`maxConns` 范围为 1-32767。同一 trunk 仅应由一端使用此接口分配 ID，另一端通过接收回调处理新连接。

虚拟连接在双方交换 `CmdCloseConn` 后可复用，`GetConn` 会为可复用的 ID 创建新实例。双方都需升级到支持关闭确认的版本；报文头格式未变。

#### `Close() error`

关闭 TrunkKCP，包括所有虚拟连接和物理连接。

#### `SetMtu(mtu int)` / `SetNoDelay(nodelay, interval, resend, nc int)` / `SetWindowSize(sndwnd, rcvwnd int)`

在线调整 KCP 的 MTU、时序参数与收发窗口（负值/0 表示保持当前值）。**必须在跑流量前调用，
且两端配置一致**；`SetWindowSize` 的取法见下方「性能调优 → KCP 参数」——窗口是限速链路上
唯一的流控手段，设错会把带宽全变成重传。

#### `SetIdleTimeout(idleTimeout time.Duration, onIdleConn OnIdleConnFunc)`

配置空闲连接检测。当某条物理连接在指定时间内没有收到任何数据时，会调用回调函数获取新连接进行替换。

**参数** / **Parameters**:
- `idleTimeout`: 空闲超时时间，例如 `60*time.Second`。设置为 0 禁用空闲检测
- `onIdleConn`: 回调函数，参数为空闲连接的 ID，返回新连接用于替换。返回 `nil` 则只移除旧连接不替换

**使用场景** / **Use Cases**:
- 自动清理长时间无流量的连接
- 保持连接池活跃，避免 NAT 超时
- 仅应在客户端配置，服务端应被动接受连接

**示例** / **Example**:
```go
trunk.SetIdleTimeout(60*time.Second, func(connID int) io.ReadWriteCloser {
    // 创建新连接替换空闲连接
    newConn, err := createNewConnection()
    if err != nil {
        log.Warn("failed to create replacement conn:", err)
        return nil
    }
    return newConn
})
```

#### `SetSlowConnDetection(threshold float64, minAge time.Duration, onSlowConn OnIdleConnFunc)`

配置慢速连接检测。当某条物理连接创建时间超过 `minAge` 且传输速率（发送或接收）低于所有连接平均速率的 `threshold` 倍时，会调用回调函数获取新连接进行替换。

**参数** / **Parameters**:
- `threshold`: 慢速连接阈值，相对于平均速率的比例。例如 `0.1` 表示速率低于平均值的 10% 视为慢速
- `minAge`: 只检测创建时间超过此值的连接，避免误判新建立的连接。例如 `30*time.Minute`
- `onSlowConn`: 回调函数，参数为慢速连接的 ID，返回新连接用于替换。返回 `nil` 则只移除旧连接不替换

**使用场景** / **Use Cases**:
- 自动替换性能下降的连接
- 识别并剔除网络质量差的路径
- 保持连接池的高吞吐量
- 仅应在客户端配置，服务端应被动接受连接

**检测机制** / **Detection Mechanism**:
- 每 30 秒采样一次所有连接的传输速率
- 计算每条连接的发送速率和接收速率（字节/秒）
- 计算所有连接的平均发送速率和平均接收速率
- 打印每条连接的速率统计到日志
- 对于创建时间超过 `minAge` 的连接，如果任一方向的速率低于平均速率的 `threshold` 倍，则替换

**示例** / **Example**:
```go
// 连接创建超过30分钟且速率低于平均值的10%则替换
trunk.SetSlowConnDetection(0.1, 30*time.Minute, func(connID int) io.ReadWriteCloser {
    newConn, err := createNewConnection()
    if err != nil {
        log.Warn("failed to create replacement conn:", err)
        return nil
    }
    return newConn
})
```

**速率统计日志** / **Rate Statistics Logs**:

每 30 秒输出每条连接的统计信息：
```json
{"level":"info","conn_id":5,"age":"35m12s","send_bytes":1048576,"recv_bytes":2097152,
 "send_rate_bps":34952.5,"recv_rate_bps":69905.1,"avg_send_rate_bps":52428.8,
 "avg_recv_rate_bps":104857.6,"message":"conn rate stats"}
```

检测到慢速连接时输出警告：
```json
{"level":"warn","conn_id":5,"age":"35m12s","send_rate":3495.2,"recv_rate":6990.5,
 "avg_send_rate":52428.8,"avg_recv_rate":104857.6,"slow_send":true,"slow_recv":true,
 "message":"slow connection detected, replacing"}
```

**注意事项** / **Notes**:
- 空闲检测和慢速检测可以同时启用，互不干扰
- 空闲检测基于接收时间，慢速检测基于双向传输速率
- 慢速检测只针对创建时间超过 `minAge` 的连接，避免误判
- 当连接数较少（1-2 条）时，平均速率计算可能不准确，建议至少保持 3 条以上连接
- 速率统计每 30 秒采样一次，计算的是最近 30 秒的平均速率

#### `AddConn(rw io.ReadWriteCloser) (int, error)`

动态添加一条物理连接到运行中的 TrunkKCP。返回该连接的内部 ID。

#### `RemoveConn(id int) error`

移除并关闭指定 ID 的物理连接。如果移除后没有剩余连接，会关闭整个 TrunkKCP。

#### `ConnCount() int`

返回当前活跃的物理连接数量。

### VirtualConn

#### `Write(p []byte) (n int, err error)`

向虚拟连接写入数据。数据会自动添加 ConnID 头部，经过 KCP 编码后发送到物理连接。

#### `Read(p []byte) (n int, err error)`

从虚拟连接读取数据。返回的数据已经过 KCP 解码和 ConnID 解析。

#### `Close() error`

关闭虚拟连接，会向对端发送关闭命令。

## 与 trunk 的对比 / Comparison with trunk

| 特性 | trunk | trunk_kcp |
|------|-------|-----------|
| **可靠性** | 依赖底层连接 | KCP 协议保证 |
| **丢包恢复** | 无自动重传 | KCP 自动重传 |
| **顺序保证** | 需要 `Idx` 重排序 | KCP 保证顺序，无需 `Idx` |
| **延迟** | 低 | 中等（KCP 开销）|
| **吞吐量** | 高 | 中等（KCP 开销）|
| **适用场景** | 可靠网络 | 不可靠网络 |
| **CPU 开销** | 低 | 中等 |

### 何时使用 trunk_kcp / When to Use trunk_kcp

**适合使用 trunk_kcp 的场景**:
- 底层网络不可靠，有丢包或乱序
- 需要在 UDP 等不可靠协议上实现链路聚合
- 需要应用层可靠性保证
- 可以接受适度的延迟和 CPU 开销

**适合使用 trunk 的场景**:
- 底层连接已经可靠（TCP, QUIC 等）
- 追求最低延迟和最高吞吐量
- CPU 资源受限
- 内网环境，网络质量好

## 性能调优 / Performance Tuning

### KCP 参数 / KCP Parameters

**窗口（`WndSize`）必须按带宽时延积 BDP 设**——限速链路上最关键、也最容易踩的一项：

```
sndwnd ≈ 链路速率(B/s) × RTT(s) / (mtu-24)      # 再留 20%~50% 余量；两端一致
```

KCP 的窗口就是"在途数据上限"，而关闭拥塞控制（`nc=1`，默认）时它是**唯一**的流控手段。
两种踩法都实测过：

| 配置 | 现象 |
| --- | --- |
| 窗口 1024 段（库默认，≈8×BDP） | 每轮把限速/整形队列灌爆 → 丢包 → 重传占满链路：**放大 3.2~3.8x、重传 68%**，有效吞吐只有链路的 60% |
| 窗口 128 段（远小于长 RTT 链路的 BDP） | 发送端被自己饿死：真机只占 **7Mbps**、下载 **300kB/s**（链路 30Mbps） |

实测（`ratelimit_test.go`：24Mbps、RTT 40ms、排队上限 50ms、4 条物理连接共享出口）：

| 发送窗口（段） | 线上放大 | 重传段占比 | 瓶颈丢包 | 有效吞吐 | 链路利用 |
| --- | --- | --- | --- | --- | --- |
| 1024（库默认） | 3.23~3.86x | 68% | 62~65% | 2.3 MB/s | 101% |
| 512 | 2.24~2.36x | 50% | 43% | 2.4 MB/s | 101% |
| 256 | 1.38~1.65x | 25~34% | 12~19% | 2.3 MB/s | 94~101% |
| **128（≈BDP）** | **1.17~1.26x** | 11~17% | ~0% | **2.4~2.6 MB/s** | **98~101%** |
| 64 | 1.12x | 8~9% | 0% | 1.7~2.0 MB/s | 65~73% |
| 32 | 1.05~1.07x | 2~6% | 0% | 0.9~1.0 MB/s | 32~36% |
| 1024 + 开拥塞控制（`nc=0`） | 1.05x | 2% | 0% | 0.1~0.2 MB/s | **5~8%** |

结论：

1. **按 BDP 设 `sndwnd`**，两端一致（生效值取较小者）；不要用"历史最高速率"或 RTT
   去自动猜——发送端一次 flush 会把整窗突发出去，突发自身的串行化时间会被算进后几个
   包的 RTT，把窗口一路估小。取法就是上面的公式（`ping` 出 RTT）。
2. **`rcvwnd` 不要跟着调小**：它是接收缓冲而不是限速，调小后应用（下游 TCP/浏览器）
   稍有停顿就关窗，把发送端饿死。默认 1024 通常不用动。
3. **`nc=0`（kcp-go 自带拥塞控制）不是解**：cwnd 爬升太慢，实测只用 5~8% 链路。
4. **长 RTT 链路用 `nodelay=0`**（minRTO 100ms）：30ms 的 minRTO 会把还在路上的包判成
   丢包（伪重传）；实测 150ms RTT 场景下 100ms minRTO 明显更优。短 RTT/不可靠底层用
   默认 `nodelay=1` 即可。

```go
t.SetWindowSize(128, 1024)   // 例：30Mbps × 40ms ≈ 150KB ≈ 128 段（mtu 1200）
t.SetNoDelay(0, 10, 32, 1)   // 长 RTT：minRTO 100ms，减少伪重传
```

**实验性自动调窗**：`SetAutoWindow(maxWnd, rcvWnd)` 会按"线上放大率"（线上字节/对端
累计确认字节）做 AIMD：放大 ≤1.5 加性增、>1.5 乘性减，因此不需要知道 RTT 与链路速率。
40ms 无损限速链路上实测收敛良好（利用 ~100%、放大 ~1.2~1.5）；长 RTT/有损链路上仍会
振荡，**生产先用固定窗口按 BDP 设**。`Stats()` 暴露 `SndWnd/RcvWnd/Backlog/Push/Acked/
Retrans/WireBytes/SRTT/Amp` 等指标，`TrunkKCP` 每 3s 会打一行 debug 自诊断，可直接看到
"窗口该多大"的依据。

### 通道大小 / Channel Size

默认通道大小为 1024，可以根据实际情况调整：

```go
sendChan: make(chan []byte, 2048)  // 增加发送缓冲
recvChan: make(chan []byte, 2048)  // 增加接收缓冲
```

### 更新频率 / Update Frequency

默认每 10ms 更新一次 KCP，可以根据需求调整：

```go
// 更快的响应（CPU 开销高）
ticker := time.NewTicker(5 * time.Millisecond)

// 更低的 CPU 开销（响应慢）
ticker := time.NewTicker(20 * time.Millisecond)
```

## 限制和注意事项 / Limitations and Notes

1. **Conv ID 一致性**: 通信双方必须使用相同的 `conv` 参数
2. **ConnID 范围**: 虚拟连接 ID 范围为 0-32767（15 位）
3. **MTU 限制**: 默认 MTU 为 1400 字节，大数据会被 KCP 自动分片
4. **内存使用**: 每个 TrunkKCP 实例维护一个双向 KCP 实例、多个通道和虚拟连接队列
5. **并发安全**: 所有 API 都是并发安全的
6. **关闭顺序**: 建议先关闭虚拟连接，再关闭 TrunkKCP

## 测试 / Testing

运行测试：

```bash
# 运行所有测试
go test ./trunk_kcp -v

# 运行特定测试
go test -run TestTrunkKCP_SingleConn ./trunk_kcp -v

# 基准测试
go test -bench . ./trunk_kcp
```

## 许可证 / License

与主项目相同的许可证。

## 参考资料 / References

- [KCP 协议](https://github.com/skywind3000/kcp)
- [kcp-go 实现](https://github.com/xtaci/kcp-go)
- [trunk 模块](../trunk/)
