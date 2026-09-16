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

trunk_kcp 默认使用快速模式：

```go
kcp.NoDelay(1, 10, 2, 1)
kcp.WndSize(1024, 1024)
```

可以根据网络环境调整：

```go
// 普通模式（延迟较高，CPU 开销低）
kcp.NoDelay(0, 10, 0, 1)
kcp.WndSize(1024, 1024)

// 快速模式（默认，平衡延迟和 CPU）
kcp.NoDelay(1, 10, 2, 1)
kcp.WndSize(1024, 1024)

// 超快模式（最低延迟，CPU 开销高）
kcp.NoDelay(1, 5, 2, 1)
kcp.WndSize(256, 256)
```

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
