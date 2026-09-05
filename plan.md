# trunk_kcp 实现计划 / trunk_kcp Implementation Plan

## 项目目标 / Project Goals

实现一个基于 KCP 协议的链路聚合模块 `trunk_kcp`，将多个网络连接聚合成一个逻辑连接，通过 KCP 提供可靠传输保障。

Implement a KCP-based link aggregation module `trunk_kcp` that merges multiple network connections into one logical connection with reliable transmission guaranteed by KCP.

---

## 阶段一：分析和修复现有 trunk 的 BUG / Phase 1: Analyze and Fix Existing trunk Bugs

### 1.1 问题调查 / Bug Investigation

**任务 / Tasks:**
- [ ] 运行现有的 trunk 测试用例，记录失败情况
  - `go test -run '^TestTrunk0$' -count=1 -timeout 60s ./trunk`
  - `go test -run '^TestTrunk$' -count=1 -timeout 60s ./trunk`
- [ ] 分析 `trunk/trunk.go` 中的并发安全问题
  - 检查 `Conn.Write()` 和 `Trunk.write()` 的锁机制
  - 检查 `SavePackLoop` 中的 `packages` 切片操作
  - 检查 `chReader` 通道的关闭时序
- [ ] 检查 `Conn.Close()` 的竞态条件
  - 验证 `closed.CompareAndSwap` 的使用是否正确
  - 确认 `close(p.chReader)` 只执行一次

**已知问题 / Known Issues:**
1. `trunk/trunk.go:396` - `chReader` 容量满时可能丢包
2. `trunk/trunk.go:412` - `closed.Load()` 后继续写入 `chReader` 可能 panic
3. `trunk/trunk_test.go:88` - `SendEvent` after close 的错误处理

**预期产出 / Expected Output:**
- Bug 分析报告（记录在注释或单独文档中）
- 针对性的修复补丁
- 所有 trunk 测试用例通过

---

## 阶段二：设计 trunk_kcp 架构 / Phase 2: Design trunk_kcp Architecture

### 2.1 核心架构设计 / Core Architecture Design

**设计原则 / Design Principles:**
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

### 2.2 目录结构 / Directory Structure

```
trunk_kcp/
├── trunk_kcp.go       # 主实现文件
├── header.go          # 数据包头定义（复用或扩展 trunk/header.go）
├── conn.go            # 虚拟连接实现
├── trunk_kcp_test.go  # 测试用例
└── README.md          # 文档说明
```

### 2.3 核心数据结构 / Core Data Structures

**TrunkKCP:**
```go
type TrunkKCP struct {
    // 底层物理连接
    rws []io.ReadWriteCloser
    
    // KCP 实例
    sendKCP *kcp.KCP  // 发送端 KCP
    recvKCP *kcp.KCP  // 接收端 KCP
    
    // KCP 锁
    sendLock sync.Mutex
    recvLock sync.Mutex
    
    // 数据通道
    sendChan chan []byte  // KCP 输出 -> 网络发送
    recvChan chan []byte  // 网络接收 -> KCP 输入
    
    // 虚拟连接管理
    conns    []*VirtualConn
    connLock sync.RWMutex
    
    // 写索引（轮询发送）
    wIdx     atomic.Int32
    
    // 控制信号
    done   chan struct{}
    closed atomic.Bool
}
```

**VirtualConn:**
```go
type VirtualConn struct {
    *TrunkKCP
    connID uint16
    
    // 读缓冲
    readBuf  []byte
    readChan chan []byte
    readLock sync.Mutex
    
    // 写缓冲
    writeLock sync.Mutex
    
    // 状态
    closed atomic.Bool
}
```

---

## 阶段三：实现 trunk_kcp 核心功能 / Phase 3: Implement trunk_kcp Core Functions

### 3.1 创建目录和基础文件 / Create Directory and Base Files

**任务 / Tasks:**
- [ ] 创建 `trunk_kcp/` 目录
- [ ] 复制 `trunk/header.go` 到 `trunk_kcp/header.go`（如需修改再调整）
- [ ] 创建 `trunk_kcp/trunk_kcp.go` 基础框架
- [ ] 创建 `trunk_kcp/conn.go` 虚拟连接实现
- [ ] 创建 `trunk_kcp/trunk_kcp_test.go` 测试框架

### 3.2 实现发送路径 / Implement Send Path

**流程 / Flow:**
