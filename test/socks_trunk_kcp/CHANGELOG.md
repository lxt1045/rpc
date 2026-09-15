# Changelog - socks_trunk_kcp

## 2026-09-15 - 按需创建虚拟连接优化

### 背景

原有实现在 `TrunkStart` 时预先为所有可能的虚拟连接 ID（默认 1-256）创建 goroutine，即使大部分连接从未被使用。这导致：

- 资源浪费：256 个 goroutine 大部分处于空闲状态
- 启动延迟：需要等待所有 goroutine 创建完成
- 内存占用：每个 goroutine 需要栈空间（至少 2KB）

### 改进

利用 `trunk_kcp` 的 `OnNewConnFunc` 回调机制，改为**按需创建**：

- 只有当客户端真正使用某个虚拟连接 ID 时，服务端才创建对应的处理 goroutine
- 大幅减少资源占用（从固定 256 个降低到实际使用数量）
- 提升启动速度

### 代码变更

#### 修改文件：`session.go`

**删除的函数：**
```go
func (p *SocksSvc) serveVirtualConns(ctx context.Context, sess *session) {
	maxVConn := 256
	sess.mu.Lock()
	if sess.maxVConn > 0 {
		maxVConn = sess.maxVConn
	}
	trunk := sess.trunk
	sess.mu.Unlock()
	if trunk == nil {
		return
	}
	for i := 1; i <= maxVConn; i++ {
		id := uint16(i)
		go p.serveVirtualConn(ctx, trunk.GetConn(id))
	}
}
```

**修改的函数：`TrunkStart`**
```go
// 创建回调函数，按需处理新的虚拟连接
onNewConn := func(vconn *trunk_kcp.VirtualConn) {
	p.serveVirtualConn(ctx, vconn)
}

trunk := trunk_kcp.NewTrunkKCP(req.TrunkId, onNewConn, rws...)
// ... 其余代码不变
go trunk.Run(ctx)
// 移除了 p.serveVirtualConns(ctx, sess) 调用
return &pb.TrunkStartRsp{}, nil
```

**保持不变：`serveVirtualConn`**
- 该函数逻辑完全不变，只是调用方式从预先批量调用改为回调触发

#### 新增文件：`session_test.go`

新增两个单元测试验证按需创建功能：

1. **TestOnDemandVirtualConn**
   - 验证只有被使用的虚拟连接 ID 才会触发回调
   - 测试场景：使用 5 个不连续的 ConnID (1, 5, 10, 20, 50)
   - 预期结果：回调被触发 5 次（而非 256 次）

2. **TestNoCallbackBeforeUse**
   - 验证在虚拟连接被使用之前不会预先创建任何 goroutine
   - 测试场景：启动 trunk 后等待，不发送任何数据
   - 预期结果：回调触发次数为 0

### 测试结果

```bash
$ go test -run TestOnDemand ./test/socks_trunk_kcp -v
=== RUN   TestOnDemandVirtualConn
    session_test.go:26: onNewConn called for ConnID: 50 (total callbacks: 1)
    session_test.go:26: onNewConn called for ConnID: 20 (total callbacks: 2)
    session_test.go:26: onNewConn called for ConnID: 10 (total callbacks: 3)
    session_test.go:26: onNewConn called for ConnID: 5 (total callbacks: 4)
    session_test.go:26: onNewConn called for ConnID: 1 (total callbacks: 5)
    session_test.go:86: Callback invoked correct number of times: 5
    session_test.go:90: On-demand virtual connection creation test passed
--- PASS: TestOnDemandVirtualConn (0.25s)

$ go test -run TestNoCallbackBeforeUse ./test/socks_trunk_kcp -v
=== RUN   TestNoCallbackBeforeUse
    session_test.go:139: No callbacks invoked before virtual connections are used - correct behavior
--- PASS: TestNoCallbackBeforeUse (0.25s)
```

### 性能影响

#### 资源占用对比（假设实际只使用 10 个虚拟连接）

| 指标 | 修改前 | 修改后 | 优化比例 |
|------|--------|--------|----------|
| Goroutine 数量 | 256 | 10 | 96% ↓ |
| 内存占用（栈） | ~512 KB | ~20 KB | 96% ↓ |
| 启动时间 | 创建 256 个协程 | 0（延迟到使用时） | 100% ↓ |

#### 实际使用场景收益

- **低并发场景**（<10 并发）：资源节省 95%+
- **中等并发场景**（10-50 并发）：资源节省 70-90%
- **高并发场景**（接近 256）：无显著差异，但也不会有性能损失

### 兼容性

- ✅ **客户端无需修改**：`peer_client.go` 完全不变
- ✅ **协议兼容**：未改变任何网络协议
- ✅ **行为一致**：从客户端视角，功能完全相同
- ✅ **向后兼容**：老客户端可以连接新服务端

### 依赖

此改进依赖于 `trunk_kcp` 模块的回调功能：

```go
type OnNewConnFunc func(*VirtualConn)

func NewTrunkKCP(conv uint32, onNewConn OnNewConnFunc, rws ...io.ReadWriteCloser) *TrunkKCP
```

该功能已在 `trunk_kcp/trunk_kcp.go` 中实现并测试。

### 未来优化方向

1. **连接池预热**：如果需要，可以在低负载时预创建少量连接（如 5-10 个）
2. **连接复用**：虚拟连接关闭后，可以考虑复用 goroutine 而非销毁
3. **监控指标**：暴露实际创建的虚拟连接数量，用于容量规划

### 相关文件

- `session.go` - 服务端会话管理（已修改）
- `session_test.go` - 新增测试（新建）
- `peer_client.go` - 客户端逻辑（无需修改）
- `README.md` - 文档更新（已更新）
- `trunk_kcp/trunk_kcp.go` - 底层 trunk 模块（提供回调支持）

### 提交信息

```
feat(socks_trunk_kcp): 改为按需创建虚拟连接处理协程

利用 trunk_kcp 的 OnNewConnFunc 回调机制，只在客户端真正使用某个虚拟连接 ID 时
才创建对应的服务端处理 goroutine，而非预先为所有可能的 256 个连接创建协程。

优势：
- 资源占用降低 70-96%（取决于实际并发数）
- 启动延迟消除（不再需要预创建 256 个协程）
- 功能完全兼容，客户端无需任何修改

变更：
- session.go: 移除 serveVirtualConns，在 TrunkStart 中传入 onNewConn 回调
- session_test.go: 新增 TestOnDemandVirtualConn 和 TestNoCallbackBeforeUse
- README.md: 更新建连流程和并发模型说明

测试：go test -run TestOnDemand ./test/socks_trunk_kcp -v
```
