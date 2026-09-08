# test/socks_trunk_kcp 实施计划

> 状态：已按本计划完成可运行首版；断线重建与 UDP 后续继续完善。
> 目标：参考 `test/socks_trunk`，在 `test/socks_trunk_kcp` 下做一个基于 `trunk_kcp` 的 SOCKS/HTTP 代理模块。
> 原则：只做必要功能，代码直白，尽量去掉 demo 中验证性分支。

---

## 1. 定位

`trunk_kcp` 提供：

- 多个底层连接聚合成一个 `TrunkKCP`
- `VirtualConn` 提供多个逻辑连接
- KCP 负责可靠传输、顺序、重传

我们要做的不是再实现一个 trunk，而是把 `trunk_kcp` 当作“远程数据面”，在它上面实现代理。

参考关系：

```text
test/socks_trunk        = RPC + trunk(TCP) + SOCKS/HTTP 代理
test/socks_trunk_kcp    = RPC + trunk_kcp(KCP) + SOCKS/HTTP 代理
```

---

## 2. 模块边界

只做两端：

- **client**：本地监听 SOCKS5/HTTP CONNECT，接受用户连接
- **server**：接收 client 发来的“目标地址”，由 server 主动连接目标网站
- **transport**：client 与 server 之间建立 1 个或多个 `trunk_kcp` 底层连接，组成 `TrunkKCP`
- **control plane**：用 RPC/Proto 做认证、Trunk 会话协商，逻辑和 `test/socks_trunk` 保持一致
- **data plane**：每个用户 TCP 连接占用 1 个 `trunk_kcp.VirtualConn`

协议不要过度简化：继续使用 RPC/Proto 描述消息，避免散落自定义二进制协议。

---

## 3. 目标目录

```text
test/socks_trunk_kcp/
├── plan.md
├── README.md
├── Makefile
├── peer_client.go        # client 端逻辑：SOCKS/HTTP、建立 trunk_kcp、地址请求
├── peer_server.go        # server 端逻辑：建立 trunk_kcp、解析地址请求、外联目标
├── config.go             # embed 配置解析
├── pb/
│   ├── service.proto
│   ├── service.pb.go
│   └── README.md
├── filesystem/
│   └── filesystem.go
│   └── static/
│       ├── ca/...        # 本地测试证书
│       └── conf/default.yml
├── cmd/
│   ├── socks-trunk-kcp-client/
│   │   └── main.go
│   └── socks-trunk-kcp-server/
│       └── main.go
├── deploy/
│   └── systemd/
└── scripts/
    └── local_integration_test.sh
```

---

## 4. 协议设计（沿用 RPC/Proto）

### 4.1 控制面

沿用 `test/socks_trunk/pb/service.proto` 的服务定义，只保留必要接口：

```proto
service SocksSvc {
  rpc Auth(AuthReq) returns (AuthRsp);
  rpc TrunkUpgrade(TrunkUpgradeReq) returns (TrunkUpgradeRsp);
  rpc TrunkStart(TrunkStartReq) returns (TrunkStartRsp);
}
```

- `Auth`：每个底层 RPC 连接建立后先认证。
- `TrunkUpgrade`：把当前 RPC 连接升级为原始 `io.ReadWriteCloser`，供 `trunk_kcp` 使用。
- `TrunkStart`：服务端确认 N 条底层连接已到齐，开始运行 `TrunkKCP`。

### 4.2 数据面

- `trunk_kcp` 建立后，每个用户 TCP 连接分配 1 个 `VirtualConn`。
- 第一条数据使用 protobuf 编码的“打开目标”消息，例如：

```proto
message OpenReq {
  string addr = 1;
  Network network = 2;
  bytes head = 3;  // 可选首包
}
```

- server 收到 `OpenReq` 后校验 token/ACL，并 `net.Dial("tcp", addr)`。
- 后续数据直接在同一个 `VirtualConn` 上双向 Copy。

### 4.3 虚拟连接分配

- 客户端每接收一个本地 TCP 连接，就向 `TrunkKCP.GetConn(id)` 申请一个虚拟连接。
- 使用同一个 `TrunkKCP` 的多个虚拟连接，表示多路代理并发。

## 5. 功能清单

- [x] client 监听 SOCKS5 TCP
- [x] client 监听 HTTP CONNECT
- [x] 控制面使用 RPC/Proto 完成认证与 Trunk 协商
- [x] client 到 server 建立 `TrunkKCP`（多条底层连接，KCP conv 两端一致）
- [x] server 接受底层连接并建立对应 `TrunkKCP`
- [x] 底层连接断开后自动剔除（trunk_kcp.RemoveConn/对端同步剔除）
- [x] 底层连接不足时自动补一条（TrunkUpgrade + trunk_kcp.AddConn）
- [x] 新增 RPC/Proto 方法 `TrunkRemoveConn`
- [x] 使用 `VirtualConn` + Proto 消息传输“目标地址 + 数据”
- [x] server 外连目标 TCP
- [x] 双向 `Copy` 与关闭传播
- [x] token 校验（Auth）
- [x] ACL 简单校验（server 端拒绝内网/黑名单）
- [x] 配置嵌入 fs.FS
- [x] 日志精简
- [x] 单测：地址头编解码、认证、ACL
- [x] 集成测试：本地 echo + SOCKS5 走通

---

## 6. 执行步骤

### 阶段 0：目录骨架

- [x] 创建 `test/socks_trunk_kcp/`
- [x] 复制 filesystem 作为 embed 配置模板
- [x] 添加 `Makefile`、`README.md`
- [x] 创建 `pb/`（复制现有 proto/pb.go）

### 阶段 1：配置

- [x] 在 `config.go` 定义 ServerConfig/ClientConfig/TrunkKCPConfig
- [x] main 通过 `config.UnmarshalFS` 加载 embed 配置
- [x] 配置文件写入 `filesystem/static/conf/default.yml`

### 阶段 2：底层连接

- [ ] client 拨 N 条到 server 的 TCP/TLS 或 UDP KCP 连接
  - 开发期先用 TCP，后续可切 UDP
- [ ] 两端 `NewTrunkKCP(conv, rws...)`
- [ ] `go trunk.Run(ctx)`
- [ ] 实现断线重建：
  - 定期检查 `TrunkKCP` 状态
  - 断线后重连并重建整个 `TrunkKCP`，暂不做动态增删底层连接

### 阶段 3：代理协议

- [x] `protocol.go` 实现 OpenReq 编解码（基于 protobuf）
- [x] 控制面 RPC：Auth / TrunkUpgrade / TrunkStart
- [x] `auth.go` 实现 token 校验
- [x] `peer_client.go`：
  - SOCKS5 握手得到目标
  - 向本地 TCP 读取首包
  - `GetConn(id)`，发送地址头
  - 双向 copy
- [ ] `peer_server.go`：
  - `GetConn(id)` 等待数据
  - 解析地址头
  - token 校验
  - ACL 校验
  - `net.Dial` 目标
  - 双向 copy

### 阶段 4：HTTP CONNECT

- [x] client 支持 HTTP CONNECT 解析
- [x] 与 SOCKS5 共用 VirtualConn 数据面

### 阶段 5：可观测性与优雅退出

- [x] 每连接/每虚拟连接有日志
- [x] 正常关闭不打 error 日志
- [x] 监听器与 handler 有 recover
- [x] SIGTERM/SIGINT 优雅退出

### 阶段 6：测试

- [x] `protocol_test.go`：地址头 round-trip
- [x] ACL/默认值测试
- [x] 集成脚本已通过：本地 echo + SOCKS5 -> server 外联 echo
- [x] `go test ./test/socks_trunk_kcp/... ./trunk_kcp` 通过

---

## 7. 验收标准

- [x] client 能通过 SOCKS5 TCP 访问 server 能访问的地址（集成脚本通过）
- [ ] 多个并发用户连接互不干扰（代码已按 VirtualConn 并发设计，尚未完成并发压测）
- [x] 未带正确 token 的请求被 server 拒绝（auth_test 覆盖）
- [ ] 任一用户连接断开，不泄漏 goroutine（尚未做泄漏压测）
- [x] server/client 都能优雅退出（代码已实现；集成脚本会触发退出）
- [x] 配置通过 embed fs.FS 编译进二进制
- [ ] 代码中不存在验证性分支/重复数据通道（基本清理，仍保留未使用的 Conn/ConnUpgrade stub）

---

## 8. 非目标（本期不做）

- UDP Associate
- Web 管理界面
- KCP 参数动态调优
- trunk_kcp 动态 AddConn/RemoveConn
- 多级代理链
