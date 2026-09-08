# test/socks_trunk 上线实用化改造计划

> 状态：执行中（首轮已落地，未全部完成）  
> 目标：把 `test/socks_trunk` 从“验证 demo”改造成可部署、可运维、可长期运行的 Trunk SOCKS/HTTP 代理系统。
> 本文是执行计划，先评审；确认后再按阶段编码。

## 当前执行进度（首轮）

- [x] 清理运行日志、未使用 `peer_proxy.go`/`service/service.go`、补充 `.gitignore`/README。
- [x] 配置已改回基于 embed `fs.FS` 编译进二进制；保留环境变量 token、认证、`SessionManager`、ACL、基础 metrics/healthz。
- [x] SOCKS5 TCP 改走 Trunk；HTTP CONNECT 使用 Trunk 模式。
- [x] 新增 `config_test.go` 与 `session_test.go`，`go test ./test/socks_trunk/...` 通过。
- [x] 目录迁移：已在本目录内创建 `socks_trunk/`、`cmd/socks-trunk-client`、`cmd/socks-trunk-server`。
- [ ] 剩余：完整 Dockerfile/镜像、正式删除 test 旧代码、HTTP CONNECT 与断链恢复扩展压测。
- [x] 已继续：清理 peer_client demo 旁路、增加 Trunk IsClosed/Rebuild/Maintain、增加 pprof。

---

## 1. 背景与现状

当前 `test/socks_trunk` 已经跑通了 Trunk 链路聚合的验证流程，主要包括：

- `service/main.go`：服务端，监听 TLS + RPC，接收客户端多条 RPC/Upgrade 连接；
- `client/main.go`：客户端，本地启动 SOCKS5/HTTP 代理，通过多条链路与远程服务端通信；
- `peer_service.go` / `peer_client.go`：核心转发逻辑，包含普通流、Upgrade、Trunk 三种数据通道；
- `peer_proxy.go` / `service/service.go`：原本未被任何 main 使用，属于 demo 阶段遗留，已在首轮删除。

### 已知的主要问题

| # | 问题 | 风险 | 位置/说明 |
|---|------|------|-----------|
| 1 | 代码位于 `test/`，包路径、目录含义仍为示例 | 不适合直接发布、被误当作测试样例 | `test/socks_trunk` |
| 2 | 没有任何身份认证，`Auth` 为空实现 | 谁能连上服务端谁就能开代理，等于开放代理 | `peer_service.go` |
| 3 | Trunk 状态使用包级全局 map `mTrunkConn` | 多客户端串数据、泄漏、无法清理、恶意占用 | `peer_service.go` |
| 4 | Trunk 一次性建立：固定 `trunkID=10086`、固定 32 条，不自动恢复 | 任一条底层链路断开/抖动后没有补偿；无法长期运行 | `peer_client.go` `InitTrunk` |
| 5 | 服务端持有 `SocksSvc` 的切片全局引用，连接关闭依赖定时器清理 | 内存泄漏、优雅退出不完整 | `service/main.go` |
| 6 | 大量遗留/重复数据路径和死代码 | 维护困难，行为不确定 | `OutToTCPPeer1/2`、`connect1`、`connect2`、`CopyLoop`、`peer_proxy.go` |
| 7 | 端口、Trunk 参数、日志文件、远端地址写死在代码或 demo YAML | 无法按环境配置、上线配置混乱 | `client/main.go`、`static/conf/default.yml` |
| 8 | 缺少限速、并发限制、空闲超时、TCP keepalive 细节、目标地址 ACL | 被滥用或耗尽连接 | 数据路径各文件 |
| 9 | 缺少可观测性：无健康检查、无指标、无统一审计日志 | 线上故障无法定位 | 整体 |
| 10 | 缺少自动化测试、压测、部署文件 | 回归无法保证 | 整体 |
| 11 | 证书/私钥/本地 YAML 在运行时依赖 embed 目录，且 `.pem`/`.yml` 被 Git 忽略 | 无法自动构建部署，密钥管理不规范 | `filesystem/`、`.gitignore` |
| 12 | 部分 goroutine 和 channel 生命周期不完整 | 代理长连接场景下可能泄漏 | `peer_service.go`、`peer_client.go` |

---

## 2. 目标与验收标准

### 2.1 业务目标

将 Trunk 链路聚合能力用于生产环境的安全代理/内网穿透：

- 客户端到服务端必须认证，未授权连接直接拒绝；
- 支持 SOCKS5 TCP 和 HTTP CONNECT 两种入口，目标协议先以 TCP 为准；
- 多条底层 RPC 连接聚合为一个可用的 Trunk，具备断线重建能力；
- 服务端可并发服务多个客户端，客户端之间状态隔离；
- 进程可优雅退出，不泄漏 goroutine/连接；
- 配置、证书、日志、监控均为外部化/可运维；
- 可通过自动化测试与本地压测后再上线。

### 2.2 非目标（本期可明确不做）

- SOCKS5 UDP Associate（若产品不需要 UDP，明确关闭；如需支持另立任务）；
- 多级中继/代理链；
- Web 管理控制台；
- 与既有 Service/Auth/Clients/ConnPeer 管理服务整合（该套代码当前未用于 socks_trunk）。

### 2.3 上线验收标准

- [ ] 通过 `go build ./...`（本目录相关包）。
- [ ] 服务端无有效 token 的连接不能触发任何外联。
- [ ] 两个客户端并发使用互不串流、互不影响。
- [ ] 杀掉 Trunk 中任意 1~2 条底层链路，已有 TCP 连接不断（或按设计快速失败），后续新连接能自动恢复。
- [ ] 进程收到 SIGTERM/SIGINT 后能主动关闭全部监听、会话与出站连接。
- [ ] 全链路 24h 跑测无 goroutine 增长、无内存明显增长。
- [ ] 提供 healthz/pprof/metrics 基础接口。
- [ ] 提供一键本地启动脚本与部署文档。

---

## 3. 推荐目标布局

由于仓库 `AGENTS.md` 明确 `test/` 是“examples and integration programs, not a conventional unit-test suite”，生产代码不建议长期放在 `test/`。推荐：

```
socks_trunk/                        # 或使用现有模块下更正式的名字
├── README.md
├── client.go                       # SocksCli：客户端核心代理逻辑
├── service.go                      # SocksSvc：服务端核心转发逻辑
├── session.go                      # 会话/连接管理（替代全局 map）
├── config.go                       # 加载/校验配置
├── auth.go                         # token/ACL
├── conn.go                         # 统一 Conn/IoCopy 生命周期
├── peer_client.go
├── peer_service.go
├── socks5.go                       # SOCKS5 握手（含 auth 可选）
├── httpproxy.go                    # HTTP CONNECT
├── metrics.go
└── *_test.go
cmd/
├── socks-trunk-client/
│   └── main.go
└── socks-trunk-server/
    └── main.go
deploy/socks-trunk/
├── Makefile
├── systemd/
└── scripts/
```

> 折中方案：如果本期不想搬目录，可以保留 `test/socks_trunk` 路径，但必须清理其中的 demo 文件和路径语义，并将运行产物入口放到 `cmd/` 下；下个迭代再正式迁移。计划默认按“先原地重构+迁移”执行。

---

## 4. 执行阶段

### 阶段 0：基线与清理

**目标：建立可构建、可评审的基线。**

- [ ] 先提交/暂存当前 whitespace 改动（当前 git diff 显示大量文件为 CRLF 或空白差异，避免混入）。
- [ ] 确认 Go 工具链可用（本机 Go build cache 权限问题需先解决，或用 CI 构建）。
- [x] 删除 `test/socks_trunk/service/logs/` 运行产物，并在 `.gitignore` 补充 `test/socks_trunk/**/logs/`。
- [x] 盘点未使用的 demo 代码：
  - 已删除 `test/socks_trunk/peer_proxy.go`、`service/service.go`；
  - 已清理 `peer_client.go` 中 `RunHttpProxyLocal`、`OutToTCPPeer1/OutToTCPLocal/connect/connect1/connect2/CopyLoop` 等 demo 函数。
- [x] 建立目标目录骨架（`socks_trunk/`、`cmd/`），正式代码在本目录内已可构建。
- [x] 为原目录保留说明：`test/socks_trunk/README.md` 标注当前改造状态，并列出已落地能力。

**退出条件**：新旧目录都能独立 `go build`，无冗余 main 包。

---

### 阶段 1：配置与密钥管理

**目标：去掉“代码写死 + embed 私有文件”的 demo 方式。**

- [x] 定义生产配置结构（`Config`），配置使用 `filesystem.Static` 的 `config.UnmarshalFS` 在编译期嵌入，不再依赖独立 YAML 文件：
  ```yaml
  server:
    listen: ":18086"
    tls:            # 路径来自外部卷/环境变量，不 embed
      ca: "/etc/socks-trunk/certs/ca.crt"
      cert: "/etc/socks-trunk/certs/server.crt"
      key: "/etc/socks-trunk/certs/server.key"
    token: "replace-me"        # 至少 32 字符；或用独立 secret 文件
    max_clients: 1024
    max_conns_per_client: 128
    dial_timeout: 30s
    idle_timeout: 15m
    write_timeout: 30s
    acl:
      enabled: false
      allow_networks: []
      deny_hosts: []
  client:
    server_addr: "proxy.example.com:18086"
    server_name: "proxy.example.com"
    tls:
      ca: "..."
      cert: "..."
      key: "..."
    token: "replace-me"
    socks_listen: "127.0.0.1:10080"
    http_listen: "127.0.0.1:18080"
    trunk:
      min_conns: 4
      max_conns: 32
      reconnect_interval: 3s
      health_interval: 10s
      conn_idle_timeout: 5m
  log:
    level: "info"
    file: "/var/log/socks-trunk/running.log"
    to_console: false
  debug:
    pprof_addr: "127.0.0.1:6060"
    metrics_addr: "127.0.0.1:9090"
  ```
- [x] 新增 `config.go`：加载外部文件、校验必填字段、禁止默认 token、读取 ACL。
- [ ] 保留 `filesystem/` 仅作为本地开发模板，生产入口不依赖 embed。
- [ ] 证书管理：
  - 服务端使用真实 CA 签发的服务端证书，或内部 PKI；
  - mTLS 建议启用；若担心运维成本，至少在 TLS 之上做 token 认证；
  - 密钥文件权限 0600，容器内用 Secret 挂载。
- [ ] README 增加“证书生成与部署”章节（README 已初步补充运行说明）。

**退出条件**：修改嵌入的 `static/conf/default.yml` 后重新 `go build`，二进制自带配置；未配置 token 时非 debug 模式拒绝启动。

---

### 阶段 2：身份认证与会话隔离

**目标：杜绝开放代理，所有状态按会话隔离。**

- [ ] 在 proto/service 中扩展认证协议（当前复用 `AuthReq.Name` 作为共享 token，未改 proto）。
- [x] `SocksSvc.Auth` 实现真实校验：
  - 校验客户端 token/用户；
  - 服务端保存该连接的 `session`，记录远端地址、连接时间、计数；
  - 未认证连接无法执行 `ConnUpgrade/TrunkUpgrade/TrunkStart`（`Conn`/`ConnUpgrade`/`TrunkStart` 均已加鉴权）。
- [x] 用 `SessionManager` 替代 `var mTrunkConn map[uint32][]Conn`：
  - key：`clientID/sessionID` 而不是简单 `trunk_id`；
  - 保存每个客户端的底层 `TrunkUpgrade` 连接；
  - 客户端断开时通过 service 清理协程/`SvcClosed` 自动清理；
  - 增加 Mutex/RWMutex 与 Close 方法。
- [ ] 服务端接入上限与并发保护：
  - `max_clients`、`max_conns_per_client`、单连接待建 trunk 上限；
  - 超过阈值直接关闭新连接。
- [x] 目标地址 ACL（IP/CIDR 白名单 + 域名后缀黑名单，暂未实现域名白名单）：
  - 支持 `allow_networks`、`deny_hosts`；
  - 命中 ACL 时在服务端记录 warn 日志。

**退出条件**：
- 无 token 客户端无法连接外网；
- 两个客户端并发建立 Trunk 后数据互相隔离；
- 客户端断开后服务端对应 Trunk 资源自动释放（测试/压测可观测）。

---

### 阶段 3：Trunk 生命周期与断线恢复

**目标：当前一次性 Trunk 改为可维护、可恢复。**

需要同时评估底层 `trunk.Trunk` 是否支持动态加/删物理连接。若当前不支持（已确认 `rws` 为固定切片），方案为：

- [ ] 在 `trunk` 包增加“Trunk 级管理”能力（或由 socks_trunk 自行重建）：
  - `AddConn(io.ReadWriteCloser) error`；
  - `RemoveConn(idx)`；
  - 断线检测：底层 rw 的 Read 返回错误时从 Trunk 移除并触发重建回调；
  - 保证并发锁安全，复用现有 `CmdAddConn` 语义。
- [x] 客户端 `TrunkManager`（基础版）：
  - 增加 `MaintainTrunk`/`RebuildTrunk` 周期检测；
  - 新拨号前完成认证并加入同 session；
  - `trunk_id` 为进程内单调/随机且重建复用；
  - 仍无指数退避与真正动态 AddConn，属于后续增强。
- [ ] 服务端 `SessionManager.TrunkUpgrade/TrunkStart` 支持“会话的 Trunk 已存在则复用或重建”。
- [x] 数据面断线语义（基础）：`connectTrunk` 双向 Copy 会在任一端结束时关闭另一端；新连接在 `trunk==nil`/重建中直接返回错误。
- [ ] 增加连接数/速率/健康状态统计。

**退出条件**：
- 客户端启动时服务端不可达，进程不退出并持续重试；
- 启动后随机关闭 1 条底层 TCP，不影响已建立连接或自动补足；
- 连续重启服务端 3 次，客户端可自动恢复会话并继续代理。

---

### 阶段 4：客户端代理与数据面重构

**目标：只保留一条清晰、可控、可测试的 TCP 数据面。**

- [x] 入口层（部分）：SOCKS5 TCP 已改走 Trunk；HTTP CONNECT 仍使用 Trunk mode；UDP/超时/accept 上限未完全实现。
- [ ] 出站策略：
  - 默认走 Trunk 虚拟连接；Trunk 不可用时拒绝新连接并返回可理解错误；
  - 保留本地直连仅作为显式配置开关，不作为自动 fallback（避免安全旁路）。
- [ ] 数据通道选择：SOCKS/HTTP 已使用 Trunk `Conn` 主路径，但 Stream/Upgrade 旁路函数仍在文件中待后续删除。
- [ ] 统一的 `Copy` / `Relay`：
  - 使用有界 channel、读写协程配对、`context` 取消；
  - 任一方向 EOF/错误必须关闭/唤醒另一方向；
  - 大包内存复用通过 `sync.Pool`；通道满时使用 select/ctx 而不是无限阻塞；
  - 正常关闭优先 `CloseWrite`/发送 FIN，非必须不 RST。
- [ ] 超时/限制：
  - 服务端出站 `Dialer.Timeout`、TCP keepalive；
  - 每代理连接 `IdleTimeout`（可选）；
  - 全局/每客户端最大活动代理连接数。

**退出条件**：
- SOCKS5 TCP 和 HTTP CONNECT 均能通过 Trunk 上网；
- 浏览器短连接大量开关、目标 RST、本地断网等场景无 goroutine 泄漏、无 error 日志刷屏；
- 删除/隔离所有未使用 demo 数据路径。

---

### 阶段 5：可靠性与可观测性

- [ ] 日志规范化：
  - 每会话/每代理连接有唯一 ID；
  - 启动/停止/认证/建连/断连/错误分级输出；
  - 对“正常关闭”类错误只输出 debug，不对错误级别；
  - 不再把 defer 中的人造 error 当 Info 日志。
- [ ] 心跳与健康：
  - RPC 连接级心跳（config 可配）；
  - 服务端 `Health/Latency` 用于会话活性检查；
  - `http /healthz` 返回存活与依赖（如 Trunk 连接数）。
- [x] 指标（基础版）：`expvar` 暴露 sessions/auth failures/trunk conns/active conns，服务端 `/debug/vars` + `/healthz`。
- [x] pprof：服务端 `metrics_addr` 开启 `/debug/pprof/*`，生产应绑定本机/内网。
- [ ] panic 兜底：
  - 所有 listener accept 协程、handler、relay 协程统一 recover，记录堆栈但不使进程退出；
  - 保留核心库已 recover 的前提下，在应用入口也包一层。

**退出条件**：运行 `curl /healthz` 返回 ok；指标能看到数字；用 `go tool pprof` 可采样；关闭服务端时日志可追踪到完整清理过程。

---

### 阶段 6：自动化测试与本地联调

- [x] 单测：已新增 `config_test.go`、`session_test.go`、`relay_test.go`、`socks5_test.go`。
- [x] 集成测试（脚本已通过）：`scripts/local_integration_test.sh` 起本地 HTTP echo + service + client，通过 SOCKS5 校验；HTTP CONNECT 与断链恢复仍待扩展。
- [ ] 压测脚本：
  - 多客户端并发、大小包、长短连接混合；
  - 记录吞吐、延迟、goroutine 数、内存。
- [x] CI 示例：`.github/workflows/socks-trunk.yaml`（rpc + utils checkout，执行 socks_trunk/trunk test/vet）。

**退出条件**：上述单测/集成测试命令可一键运行；压测报告说明在目标带宽/并发下稳定。

---

### 阶段 7：打包与部署

- [ ] `Makefile`：
  - `make build`、`make test`、`make docker`、`make fmt`；
  - 交叉编译 Linux amd64/arm64。
- [ ] `cmd/socks-trunk-client/Dockerfile`、`cmd/socks-trunk-server/Dockerfile`：
  - 多阶段构建，镜像只含二进制和示例配置；
  - 非 root 运行，`/etc`、`/var/log` 挂载。
- [x] systemd 样例（`test/socks_trunk/deploy/systemd/`）：
  - `Restart=always`；
  - 使用 `SIGTERM` 优雅退出；
  - 服务端健康检查可指向 `metrics_addr` 的 `/healthz`。
- [ ] 部署文档：已提供 `deploy/README.md` 基础 systemd 说明；网络拓扑/防火墙/灰度回滚仍待补。

---

## 5. 风险与注意点

| 风险 | 应对 |
|------|------|
| `trunk` 底层当前不支持动态增删物理连接 | 阶段 3 先做 API 评审；如工作量大，可先做“整 Trunk 重建”方案保证上线 |
| 改包路径/目录会引起多处 import 变更 | 统一用 `gofmt` + `go build`/`go test` 验证，分次提交 |
| 认证方案引入后需兼容已部署客户端 | 先按新协议实现并预留 `AuthReq.version`，旧 demo 客户端不兼容是预期的 |
| 断线恢复可能造成 TCP 数据重复/乱序 | Trunk 恢复只用于“新连接”，不承诺保活已死虚拟连接；避免在应用层做 TCP 重传 |
| 日志过大会占满磁盘 | 使用 log rotate（已有 lumberjack 配置），生产 level 设为 info/warn |
| 本机无法 go build（cache 权限） | 使用可写 `GOCACHE`/CI 环境，先把环境问题记入 TODO |

---

## 6. 执行顺序建议（首批任务）

1. 先提交当前 whitespace-only 改动（`git diff -w` 确认无实质修改）。
2. 创建目标目录骨架并复制现有可用代码。
3. 实现 `config.go` + 外部配置样例。
4. 实现 `Auth` + `SessionManager`，去掉全局 map。
5. 重构客户端入口，只保留 SOCKS5 TCP + HTTP CONNECT + Trunk 数据面。
6. 评审 `trunk` 是否需要动态 AddConn/RemoveConn，再实现重建。
7. 补测试、压测、部署文件。

---

## 7. 本文档后续维护

- 每个阶段开工前，将对应任务拆成 GitHub issue/PR。
- 每个阶段完成时更新本文件 checkbox 和“完成记录”。
- 代码迁移结束后，将本计划移动到正式文档目录（如本目录 `socks_trunk/plan.md`），并在 `test/socks_trunk` 保留索引。
