# socks_trunk（历史/迁移中）

> 正式生产代码位于本目录：`socks_trunk/` + `cmd/`；本 README 和 plan 继续保留。

本目录正在从验证 demo 改造为可部署的 Trunk SOCKS/HTTP 代理。生产代码布局仍在迁移中，最终见根目录计划 `plan.md`。

## 快速启动（本地开发）

```bash
# 需要先生成/拷贝证书到 filesystem/static/ca/ 和 static/ca/，并设置相同 token
export SOCKS_TRUNK_TOKEN=dev-insecure-token

# 服务端
cd test/socks_trunk/service
go run . -config static/conf/default.yml   # 或 config.example.yaml

# 客户端（另一个终端）
cd test/socks_trunk/client
go run . -config static/conf/default.yml   # 或 config.example.yaml
```

## 当前已落地

- 删除未使用的 `peer_proxy.go` 和 `service/service.go`
- 外部配置加载（`-config`）与 token 校验
- 服务端 `Auth` 真实校验
- 会话级 SessionManager，替代包级 `mTrunkConn`
- SOCKS5 TCP 优先走 Trunk 数据面
- 基础 healthz/metrics 端点（服务端配置 `metrics_addr`）

## 安全提醒

- 生产环境必须配置强随机 token（或 `SOCKS_TRUNK_TOKEN`）
- 证书/私钥不要提交到 Git；示例证书仅用于本地验证

## 构建与检查

```bash
cd test/socks_trunk
make fmt vet test build
```

## 第二轮新增

- 清理 `peer_client.go` 中 Stream/Upgrade/Local 等 demo 数据路径
- `trunk.Trunk.IsClosed()` 用于健康检测
- 客户端 `MaintainTrunk` / `RebuildTrunk` 基础断线重建
- 服务端 `/debug/pprof/*`、客户端 handler panic recover

## 本地集成测试

```bash
test/socks_trunk/scripts/local_integration_test.sh
```

CI 示例见仓库根目录 `.github/workflows/socks-trunk.yml`。
