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

## 测试

```bash
go test -count=1 ./test/socks_trunk_kcp/... ./trunk_kcp
test/socks_trunk_kcp/scripts/local_integration_test.sh
```
