# socks_trunk

Trunk 聚合 SOCKS5/HTTP 代理的生产代码目录。

- `test/socks_trunk/socks_trunk/`：核心库
- `test/socks_trunk/cmd/socks-trunk-client/`：客户端入口
- `test/socks_trunk/cmd/socks-trunk-server/`：服务端入口

## 构建

```bash
go build ./test/socks_trunk/socks_trunk/... ./test/socks_trunk/cmd/...
```

## 本地运行

先修改编译期嵌入的配置 `test/socks_trunk/socks_trunk/filesystem/static/conf/default.yml`，再重新构建；运行不需要独立 `-config` 文件。

```bash
SOCKS_TRUNK_TOKEN=your-token go run ./test/socks_trunk/cmd/socks-trunk-server
SOCKS_TRUNK_TOKEN=your-token go run ./test/socks_trunk/cmd/socks-trunk-client
```

## 测试

```bash
go test -count=1 ./test/socks_trunk/socks_trunk/... ./test/socks_trunk/cmd/... ./trunk
go vet ./test/socks_trunk/socks_trunk/... ./test/socks_trunk/cmd/... ./trunk
```
