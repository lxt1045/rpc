# 生成目标文件

本目录的 `service.pb.go` 由 `service.proto` 生成，内容与 `test/socks_trunk_kcp/pb/`
一致（同一份控制面协议）。重新生成时注意 `--gogofast_out=plugins=grpc`（gogo），
不要用 `--go_out`。

## 1. linux

```sh
protoc -I=. *.proto --gogofast_out=plugins=grpc:./
```

## 2. windows 下需要全路径

```ps1
$env:dir="D:/project/go/src/github.com/lxt1045"
protoc -I="$env:dir" $env:dir/rpc/test/socks_faux_trunk_kcp/pb/*.proto --gogofast_out=plugins=grpc:"$env:dir/rpc/test/socks_faux_trunk_kcp/pb/"
```

注意：生成的 `service.pb.go` 头部 `source:` 注释会写入传入的 proto 路径，
属于生成器元信息，不影响编译与包名（`option go_package = "./;pb"`）。
