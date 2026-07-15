

# gRPC四种通信方式
    1. 简单Rpc（Simple RPC）：就是一般的rpc调用，一个请求对象对应一个返回对象。 　　
    2. 服务端流式rpc（Server-side streaming RPC）：一个请求对象，服务端可以传回多个结果对象。 　　
    3. 客户端流式rpc（Client-side streaming RPC）：客户端传入多个请求对象，服务端返回一个响应结果。 　　
    4. 双向流式rpc（Bidirectional streaming RPC）：结合客户端流式rpc和服务端流式rpc，可以传入多个对象，返回多个响应对象。

# 生成目标文件
## 1. linux
生成 xxx.pb.go 文件
```sh
protoc --go_out=plugins=grpc:./ service.proto
# 或
protoc -I=. service.proto --gogofast_out=plugins=grpc:./gogofastgen
```

## 2. windows下需要全路径:
```ps1

$env:dir="[github.com/lxt1045/utils]"
$env:dir="D:/project/go/src/github.com/lxt1045/utils"
protoc -I="$env:dir" $env:dir/rpc/base/*.proto --gogofast_out=plugins=grpc:"$env:dir/rpc/base/" 

```

