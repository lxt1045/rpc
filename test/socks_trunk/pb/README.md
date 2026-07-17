
# 生成目标文件
## 1. linux
生成 xxx.pb.go 文件
```sh
protoc --go_out=plugins=grpc:./ *.proto
# 或
protoc -I=. *.proto --gogofast_out=plugins=grpc:./gogofastgen
```

## 2. windows下需要全路径:
```ps1
$env:dir="D:/project/go/src/github.com/lxt1045"
protoc -I="$env:dir" $env:dir/rpc/test/socks_trunk/pb/*.proto --gogofast_out=plugins=grpc:"$env:dir/rpc/test/socks_trunk/pb/" 

# protoc -I="$env:dir" $env:dir/rpc/test/socks/pb/*.proto --go_out=plugins=grpc:"$env:dir/rpc/test/socks/pb/" 
```

