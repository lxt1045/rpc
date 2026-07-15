
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
$env:dir="D:/project/go/src/github.com/lxt1045/utils"
protoc -I="$env:dir" $env:dir/msg/nat/pb/*.proto --gogofast_out=plugins=grpc:"$env:dir/msg/nat/pb/" 
```

