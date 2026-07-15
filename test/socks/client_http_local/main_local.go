package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/lxt1045/rpc/test/socks"
	"github.com/lxt1045/utils/config"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
	_ "go.uber.org/automaxprocs"
)

/*
$env:CGO_ENABLED=0; $env:GOOS="linux"; $env:GOARCH="amd64"; go build ./

scp -P 2222 ./client_http_local fhaio@10.1.1.171:/home/fhaio/project/proxy


vim  /etc/systemd/system/client_http_local.service

sudo systemd-analyze verify /etc/systemd/system/client_http_local.service  # 检查服务文件语法是否正确
sudo systemctl daemon-reload  # 更新 systemd 的服务配置
sudo systemctl start client_http_local      # 启动服务
sudo systemctl stop client_http_local
systemctl status client_http_local
sudo systemctl enable client_http_local
sudo systemctl disable client_http_local

```

```yml
[Unit]
Description=npc client Service

[Service]
Type=simple
Restart=always
WorkingDirectory=/home/fhaio/project/proxy
ExecStart=/home/fhaio/project/proxy/client_http_local
TimeoutStartSec=10

[Install]
WantedBy=multi-user.target
```

*/

func main() {
	var flags struct {
		port string
	}

	flag.StringVar(&flags.port, "port", "18081", "port")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	confLog := config.Log{
		MaxSize:    20,
		MaxBackups: 20,
		MaxAge:     60,
		LocalTime:  false,
		Compress:   true,
		ToConsole:  true,
		Filename:   "./logs/running.log",
	}
	err := log.Init(ctx, confLog)
	if err != nil {
		log.Ctx(ctx).Fatal().Caller().Err(err).Send()
		return
	}
	// log.Init()
	ctx, _ = log.WithLogid(ctx, gid.GetGID())

	cli := &socks.SocksCli{
		ChPeer:       make(chan *socks.Peer, 1),
		ChPeerReuser: make(chan *socks.Peer, 10),
	}
	go cli.RunHttpProxyLocal(ctx, ":18088", 2)

	//

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
