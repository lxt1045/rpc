package rpc

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	base "github.com/lxt1045/rpc/base"
	"github.com/lxt1045/rpc/conn"
	"github.com/lxt1045/utils/log"
	"github.com/xtaci/kcp-go"
)

func TestKCPConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addr := ":18080"
	ch := make(chan struct{})
	go func() {
		testKcpService(ctx, cancel, t, addr, ch)
	}()

	<-ch
	time.Sleep(time.Millisecond * 10)
	testKcpClient(ctx, cancel, t, addr)
}

func testKcpService(ctx context.Context, cancel context.CancelFunc, t *testing.T, addr string, ch chan struct{}) {
	go conn.KcpUpdata(ctx, conn.KcpConfig{}) //每一个kcp连接都需要定期巧用updata()以驱动kcp循环

	// udpAddr, err := net.ResolveUDPAddr("udp", "0.0.0.0"+addr)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	udpAddr := &net.UDPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: 18080,
	}
	// ln, err := net.Listen("tcp", addr)
	ln, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	// go func(ctx context.Context) { //判断ctx是否被取消了，如果是就退出
	// 	<-ctx.Done()
	// 	ln.Close()
	// }(ctx)
	defer func() {
		ln.Close()
	}()
	var (
		bufRecv = make([]byte, conn.UdpBufLen)
	)
	ch <- struct{}{}
	for {
		// 接收输入流
		// conn, err := ln.Accept()
		n, addrUdp, err := ln.ReadFromUDP(bufRecv)
		if err != nil {
			t.Fatal(err)
		}
		if err != nil || n <= 0 {
			log.Ctx(ctx).Error().Err(err).Msgf("error during read:%v, n:%d", err, n)
			continue
			//break
		}
		c, isFirstTime := conn.Addr2Conn(addrUdp)
		m := func() int {
			c.Lock()
			defer c.Unlock()

			if c.KCP == nil {
				// var conv uint32 = 8888
				// decode32u(bufRecv[1:], &conv) //获取数据包的conv
				conv := binary.LittleEndian.Uint32(bufRecv[:])
				c.KCP = kcp.NewKCP(conv, conn.KcpOutoput(ctx, ln, addrUdp))
				c.KCP.WndSize(128, 128) //设置最大收发窗口为128
				// 第1个参数 nodelay-启用以后若干常规加速将启动
				// 第2个参数 interval为内部处理时钟，默认设置为 10ms
				// 第3个参数 resend为快速重传指标，设置为2
				// 第4个参数 为是否禁用常规流控，这里禁止
				// c.KCP.NoDelay(0, 10, 0, 0) // 默认模式
				// c.KCP.NoDelay(0, 10, 0, 1) // 普通模式，关闭流控等
				c.KCP.NoDelay(1, 10, 2, 1) // 启动快速模式
				conn.UpdataAdd(c)          //通知updata()协程增加kcp
			}

			//以下操作会将bufRecv的数据复制到kcp的底层缓存，所以bufRecv可以快速重用
			c.M = c.KCP.Input(bufRecv[:n], true, false) //bufRecv[1:], true, false：数据，正常包，ack延时发送

			// // 有完整数据可以接收
			// for m >= 0 {
			// 	//这里要确认一下，kcp.Recv()是否要经过update()驱动，如果要驱动，则不能在这里处理
			// 	m = c.KCP.Recv(bufRecv)
			// 	if m <= 0 {
			// 		if m == -3 {
			// 			bufRecv = make([]byte, len(bufRecv)*2)
			// 			m = 0
			// 			continue
			// 		}
			// 		break
			// 	}
			// }
			return c.M
		}()
		_ = m

		if isFirstTime {
			s, err := StartService(ctx, c, &server{Str: "test"}, base.RegisterHelloServer)
			if err != nil {
				t.Fatal(err)
			}

			all := s.AllInterfaces()
			for i, m := range all {
				svc := (*server)(m.SvcPointer)
				t.Logf("addrUdp:%s, idx:%d, service.Str:%v, func_key:%s, req:%s",
					addrUdp, i, svc.Str, m.Name, m.reqType.String())
			}
		}
	}
}

func testKcpClient(ctx context.Context, cancel context.CancelFunc, t *testing.T, addr string) {
	addr = "127.0.0.1" + addr
	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	conn := conn.NewKcpCli(ctx, 888, c)
	if err != nil {
		t.Fatal(err)
	}

	client, err := StartClient(ctx, conn, base.NewHelloClient)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}

	req := base.HelloReq{
		Name: "call 10086",
	}
	resp := base.HelloRsp{}

	err = client.Invoke(ctx, "SayHello", &req, &resp)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("resp.Msg:\"%s\"", resp.Msg)
}
