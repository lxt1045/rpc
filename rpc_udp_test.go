package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	base "github.com/lxt1045/rpc/base"
	"github.com/lxt1045/rpc/conn"
	"github.com/lxt1045/utils/log"
)

func TestUDPConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addr := ":18081"
	ch := make(chan struct{}, 1)
	go func() {
		testUdpService(ctx, cancel, t, addr, ch)
	}()

	<-ch
	time.Sleep(time.Millisecond * 10)
	testUdpClient(ctx, cancel, t, addr)
}

func TestUDPConnSvc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addr := ":18081"
	ch := make(chan struct{}, 1)
	go func() {
		testUdpService(ctx, cancel, t, addr, ch)
	}()

	<-ch
	select {}
}
func TestUDPConnCli(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addr := ":18081"
	testUdpClient(ctx, cancel, t, addr)
}

func testUdpService(ctx context.Context, cancel context.CancelFunc, t *testing.T, addr string, ch chan struct{}) {

	// udpAddr, err := net.ResolveUDPAddr("udp", "0.0.0.0"+addr)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	udpAddr := &net.UDPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: 18081,
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
	ch <- struct{}{}
	for {
		// 接收输入流

		bufRecv := make([]byte, conn.UdpBufLen)
		n, addrUdp, err := ln.ReadFromUDP(bufRecv)
		if err != nil {
			t.Error(err)
			return
		}
		if err != nil || n <= 0 {
			log.Ctx(ctx).Error().Err(err).Msgf("error during read:%v, n:%d", err, n)
			continue
			//break
		}
		c, isFirstTime := conn.ToUdpConn(ctx, addrUdp, ln)
		if isFirstTime {
			s, err := StartService(ctx, c, &server{Str: "test"}, base.RegisterHelloServer)
			if err != nil {
				t.Fatal(err)
			}
			all := s.AllInterfaces()
			for i, m := range all {
				svc := (*server)(m.SvcPointer)
				t.Logf("idx:%d, service.Str:%v, func_key:%s, req:%s",
					i, svc.Str, m.Name, m.reqType.String())
			}
		}
		c.ChRead <- bufRecv[:n]
	}
}

func testUdpClient(ctx context.Context, cancel context.CancelFunc, t *testing.T, addr string) {
	addr = "127.0.0.1" + addr
	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	conn := conn.UdpConnCli{Conn: c}
	client, err := StartClient(ctx, &conn, base.NewHelloClient)
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
