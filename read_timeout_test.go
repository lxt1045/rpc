package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	base "github.com/lxt1045/rpc/base"
)

func TestTimeoutConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addr := ":18080"
	ch := make(chan struct{})
	go func() {
		testService1(ctx, cancel, t, addr, ch)
	}()

	<-ch
	time.Sleep(time.Millisecond * 10)
	testClient1(ctx, cancel, t, addr)
}

func testService1(ctx context.Context, cancel context.CancelFunc, t *testing.T, addr string, ch chan struct{}) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Errorf("net.Listen: %v", err)
		return
	}
	for {
		ch <- struct{}{}

		// 接收输入流
		conn, err := ln.Accept()
		if err != nil {
			t.Errorf("ln.Accept: %v", err)
			return
		}
		// 处理流程尽量要和Listen流程分开，避免相互影响
		go func() {
			s, err := StartService(ctx, conn, &server{Str: "test"}, base.RegisterHelloServer)
			if err != nil {
				t.Errorf("StartService: %v", err)
				return
			}

			all := s.AllInterfaces()
			for i, m := range all {
				svc := (*server)(m.SvcPointer)
				t.Logf("idx:%d, service.Str:%v, func_key:%s, req:%s",
					i, svc.Str, m.Name, m.reqType.String())
			}
		}()
	}
}

func testClient1(ctx context.Context, cancel context.CancelFunc, t *testing.T, addr string) {
	addr = "127.0.0.1" + addr
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
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
