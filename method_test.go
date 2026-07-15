package rpc

import (
	"context"
	"testing"

	"github.com/lxt1045/rpc/base"
)

type serverEm struct {
	server
}

func TestMethod(t *testing.T) {
	methods, err := getSvcMethods(base.RegisterHelloServer, &server{})
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Logf("methods:%v", methods)

	methods, err = getSvcMethods(base.RegisterHelloServer, &serverEm{})
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Logf("methods:%v", methods)
}

func TestClientEm(t *testing.T) {
	ctx := context.Background()
	client, err := NewMockClient(ctx, base.RegisterHelloServer, &serverEm{
		server: server{
			Str: "test",
		},
	})
	if err != nil {
		panic(err)
	}

	req := base.HelloReq{
		Name: "call 10086",
	}

	ir, err := client.Invoke(ctx, "SayHello", &req)
	if err != nil {
		t.Fatal(err)
	}

	resp, ok := ir.(*base.HelloRsp)
	if !ok {
		t.Fatal("!ok")
	}
	t.Logf("resp.Msg:\"%s\"", resp.Msg)
}
