package rpc

import (
	"context"
	"math/rand"
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

func TestPassword(t *testing.T) {
	t.Logf("RandPwd:%s", RandPwd(8))
	t.Logf("RandPwd:%s", RandPwd(16))
}

func RandPwd(l int) (salt string) {
	const Bytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()-=_+"
	bs := make([]byte, 0, l)
	for ; l > 0; l-- {
		idx := rand.Int31n(int32(len(Bytes)))
		bs = append(bs, Bytes[idx])
	}
	return string(bs)
}

func RandPwd2(l int) (salt string) {
	const Bytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()-=_+"
	bs := make([]byte, 0, l)
	for ; l > 0; l-- {
		idx := rand.Int31n(int32(len(Bytes)))
		bs = append(bs, Bytes[idx])
	}
	return string(bs)
}
