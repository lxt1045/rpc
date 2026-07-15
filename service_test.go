package rpc

import (
	"context"
	"io"
	"reflect"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/lxt1045/rpc/base"
	"github.com/lxt1045/rpc/conn"
	"github.com/lxt1045/utils/log"
)

func TestCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := StartService(ctx, nil, &server{Str: "test"}, base.RegisterHelloServer)
	if err != nil {
		t.Fatal(err)
	}

	all := s.AllInterfaces()
	for i, m := range all {
		svc := (*server)(m.SvcPointer)
		t.Logf("idx:%d, service.Str:%v, func_key:%s, req:%s",
			i, svc.Str, m.Name, m.reqType.String())
	}

	idx, exist := s.MethodIdx("base.HelloServer.SayHello")
	if !exist {
		t.Fatal("exist")
	}

	req := base.HelloReq{
		Name: "call 1",
	}
	r, err := s.Call(ctx, idx, unsafe.Pointer(&req))
	if err != nil {
		t.Fatal(err)
	}
	resp := (*base.HelloRsp)(r)
	t.Logf("resp.Msg:\"%s\"", resp.Msg)
}

func BenchmarkMethod(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := StartService(ctx, nil, &server{Str: "test"}, base.RegisterHelloServer)
	if err != nil {
		b.Fatal(err)
	}

	req := base.BenchmarkReq{}
	idx, exist := s.MethodIdx("base.HelloServer.Benchmark")
	if !exist {
		b.Fatal("exist")
	}

	b.Run("Call", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := s.Call(ctx, idx, unsafe.Pointer(&req))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("CallByReflect", func(b *testing.B) {
		svc := &server{}
		svcImpMethod, ok := reflect.TypeOf(svc).MethodByName("Benchmark")
		if !ok {
			b.Fatal("!ok")
		}
		fCall := func(ctx context.Context, methodIdx uint32, req interface{}) (resp interface{}, err error) {
			rvs := svcImpMethod.Func.Call([]reflect.Value{reflect.ValueOf(svc), reflect.ValueOf(ctx), reflect.ValueOf(req)})
			resp = rvs[0].Interface()
			err, _ = rvs[1].Interface().(error)
			return
		}
		for i := 0; i < b.N; i++ {
			_, err := fCall(ctx, idx, &req)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestPipe(t *testing.T) {
	if (SvcMethod{}).RespType() == nil {
		t.Log("nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, c := NewFakeConnPipe()
	svc, err := conn.NewZip(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := conn.NewZip(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	_, err = StartService(ctx, svc, &server{Str: "test"}, base.RegisterHelloServer)
	if err != nil {
		t.Fatal(err)
	}

	// time.Sleep(time.Millisecond * 10)
	client, err := StartClient(ctx, cli, base.NewHelloClient)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
	{
		req := base.HelloReq{Name: "call 1"}
		resp := base.HelloRsp{}
		err := client.Invoke(ctx, "SayHello", &req, &resp)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("resp.Msg:\"%s\"", resp.Msg)
	}
	{
		req := base.HelloReq{Name: "call 2"}
		resp := base.HelloRsp{}
		err := client.Invoke(ctx, "SayHello", &req, &resp)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("resp.Msg:\"%s\"", resp.Msg)
	}
}

func TestPipeStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, c := NewFakeConnPipe()
	svc, err := conn.NewZip(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := conn.NewZip(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	_, err = StartService(ctx, svc, &server{Str: "test"}, base.RegisterHelloServer)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)
	client, err := StartClient(ctx, cli, base.NewHelloClient)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// base.NewHelloClient(nil)

	stream, err := client.Stream(ctx, "SayHello")
	if err != nil {
		t.Fatal(err)
	}

	// Recv 与 Send 次数对齐，避免 stream.Close 之后再 Recv 得到 EOF 被当作错误。
	const iterations = 5
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for i := 0; i < iterations; i++ {
			r, err := stream.Recv(ctx)
			if err != nil {
				if err == io.EOF {
					return
				}
				t.Errorf("stream.Recv: %v", err)
				return
			}
			resp, ok := r.(*base.HelloRsp)
			if !ok {
				t.Errorf("unexpected response type: %T", r)
				return
			}
			log.Ctx(ctx).Info().Caller().Msg("client recv: " + resp.Msg)
		}
	}()
	for i := 0; i < iterations; i++ {
		req := base.HelloReq{
			Name: "client Send " + strconv.Itoa(i),
		}
		log.Ctx(ctx).Info().Caller().Msg("client send: " + req.Name)
		err = stream.Send(ctx, &req)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond * 100)
	}
	<-recvDone
	stream.Close(ctx)
}
