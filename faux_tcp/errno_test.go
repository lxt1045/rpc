package faux_tcp

import (
	"context"
	"testing"
	"time"

	"github.com/lxt1045/errors"
)

// TestErrnoCodesUnique errno.go 的错误码必须唯一且落在本模块占用段内
func TestErrnoCodesUnique(t *testing.T) {
	all := []struct {
		name string
		e    *errors.Code
	}{
		{"ErrUnsupportedPlatform", ErrUnsupportedPlatform},
		{"ErrNeedRoot", ErrNeedRoot},
		{"ErrHandshakeTimeout", ErrHandshakeTimeout},
		{"ErrConnReset", ErrConnReset},
		{"ErrPeerTimeout", ErrPeerTimeout},
		{"ErrConnClosed", ErrConnClosed},
		{"ErrReadTimeout", ErrReadTimeout},
		{"ErrPacketTooBig", ErrPacketTooBig},
		{"ErrInvalidConfig", ErrInvalidConfig},
		{"ErrInvalidAddr", ErrInvalidAddr},
		{"ErrRawSocket", ErrRawSocket},
		{"ErrFirewall", ErrFirewall},
		{"ErrInvalidPacket", ErrInvalidPacket},
		{"ErrWriteTimeout", ErrWriteTimeout},
	}
	seen := make(map[int]string, len(all))
	for _, it := range all {
		c := errors.AsCode(it.e)
		if c == nil {
			t.Fatalf("%s: 不是带错误码的 error", it.name)
		}
		if c.Code() < moduleCode+1 || c.Code() > moduleCode+99 {
			t.Fatalf("%s: code=%d 超出模块段 %d", it.name, c.Code(), moduleCode)
		}
		if prev, dup := seen[c.Code()]; dup {
			t.Fatalf("%s 与 %s 错误码重复: %d", it.name, prev, c.Code())
		}
		seen[c.Code()] = it.name
		// New/Newf 实例与模板同码
		if got := errors.AsCode(it.e.New()); got == nil || got.Code() != c.Code() {
			t.Fatalf("%s: New() 实例错误码不一致", it.name)
		}
	}
}

// TestErrnoBehavior 关键路径的错误分类（调用方可用 AsCode 判断）
func TestErrnoBehavior(t *testing.T) {
	assertCode := func(t *testing.T, err error, want *errors.Code, what string) {
		t.Helper()
		c := errors.AsCode(err)
		if c == nil || c.Code() != want.Code() {
			t.Fatalf("%s: want code %d, got %v", what, want.Code(), err)
		}
	}

	cli, _, net0, _ := testPair(t, nil, nil)

	// Write 超过 MSS
	if _, err := cli.Write(make([]byte, 1449)); err == nil {
		t.Fatal("oversized write should fail")
	} else {
		assertCode(t, err, ErrPacketTooBig, "Write > MSS")
	}

	// 读超时
	cli.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	buf := make([]byte, 16)
	if _, err := cli.Read(buf); err == nil {
		t.Fatal("read should time out")
	} else {
		assertCode(t, err, ErrReadTimeout, "read deadline")
	}

	// 对端 RST
	cfg := Config{}
	cfg.defaults()
	rst := buildPacket(&cfg, testEndpoint(testServerIP, 8080), testEndpoint(testClientIP, 40000),
		1, 1, flagRST, clockMS(), 0, nil, 1)
	net0.deliver(testServerIP, testClientIP, rst)
	waitFor(t, time.Second, func() bool { return cli.(*Conn).c.getState() == stClosed }, "rst closes conn")
	cli.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := cli.Read(buf); err == nil {
		t.Fatal("read after rst should fail")
	} else {
		assertCode(t, err, ErrConnReset, "peer RST")
	}

	// 已关闭连接上写
	if _, err := cli.Write([]byte("x")); err == nil {
		t.Fatal("write on closed conn should fail")
	} else {
		assertCode(t, err, ErrConnClosed, "write on closed")
	}

	// 非法配置：PSK 预留字段置非空
	bad := Config{PSK: []byte("k")}
	bad.defaults()
	if err := bad.validate(); err == nil {
		t.Fatal("PSK should be rejected")
	} else {
		assertCode(t, err, ErrInvalidConfig, "PSK validate")
	}

	// 非法地址
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := Dial(ctx, Config{}, "", "not-an-addr"); err == nil {
		t.Fatal("bad addr should fail")
	} else {
		assertCode(t, err, ErrInvalidAddr, "parse raddr")
	}
}
