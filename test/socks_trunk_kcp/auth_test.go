package socks_kcp

import (
	"context"
	"testing"

	"github.com/lxt1045/rpc/test/socks_trunk_kcp/pb"
)

func TestAuthWrongTokenRejected(t *testing.T) {
	if err := InitServerSecurity("good-token", 8, 12345, 16); err != nil {
		t.Fatal(err)
	}
	svc := &SocksSvc{}
	resp, err := svc.Auth(context.Background(), &pb.AuthReq{Name: "bad-token"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != pb.AuthRsp_Fail {
		t.Fatalf("wrong token should be rejected, got %v", resp.Status)
	}
	CloseAllSessions()
}

func TestAuthSuccess(t *testing.T) {
	if err := InitServerSecurity("good-token", 8, 12345, 16); err != nil {
		t.Fatal(err)
	}
	svc := &SocksSvc{}
	resp, err := svc.Auth(context.Background(), &pb.AuthReq{Name: "good-token"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != pb.AuthRsp_Succ {
		t.Fatalf("good token should succeed, got %v", resp.Status)
	}
	CloseAllSessions()
}
