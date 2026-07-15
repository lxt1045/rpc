package rpc

import (
	"context"

	"github.com/lxt1045/rpc/codec"
)

type MiddlewareReqFunc = func(*CliParam)
type MiddlewareRespFunc = func(*SvcParam)

type CliParam struct {
	Ctx    context.Context
	Method string
	Req    codec.Msg
	Resp   codec.Msg
	Err    error

	idx uint
	cli *Client
}

func (p *CliParam) Next() {
	if p == nil {
		return
	}
	p.idx++
	if p.cli != nil {
		if p.idx <= uint(len(p.cli.middleware)) {
			for p.idx <= uint(len(p.cli.middleware)) {
				if f := p.cli.middleware[p.idx-1]; f != nil {
					f(p)
					if p.idx <= uint(len(p.cli.middleware)) {
						p.idx++
					}
				}
			}
		}
		if p.idx == uint(len(p.cli.middleware))+1 {
			p.Err = p.cli.invoke(p.Ctx, p.Method, p.Req, p.Resp)
		}
	}
	return
}

type SvcParam struct {
	Ctx    context.Context
	Method string
	Req    codec.Msg
	Resp   *codec.Msg
	Err    error

	idx uint
	svc *WrapSvcMethod
}

func (p *SvcParam) Next() {
	if p == nil {
		return
	}
	p.idx++
	if len(p.svc.middleware) > 0 {
		if p.idx <= uint(len(p.svc.middleware)) {
			for p.idx <= uint(len(p.svc.middleware)) {
				if f := p.svc.middleware[p.idx-1]; f != nil {
					f(p)
					if p.idx <= uint(len(p.svc.middleware)) {
						p.idx++
					}
				}
			}
		}
		if p.idx == uint(len(p.svc.middleware))+1 {
			*p.Resp, p.Err = p.svc.SvcMethod.SvcInvoke(p.Ctx, p.Req)
		}
	}
	return
}

type WrapSvcMethod struct {
	SvcMethod
	middleware []MiddlewareRespFunc
}

func (m WrapSvcMethod) SvcInvoke(ctx context.Context, req codec.Msg) (resp codec.Msg, err error) {
	mResp := &SvcParam{
		Ctx:    ctx,
		Method: m.Name,
		Req:    req,
		Resp:   &resp,
		svc:    &m,
	}
	mResp.Next()
	err = mResp.Err
	return
}

// func SvcLogid(p *SvcParam) {
// 	logid, _ := p.Ctx.Value(LogidKey{}).(uint64)
// 	if logid == 0 {
// 		logid = uint64(gid.GetGID())
// 	}
// 	p.Ctx, _ = log.WithLogid(p.Ctx, int64(logid))
// }

// func CliLogid(p *CliParam) {
// 	// logid, _ := p.Ctx.Value(LogidKey{}).(uint64)
// 	// if logid != 0 {
// 	// 	p.Ctx, _ = log.WithLogid(p.Ctx, int64(logid))
// 	// }
// 	logid, _ := log.Logid(p.Ctx)
// 	if logid != 0 {
// 		p.Ctx = context.WithValue(p.Ctx, LogidKey{}, uint64(logid))
// 	}
// }
