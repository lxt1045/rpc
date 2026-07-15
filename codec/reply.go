package codec

import (
	"context"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc/base"
	"github.com/lxt1045/utils/log"
)

func (c *Codec) Handler(ctx context.Context, caller Method, header Header, req Msg) (resp Msg, err error) {
	defer func() {
		// resp 发送放 defer 中，及时panic也有返回值
		e := recover()
		if e != nil {
			err = errors.Errorf("func: %s, recover: %v", caller.FuncName(), e)
		}
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
		}
	}()
	resp, err = caller.SvcInvoke(ctx, req)
	if err != nil {
		if header.Ver == VerCallReq || header.Ver == VerUpgradeReq {
			log.Ctx(ctx).Debug().Caller().Err(err).Msg("rpc resp err")
			pbErr := &base.Err{}
			code, ok := err.(interface {
				Code() int
				Msg() string
			})
			if ok {
				pbErr.Code = int64(code.Code())
				pbErr.Msg = code.Msg()
			} else {
				pbErr.Code = -1
				pbErr.Msg = err.Error()
			}
			if stack, ok := err.(interface{ Stack() (stack []string) }); ok {
				pbErr.Stack = stack.Stack()
			}
			ver := uint16(VerCallErrResp)
			if header.Ver == VerUpgradeReq {
				ver = VerUpgradeErrResp
			}
			err = c.SendMsg(ctx, ver, header.CallID, header.CallSN, pbErr)
			if err != nil {
				return
			}
		}
		return
	}
	if resp == nil || caller.RespType() == nil {
		return
	}

	ver := uint16(VerCallResp)
	if header.Ver == VerStreamReq {
		ver = VerStreamResp
	} else if header.Ver == VerUpgradeReq {
		ver = VerUpgradeResp
	}

	err = c.SendMsg(ctx, ver, header.CallID, header.CallSN, resp)
	if err != nil {
		return
	}
	return
}

// 收到 Req
func (c *Codec) VerCallReq(ctx context.Context, header Header, bsBody []byte) (err error) {
	if uint16(len(c.callers)) <= header.CallID {
		log.Ctx(ctx).Error().Caller().Interface("header", header).Msg("drop, caller is nil")
		return
	}
	caller := c.callers[header.CallID]
	if caller == nil {
		log.Ctx(ctx).Error().Caller().Interface("header", header).Msg("drop, caller is nil")
		return
	}
	req := caller.NewReq()
	err = proto.Unmarshal(bsBody, req)
	if err != nil {
		err = errors.Errorf(err.Error())
		log.Ctx(ctx).Error().Caller().Interface("header", header).Err(err).Msg("drop")
		return
	}

	go c.Handler(ctx, caller, header, req)
	return
}

// 收到 Resp
func (c *Codec) VerCallResp(ctx context.Context, header Header, bsBody []byte) (err error) {
	res := func() resp {
		key := respsKey(header.CallSN)
		c.respsLock.Lock()
		defer c.respsLock.Unlock()
		res := c.resps[key]
		delete(c.resps, key)
		return res
	}()
	if res.r == nil {
		log.Ctx(ctx).Error().Caller().Interface("header", header).Msg("drop, res.r is nil")
		return
	}
	if header.Ver == VerCallErrResp || header.Ver == VerUpgradeErrResp {
		pbErr := &base.Err{}
		err = proto.Unmarshal(bsBody, pbErr)
		if err != nil {
			err = errors.Errorf(err.Error())
			log.Ctx(ctx).Error().Caller().Interface("header", header).Err(err).Msg("proto.Unmarshal(&base.Err{}) drop")
			res.done <- err
			return
		}
		select {
		case res.done <- errors.NewCodeWithStack(int(pbErr.Code), pbErr.Msg, pbErr.Stack):
		default:
		}
		return
	}

	err = proto.Unmarshal(bsBody, res.r)
	if err != nil {
		err = errors.Errorf(err.Error())
		log.Ctx(ctx).Error().Caller().Interface("header", header).Err(err).Msg("drop")
		select {
		case res.done <- err:
		default:
		}
		return
	}

	select {
	case res.done <- nil:
	default:
	}
	return
}

// 收到 Req
func (c *Codec) VerCmdReq(ctx context.Context, header Header, bsBody []byte) (err error) {
	req := &base.CmdReq{}
	err = proto.Unmarshal(bsBody, req)
	if err != nil {
		err = errors.Errorf(err.Error())
		log.Ctx(ctx).Error().Caller().Interface("header", header).Err(err).Msg("drop")
		return
	}

	switch req.Cmd {
	case base.CmdReq_Stream:
		res := &base.CmdRsp{
			Status: base.CmdRsp_Succ,
		}
		var caller Method
		if uint16(len(c.callers)) > header.CallID {
			caller = c.callers[header.CallID]
		}
		if caller == nil {
			log.Ctx(ctx).Error().Caller().Interface("header", header).Msg("drop, caller is nil")
			return
		}
		stream := &Stream{
			codec:      c,
			callID:     header.CallID,
			callSN:     header.CallSN,
			cacheCh:    make(chan struct{}),
			bSvc:       true,
			connectAt:  time.Now().Unix(),
			caller:     caller,
			bFirstCall: true,
		}
		func() {
			key := respsKey(header.CallSN)
			c.streamsLock.Lock()
			defer c.streamsLock.Unlock()
			c.streams[key] = stream
			// log.Ctx(ctx).Error().Caller().Interface("c.streams", c.streams).Uint64("key", key).Msg("CmdReq_Stream")
		}()

		err = c.SendMsg(ctx, VerCmdResp, header.CallID, header.CallSN, res)
		if err != nil {
			return
		}
	case base.CmdReq_StreamClose:
		func() {
			key := respsKey(header.CallSN)
			c.streamsLock.Lock()
			defer c.streamsLock.Unlock()
			stream := c.streams[key]
			if stream != nil {
				stream.close()
				delete(c.streams, key)
			}
		}()
		res := &base.CmdRsp{
			Status: base.CmdRsp_Succ,
		}
		err = c.SendMsg(ctx, VerCmdResp, header.CallID, header.CallSN, res)
		if err != nil {
			return
		}
		log.Ctx(ctx).Info().Caller().Interface("header", header).Msg("stream close by peer")
	case base.CmdReq_Auth:
	case base.CmdReq_CallIDs:
		c.svcPassKeys = req.Fields
		res := &base.CmdRsp{
			Status: base.CmdRsp_Succ,
			// Fields: c.callIDs,
		}
		for _, caller := range c.callers {
			res.Fields = append(res.Fields, caller.FuncName())
		}

		err = c.SendMsg(ctx, VerCmdResp, header.CallID, header.CallSN, res)
		if err != nil {
			return
		}
	default:
	}
	return
}

type PbErr struct {
	base.Err
}

func (e PbErr) Error() string {
	if e.Logid == 0 {
		return strconv.FormatInt(e.Err.Code, 10) + ", " + e.Err.Msg
	}
	return strconv.FormatInt(e.Err.Code, 10) + ", " + e.Err.Msg + ", " + "logid=" + strconv.FormatInt(e.Logid, 10)
}

func (e PbErr) Code() int {
	return int(e.Err.Code)
}
func (e PbErr) Msg() string {
	return e.Err.Msg
}
