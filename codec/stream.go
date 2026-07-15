package codec

import (
	"context"
	stderr "errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/lxt1045/rpc/base"
	"github.com/lxt1045/utils/log"
)

/*
TODO:
  切换stream后，放弃此连接，全力供应stream，类似 websocket那种方式！
*/

type Stream struct {
	codec     *Codec
	callID    uint16
	callSN    uint32
	cache     []Msg
	cacheLock sync.Mutex
	cacheCh   chan struct{}

	caller     Method
	connectAt  int64
	deadline   int64 // 超时时间，超时后删除
	bSvc       bool
	bClosed    bool
	bFirstCall bool // 流的第一次调用
}

func (s *Stream) Close(ctx context.Context) {
	// 1. 先发送关闭请求
	req := &base.CmdReq{Cmd: base.CmdReq_StreamClose}
	res := &base.CmdRsp{}
	done, err := s.codec.clientCall(ctx, VerCmdReq, s.callID, s.callSN, req, res)
	if err != nil {
		return
	}
	if done != nil {
		select {
		case err = <-done:
		case <-ctx.Done():
			err = stderr.New("resp timeout")
		}
		if err != nil {
			log.Ctx(ctx).Warn().Caller().Err(err).Msg("Stream.close")
		}
	}

	// 2. 从 s.codec.streams 删除
	key := respsKey(s.callSN)
	s.codec.streamsLock.Lock()
	defer s.codec.streamsLock.Unlock()
	delete(s.codec.streams, key)

	// 3. 关闭标志
	s.close()
}

func (s *Stream) close() {
	s.bClosed = true
	select {
	case s.cacheCh <- struct{}{}:
	default:
	}
	// close(s.cacheCh)

}

// func (s *Stream) Method() (method string) {
// 	return s.codec.callers[s.callID].FuncName()
// }

func (s *Stream) Recv(ctx context.Context) (resp Msg, err error) {
	recv := func() (req Msg) {
		s.cacheLock.Lock()
		defer s.cacheLock.Unlock()
		if len(s.cache) > 0 {
			req = s.cache[0]
			s.cache = s.cache[1:]
			select {
			case s.cacheCh <- struct{}{}:
			default:
			}
			return
		}
		return
	}

	for {
		resp = recv()
		if resp != nil {
			return
		}
		if s.bClosed {
			err = io.EOF
			return
		}
		select {
		case <-s.cacheCh:
		case <-ctx.Done():
		}
	}
}

// Clear 清空以前接收的数据（准备好接收下一个数据包）
func (s *Stream) Clear(ctx context.Context) {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	s.cache = s.cache[:0]
}

func (s *Stream) write(m Msg) (err error) {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	if len(s.cache) > 10240 {
		err = ErrUnexpected.Clonef("The cache size exceeds the limit:%d", len(s.cache))
		return
	}
	s.cache = append(s.cache, m)
	select {
	case s.cacheCh <- struct{}{}:
	default:
	}
	return
}

func (s *Stream) Send(ctx context.Context, req Msg) (err error) {
	ver := uint16(VerStreamReq)
	typ := s.caller.ReqType()
	if s.bSvc {
		ver = VerStreamResp
		typ = s.caller.RespType()
	}
	if t := reflect.TypeOf(req); req != nil && t.Elem() != typ {
		err = ErrUnexpected.Clonef("req type error: should be %s, not %s", typ.String(), t.Elem().String())
		return
	}

	err = s.codec.StreamCall(ctx, ver, s.callID, s.callSN, req)
	if err != nil {
		return
	}
	return
}

func (c *Codec) Stream(ctx context.Context, callID uint16, caller Method, sync bool) (stream *Stream, err error) {
	stream = &Stream{
		codec:     c,
		callID:    callID,
		cacheCh:   make(chan struct{}),
		connectAt: time.Now().Unix(),
		caller:    caller,
	}
	if c.IsClosed() {
		err = stderr.New("Codec is closed")
		return
	}
	func() {
		c.streamsLock.Lock()
		defer c.streamsLock.Unlock()
		if c.streams == nil {
			err = stderr.New("c.streams == nil")
			return
		}
		for {
			callSN := atomic.AddUint32(&c.tmpCallSN, 1)
			key := respsKey(callSN)
			if _, ok := c.streams[key]; ok {
				continue
			}
			stream.callSN = callSN
			c.streams[key] = stream
			return
		}
	}()
	req := &base.CmdReq{Cmd: base.CmdReq_Stream}
	res := &base.CmdRsp{}
	done, err := c.clientCall(ctx, VerCmdReq, callID, stream.callSN, req, res)
	if err != nil {
		return
	}
	if sync && done != nil {
		select {
		case err = <-done:
		case <-c.Done():
			err = stderr.New("resp timeout")
		case <-ctx.Done():
			err = stderr.New("resp timeout")
		}
	}
	return
}

type ctxStreamKey struct{}
type ctxUpgradeKey struct{}

func GetStream(ctx context.Context) (stream *Stream) {
	s := ctx.Value(ctxStreamKey{})
	stream, ok := s.(*Stream)
	if !ok {
		log.Ctx(ctx).Info().Caller().Msgf("unkonw type: %T", s)
	}
	return
}
func GetUpgrade(ctx context.Context) (upgrade *Upgrade) {
	s := ctx.Value(ctxUpgradeKey{})
	upgrade, ok := s.(*Upgrade)
	if !ok {
		log.Ctx(ctx).Info().Caller().Msgf("unkonw type: %T", s)
	}
	return
}

func (c *Codec) VerStreamReq(ctx context.Context, header Header, bsBody []byte) (err error) {
	stream, bFirstCall := func() (stream *Stream, bFirstCall bool) {
		key := respsKey(header.CallSN)
		c.streamsLock.RLock()
		defer c.streamsLock.RUnlock()
		stream = c.streams[key]
		if stream != nil && stream.bFirstCall {
			bFirstCall = true
			stream.bFirstCall = false
		}
		stream.deadline = time.Now().Unix() + 30*60 // 连续2min没有数据就删除
		// log.Ctx(ctx).Error().Caller().Interface("c.streams", c.streams).Uint64("key", key).Msg("VerStreamReq")
		return
	}()
	if stream == nil || stream.caller == nil {
		err = ErrUnexpected.Clonef("CallSN is not exist, header.Channel: %d, header.CallSN: %d", header.CtxLen, header.CallSN)
		log.Ctx(ctx).Error().Caller().Err(err).Interface("header", header).Msg("drop, stream is nil")
		return
	}

	req := stream.caller.NewReq()
	err = proto.Unmarshal(bsBody, req)
	if err != nil {
		err = ErrUnexpected.WithErr(err)
		log.Ctx(ctx).Error().Caller().Interface("header", header).Err(err).Msg("drop")
		return
	}

	if bFirstCall {
		ctx = context.WithValue(ctx, ctxStreamKey{}, stream)
		go c.Handler(ctx, stream.caller, header, req)
		return
	}

	err = stream.write(req)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Interface("header", header).Err(err).Msg("drop")
	}
	return
}

func (c *Codec) VerStreamResp(ctx context.Context, header Header, bsBody []byte) (err error) {
	key := respsKey(header.CallSN)
	stream := func() *Stream {
		c.streamsLock.RLock()
		defer c.streamsLock.RUnlock()
		stream := c.streams[key]
		if stream != nil {
			stream.deadline = time.Now().Unix() + 30*60 // 连续2min没有数据就删除
		}
		return stream
	}()
	if stream == nil || stream.caller == nil {
		stream = func() *Stream {
			// time.Sleep(time.Millisecond * 10)
			c.streamsLock.Lock()
			defer c.streamsLock.Unlock()
			stream := c.streams[key]
			if stream != nil {
				stream.deadline = time.Now().Unix() + 30*60 // 连续2min没有数据就删除
			}
			return stream
		}()
		if stream == nil || stream.caller == nil {
			err = ErrUnexpected.Clonef("CallSN is not exist, header.Channel: %d, header.CallSN: %d", header.CtxLen, header.CallSN)
			return
		}
	}

	resp := stream.caller.NewResp()
	err = proto.Unmarshal(bsBody, resp)
	if err != nil {
		err = ErrUnexpected.Clonef(err.Error())
		log.Ctx(ctx).Error().Caller().Interface("header", header).Err(err).Msg("drop")
		return
	}
	err = stream.write(resp)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Interface("header", header).Err(err).Msg("drop")
	}
	return
}
