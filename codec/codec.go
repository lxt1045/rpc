package codec

import (
	"context"
	stderr "errors"
	"io"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc/base"
	"github.com/lxt1045/utils/delay"
	"github.com/lxt1045/utils/gid"
	"github.com/lxt1045/utils/log"
)

var (
	ErrUnexpected = errors.NewCode(0, 1, "codec unexpected error")
)

type LogidKey struct{}

var (
	bufPool = sync.Pool{
		New: func() any {
			return make([]byte, math.MaxUint16)
		},
	}
)

type Msg interface {
	proto.Message
}

// Method 表示一个 struct 的可以调用的方法
type Method interface {
	ReqType() reflect.Type
	RespType() reflect.Type
	NewReq() Msg
	NewResp() Msg
	SvcInvoke(ctx context.Context, req Msg) (resp Msg, err error)
	FuncName() string
}

type Codec struct {
	cancel context.CancelFunc
	// chDone <-chan struct{}
	ctx context.Context

	// rwcLock sync.Mutex
	rwc       io.ReadWriteCloser
	tmpCallSN uint32

	upgradeLock sync.Mutex
	upgrade     *Upgrade
	status      uint32 // 0: normal, 1: upgrading, 2: upgraded

	respsLock    sync.Mutex
	resps        map[uint64]resp
	segmentsLock sync.Mutex
	segments     map[uint64][]byte // 分片
	delay        *delay.Queue[post]
	delayTime    int64

	callers []Method

	streamsLock sync.RWMutex
	streams     map[uint64]*Stream

	cliPassKeys []string // 客户端: ctx 透传 key
	svcPassKeys []string // 服务端: ctx 透传 key

	local  string
	remote string
}

type resp struct {
	r    Msg
	done chan<- error
}

type post struct {
	key   uint64
	Codec *Codec
	done  chan error
}

func (d post) Post() {
	if d.done != nil {
		select {
		case d.done <- stderr.New("resp timeout"):
		default:
		}
		func() {
			d.Codec.respsLock.Lock()
			defer d.Codec.respsLock.Unlock()

			delete(d.Codec.resps, d.key)
		}()
	} else {
		func() {
			d.Codec.segmentsLock.Lock()
			defer d.Codec.segmentsLock.Unlock()

			delete(d.Codec.segments, d.key)
		}()
	}
}

// ctxPassKeys: 作为cli时, ctx 中需要透传给对方的key
func NewCodec(ctx context.Context, rwc io.ReadWriteCloser, callers []Method, ctxPassKeys []string, needHeartbeat bool) (c *Codec, err error) {
	ctx, cancel := context.WithCancel(ctx)
	c = &Codec{
		// chDone:      ctx.Done(),
		ctx:         ctx,
		cancel:      cancel,
		rwc:         rwc,
		resps:       make(map[uint64]resp),
		segments:    make(map[uint64][]byte),
		delay:       delay.New[post](64, int64(time.Second)*30, true),
		streams:     make(map[uint64]*Stream),
		callers:     callers,
		cliPassKeys: ctxPassKeys,
	}

	rw, ok := c.rwc.(interface {
		RemoteAddr() net.Addr
		LocalAddr() net.Addr
	})
	if ok {
		c.local, c.remote = rw.LocalAddr().String(), rw.RemoteAddr().String()
	}
	go c.ReadLoop()
	if needHeartbeat {
		go c.Heartbeat()
	}
	return
}

func (c *Codec) Close() (err error) {
	if c == nil {
		return
	}

	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	if c.delay != nil {
		c.delay.Range(func(t post) {
			t.Post()
		})
		c.delay.Close()
		c.delay = nil
	}

	if c.streams != nil {
		func() {
			c.streamsLock.Lock()
			defer c.streamsLock.Unlock()
			for _, s := range c.streams {
				s.close()
			}
		}()
		c.streams = nil
	}

	if atomic.LoadUint32(&c.status) == 0 {
		c.SendCloseMsg(c.ctx)
		time.Sleep(time.Millisecond * 10)
	}

	c.respsLock.Lock()
	defer c.respsLock.Unlock()
	if c.rwc != nil {
		if tcpConn, ok := c.rwc.(*net.TCPConn); ok {
			// 不能直接直接调用 conn.Close()，会发送RST 直接断开tcp 链接
			tcpConn.CloseWrite()
			log.Ctx(context.TODO()).Info().Caller().Msg("Codec.Close(), Copy CloseWrite, Send FIN")
		} else {
			err = c.rwc.Close()
		}
		c.rwc = nil
	}
	return
}

func (c *Codec) IsClosed() (yes bool) {
	return c == nil || c.rwc == nil
}

func (rpc *Codec) Done() <-chan struct{} {
	return rpc.ctx.Done()
}

func (c *Codec) ClientCall(ctx context.Context, callID uint16, req, res Msg) (done <-chan error, err error) {
	return c.clientCall(ctx, VerCallReq, callID, 0, req, res)
}

func (c *Codec) StreamCall(ctx context.Context, ver, callID uint16, callSN uint32, req Msg) (err error) {
	yes := func() bool {
		key := respsKey(callSN)
		c.streamsLock.RLock()
		defer c.streamsLock.RUnlock()
		stream := c.streams[key]
		return stream != nil
	}()
	if !yes {
		return errors.Errorf("stream has been closed")
	}
	_, err = c.clientCall(ctx, ver, callID, callSN, req, nil)
	return
}

func respsKey(callSN uint32) uint64 {
	return uint64(callSN)
}

func (c *Codec) Heartbeat() {
	ctx := c.ctx
	tickerHeartbeat := time.NewTicker(time.Duration(time.Second * 60)) // 心跳包; client 发送
	defer func() {
		tickerHeartbeat.Stop()
		if atomic.LoadUint32(&c.status) == 0 {
			log.Ctx(ctx).Info().Caller().Str("local", c.local).Str("remote", c.remote).Msg("Heartbeat.defer()")
			err := c.Close()
			if err != nil && !strings.Contains(err.Error(), "has been closed") {
				log.Ctx(ctx).Info().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Msg("Heartbeat.defer()")
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerHeartbeat.C:
		}

		delKeys := []uint64{}
		tsNow := time.Now().Unix()
		func() {
			c.streamsLock.RLock()
			defer c.streamsLock.RUnlock()
			for k, s := range c.streams {
				if s.deadline > 0 && s.deadline < tsNow {
					delKeys = append(delKeys, k)
				}
			}
		}()
		func() {
			if len(delKeys) > 0 {
				c.streamsLock.Lock()
				defer c.streamsLock.Unlock()
				for _, k := range delKeys {
					delete(c.streams, k)
				}
			}
		}()

		if atomic.LoadUint32(&c.status) != 0 {
			// <-ctx.Done()
			return
		}
		err := c.SendHeartbeatMsg(ctx)
		// if ErrHasBeenClosed.Is(err) {
		// 	break
		// }
		if err != nil {
			log.Ctx(ctx).Error().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Msg("Heartbeat")
			return
		}
		log.Ctx(ctx).Trace().Caller().Str("local", c.local).Str("remote", c.remote).Msg("Heartbeat")
	}
}

func (c *Codec) ReadLoop() {
	var err error
	ctx := c.ctx
	isUpgrade := false
	defer func() {
		e := recover()
		if e != nil {
			err = ErrUnexpected.Clonef("recove:%v", e)
		}
		if err != nil {
			log.Ctx(ctx).Info().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Msg("ReadLoop defer")
		} else if !isUpgrade {
			log.Ctx(ctx).Debug().Caller().Bool("isUpgrade", isUpgrade).Str("local", c.local).Str("remote", c.remote).Err(err).Msg("ReadLoop defer")
		}

		if isUpgrade {
			c.delay.Close()
		} else if c.rwc != nil {
			c.Close()
		}
	}()

	rbuf := make([]byte, 0, math.MaxUint16)
	for {
		select {
		case <-ctx.Done():
			return // 检查是否已经退出，如果退出则返回
		default:
		}

		var header Header
		var bsBody []byte
		if atomic.LoadUint32(&c.status) != 0 {
			err = ErrHasBeenClosed.Clonef("ReadLoop c.status: %d", c.status)
			return
		}
		header, bsBody, err = ReadPack(ctx, c.rwc, rbuf) // TODO: 设置读超时？？
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}
			// err = errors.Errorf(err.Error())
			return
		}

		// 需要分片时
		if header.SegmentCount > 1 {
			key := respsKey(header.CallSN)
			bContinue := func() (bContinue bool) {
				c.segmentsLock.Lock()
				defer c.segmentsLock.Unlock()

				bsOld, ok := c.segments[key]

				if header.SegmentIdx == 0 {
					c.delay.Push(post{Codec: c, key: key})
				} else if !ok {
					log.Ctx(ctx).Error().Caller().Str("local", c.local).Str("remote", c.remote).Interface("header", header).Msg("the stream has been closed, drop")
					return true
				}

				bsBody = append(bsOld, bsBody...) // 都是再同一个 ClientReadLoop 协程中，无需加锁

				// 不是最后一个分片
				if header.SegmentCount-1 > header.SegmentIdx {
					c.segments[key] = bsBody
					return true
				}
				delete(c.segments, key) // 会不会出现意外而无法清理的情况
				return false
			}()
			if bContinue {
				continue
			}
		}

		ctxDo, logID := ctx, uint64(gid.GetGID())
		if header.CtxLen != 0 {
			m := base.Ctx{}
			err = proto.Unmarshal(bsBody[:header.CtxLen], &m)
			if err != nil {
				err = ErrUnexpected.WithErr(err)
				return
			}
			if m.LogID > 0 {
				logID = m.LogID
			}

			if len(c.svcPassKeys) == len(m.Fields) {
				for i, k := range c.svcPassKeys {
					ctxDo = context.WithValue(ctxDo, k, m.Fields[i])
				}
			}
			bsBody = bsBody[header.CtxLen:]
		}
		ctxDo = context.WithValue(ctxDo, LogidKey{}, logID)
		ctxDo, _ = log.WithLogid(ctxDo, int64(logID))

		switch header.Ver {
		case VerHeartbeat:
			log.Ctx(ctxDo).Trace().Caller().Str("local", c.local).Str("remote", c.remote).Msg("heartbeat by peer")

		case VerCallReq:
			err = c.VerCallReq(ctxDo, header, bsBody)
			if err != nil {
				log.Ctx(ctxDo).Error().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Send()
			}
		case VerCallErrResp, VerUpgradeErrResp: // 返回Error
			fallthrough
		case VerCallResp:
			err = c.VerCallResp(ctxDo, header, bsBody)
			if err != nil {
				log.Ctx(ctxDo).Error().Caller().Err(err).Send()
			}
		case VerClose:
			log.Ctx(ctxDo).Debug().Caller().Str("local", c.local).Str("remote", c.remote).Msg("close by peer")
			return
		case VerCmdReq:
			err = c.VerCmdReq(ctxDo, header, bsBody)
			if err != nil {
				log.Ctx(ctxDo).Error().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Send()
			}
		case VerCmdResp:
			err = c.VerCallResp(ctxDo, header, bsBody)
			if err != nil {
				log.Ctx(ctxDo).Error().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Send()
			}
		case VerStreamReq:
			// log.Ctx(ctx).Info().Caller().Str("local", c.local).Str("remote", c.remote).Msgf("Get VerStreamReq: %s", bsBody)
			err = c.VerStreamReq(ctxDo, header, bsBody)
			if err != nil {
				log.Ctx(ctxDo).Error().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Send()
			}
		case VerStreamResp:
			err = c.VerStreamResp(ctxDo, header, bsBody)
			if err != nil {
				log.Ctx(ctxDo).Error().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Send()
			}

		case VerUpgradeReq:
			err = c.VerUpgradeReq(ctxDo, header, bsBody)
			if err != nil {
				log.Ctx(ctxDo).Error().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Send()
			} else {
				// <-c.Done() // conn 已被接管，等待连接关闭
				isUpgrade = true
				return
			}
		case VerUpgradeResp:
			err = c.VerUpgradeResp(ctxDo, header, bsBody)
			if err != nil {
				log.Ctx(ctxDo).Error().Caller().Str("local", c.local).Str("remote", c.remote).Err(err).Send()
			} else {
				// <-c.Done() // conn 已被接管，等待连接关闭
				isUpgrade = true
				return
			}
		default:
			log.Ctx(ctxDo).Error().Caller().Str("local", c.local).Str("remote", c.remote).Interface("header", header).Msg("unknown version")
		}
	}
}

// ReadPack 读一个裸消息
func ReadPack(ctx context.Context, r io.ReadCloser, buf []byte) (header Header, bsBody []byte, err error) {
	n, err := io.ReadFull(r, buf[:2]) // 先读长度
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}
		err = errors.Errorf("%d: %s", n, err.Error())
		return
	}
	if n != 2 {
		err = errors.Errorf("n != 2,%d", n)
		return
	}
	lenNeed := ParseHeaderLen(buf[:2])
	if cap(buf) < int(lenNeed) {
		bufnew := make([]byte, lenNeed)
		bufnew[0], bufnew[1] = buf[0], buf[1] // copy(bufnew, bsHeader[:2])
		buf = bufnew
	}
	n, err = io.ReadFull(r, buf[2:lenNeed]) // 读取剩下的部分
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	if n != int(lenNeed-2) {
		err = errors.Errorf("n != 2,%d", n)
		return
	}

	header, lHeader := ParseHeader(buf[:HeaderSize])
	bsBody = buf[lHeader:lenNeed]
	return
}

func (c *Codec) SendCloseMsg(ctx context.Context) (err error) {
	if c == nil {
		return
	}
	wbuf := make([]byte, HeaderSize)
	h := Header{
		Ver:          VerClose,
		CtxLen:       0,
		Len:          uint16(len(wbuf)),
		SegmentCount: 0,
		SegmentIdx:   0,
		CallID:       0,
		CallSN:       0,
	}
	h.Format(wbuf)
	if c.rwc == nil {
		err = errors.Errorf("has been closed")
		return
	}
	if atomic.LoadUint32(&c.status) != 0 {
		err = ErrHasBeenClosed.Clonef("SendCloseMsg c.status: %d", c.status)
		return
	}
	_, err = c.rwc.Write(wbuf)
	if err != nil {
		err = errors.New(err.Error())
		return
	}
	return
}

var ErrHasBeenClosed = errors.NewCode(0, 0x1111, "has been closed")
var ErrHasBeenUpgraded = errors.NewCode(0, 0x1111, "has been upgraded")

func (c *Codec) SendHeartbeatMsg(ctx context.Context) (err error) {
	wbuf := make([]byte, HeaderSize)
	h := Header{
		Ver:          VerHeartbeat,
		CtxLen:       0,
		Len:          uint16(len(wbuf)),
		SegmentCount: 0,
		SegmentIdx:   0,
		CallID:       0,
		CallSN:       0,
	}
	h.FormatRaw(wbuf)
	if c.rwc == nil {
		err = ErrHasBeenClosed.Clone("SendHeartbeatMsg rwc is nil")
		return
	}
	_, err = c.rwc.Write(wbuf)
	if err != nil {
		err = errors.New(err.Error())
	}
	return
}

func (c *Codec) SendCmd(ctx context.Context, req *base.CmdReq) (res *base.CmdRsp, err error) {
	res = &base.CmdRsp{}
	if base.CmdReq_CallIDs == req.Cmd {
		if len(req.Fields) != 0 {
			err = ErrUnexpected.Clonef("CmdReq_CallIDs's Fields must be empty")
			return
		}
		req.Fields = c.cliPassKeys
	}
	done, err := c.clientCall(ctx, VerCmdReq, 0, 0, req, res)
	if err != nil {
		return
	}
	if done != nil {
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

func (c *Codec) clientCall(ctx context.Context, ver, callID uint16, callSN uint32, req, res Msg) (done <-chan error, err error) {
	if callSN == 0 {
		callSN = atomic.AddUint32(&c.tmpCallSN, 1)
	}

	// 先安排好返回路径，再发送请求
	if res != nil {
		func() {
			ch := make(chan error, 1)
			done = ch
			res := resp{
				r:    res,
				done: ch,
			}
			key := respsKey(callSN)
			c.delay.Push(post{Codec: c, key: key, done: ch}) // 写入超时队列

			c.respsLock.Lock()
			defer c.respsLock.Unlock()
			c.resps[key] = res
		}()
	}

	err = c.SendMsg(ctx, ver, callID, callSN, req)
	return
}

func (c *Codec) SendMsg(ctx context.Context, ver, callID uint16, callSN uint32, msg Msg) (err error) {
	bs := bufPool.Get().([]byte)
	wbuf := bs[:HeaderSize]      // 给 Header 预留足够的内存
	buf := proto.NewBuffer(wbuf) //
	ctxlen := uint16(0)
	{
		m := base.Ctx{}
		m.LogID, _ = ctx.Value(LogidKey{}).(uint64)
		if m.LogID == 0 {
			logid, _ := log.Logid(ctx)
			m.LogID = uint64(logid)
		}
		if len(c.cliPassKeys) > 0 {
			for _, k := range c.cliPassKeys {
				v, _ := ctx.Value(k).(string)
				m.Fields = append(m.Fields, v)
			}
		}
		if m.LogID > 0 || len(c.cliPassKeys) > 0 {
			l := len(buf.Bytes())
			err = buf.Marshal(&m)
			if err != nil {
				bufPool.Put(bs)
				return
			}
			if len(buf.Bytes()) > math.MaxUint16 {
				bufPool.Put(bs)
				err = errors.Errorf("the length of the ctx value exceeds %d: %d", math.MaxUint16-l, len(buf.Bytes()))
				return
			}
			ctxlen = uint16(len(buf.Bytes()) - l)
		}
	}
	if msg != nil {
		err = buf.Marshal(msg)
		if err != nil {
			bufPool.Put(bs)
			return
		}
		wbuf = buf.Bytes()
	}

	err = c.Send(ctx, wbuf, ver, callID, ctxlen, callSN)
	bufPool.Put(wbuf)
	return
}

func (c *Codec) Send(ctx context.Context, wbuf []byte, ver, callID, ctxlen uint16, callSN uint32) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.Errorf("%+v", e)
		}
	}()
	if len(wbuf) <= math.MaxUint16 {
		h := Header{
			Ver:          ver,
			CtxLen:       ctxlen,
			Len:          uint16(len(wbuf)),
			SegmentCount: 0,
			SegmentIdx:   0,
			CallID:       callID,
			CallSN:       callSN,
		}
		h.FormatCall(wbuf)
		if c.rwc == nil {
			err = ErrHasBeenClosed.Clone()
			return
		}
		if status := atomic.LoadUint32(&c.status); status > 1 ||
			(status == 1 && ver != VerUpgradeReq && ver != VerUpgradeResp) { // 升级过程中允许发送 UpgradeReq 和 UpgradeResp
			err = ErrHasBeenUpgraded.Clonef("Send c.status: %d, callID: %d", c.status, callID)
			return
		}
		_, err = c.rwc.Write(wbuf)
		if err != nil {
			err = errors.New(err.Error())
		}
		return
	}

	wbuf0, lBodyAll, lBodyOne := wbuf, len(wbuf)-HeaderSize, int(math.MaxUint16-HeaderSize)
	idx, maxIdx := uint16(0), uint16(lBodyAll/lBodyOne)
	for ; idx < maxIdx; idx++ {
		h := Header{
			Ver:          ver,
			CtxLen:       ctxlen,
			Len:          math.MaxUint16, // uint16(len(wbuf0)),
			SegmentCount: maxIdx + 1,
			SegmentIdx:   idx,
			CallID:       callID,
			CallSN:       callSN,
		}
		h.FormatCall(wbuf0)
		if c.rwc == nil {
			err = errors.Errorf("has been closed")
			return
		}
		if atomic.LoadUint32(&c.status) != 0 {
			err = ErrHasBeenClosed.Clonef("Send c.status: %d", c.status)
			return
		}
		_, err = c.rwc.Write(wbuf0[:math.MaxUint16]) // 原子写，内部有锁
		if err != nil {
			err = errors.New(err.Error())
		}
		wbuf0 = wbuf0[lBodyOne:] // 这样 header 部分还有
	}

	// 刚好发完，没有遗漏
	if len(wbuf0) <= HeaderSize {
		return
	}

	// 最后一个数据包
	h := Header{
		Ver:          ver,
		CtxLen:       ctxlen,
		Len:          uint16(len(wbuf0)),
		SegmentCount: maxIdx + 1,
		SegmentIdx:   idx,
		CallID:       callID,
		CallSN:       callSN,
	}
	h.FormatCall(wbuf0)
	if c.rwc == nil {
		err = errors.Errorf("has been closed")
		return
	}
	if atomic.LoadUint32(&c.status) != 0 {
		err = ErrHasBeenClosed.Clonef("SendHeartbeatMsg c.status: %d", c.status)
		return
	}
	_, err = c.rwc.Write(wbuf0)
	if err != nil {
		err = errors.New(err.Error())
	}
	return
}
