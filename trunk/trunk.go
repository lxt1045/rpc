package trunk

import (
	"context"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc/base"
	"github.com/lxt1045/utils/log"
	"golang.org/x/sync/errgroup"
)

// Trunk: 带宽聚合、链路聚合 (Link Aggregation)
// 多个 conn 聚合成一个，也可以将一个 conn 拆分成多个

type Trunk struct {
	rws  []io.ReadWriteCloser
	wIdx int // 写索引: 当前写到哪个连接了
	lRws sync.Mutex

	payloads  []*Conn // [ConnID]Payload
	lPayloads sync.RWMutex

	done   chan struct{} // 等待退出
	closed atomic.Bool
	err    error
}

type Conn struct {
	*Trunk
	connID   uint16
	startIdx uint16    // 序列号开始的地方
	packages []Package // [Idx-startIdx] []byte; 注意 Idx 溢出时要特殊处理

	// reader
	chReader chan []byte // 等待队列
	rBuf     []byte      // 一次没读完的数据
	rl       sync.Mutex

	// 发送数据时需要，只支持一个消息并发
	chEvent      atomic.Pointer[CallInfo]
	eventHandler func([]byte) error // 处理上层数据

	// writer
	idxPkg uint16

	tsLastData int64 // 上次获取数据的时间
	closed     atomic.Bool
}

type CallInfo struct {
	done chan error
}

var _ io.Closer = &Trunk{}
var _ io.ReadWriter = &Conn{}

type Package struct {
	Header
	Body []byte
}

func (p *Conn) SetEventHandler(handler func([]byte) error) {
	p.eventHandler = handler
}

func (p *Conn) Close() (err error) {
	header := Header{
		ConnID: p.connID,
		Cmd:    CmdCloseConn,
	}
	_, err = p.write(header, nil)
	if err != nil {
		return
	}
	return p.close()
}

func (p *Conn) close() (err error) {
	if p.closed.CompareAndSwap(false, true) {
		p.Trunk.RemoveConn(int(p.connID))
		close(p.chReader) // 只执行一次
		return
	}
	return errors.New("has beed closed")
}

func (p *Conn) Read(bs []byte) (n int, err error) {
	p.rl.Lock()
	defer p.rl.Unlock()

	for {
		if len(p.rBuf) > 0 {
			m := copy(bs[n:], p.rBuf)
			p.rBuf = p.rBuf[m:]

			n += m
			if n == len(bs) || len(p.chReader) == 0 {
				return
			}
		}
		var ok bool
		p.rBuf, ok = <-p.chReader
		if !ok {
			err = errors.New("has been closed")
			return
		}
	}
}

func (p *Conn) SendEvent(data []byte) (err error) {
	info := &CallInfo{
		done: make(chan error, 1),
	}
	swapped := p.chEvent.CompareAndSwap(nil, info)
	if !swapped {
		err = errors.Errorf("only one concurrent call is supported, and the previous call has not returned yet")
		return
	}
	defer p.chEvent.CompareAndSwap(info, nil)

	err = p.sendEvent(data)

	select {
	case err = <-info.done:
	case <-time.After(time.Second * 30):
		err = errors.New("time out")
	}
	return
}

func (p *Conn) sendEvent(data []byte) (err error) {
	header := Header{
		ConnID: p.connID,
		Cmd:    CmdEventReq,
	}
	_, err = p.write(header, data)
	if err != nil {
		return
	}
	return
}
func (p *Conn) sendEventResp(data []byte) (err error) {
	header := Header{
		ConnID: p.connID,
		Cmd:    CmdEventRes,
	}
	_, err = p.write(header, data)
	if err != nil {
		return
	}
	return
}

func (p *Conn) Write(bs []byte) (n int, err error) {
	header := Header{
		ConnID: p.connID,
	}
	return p.write(header, bs)
}

func (p *Conn) write(header Header, bs []byte) (n int, err error) {
	if p.closed.Load() {
		err = errors.New("has been closed")
		return
	}
	p.Trunk.lRws.Lock()
	defer p.Trunk.lRws.Unlock()

	const MTU = 1460
	m := 0
	// for i := 0; i < len(bs); i += MTU {
	idx := p.Trunk.wIdx % len(p.Trunk.rws)
	p.Trunk.wIdx++

	if header.Cmd > 0 {
		header.Len = uint16(len(bs) + CmdHeaderSize)
	} else {
		header.Len = uint16(len(bs) + HeaderSize)
	}
	header.Idx = p.idxPkg
	p.idxPkg++

	bsH := make([]byte, CmdHeaderSize)
	bsH = header.Format(bsH)

	_, err = p.Trunk.rws[idx].Write(bsH)
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return
	}
	m, err = p.Trunk.rws[idx].Write(bs)
	if err != nil {
		return
	}
	n += m
	// }
	return
}

func NewTrunk(rws ...io.ReadWriteCloser) (t *Trunk) {
	t = &Trunk{
		rws:  rws,
		done: make(chan struct{}),
		// payloads: make([]*Payload, 0, 100),
	}
	return
}

func (t *Trunk) Close() (err error) {
	if t.closed.CompareAndSwap(false, true) {
		func() {
			t.lPayloads.Lock()
			defer t.lPayloads.Unlock()
			for _, p := range t.payloads {
				if p == nil {
					continue
				}
				p.Close()
			}
		}()
		close(t.done) // 只执行一次
		return
	}
	return errors.New("has beed closed")
}

func (t *Trunk) RemoveConn(connID int) {
	if connID > math.MaxInt16 {
		return
	}
	t.lPayloads.Lock()
	defer t.lPayloads.Unlock()

	if connID >= len(t.payloads) {
		return
	}
	t.payloads[connID] = nil
}
func (t *Trunk) GetConn(connID uint16) (conn *Conn) {
	if connID > math.MaxInt16 {
		return nil
	}
	conn = func(connID uint16) (payload *Conn) {
		t.lPayloads.RLock()
		defer t.lPayloads.RUnlock()

		if int(connID) < len(t.payloads) {
			payload = t.payloads[connID]
		}
		return
	}(connID)
	if conn != nil {
		return
	}

	conn = func(i uint16) (payload *Conn) {
		t.lPayloads.Lock()
		defer t.lPayloads.Unlock()

		if int(connID) >= len(t.payloads) {
			// t.payloads = append(t.payloads, make([]*Payload, connID+1-len(t.payloads))...)
			t.payloads = slices.Grow(t.payloads, int(connID)+1-len(t.payloads))[:connID+1]
		}
		payload = t.payloads[i]
		if payload != nil {
			return
		}

		payload = &Conn{
			Trunk:    t,
			connID:   uint16(connID),
			chReader: make(chan []byte, 64),
		}
		t.payloads[connID] = payload
		return
	}(connID)

	return
}

func (t *Trunk) GetReadWriter(connID uint16) io.ReadWriter {
	payload := t.GetConn(connID)
	return payload
}

func (t *Trunk) Run(ctx context.Context) {
	var g errgroup.Group
	ch := make(chan Package, 64)

	defer close(ch) // close ch SavePackLoop 才会退出
	go func() {
		var err error
		defer func() {
			if e := recover(); e != nil {
				err = errors.Errorf("recove:%v", e)
			}
			if err != nil {
				log.Ctx(ctx).Info().Caller().Err(err).Msg("SavePackLoop defer")
			}
			t.Close()
		}()
		err = t.SavePackLoop(ch)
	}()

	for i, rw := range t.rws {
		func(idx int, rw io.ReadWriteCloser) {
			g.Go(func() (err error) {
				defer func() {
					if e := recover(); e != nil {
						err = errors.Errorf("recove:%v", e)
					}
				}()
				log.Ctx(ctx).Info().Msgf("[Run] idx:%d", idx)

				rbuf := make([]byte, 0, math.MaxUint16)
				for {
					if t.closed.Load() {
						return
					}
					header, bsBody, err := ReadPack(rw, rbuf)
					if err != nil {
						if err == io.EOF || err == io.ErrUnexpectedEOF {
							return err
						}
						return err
					}

					pkg := Package{
						Header: header,
						Body:   append(bsBody[:0:0], bsBody...),
					}
					ch <- pkg
				}
			})
		}(i, rw)
	}
	err := g.Wait()
	if err != nil {
		log.Ctx(ctx).Info().Caller().Err(err).Msg("ReadLoop defer")
	}
}

func (t *Trunk) SavePackLoop(ch chan Package) (err error) {
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()
	tsNextClean := time.Now().Unix() + 30*60
	for {
		select {
		case <-ticker.C:
			tsNow := time.Now().Unix()
			if tsNow > tsNextClean {
				continue
			}
			tsNextClean = tsNow + 30*60

			// 长时间(30min) 收不到数据的 ConnID 主动清理？

		case pkg, ok := <-ch:
			if !ok {
				return
			}
			connID := pkg.Header.ConnID

			conn := t.GetConn(connID)
			idx := pkg.Header.Idx - conn.startIdx // uint16 类型模运算, 会自动溢出为对应模运算结果
			i := int(idx)
			if i >= len(conn.packages) {
				conn.packages = append(conn.packages, make([]Package, i+1-len(conn.packages))...)
				// payload.bodys = slices.Grow(payload.bodys, i+1-len(payload.bodys))[:i+1]  // 数据太精确了，可以多分配点减少分配次数
			}
			conn.packages[i] = pkg

			// 如果 payload.chReader 当前不可写入，则不尝试了
			if cap(conn.chReader) == len(conn.chReader) {
				continue
			}

			// 看一下可以有多少个body移动到 chReader
			lNeedMove := 0
			cmds := []Package{}
			for _, pkg := range conn.packages {
				if pkg.Len == 0 {
					break
				}
				lNeedMove++

				// 最高位位类型, 0: ConnID(数据包), 1: 命令类型(命令数据包)
				if pkg.Header.Cmd == 0 {
					if conn != nil {
						conn.chReader <- pkg.Body
					}
					pkg.Body = nil
					continue
				} else {
					cmds = append(cmds, pkg)
				}
			}
			if lNeedMove > 0 {
				conn.packages = conn.packages[lNeedMove:]
				conn.startIdx += uint16(lNeedMove)
			}
			for _, pkg := range cmds {
				conn.DoCmd(pkg)
			}

		}
	}
}

func (conn *Conn) DoCmd(pkg Package) {

	switch pkg.Cmd {
	case CmdCloseConn:
		conn.close()
	case CmdAddConn:
		// data = pkg.Body[CmdSize:]
	case CmdEventReq:
		data := pkg.Body
		if conn.eventHandler != nil {
			go func() {
				var err error
				defer func() {
					if e := recover(); e != nil {
						err := errors.Errorf("recove:%v", e)
						log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("t.fData defer")
					}
					bs := []byte{}
					if err != nil {
						result := base.Err{
							Code: 1,
							Msg:  err.Error(),
							// Logid
						}
						buf := proto.NewBuffer(bs[:0]) //
						err = buf.Marshal(&result)
						if err != nil {
							log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("CmdEventReq error")
							bs = []byte(err.Error())
						} else {
							bs = buf.Bytes()
						}
					}

					err = conn.sendEventResp(bs)
					if err != nil {
						log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("CmdEventReq error")
					}
				}()

				err = conn.eventHandler(data)
			}()
		}
	case CmdEventRes:
		data := pkg.Body
		result := base.Err{}
		err := proto.Unmarshal(data, &result)
		if err != nil {
			log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("CmdEventRes error")
			result = base.Err{
				Code: -1,
				Msg:  string(data),
			}
		}

		info := conn.chEvent.Load()
		if info != nil {
			info.done <- errors.NewCode(0, int(result.Code), result.Msg)
		}
	default:
	}
}
