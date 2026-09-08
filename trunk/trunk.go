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

	payloads     []*Conn // [ConnID]Payload
	lPayloads    sync.RWMutex
	eventHandler func(uint16, []byte) error // 处理上层数据

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
	readDone chan struct{}
	rBuf     []byte // 一次没读完的数据
	rl       sync.Mutex

	// 发送数据时需要，只支持一个消息并发
	chEvent      atomic.Pointer[CallInfo]
	eventHandler func([]byte) error // 处理上层数据
	el           sync.Mutex         // 串行化 SendEvent, 并发调用要排队而不是报错

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
func (p *Trunk) SetEventHandler(handler func(uint16, []byte) error) {
	p.eventHandler = handler
}

func (p *Conn) Close() (err error) {
	if err = p.close(); err != nil {
		return err
	}
	header := Header{
		ConnID: p.connID,
		Cmd:    CmdCloseConn,
	}
	_, err = p.write(header, nil)
	return err
}

func (p *Conn) close() (err error) {
	if p.closed.CompareAndSwap(false, true) {
		p.Trunk.RemoveConn(int(p.connID))
		close(p.readDone)
		return
	}
	return errors.New("has been closed")
}

func (p *Conn) Read(bs []byte) (n int, err error) {
	if len(bs) == 0 {
		return 0, nil
	}
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
		// Drain buffered data before reporting the ordered close.
		select {
		case p.rBuf = <-p.chReader:
			continue
		default:
		}
		select {
		case p.rBuf = <-p.chReader:
		case <-p.readDone:
			return n, io.EOF
		}
	}
}

func (p *Conn) SendEvent(data []byte) (err error) {
	p.el.Lock()
	defer p.el.Unlock()

	if p.closed.Load() {
		return errors.New("has been closed")
	}
	info := &CallInfo{
		done: make(chan error, 1),
	}
	p.chEvent.Store(info)
	defer p.chEvent.Store(nil)

	if err = p.sendEvent(data); err != nil {
		return
	}

	select {
	case err = <-info.done:
	case <-p.readDone:
		err = io.ErrClosedPipe
	case <-time.After(time.Second * 10):
		err = errors.Errorf("time out, connID: %d", p.connID)
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
	p.Trunk.lRws.Lock()
	defer p.Trunk.lRws.Unlock()

	if p.Trunk.closed.Load() || (p.closed.Load() && header.Cmd != CmdCloseConn) {
		return 0, io.ErrClosedPipe
	}
	if len(p.Trunk.rws) == 0 {
		return 0, errors.New("no physical connections")
	}
	headerSize := HeaderSize
	if header.Cmd != 0 {
		headerSize = CmdHeaderSize
		if len(bs) > math.MaxUint16-headerSize {
			return 0, errors.New("event exceeds frame size")
		}
	}
	for {
		chunkSize := min(len(bs)-n, math.MaxUint16-headerSize)
		header.Len = uint16(chunkSize + headerSize)
		header.Idx = p.idxPkg
		p.idxPkg++
		frame := make([]byte, headerSize+chunkSize)
		header.Format(frame)
		copy(frame[headerSize:], bs[n:n+chunkSize])
		idx := p.Trunk.wIdx
		p.Trunk.wIdx = (idx + 1) % len(p.Trunk.rws)
		written, writeErr := p.Trunk.rws[idx].Write(frame)
		if writeErr == nil && written != len(frame) {
			writeErr = io.ErrShortWrite
		}
		if writeErr != nil {
			// A partial frame cannot be retried without corrupting the stream.
			p.Trunk.Close()
			return n + max(0, written-headerSize), writeErr
		}
		n += chunkSize
		if n == len(bs) {
			return n, nil
		}
	}
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
		close(t.done)
		t.lPayloads.Lock()
		conns := append([]*Conn(nil), t.payloads...)
		t.lPayloads.Unlock()
		for _, p := range conns {
			if p != nil {
				p.close()
			}
		}
		for _, rw := range t.rws {
			rw.Close()
		}
		return
	}
	return errors.New("has been closed")
}

func (t *Trunk) RemoveConn(connID int) {
	if connID < 0 || connID > math.MaxInt16 {
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
	if connID > math.MaxInt16 || t.closed.Load() {
		return nil
	}
	conn = func(connID uint16) (conn *Conn) {
		t.lPayloads.RLock()
		defer t.lPayloads.RUnlock()

		if int(connID) < len(t.payloads) {
			conn = t.payloads[connID]
		}
		return
	}(connID)
	if conn != nil {
		return
	}

	conn = func(i uint16) (conn *Conn) {
		t.lPayloads.Lock()
		defer t.lPayloads.Unlock()

		if t.closed.Load() {
			return nil
		}
		if int(connID) >= len(t.payloads) {
			// t.payloads = append(t.payloads, make([]*Payload, connID+1-len(t.payloads))...)
			t.payloads = slices.Grow(t.payloads, int(connID)+1-len(t.payloads))[:connID+1]
		}
		conn = t.payloads[i]
		if conn != nil {
			return
		}

		conn = &Conn{
			Trunk:    t,
			connID:   uint16(connID),
			chReader: make(chan []byte, 64),
			readDone: make(chan struct{}),
		}
		t.payloads[connID] = conn
		return
	}(connID)

	return
}

func (t *Trunk) GetReadWriter(connID uint16) io.ReadWriter {
	payload := t.GetConn(connID)
	return payload
}

func (t *Trunk) Run(ctx context.Context) {
	defer t.Close()
	stop := context.AfterFunc(ctx, func() { t.Close() })
	defer stop()
	var readers errgroup.Group
	ch := make(chan Package, 64)
	for _, rw := range t.rws {
		readers.Go(func() error {
			defer t.Close()
			buf := make([]byte, math.MaxUint16)
			for {
				header, body, err := ReadPack(rw, buf)
				if err != nil {
					return err
				}
				select {
				case ch <- Package{Header: header, Body: append([]byte(nil), body...)}:
				case <-t.done:
					return nil
				}
			}
		})
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		readers.Wait()
		close(ch)
	}()
	if err := t.SavePackLoop(ch); err != nil {
		log.Ctx(ctx).Info().Err(err).Msg("SavePackLoop")
	}
	t.Close()
	<-finished
}

func (t *Trunk) SavePackLoop(ch chan Package) (err error) {
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()
	tsNextClean := time.Now().Unix() + 30*60
	for {
		select {
		case <-t.done:
			return nil
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
			if conn == nil {
				log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("t.GetConn(connID) got nil")
				continue
			}
			idx := pkg.Header.Idx - conn.startIdx // uint16 类型模运算, 会自动溢出为对应模运算结果
			i := int(idx)
			if i >= len(conn.packages) {
				conn.packages = append(conn.packages, make([]Package, i+1-len(conn.packages))...)
				// payload.bodys = slices.Grow(payload.bodys, i+1-len(payload.bodys))[:i+1]  // 数据太精确了，可以多分配点减少分配次数
			}
			conn.packages[i] = pkg

			// 看一下可以有多少个body移动到 chReader
			lNeedMove := 0
			for j, pkg := range conn.packages {
				if pkg.Len == 0 {
					break
				}
				lNeedMove++
				conn.packages[j] = Package{}

				// 最高位位类型, 0: ConnID(数据包), 1: 命令类型(命令数据包)
				if pkg.Header.Cmd == 0 {
					// 连接已关闭, 丢弃剩余数据, 避免阻塞在 chReader 上
					if conn.closed.Load() {
						continue
					}
					select {
					case conn.chReader <- pkg.Body:
					case <-conn.readDone:
					}
					pkg.Body = nil
					continue
				} else {
					conn.DoCmd(pkg)
				}
			}
			if lNeedMove > 0 {
				conn.packages = conn.packages[lNeedMove:]
				conn.startIdx += uint16(lNeedMove)
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
		if conn.eventHandler != nil || conn.Trunk.eventHandler != nil {
			go func() {
				var err error
				// 无论 handler 成功还是失败，都要回一个 CmdEventRes，否则对端
				// SendEvent 会一直等到 10s 超时。
				defer func() {
					if e := recover(); e != nil {
						err = errors.Errorf("recove:%v", e)
						log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("t.fData defer")
					}

					result := base.Err{}
					if err != nil {
						result.Code = 1
						result.Msg = err.Error()
						// Logid
					}
					buf := proto.NewBuffer(nil)
					err = buf.Marshal(&result)
					if err != nil {
						log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("CmdEventReq error")
						// 编码失败时直接把错误文本透传, 让对端至少能拿到一个错误
						conn.sendEventResp([]byte(err.Error()))
						return
					}

					err = conn.sendEventResp(buf.Bytes())
					if err != nil {
						log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("CmdEventReq error")
					}
				}()

				if conn.eventHandler != nil {
					err = conn.eventHandler(data)
					if err != nil {
						log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("conn.eventHandler  error")
					}
				}

				if conn.Trunk.eventHandler != nil {
					err = conn.Trunk.eventHandler(pkg.ConnID, data)
					if err != nil {
						log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("conn.Trunk.eventHandler error")
					}
				}
			}()
		}
	case CmdEventRes:
		info := conn.chEvent.Load()
		if info != nil {
			data := pkg.Body
			result := base.Err{}
			err := proto.Unmarshal(data, &result)
			if err != nil {
				log.Ctx(context.TODO()).Info().Caller().Err(err).Msg("CmdEventRes error")
				result = base.Err{
					Code: -1,
					Msg:  string(data),
				}
				info.done <- errors.NewCode(0, int(result.Code), result.Msg)
			} else if result.Code != 0 || result.Msg != "" {
				info.done <- errors.NewCode(0, int(result.Code), result.Msg)
			} else {
				// 成功: 空 base.Err 编码出来是 0 字节, 这里 send(nil) 表示无错
				info.done <- nil
			}
		}
	default:
	}
}
