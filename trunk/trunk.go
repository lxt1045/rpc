package trunk

import (
	"context"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/utils/log"
	"golang.org/x/sync/errgroup"
)

// Trunk: 带宽聚合、链路聚合 (Link Aggregation)
// 多个 conn 聚合成一个，也可以将一个 conn 拆分成多个

type Trunk struct {
	rws  []io.ReadWriteCloser
	wIdx int // 写索引: 当前写到哪个连接了
	lRws sync.Mutex

	payloads  []*Payload // [ConnID]Payload
	lPayloads sync.RWMutex

	done   chan struct{} // 等待退出
	closed atomic.Bool
	err    error
}

type Payload struct {
	*Trunk
	connID   uint16
	startIdx uint16    // 序列号开始的地方
	packages []Package // [Idx-startIdx] []byte; 注意 Idx 溢出时要特殊处理

	// reader
	chReader chan []byte // 等待队列
	rBuf     []byte      // 一次没读完的数据
	rl       sync.Mutex

	// writer
	idxPkg uint16

	tsLastData int64 // 上次获取数据的时间
	closed     atomic.Bool
}

var _ io.Closer = &Trunk{}
var _ io.ReadWriter = &Payload{}

type Package struct {
	Header
	body []byte
}

func (p *Payload) Close() (err error) {
	header := Header{
		ConnID: p.connID | 0x80,
	}
	cmd := Cmd{
		Cmd: CmdCloseConn,
	}
	bsCmd := make([]byte, CmdSize)
	bsCmd = cmd.Format(bsCmd)
	_, err = p.write(header, bsCmd)
	if err != nil {
		return
	}
	return p.close()
}
func (p *Payload) close() (err error) {
	if p.closed.CompareAndSwap(false, true) {
		p.Trunk.RemovePayload(int(p.connID))
		close(p.chReader) // 只执行一次
		return
	}
	return errors.New("has beed closed")
}

func (p *Payload) Read(bs []byte) (n int, err error) {
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

func (p *Payload) Write(bs []byte) (n int, err error) {
	header := Header{
		ConnID: p.connID & 0x8f,
	}
	return p.write(header, bs)
}

func (p *Payload) write(header Header, bs []byte) (n int, err error) {
	if p.closed.Load() {
		err = errors.New("has been closed")
		return
	}
	p.Trunk.lRws.Lock()
	defer p.Trunk.lRws.Unlock()

	idx := p.Trunk.wIdx % len(p.Trunk.rws)
	p.Trunk.wIdx++

	header.Len = uint16(len(bs))
	header.Idx = p.idxPkg
	p.idxPkg++

	bsH := make([]byte, HeaderSize)
	bsH = header.Format(bsH)

	_, err = p.Trunk.rws[idx].Write(bsH)
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return
	}
	return p.Trunk.rws[idx].Write(bs)
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

func (t *Trunk) RemovePayload(connID int) {
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
func (t *Trunk) GetPayload(connID int) (payload *Payload) {
	if connID > math.MaxInt16 {
		return nil
	}
	payload = func(i int) (payload *Payload) {
		t.lPayloads.RLock()
		defer t.lPayloads.RUnlock()

		if connID < len(t.payloads) {
			payload = t.payloads[i]
		}
		return
	}(connID)
	if payload != nil {
		return
	}

	payload = func(i int) (payload *Payload) {
		t.lPayloads.Lock()
		defer t.lPayloads.Unlock()

		if connID >= len(t.payloads) {
			// t.payloads = append(t.payloads, make([]*Payload, connID+1-len(t.payloads))...)
			t.payloads = slices.Grow(t.payloads, connID+1-len(t.payloads))[:connID+1]
		}
		payload = t.payloads[i]
		if payload != nil {
			return
		}

		payload = &Payload{
			Trunk:    t,
			connID:   uint16(connID),
			chReader: make(chan []byte, 64),
		}
		t.payloads[connID] = payload
		return
	}(connID)

	return
}

func (t *Trunk) GetReadWriter(connID int) io.ReadWriter {
	payload := t.GetPayload(connID)
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
						body:   append(bsBody[:0:0], bsBody...),
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
			connID := pkg.Header.ConnID & 0x7f

			payload := t.GetPayload(int(connID))
			idx := pkg.Header.Idx - payload.startIdx // uint16 类型模运算, 会自动溢出为对应模运算结果
			i := int(idx)
			if i >= len(payload.packages) {
				payload.packages = append(payload.packages, make([]Package, i+1-len(payload.packages))...)
				// payload.bodys = slices.Grow(payload.bodys, i+1-len(payload.bodys))[:i+1]  // 数据太精确了，可以多分配点减少分配次数
			}
			payload.packages[i] = pkg

			// 如果 payload.chReader 当前不可写入，则不尝试了
			if cap(payload.chReader) == len(payload.chReader) {
				continue
			}

			// 看一下可以有多少个body移动到 chReader
			lNeedMove := 0
			cmds := []Package{}
			for _, pkg := range payload.packages {
				if len(pkg.body) == 0 {
					break
				}
				lNeedMove++

				// 最高位位类型, 0: ConnID(数据包), 1: 命令类型(命令数据包)
				if pkg.Header.ConnID&0x80 == 0 {
					if payload != nil {
						payload.chReader <- pkg.body
					}
					pkg.body = nil
					continue
				} else {
					cmds = append(cmds, pkg)
				}
			}
			if lNeedMove > 0 {
				payload.packages = payload.packages[lNeedMove:]
				payload.startIdx += uint16(lNeedMove)
			}
			for _, pkg := range cmds {
				data := t.DoCmd(pkg)
				_ = data
			}

		}
	}
}

func (t *Trunk) DoCmd(pkg Package) (data []byte) {
	if len(pkg.body) < CmdSize {
		return
	}
	// 最高位位类型, 0: ConnID(数据包), 1: 命令类型(命令数据包)
	connID := pkg.Header.ConnID & 0x7f
	cmd := ParseCmd(pkg.body)

	switch cmd.Cmd {
	case CmdCloseConn:
		payload := t.GetPayload(int(connID))
		payload.close()
	case CmdAddConn:
		data = pkg.body[CmdSize:]
	case CmdAppData:
		data = pkg.body[CmdSize:]
	default:
	}
	return
}
