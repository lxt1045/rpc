package trunk_kcp

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/utils/log"
	"github.com/xtaci/kcp-go"
	"golang.org/x/sync/errgroup"
)

// TrunkKCP 基于 KCP 协议的链路聚合
// 将多个网络连接聚合成一个逻辑连接，通过 KCP 提供可靠传输保障
type TrunkKCP struct {
	// 底层物理连接
	rws []io.ReadWriteCloser

	// KCP 实例（单实例处理双向通信）
	kcp     *kcp.KCP
	kcpLock sync.Mutex

	// 数据通道
	sendChan chan []byte // KCP 输出 -> 网络发送
	recvChan chan []byte // 网络接收 -> KCP 输入

	// 虚拟连接管理
	conns    []*VirtualConn
	connLock sync.RWMutex

	// 写索引（轮询发送）
	wIdx atomic.Int32

	// 控制信号
	done   chan struct{}
	closed atomic.Bool
}

// NewTrunkKCP 创建一个新的 TrunkKCP 实例
// conv: KCP conversation ID，两端必须相同
// rws: 物理连接列表
func NewTrunkKCP(conv uint32, rws ...io.ReadWriteCloser) *TrunkKCP {
	t := &TrunkKCP{
		rws:      rws,
		sendChan: make(chan []byte, 1024),
		recvChan: make(chan []byte, 1024),
		done:     make(chan struct{}),
	}

	// 创建 KCP 实例，output 回调写入 sendChan
	t.kcp = kcp.NewKCP(conv, func(buf []byte, size int) {
		packet := make([]byte, size)
		copy(packet, buf[:size])
		select {
		case t.sendChan <- packet:
		case <-t.done:
		}
	})
	// 增大窗口以提高吞吐量：发送窗口 1024，接收窗口 1024
	t.kcp.WndSize(1024, 1024)
	// 设置 MTU 为 1400（典型以太网 MTU 1500 - IP/UDP 头部）
	// MSS = MTU - KCP头部(24) = 1376，更大的 MSS 减少分片
	t.kcp.SetMtu(1400)
	t.kcp.NoDelay(1, 10, 2, 1) // 快速模式

	return t
}

// Run 启动 TrunkKCP 的所有协程
func (t *TrunkKCP) Run(ctx context.Context) error {
	defer t.Close()
	if len(t.rws) == 0 {
		return errors.New("no physical connections")
	}
	stop := context.AfterFunc(ctx, func() { t.Close() })
	defer stop()
	var g errgroup.Group

	// 启动 KCP 更新协程
	g.Go(func() error {
		defer t.Close()
		return t.kcpUpdateLoop(ctx)
	})

	// 启动发送协程
	for i := range t.rws {
		idx := i
		g.Go(func() error {
			defer t.Close()
			return t.sendLoop(ctx, idx)
		})
	}

	// 启动接收协程（每个物理连接一个）
	for i := range t.rws {
		idx := i
		g.Go(func() error {
			defer t.Close()
			return t.recvLoop(ctx, idx)
		})
	}

	// 启动 KCP 输入处理协程
	g.Go(func() error {
		defer t.Close()
		return t.kcpInputLoop(ctx)
	})

	return g.Wait()
}

// sendLoop 从 sendChan 读取 KCP 输出的数据包，轮询发送到物理连接
func (t *TrunkKCP) sendLoop(ctx context.Context, idx int) error {
	log.Ctx(ctx).Info().Msgf("sendLoop conn %d", idx)
	rw := t.rws[idx]
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return nil
		case packet := <-t.sendChan:
			n, err := rw.Write(packet)
			if err == nil && n != len(packet) {
				err = io.ErrShortWrite
			}
			if err != nil {
				log.Ctx(ctx).Error().Err(err).
					Msgf("sendLoop conn %d write error", idx)
				return err
			}
		}
	}
}

// recvLoop 从物理连接读取数据包，发送到 recvChan
func (t *TrunkKCP) recvLoop(ctx context.Context, connIdx int) error {
	buf := make([]byte, math.MaxUint16)
	pending := make([]byte, 0, math.MaxUint16)
	rw := t.rws[connIdx]
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return nil
		default:
		}

		n, readErr := rw.Read(buf)

		pending = append(pending, buf[:n]...)
		for {
			if len(pending) < kcpHeaderSize {
				break
			}

			payloadLen := binary.LittleEndian.Uint32(pending[20:24])
			if payloadLen > 1400-kcpHeaderSize {
				return errors.Errorf("invalid KCP segment length: %d", payloadLen)
			}
			packetLen := kcpHeaderSize + int(payloadLen)
			if len(pending) < packetLen {
				break
			}

			packet := append([]byte(nil), pending[:packetLen]...)
			pending = pending[packetLen:]
			select {
			case t.recvChan <- packet:
			case <-t.done:
				return nil
			}
		}
		if readErr != nil {
			return readErr
		}
	}
}

// kcpInputLoop 从 recvChan 读取数据包，喂给接收端 KCP，然后从 KCP 读取完整数据并分发
func (t *TrunkKCP) kcpInputLoop(ctx context.Context) error {
	buf := make([]byte, math.MaxUint16)
	pending := make([]byte, 0, math.MaxUint16)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return nil
		case packet := <-t.recvChan:
			t.kcpLock.Lock()
			if ret := t.kcp.Input(packet, true, false); ret < 0 {
				t.kcpLock.Unlock()
				return errors.Errorf("kcp input failed: %d", ret)
			}

			for {
				n := t.kcp.Recv(buf)
				if n < 0 {
					break
				}
				pending = append(pending, buf[:n]...)
			}
			t.kcpLock.Unlock()

			// Consume every completed virtual-connection frame before accepting more KCP input.
			for {
				if len(pending) < HeaderSize {
					break
				}
				if binary.LittleEndian.Uint16(pending[4:6])&0x8000 != 0 && len(pending) < CmdHeaderSize {
					break
				}

				header, headerLen := ParseHeader(pending)
				packetLen := headerLen + int(header.Len)
				if len(pending) < packetLen {
					break
				}

				data := append([]byte(nil), pending[headerLen:packetLen]...)
				pending = pending[packetLen:]
				t.demuxData(header, data)
			}
		}
	}
}

// demuxData 根据 ConnID 将数据分发到对应的虚拟连接
func (t *TrunkKCP) demuxData(header Header, data []byte) {
	conn := t.GetConn(header.ConnID)
	if conn == nil || conn.closed.Load() {
		return
	}

	// 根据 Cmd 类型处理
	if header.Cmd == 0 {
		// 普通数据
		select {
		case conn.readChan <- data:
		case <-conn.readDone:
		case <-t.done:
		}
	} else {
		// 命令处理
		conn.handleCmd(header, data)
	}
}

// kcpUpdateLoop KCP 定时更新循环
func (t *TrunkKCP) kcpUpdateLoop(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return nil
		case <-ticker.C:
			t.kcpLock.Lock()
			t.kcp.Update()
			t.kcpLock.Unlock()
		}
	}
}

// GetConn 获取或创建虚拟连接
func (t *TrunkKCP) GetConn(connID uint16) *VirtualConn {
	if connID > math.MaxInt16 || t.closed.Load() {
		return nil
	}
	t.connLock.RLock()
	if int(connID) < len(t.conns) && t.conns[connID] != nil {
		conn := t.conns[connID]
		t.connLock.RUnlock()
		return conn
	}
	t.connLock.RUnlock()

	t.connLock.Lock()
	defer t.connLock.Unlock()
	if t.closed.Load() {
		return nil
	}

	// 扩展切片
	if int(connID) >= len(t.conns) {
		newConns := make([]*VirtualConn, int(connID)+1)
		copy(newConns, t.conns)
		t.conns = newConns
	}

	if t.conns[connID] == nil {
		t.conns[connID] = &VirtualConn{
			TrunkKCP: t,
			connID:   connID,
			readChan: make(chan []byte, 64),
			readDone: make(chan struct{}),
		}
	}

	return t.conns[connID]
}

// Close 关闭 TrunkKCP
func (t *TrunkKCP) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return errors.New("already closed")
	}

	close(t.done)

	// 关闭所有虚拟连接
	t.connLock.Lock()
	for _, conn := range t.conns {
		if conn != nil {
			conn.closeLocal()
		}
	}
	t.connLock.Unlock()

	// 关闭物理连接
	for _, rw := range t.rws {
		rw.Close()
	}

	return nil
}
