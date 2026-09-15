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

const KcpMtu = 1400

// OnNewConnFunc 当解析到一个新的 conn_id 时调用的回调函数
type OnNewConnFunc func(conn *VirtualConn)

// TrunkKCP 基于 KCP 协议的链路聚合
// 将多个网络连接聚合成一个逻辑连接，通过 KCP 提供可靠传输保障
type activeConn struct {
	id   int
	rw   io.ReadWriteCloser
	stop chan struct{}
}

func (c *activeConn) writeFull(p []byte) (int, error) {
	total := 0
	for total < len(p) {
		n, err := c.rw.Write(p[total:])
		if n < 0 || n > len(p)-total {
			return total, io.ErrShortWrite
		}
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

type TrunkKCP struct {
	// 底层物理连接
	rws []io.ReadWriteCloser

	// 动态物理连接管理
	connMu     sync.Mutex
	nextConnID int
	active     map[int]*activeConn

	// KCP 实例（单实例处理双向通信）
	kcp     *kcp.KCP
	kcpLock sync.Mutex

	// 数据通道
	sendChan chan []byte // KCP 输出 -> 网络发送
	recvChan chan []byte // 网络接收 -> KCP 输入

	// 虚拟连接管理
	conns       []*VirtualConn
	connLock    sync.RWMutex
	onNewConnFn OnNewConnFunc // 新连接回调函数

	// 写索引（轮询发送）
	wIdx atomic.Int32

	// 控制信号
	done   chan struct{}
	closed atomic.Bool
}

// NewTrunkKCP 创建一个新的 TrunkKCP 实例
// conv: KCP conversation ID，两端必须相同
// onNewConn: 当解析到一个新的 conn_id 时调用的回调函数，可以为 nil
// rws: 物理连接列表
func NewTrunkKCP(conv uint32, onNewConn OnNewConnFunc, rws ...io.ReadWriteCloser) *TrunkKCP {
	t := &TrunkKCP{
		rws:         rws,
		active:      make(map[int]*activeConn),
		sendChan:    make(chan []byte, 1024),
		recvChan:    make(chan []byte, 1024),
		done:        make(chan struct{}),
		onNewConnFn: onNewConn,
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
	t.kcp.SetMtu(KcpMtu)

	// 第1个参数 nodelay-启用以后若干常规加速将启动
	// 第2个参数 interval为内部处理时钟，默认设置为 10ms
	// 第3个参数 resend为快速重传指标，设置为2
	// 第4个参数 为是否禁用常规流控，这里禁止
	// conn.kcp.NoDelay(0, 10, 0, 0) // 默认模式
	//conn.kcp.NoDelay(0, 10, 0, 1) // 普通模式，关闭流控等
	//conn.kcp.NoDelay(1, 10, 2, 1) // 启动快速模式
	t.kcp.NoDelay(1, 10, 2, 1) // 快速模式

	return t
}

// Run 启动 TrunkKCP 的更新协程和输入协程，并启动初始物理连接。
// 单个物理连接断开不会关闭整个 TrunkKCP；可通过 RemoveConn 剔除，
// 并通过 AddConn 加入新的物理连接。
func (t *TrunkKCP) Run(ctx context.Context) error {
	defer t.Close()
	stop := context.AfterFunc(ctx, func() { t.Close() })
	defer stop()

	var g errgroup.Group
	g.Go(func() error {
		defer t.Close()
		return t.kcpInputLoop(ctx)
	})

	for _, rw := range t.rws {
		if _, err := t.AddConn(rw); err != nil {
			return err
		}
	}

	defer t.Close()
	return t.kcpUpdateLoop(ctx)
}

// sendLoop 从 sendChan 读取 KCP 输出的数据包并写到指定物理连接。
// 用 sendChan 做中介，起到了主动负载均衡的目的，发的快的消费的也快
func (t *TrunkKCP) sendLoop(ctx context.Context, ac *activeConn) {
	log.Ctx(ctx).Info().Msgf("sendLoop conn %d", ac.id)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.done:
			return
		case <-ac.stop:
			return
		case packet := <-t.sendChan:
			n, err := ac.writeFull(packet)
			if err == nil && n != len(packet) {
				err = io.ErrShortWrite
			}
			if err != nil {
				log.Ctx(ctx).Warn().Err(err).Msgf("sendLoop conn %d write error, remove", ac.id)
				t.RemoveConn(ac.id)
				return
			}
		}
	}
}

// recvLoop 从物理连接读取 KCP 数据包并交给 KCP 输入协程。
func (t *TrunkKCP) recvLoop(ctx context.Context, ac *activeConn) {
	buf := make([]byte, math.MaxUint16)
	pending := make([]byte, 0, math.MaxUint16)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.done:
			return
		case <-ac.stop:
			return
		default:
		}

		n, readErr := ac.rw.Read(buf)

		pending = append(pending, buf[:n]...)
		for {
			if len(pending) < kcpHeaderSize {
				break
			}

			// KCP 的 len 在 20~24 Byte, 直接解析即可
			payloadLen := binary.LittleEndian.Uint32(pending[20:24])
			if payloadLen > KcpMtu-kcpHeaderSize {
				log.Ctx(ctx).Warn().Msgf("recvLoop conn %d invalid KCP segment length: %d", ac.id, payloadLen)
				t.RemoveConn(ac.id)
				return
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
				return
			case <-ac.stop:
				return
			}
		}
		if readErr != nil {
			log.Ctx(ctx).Warn().Err(readErr).Msgf("recvLoop conn %d read error, remove", ac.id)
			t.RemoveConn(ac.id)
			return
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
				if binary.LittleEndian.Uint16(pending[2:4])&0x8000 != 0 && len(pending) < CmdHeaderSize {
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
		// 调用回调函数
		if t.onNewConnFn != nil {
			go t.onNewConnFn(t.conns[connID])
		}
	}

	return t.conns[connID]
}

// AddConn 动态加入一条物理连接。返回该连接在本 TrunkKCP 中的 ID。
func (t *TrunkKCP) AddConn(rw io.ReadWriteCloser) (int, error) {
	if rw == nil {
		return -1, errors.New("nil conn")
	}
	t.connMu.Lock()
	defer t.connMu.Unlock()

	if t.closed.Load() {
		return -1, errors.New("trunk closed")
	}
	id := t.nextConnID
	t.nextConnID++
	ac := &activeConn{id: id, rw: rw, stop: make(chan struct{})}
	t.active[id] = ac
	ctx := context.Background()

	go t.sendLoop(ctx, ac)
	go t.recvLoop(ctx, ac)
	return id, nil
}

// RemoveConn 剔除一条物理连接并关闭它。
func (t *TrunkKCP) RemoveConn(id int) error {
	t.connMu.Lock()
	ac := t.active[id]
	if ac != nil {
		delete(t.active, id)
		close(ac.stop)
	}
	remaining := len(t.active)
	t.connMu.Unlock()
	if ac == nil {
		return errors.New("conn not found")
	}
	_ = ac.rw.Close()

	// 如果没有剩余的物理连接，关闭整个 trunk
	if remaining == 0 {
		t.Close()
	}
	return nil
}

// CloseWriteConn 对指定物理连接执行半关闭（发送 FIN/close_notify）。
func (t *TrunkKCP) CloseWriteConn(id int) error {
	t.connMu.Lock()
	ac := t.active[id]
	t.connMu.Unlock()
	if ac == nil {
		return errors.New("conn not found")
	}
	if cw, ok := ac.rw.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// ConnCount 返回当前活跃的物理连接数。
func (t *TrunkKCP) ConnCount() int {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	return len(t.active)
}

// VirtualConnCount 返回当前活跃的虚拟连接数。
func (t *TrunkKCP) VirtualConnCount() int {
	t.connLock.RLock()
	defer t.connLock.RUnlock()
	count := 0
	for _, conn := range t.conns {
		if conn != nil {
			count++
		}
	}
	return count
}

// Close 关闭 TrunkKCP
func (t *TrunkKCP) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return errors.New("already closed")
	}

	close(t.done)

	t.connMu.Lock()
	for _, ac := range t.active {
		close(ac.stop)
		_ = ac.rw.Close()
	}
	t.active = make(map[int]*activeConn)
	t.connMu.Unlock()

	// 关闭所有虚拟连接
	t.connLock.Lock()
	for _, conn := range t.conns {
		if conn != nil {
			conn.closeLocal()
		}
	}
	t.connLock.Unlock()

	for _, rw := range t.rws {
		_ = rw.Close()
	}

	return nil
}
