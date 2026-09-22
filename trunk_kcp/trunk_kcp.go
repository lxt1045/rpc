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

// OnIdleConnFunc 当检测到底层连接空闲超时时调用的回调函数
// 参数 connID 是空闲连接的 ID
// 返回新的连接用于替换，如果返回 nil 则只移除旧连接
type OnIdleConnFunc func(connID int) io.ReadWriteCloser

// TrunkKCP 基于 KCP 协议的链路聚合
// 将多个网络连接聚合成一个逻辑连接，通过 KCP 提供可靠传输保障
type activeConn struct {
	id           int
	rw           io.ReadWriteCloser
	stop         chan struct{}
	lastRecvTime atomic.Int64 // Unix timestamp in nanoseconds
	createdAt    time.Time    // 连接创建时间

	// 流量统计
	sendBytes atomic.Int64 // 发送字节数
	recvBytes atomic.Int64 // 接收字节数

	// 速率采样
	lastSampleTime atomic.Int64 // 上次采样时间 (nanoseconds)
	lastSendBytes  atomic.Int64 // 上次采样时的发送字节数
	lastRecvBytes  atomic.Int64 // 上次采样时的接收字节数
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
	mtu     int // KCP 线上包 MTU（含 KCP 头；0 表示用默认 KcpMtu）

	// 数据通道
	sendChan chan []byte // KCP 输出 -> 网络发送
	recvChan chan []byte // 网络接收 -> KCP 输入

	// 虚拟连接管理
	conns         []*VirtualConn
	connLock      sync.RWMutex
	onNewConnFn   OnNewConnFunc  // 新连接回调函数
	onIdleConnFn  OnIdleConnFunc // 连接空闲/慢速回调函数
	nextVirtualID int

	// 写索引（轮询发送）
	wIdx atomic.Int32

	// 空闲检测配置
	idleTimeout time.Duration // 连接空闲超时时间，0 表示禁用

	// 速率检测配置
	slowConnThreshold float64       // 慢速连接阈值（相对于平均速率的比例，例如 0.1 表示 10%）
	slowConnMinAge    time.Duration // 慢速连接最小存活时间，只检测超过此时间的连接

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
		idleTimeout: 0, // 默认禁用空闲检测
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
	t.mtu = KcpMtu
	t.kcp.SetMtu(t.mtu)

	// 第1个参数 nodelay-启用以后若干常规加速将启动
	// 第2个参数 interval为内部处理时钟，默认设置为 10ms
	// 第3个参数 resend为快速重传指标，设置为2
	// 第4个参数 为是否禁用常规流控，这里禁止
	// conn.kcp.NoDelay(0, 10, 0, 0) // 默认模式
	//conn.kcp.NoDelay(0, 10, 0, 1) // 普通模式，关闭流控等
	//conn.kcp.NoDelay(1, 10, 2, 1) // 启动快速模式
	t.kcp.NoDelay(1, 10, 32, 1)

	return t
}

// SetNoDelay 调整 KCP 的重传/流控行为，参数语义同 kcp.KCP.NoDelay：
//   - nodelay: 是否启用 nodelay 模式（0 时最小 RTO=100ms，1 时最小 RTO=30ms）
//   - interval: KCP 内部处理时钟（ms，最小 10）
//   - resend: 快速重传阈值（0 表示关闭快速重传）
//   - nc: 是否关闭拥塞控制（1 关闭 / 0 开启）
//
// 传负数表示保持当前值不变。两端应使用相同的参数。
// 应在 NewTrunkKCP 之后、跑流量之前调用。
//
// 注意：当底层物理连接是 TCP/TLS 等可靠流时，KCP 的 ARQ 与 TCP 可靠性语义
// 重叠，nodelay=1（minRTO=30ms）会因延迟抖动产生大量伪重传，线上流量可被
// 放大 2~3 倍；此时建议 nodelay=0, interval=20~40, resend=0, nc=1。
// 详见 test/socks_trunk_kcp/README.md 的"线上流量放大"一节。
// SetMtu 设置 KCP 线上包 MTU（含 KCP 头；默认 KcpMtu=1400）。
// 用于路径 MTU 受限的场景（如 VPN/隧道出口把 MSS 夹到 1280，或底层是 faux_tcp
// 且其 mss 被调小）：MTU 必须 ≤ 底层单包可承载字节数，否则大于路径 MTU 的报文
// 会被丢弃（faux_tcp 的报文带 DF，不会被分片）。
// 需在 Run/AddConn 之前调用（同时影响收包长度校验）。
func (t *TrunkKCP) SetMtu(mtu int) {
	if mtu <= kcpHeaderSize || mtu > KcpMtu {
		return
	}
	t.mtu = mtu
	t.kcp.SetMtu(mtu)
}

func (t *TrunkKCP) SetNoDelay(nodelay, interval, resend, nc int) {
	t.kcpLock.Lock()
	defer t.kcpLock.Unlock()
	t.kcp.NoDelay(nodelay, interval, resend, nc)
}

// SetIdleTimeout 设置连接空闲超时时间和回调函数
// idleTimeout: 连接空闲超时时间，例如 60*time.Second
// onIdleConn: 当检测到连接空闲超时时的回调函数，返回新连接用于替换
func (t *TrunkKCP) SetIdleTimeout(idleTimeout time.Duration, onIdleConn OnIdleConnFunc) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.idleTimeout = idleTimeout
	t.onIdleConnFn = onIdleConn
}

// SetSlowConnDetection 设置慢速连接检测参数和回调函数
// threshold: 慢速连接阈值（相对于平均速率的比例），例如 0.1 表示速率低于平均值的 10% 视为慢速
// minAge: 只检测创建时间超过此值的连接，例如 30*time.Minute
// onSlowConn: 当检测到慢速连接时的回调函数，返回新连接用于替换（可以与 onIdleConn 使用同一个回调）
func (t *TrunkKCP) SetSlowConnDetection(threshold float64, minAge time.Duration, onSlowConn OnIdleConnFunc) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.slowConnThreshold = threshold
	t.slowConnMinAge = minAge
	// 使用与空闲检测相同的回调函数类型
	if onSlowConn != nil {
		t.onIdleConnFn = onSlowConn
	}
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

	// 如果启用了空闲检测，启动检测协程
	t.connMu.Lock()
	idleTimeout := t.idleTimeout
	slowConnThreshold := t.slowConnThreshold
	t.connMu.Unlock()
	if idleTimeout > 0 {
		g.Go(func() error {
			return t.idleCheckLoop(ctx)
		})
	}

	// 如果启用了速率检测，启动速率监控协程
	if slowConnThreshold > 0 {
		g.Go(func() error {
			return t.rateMonitorLoop(ctx)
		})
	}

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
			// 统计发送字节数
			ac.sendBytes.Add(int64(n))
		}
	}
}

// recvLoop 从物理连接读取 KCP 数据包并交给 KCP 输入协程。
func (t *TrunkKCP) recvLoop(ctx context.Context, ac *activeConn) {
	buf := make([]byte, math.MaxUint16)
	pending := make([]byte, 0, math.MaxUint16)

	// 初始化最后接收时间
	ac.lastRecvTime.Store(time.Now().UnixNano())

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

		// 只要收到数据就更新最后接收时间和统计接收字节数
		if n > 0 {
			ac.lastRecvTime.Store(time.Now().UnixNano())
			ac.recvBytes.Add(int64(n))
		}

		pending = append(pending, buf[:n]...)
		for {
			if len(pending) < kcpHeaderSize {
				break
			}

			// KCP 的 len 在 20~24 Byte, 直接解析即可
			payloadLen := binary.LittleEndian.Uint32(pending[20:24])
			mtu := t.mtu
			if mtu <= 0 {
				mtu = KcpMtu
			}
			if payloadLen > uint32(mtu-kcpHeaderSize) {
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
	if header.Cmd != 0 {
		// A close acknowledges the existing stream, never a replacement stream.
		t.connLock.Lock()
		if header.ConnID > math.MaxInt16 || t.closed.Load() {
			t.connLock.Unlock()
			return
		}
		var conn *VirtualConn
		if int(header.ConnID) < len(t.conns) {
			conn = t.conns[header.ConnID]
		}
		if conn == nil {
			conn = t.newConnLocked(header.ConnID, false)
		}
		t.connLock.Unlock()
		conn.handleCmd(header, data)
		return
	}
	conn := t.GetConn(header.ConnID)
	if conn == nil || conn.closed.Load() {
		return
	}

	select {
	case conn.readChan <- data:
	case <-conn.readDone:
	case <-t.done:
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
	t.connLock.Lock()
	defer t.connLock.Unlock()
	if t.closed.Load() {
		return nil
	}

	if int(connID) < len(t.conns) {
		if conn := t.conns[connID]; conn != nil && !conn.reusable() {
			return conn
		}
	}
	return t.newConnLocked(connID, true)
}

// OpenConn reserves an unused virtual connection in the range 1..maxConns.
// Only one side of a trunk should allocate IDs with this method.
func (t *TrunkKCP) OpenConn(maxConns int) (*VirtualConn, error) {
	if maxConns <= 0 || maxConns > math.MaxInt16 {
		return nil, errors.New("invalid virtual connection limit")
	}
	t.connLock.Lock()
	defer t.connLock.Unlock()
	if t.closed.Load() {
		return nil, io.ErrClosedPipe
	}
	for i := 0; i < maxConns; i++ {
		t.nextVirtualID = t.nextVirtualID%maxConns + 1
		id := uint16(t.nextVirtualID)
		if int(id) < len(t.conns) {
			if conn := t.conns[id]; conn != nil && !conn.reusable() {
				continue
			}
		}
		return t.newConnLocked(id, true), nil
	}
	return nil, errors.New("virtual connection limit reached")
}

func (t *TrunkKCP) newConnLocked(connID uint16, notify bool) *VirtualConn {
	if int(connID) >= len(t.conns) {
		newConns := make([]*VirtualConn, int(connID)+1)
		copy(newConns, t.conns)
		t.conns = newConns
	}

	t.conns[connID] = &VirtualConn{
		TrunkKCP: t,
		connID:   connID,
		readChan: make(chan []byte, 64),
		readDone: make(chan struct{}),
	}
	if notify && t.onNewConnFn != nil {
		go t.onNewConnFn(t.conns[connID])
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
	ac := &activeConn{
		id:        id,
		rw:        rw,
		stop:      make(chan struct{}),
		createdAt: time.Now(),
	}
	ac.lastRecvTime.Store(time.Now().UnixNano())
	t.active[id] = ac
	ctx := context.Background()

	go t.sendLoop(ctx, ac)
	go t.recvLoop(ctx, ac)
	return id, nil
}

// idleCheckLoop 定期检查所有连接的空闲状态
func (t *TrunkKCP) idleCheckLoop(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second) // 每10秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return nil
		case <-ticker.C:
			t.checkIdleConns(ctx)
		}
	}
}

// checkIdleConns 检查并处理空闲超时的连接
func (t *TrunkKCP) checkIdleConns(ctx context.Context) {
	t.connMu.Lock()
	idleTimeout := t.idleTimeout
	onIdleConnFn := t.onIdleConnFn
	if idleTimeout == 0 {
		t.connMu.Unlock()
		return
	}

	now := time.Now()
	var idleConns []int
	for id, ac := range t.active {
		lastRecv := time.Unix(0, ac.lastRecvTime.Load())
		if now.Sub(lastRecv) > idleTimeout {
			idleConns = append(idleConns, id)
		}
	}
	t.connMu.Unlock()

	// 处理空闲连接
	for _, id := range idleConns {
		log.Ctx(ctx).Info().Int("conn_id", id).Msg("connection idle timeout detected")

		// 如果有回调函数，调用它获取新连接
		var newConn io.ReadWriteCloser
		if onIdleConnFn != nil {
			newConn = onIdleConnFn(id)
		}

		// 先移除旧连接
		if err := t.RemoveConn(id); err != nil {
			log.Ctx(ctx).Warn().Err(err).Int("conn_id", id).Msg("failed to remove idle connection")
		}

		// 如果有新连接，添加它
		if newConn != nil {
			newID, err := t.AddConn(newConn)
			if err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("failed to add replacement connection")
				_ = newConn.Close()
			} else {
				log.Ctx(ctx).Info().Int("old_conn_id", id).Int("new_conn_id", newID).Msg("idle connection replaced")
			}
		}
	}
}

// rateMonitorLoop 定期检测连接的传输速率，并关闭速率过低的慢连接
func (t *TrunkKCP) rateMonitorLoop(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return nil
		case <-ticker.C:
			t.checkSlowConns(ctx)
		}
	}
}

// checkSlowConns 检查并处理速率过低的连接
func (t *TrunkKCP) checkSlowConns(ctx context.Context) {
	t.connMu.Lock()
	slowConnThreshold := t.slowConnThreshold
	slowConnMinAge := t.slowConnMinAge
	onIdleConnFn := t.onIdleConnFn
	if slowConnThreshold <= 0 {
		t.connMu.Unlock()
		return
	}

	// 默认最小存活时间为30分钟
	if slowConnMinAge == 0 {
		slowConnMinAge = 30 * time.Minute
	}

	now := time.Now()
	var stats []struct {
		id          int
		createdAt   time.Time
		age         time.Duration
		sendBytes   int64
		recvBytes   int64
		sendRate    float64 // bytes/s
		recvRate    float64 // bytes/s
		lastSample  int64
		lastSendCnt int64
		lastRecvCnt int64
	}

	// 收集统计信息并计算速率
	for id, ac := range t.active {
		age := now.Sub(ac.createdAt)
		sendBytes := ac.sendBytes.Load()
		recvBytes := ac.recvBytes.Load()

		// 计算自上次采样以来的增量速率
		var sendRate, recvRate float64
		lastSample := ac.lastSampleTime.Load()
		if lastSample > 0 {
			interval := float64(now.UnixNano()-lastSample) / 1e9 // 秒
			if interval > 0 {
				sendDelta := sendBytes - ac.lastSendBytes.Load()
				recvDelta := recvBytes - ac.lastRecvBytes.Load()
				sendRate = float64(sendDelta) / interval
				recvRate = float64(recvDelta) / interval
			}
		}

		// 更新采样点
		ac.lastSampleTime.Store(now.UnixNano())
		ac.lastSendBytes.Store(sendBytes)
		ac.lastRecvBytes.Store(recvBytes)

		stats = append(stats, struct {
			id          int
			createdAt   time.Time
			age         time.Duration
			sendBytes   int64
			recvBytes   int64
			sendRate    float64
			recvRate    float64
			lastSample  int64
			lastSendCnt int64
			lastRecvCnt int64
		}{
			id:          id,
			createdAt:   ac.createdAt,
			age:         age,
			sendBytes:   sendBytes,
			recvBytes:   recvBytes,
			sendRate:    sendRate,
			recvRate:    recvRate,
			lastSample:  lastSample,
			lastSendCnt: ac.lastSendBytes.Load(),
			lastRecvCnt: ac.lastRecvBytes.Load(),
		})
	}
	t.connMu.Unlock()

	if len(stats) == 0 {
		return
	}

	// 计算平均速率（只计算有数据传输的连接）
	var totalSendRate, totalRecvRate float64
	var sendCount, recvCount int
	for _, s := range stats {
		if s.sendRate > 0 {
			totalSendRate += s.sendRate
			sendCount++
		}
		if s.recvRate > 0 {
			totalRecvRate += s.recvRate
			recvCount++
		}
	}

	avgSendRate := float64(0)
	avgRecvRate := float64(0)
	if sendCount > 0 {
		avgSendRate = totalSendRate / float64(sendCount)
	}
	if recvCount > 0 {
		avgRecvRate = totalRecvRate / float64(recvCount)
	}

	// 打印速率统计
	for _, s := range stats {
		log.Ctx(ctx).Info().
			Int("conn_id", s.id).
			Dur("age", s.age).
			Int64("send_bytes", s.sendBytes).
			Int64("recv_bytes", s.recvBytes).
			Float64("send_rate_bps", s.sendRate).
			Float64("recv_rate_bps", s.recvRate).
			Float64("avg_send_rate_bps", avgSendRate).
			Float64("avg_recv_rate_bps", avgRecvRate).
			Msg("conn rate stats")
	}

	// 检查并关闭慢连接
	for _, s := range stats {
		// 只检查创建时间超过指定时间的连接
		if s.age < slowConnMinAge {
			continue
		}

		// 检查是否有任一方向速率低于平均值的指定阈值
		slowSend := avgSendRate > 0 && s.sendRate < avgSendRate*slowConnThreshold
		slowRecv := avgRecvRate > 0 && s.recvRate < avgRecvRate*slowConnThreshold

		if slowSend || slowRecv {
			log.Ctx(ctx).Warn().
				Int("conn_id", s.id).
				Dur("age", s.age).
				Float64("send_rate", s.sendRate).
				Float64("recv_rate", s.recvRate).
				Float64("avg_send_rate", avgSendRate).
				Float64("avg_recv_rate", avgRecvRate).
				Bool("slow_send", slowSend).
				Bool("slow_recv", slowRecv).
				Msg("slow connection detected, replacing")

			// 如果有回调函数，调用它获取新连接
			var newConn io.ReadWriteCloser
			if onIdleConnFn != nil {
				newConn = onIdleConnFn(s.id)
			}

			// 先移除旧连接
			if err := t.RemoveConn(s.id); err != nil {
				log.Ctx(ctx).Warn().Err(err).Int("conn_id", s.id).Msg("failed to remove slow connection")
			}

			// 如果有新连接，添加它
			if newConn != nil {
				newID, err := t.AddConn(newConn)
				if err != nil {
					log.Ctx(ctx).Warn().Err(err).Msg("failed to add replacement connection")
					_ = newConn.Close()
				} else {
					log.Ctx(ctx).Info().Int("old_conn_id", s.id).Int("new_conn_id", newID).Msg("slow connection replaced")
				}
			}
		}
	}
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
		if conn != nil && !conn.closed.Load() {
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
