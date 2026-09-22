package fake_tcp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/utils/log"
)

// session.go：极简 TCP 状态机（plan.md §4.2~§4.5）。
// 只保留外观特征（seq/ack/TS/SACK 字段照常维护），不做重传、拥塞控制与滑动窗口限流。
// 两种模式共用本状态机；差异全部收敛在各 LinkIO 实现的报文编解码里。

type sessState int32

const (
	stateInit sessState = iota
	stateSynSent         // 客户端：SYN 已发，等 SYNACK
	stateSynReceived     // 服务端：SYN 已收，SYNACK 已发（RawTCP 等最后一个 ACK）
	stateEstablished
	stateFinWait   // 本端已发 FIN
	stateCloseWait // 对端已发 FIN，本端已回复（等待收尾或被保活回收）
	stateClosed
)

// delayed ACK 参数（plan.md §4.3：每 2 个数据包或 40ms 回一次 ACK）
const (
	ackEveryPackets = 2
	ackMaxDelay     = 40 * time.Millisecond
)

// 接收位图容量上限（去重与 SACK 外观用，超出时丢弃最老条目）
const bitmapCap = 4096

// 出站队列容量（满则丢——等同链路拥塞丢包，可靠性由上层负责）
const outQueueCap = 4096

type session struct {
	cfg    Config
	link   LinkIO
	local  PeerAddr
	peer   PeerAddr
	connID uint64

	state atomic.Int32

	// ---- RawTCP 序号演艺（plan.md §4.3）----
	// sndNxt 严格递增永不回退；sndUna 仅统计不驱动行为；rcvNxt 单调不减。
	sndNxt   atomic.Uint32
	sndUna   atomic.Uint32
	rcvNxt   atomic.Uint32
	rcvMax   atomic.Uint32
	tsRecent atomic.Uint32 // 对端最新 tsval（回显用）
	tsEpoch  time.Time

	sendMu sync.Mutex // 序列化"取 seq → 发包"（sndNxt 分配）与 ACK 发送

	bitmapMu sync.Mutex
	bitmap   map[uint32]uint32 // 乱序段 seq -> seq+len（去重 / SACK 外观）

	ackPending atomic.Int32 // 已收未确认的数据包计数（delayed ACK）
	lastAckAt  atomic.Int64 // 上次回 ACK 时间（unixnano）

	recvCh chan []byte // 已接收 payload 队列（满则丢，本层不背压）

	// 出站队列：sendSeg 只入队不阻塞（读循环绝不允许阻塞在写路径上，
	// 否则两个对端的读循环互相等待会形成 ABBA 死锁），writeLoop 专门负责发出
	outCh chan *Segment

	finCh     chan struct{} // 对端 FIN 到达（读侧 EOF 信号）
	finOnce   sync.Once
	closeCh   chan struct{} // 会话完全关闭（读写皆失败）
	closeOnce sync.Once
	reset     atomic.Bool // 关闭原因：对端 RST

	estCh   chan struct{} // Established 信号（Dial 握手等待）
	estOnce sync.Once

	lastActive atomic.Int64 // 最近一次收到报文的时间（unixnano）

	onEstablished func(s *session) // Listener：推入 acceptCh
	onClose       func(key sessKey, s *session)
}

func (s *session) key() sessKey {
	return sessKey{Peer: s.peer, ConnID: s.connID}
}

func (s *session) tsNow() uint32 {
	return uint32(time.Since(s.tsEpoch).Milliseconds())
}

func (s *session) touch() {
	s.lastActive.Store(time.Now().UnixNano())
}

func (s *session) getState() sessState { return sessState(s.state.Load()) }

func (s *session) isClosed() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}

func (s *session) finReceived() bool {
	select {
	case <-s.finCh:
		return true
	default:
		return false
	}
}

// closeErr 读侧关闭时的错误：RST → ErrConnReset；对端 FIN → nil（走 io.EOF 分支）；
// 本地关闭 → ErrConnClosed
func (s *session) closeErr() error {
	if s.reset.Load() {
		return ErrConnReset.New()
	}
	return ErrConnClosed.New()
}

// markEstablished 进入 Established（幂等）
func (s *session) markEstablished() {
	s.estOnce.Do(func() {
		s.state.Store(int32(stateEstablished))
		close(s.estCh)
		if s.onEstablished != nil {
			s.onEstablished(s)
		}
	})
}

// signalFIN 通知读侧对端已 FIN（幂等）
func (s *session) signalFIN() {
	s.finOnce.Do(func() { close(s.finCh) })
}

// teardown 彻底关闭会话（幂等），从 Listener 表中摘除
func (s *session) teardown() {
	s.closeOnce.Do(func() {
		s.state.Store(int32(stateClosed))
		s.signalFIN()
		close(s.closeCh)
		if s.onClose != nil {
			s.onClose(s.key(), s)
		}
	})
}

// ---- 发送 ----

// sendSeg 构造并入队一个段（控制/ACK 用，非阻塞；队列满丢包并记日志）。
// flags 为 TCP 语义；UDP 模式由 LinkIO 映射为控制消息。
// 纯 ACK（无 payload、无 SYN/FIN/RST）在 UDP 模式下无意义，会被映射为保活消息——
// 调用方在 UDP 模式下应避免发送纯 ACK（delayed ACK 仅 RawTCP 有意义）。
func (s *session) sendSeg(flags uint8, payload []byte) error {
	return s.enqueue(flags, payload, nil, false)
}

// sendData 数据段入队（阻塞式：应用边界的本地背压；会话关闭即返回错误）。
// 注意这不构成线上滑窗——只是本地队列容量等待。
func (s *session) sendData(payload []byte) error {
	return s.enqueue(FlagPSH|FlagACK, payload, nil, true)
}

func (s *session) sendSegSACK(flags uint8, payload []byte, sack [][2]uint32) error {
	return s.enqueue(flags, payload, sack, false)
}

// enqueue 构造 Segment 并入队。
// payload 在此处拷贝（调用方缓冲会被复用，而异步入队后发送时机不可控）。
// seq 分配在 sendMu 内快速完成；入队动作在锁外（阻塞入队持锁会让读循环的 ACK 路径饿死）。
// 取舍：锁释放到入队之间存在极小窗口，并发控制段（如 FIN）理论上可能插队到
// 数据段之前——对端按空洞/重复规则容错，上层 KCP 兜底，可接受。
// TODO(性能): payload 拷贝可用 sync.Pool 缓冲池消除（连同各 LinkIO 的二次拷贝）。
func (s *session) enqueue(flags uint8, payload []byte, sack [][2]uint32, block bool) error {
	if s.isClosed() {
		return ErrConnClosed.New()
	}
	if len(payload) > 0 {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		payload = cp
	}
	s.sendMu.Lock()
	seg := &Segment{
		Peer:    s.peer,
		ConnID:  s.connID,
		Flags:   flags,
		TSval:   s.tsNow(),
		TSecr:   s.tsRecent.Load(),
		Win:     defaultWindow,
		SACK:    sack,
		Payload: payload,
	}
	if s.cfg.Mode == ModeRawTCP {
		seg.Seq = s.sndNxt.Load()
		seg.Ack = s.rcvNxt.Load()
		n := uint32(len(payload))
		if flags&FlagSYN != 0 || flags&FlagFIN != 0 { // SYN/FIN 各占一个 seq
			n++
		}
		s.sndNxt.Add(n)
	}
	if flags&FlagACK != 0 {
		s.ackPending.Store(0)
		s.lastAckAt.Store(time.Now().UnixNano())
	}
	s.sendMu.Unlock()

	if block {
		select {
		case s.outCh <- seg:
			return nil
		case <-s.closeCh:
			return ErrConnClosed.New()
		}
	}
	select {
	case s.outCh <- seg:
		return nil
	default:
		return ErrInvalidPacket.New("出站队列满，丢包") // 拥塞丢包语义：由上层重传
	}
}

// writeLoop 出站协程：排空 outCh 后才退出（保证关闭挥手的报文发完）
func (s *session) writeLoop(ctx context.Context) {
	for {
		select {
		case seg := <-s.outCh:
			if err := s.link.WriteSegment(seg); err != nil && !s.isClosed() {
				log.Ctx(ctx).Debug().Msgf("fake_tcp: 发包失败: %v", err)
			}
		case <-s.closeCh:
			for {
				select {
				case seg := <-s.outCh:
					if err := s.link.WriteSegment(seg); err != nil {
						log.Ctx(ctx).Debug().Msgf("fake_tcp: 收尾发包失败: %v", err)
					}
				default:
					return
				}
			}
		}
	}
}

// sendKeepalive 发送保活探针（plan.md §4.5：RawTCP 用 seq=sndNxt-1 的纯 ACK；
// 不占用 sndNxt，非阻塞入队）
func (s *session) sendKeepalive() error {
	if s.isClosed() {
		return ErrConnClosed.New()
	}
	seg := &Segment{
		Peer:    s.peer,
		ConnID:  s.connID,
		Flags:   FlagACK,
		TSval:   s.tsNow(),
		TSecr:   s.tsRecent.Load(),
		Win:     defaultWindow,
		Ack:     s.rcvNxt.Load(),
	}
	if s.cfg.Mode == ModeRawTCP {
		seg.Seq = s.sndNxt.Load() - 1 // 窗口前一格，标准 TCP keepalive 外观
	}
	select {
	case s.outCh <- seg:
		return nil
	default:
		return ErrInvalidPacket.New("出站队列满，保活包丢弃")
	}
}

// ---- 接收（由 Link 读循环调用，必须快速、不得阻塞）----

func (s *session) handle(ctx context.Context, seg *Segment) {
	if s.getState() == stateClosed {
		return
	}
	s.touch()

	if seg.Flags&FlagRST != 0 {
		s.reset.Store(true)
		log.Ctx(ctx).Debug().Msgf("fake_tcp: 收到 RST, peer=%v", s.peer)
		s.teardown()
		return
	}

	switch s.getState() {
	case stateSynSent:
		s.handleSynSent(ctx, seg)
	case stateSynReceived:
		s.handleSynReceived(ctx, seg)
	default:
		s.handleData(ctx, seg)
	}
}

// handleSynSent 客户端握手：等 SYNACK（RawTCP 校验 ack，回 ACK；UDP 直接建立）
func (s *session) handleSynSent(ctx context.Context, seg *Segment) {
	if seg.Flags&FlagSYN == 0 || seg.Flags&FlagACK == 0 {
		return // 不是 SYNACK，忽略
	}
	if s.cfg.Mode == ModeRawTCP {
		if seg.Ack != s.sndNxt.Load() { // SYN 已消耗 isn+1
			log.Ctx(ctx).Debug().Msgf("fake_tcp: SYNACK 的 ack 不符: %d != %d", seg.Ack, s.sndNxt.Load())
			return
		}
		s.rcvNxt.Store(seg.Seq + 1)
		s.rcvMax.Store(seg.Seq + 1)
		s.tsRecent.Store(seg.TSval)
		s.sndUna.Store(seg.Ack)
		s.markEstablished()
		if err := s.sendSeg(FlagACK, nil); err != nil { // 三次握手最后一击
			log.Ctx(ctx).Debug().Msgf("fake_tcp: 握手 ACK 发送失败: %v", err)
		}
		return
	}
	// UDP 模式：SYNACK 即建立
	s.markEstablished()
}

// handleSynReceived 服务端等三次握手最后一个 ACK（RawTCP）；
// 重复 SYN 说明 SYNACK 丢失，重发 SYNACK（UDP 模式服务端直接 Established，重 SYN 也由这里兜）
func (s *session) handleSynReceived(ctx context.Context, seg *Segment) {
	if seg.Flags&FlagSYN != 0 {
		s.resendSynAck(ctx)
		return
	}
	if s.cfg.Mode == ModeRawTCP {
		if seg.Flags&FlagACK == 0 || seg.Ack != s.sndNxt.Load() {
			return
		}
		s.sndUna.Store(seg.Ack)
		s.tsRecent.Store(seg.TSval)
	}
	s.markEstablished()
	// 最后一个 ACK 之后可能紧跟着数据，继续走数据路径
	s.handleData(ctx, seg)
}

// resendSynAck 重发 SYNACK（握手丢包重试；连接未建立前的重发不算"数据重传"）
func (s *session) resendSynAck(ctx context.Context) {
	s.sendMu.Lock()
	isn := s.sndNxt.Load() - 1 // SYN 已消耗一个 seq，回退仅用于重发同一个 SYNACK
	s.sndNxt.Store(isn)
	s.sendMu.Unlock()
	if err := s.sendSeg(FlagSYN|FlagACK, nil); err != nil {
		log.Ctx(ctx).Debug().Msgf("fake_tcp: 重发 SYNACK 失败: %v", err)
	}
}

// handleData Established 及之后的数据/关闭处理
func (s *session) handleData(ctx context.Context, seg *Segment) {
	st := s.getState()

	// ---- 重试的 SYN（SYNACK 丢失）：UDP 模式重发 SYNACK ----
	// RawTCP 模式下 SynReceived 状态的重 SYN 由 handleSynReceived 兜住；
	// Established 状态收到重 SYN 说明对端状态错乱，静默忽略。
	if seg.Flags&FlagSYN != 0 {
		if seg.Flags&FlagACK == 0 && s.cfg.Mode == ModeUDP && st == stateEstablished {
			if err := s.sendSeg(FlagSYN|FlagACK, nil); err != nil {
				log.Ctx(ctx).Debug().Msgf("fake_tcp: 重发 SYNACK 失败: %v", err)
			}
		}
		return
	}

	// ---- FIN 处理（plan.md §4.4）----
	if seg.Flags&FlagFIN != 0 {
		s.handleFIN(ctx, seg, st)
		return
	}

	// ---- 纯 ACK ----
	if len(seg.Payload) == 0 {
		s.handlePureAck(ctx, seg, st)
		return
	}

	// ---- 数据 ----
	if st != stateEstablished {
		return // 非建立状态的数据（如半开连接），丢弃
	}
	if s.cfg.Mode == ModeUDP {
		s.deliver(ctx, seg.Payload)
		return
	}
	s.handleTCPData(ctx, seg)
}

// handleTCPData RawTCP 收包规则（plan.md §4.3 表格的代码实现）
func (s *session) handleTCPData(ctx context.Context, seg *Segment) {
	s.tsRecent.Store(seg.TSval)
	if seg.Ack > s.sndUna.Load() && seg.Ack <= s.sndNxt.Load() {
		s.sndUna.Store(seg.Ack)
	}

	seq := seg.Seq
	end := seq + uint32(len(seg.Payload))
	rcvNxt := s.rcvNxt.Load()

	switch {
	case seq == rcvNxt:
		// 连续：上交并推进 rcvNxt；位图中紧随其后的乱序段一并吸收
		s.deliver(ctx, seg.Payload)
		s.bitmapMu.Lock()
		cur := end
		for {
			nxt, ok := s.bitmap[cur]
			if !ok {
				break
			}
			delete(s.bitmap, cur)
			cur = nxt
		}
		s.bitmapMu.Unlock()
		s.rcvNxt.Store(cur)
		if cur > s.rcvMax.Load() {
			s.rcvMax.Store(cur)
		}
		// delayed ACK：凑够 ackEveryPackets 个立即回，否则等 40ms 冲刷（keepalive 扫描）
		if s.ackPending.Add(1) >= ackEveryPackets {
			s.sendAck(ctx)
		}
	case seq > rcvNxt && seq <= s.rcvMax.Load()+uint32(s.cfg.MTU)*64:
		// 空洞：位图先去重，重复段直接丢；新段照常上交（可靠性是上层的事），
		// 立即回 ACK（dup-ack/SACK 外观）
		s.bitmapMu.Lock()
		if _, dup := s.bitmap[seq]; dup {
			s.bitmapMu.Unlock()
			s.sendAck(ctx)
			return
		}
		if len(s.bitmap) < bitmapCap {
			s.bitmap[seq] = end
		}
		s.bitmapMu.Unlock()
		s.deliver(ctx, seg.Payload)
		if end > s.rcvMax.Load() {
			s.rcvMax.Store(end)
		}
		s.sendAckWithSACK(ctx)
	default:
		// 完全重复（end <= rcvNxt）、半重叠（seq < rcvNxt < end，新字节一并丢弃，
		// 由上层重传补齐）或过老/过新到离谱：丢弃 payload，照常回 ACK
		s.sendAck(ctx)
	}
}

// handlePureAck 纯 ACK：推进 sndUna（仅统计）；保活探针应答；状态收尾
func (s *session) handlePureAck(ctx context.Context, seg *Segment, st sessState) {
	if s.cfg.Mode == ModeRawTCP {
		s.tsRecent.Store(seg.TSval)
		if seg.Ack > s.sndUna.Load() && seg.Ack <= s.sndNxt.Load() {
			s.sndUna.Store(seg.Ack)
		}
		// TCP keepalive 探针（seq = sndNxt-1，即对端视角 rcvNxt-1）：回 ACK 应答。
		// 普通 ACK（seq == rcvNxt）不应答，避免乒乓。
		if seg.Seq+1 == s.rcvNxt.Load() && seg.Flags == FlagACK && st == stateEstablished {
			if err := s.sendSeg(FlagACK, nil); err != nil {
				log.Ctx(ctx).Debug().Msgf("fake_tcp: keepalive 应答发送失败: %v", err)
			}
			return
		}
	} else {
		// UDP 模式：keepalive 探针（FlagACK）→ 回应答（FlagACK|URG）；应答本身不再回复
		if seg.Flags == FlagACK && st == stateEstablished {
			if err := s.sendSeg(FlagACK|FlagURG, nil); err != nil {
				log.Ctx(ctx).Debug().Msgf("fake_tcp: keepalive 应答发送失败: %v", err)
			}
			return
		}
	}
	switch st {
	case stateCloseWait:
		// 本端已回复 FIN|ACK，对端最后的 ACK 到达 → 四次挥手完成
		s.teardown()
	case stateFinWait:
		// 本端 FIN 被确认但对端 FIN 还没到：继续等（本实现对端总是 FIN|ACK 合并回复）
	}
}

// handleFIN 对端发起关闭
func (s *session) handleFIN(ctx context.Context, seg *Segment, st sessState) {
	switch st {
	case stateEstablished:
		if s.cfg.Mode == ModeRawTCP && seg.Seq >= s.rcvNxt.Load() {
			s.rcvNxt.Store(seg.Seq + 1) // FIN 占一个 seq
		}
		s.signalFIN() // 读侧 EOF
		s.state.Store(int32(stateCloseWait))
		// 回复 FIN|ACK（合并段；真实 TCP 亦常见）
		if err := s.sendSeg(FlagFIN|FlagACK, nil); err != nil {
			log.Ctx(ctx).Debug().Msgf("fake_tcp: 回复 FIN|ACK 失败: %v", err)
		}
		if s.cfg.Mode == ModeUDP {
			// UDP 模式无四次挥手外观需求，回复 FINACK 后即可收尾；
			// 但需 linger 以重发 FINACK 应答对端重试的 FIN（由保活扫描回收）
		}
	case stateFinWait:
		// 本端先关，对端的 FIN|ACK 到达：回最后的 ACK，收尾
		if s.cfg.Mode == ModeRawTCP && seg.Seq >= s.rcvNxt.Load() {
			s.rcvNxt.Store(seg.Seq + 1)
		}
		s.signalFIN()
		if err := s.sendSeg(FlagACK, nil); err != nil {
			log.Ctx(ctx).Debug().Msgf("fake_tcp: 回复最终 ACK 失败: %v", err)
		}
		s.teardown()
	case stateCloseWait:
		// 对端重试的 FIN：重发 FIN|ACK
		if err := s.sendSeg(FlagFIN|FlagACK, nil); err != nil {
			log.Ctx(ctx).Debug().Msgf("fake_tcp: 重发 FIN|ACK 失败: %v", err)
		}
	default:
	}
}

// ---- 读侧交付 ----

// deliver 上交 payload（拷贝入队；队列满则丢——本层不背压，可靠性由上层负责）
func (s *session) deliver(ctx context.Context, payload []byte) {
	b := make([]byte, len(payload))
	copy(b, payload)
	select {
	case s.recvCh <- b:
	default:
		log.Ctx(ctx).Info().Msgf("fake_tcp: 接收队列满，丢弃 %d 字节, peer=%v", len(payload), s.peer)
	}
}

func (s *session) sendAck(ctx context.Context) {
	if s.cfg.Mode != ModeRawTCP {
		return // UDP 模式无 ACK 语义
	}
	if err := s.sendSeg(FlagACK, nil); err != nil {
		log.Ctx(ctx).Debug().Msgf("fake_tcp: ACK 发送失败: %v", err)
	}
}

// sendAckWithSACK 空洞时立即回 ACK 并附 SACK block（纯外观，对端不响应它）
func (s *session) sendAckWithSACK(ctx context.Context) {
	s.bitmapMu.Lock()
	blocks := make([][2]uint32, 0, 4)
	for l, r := range s.bitmap {
		blocks = append(blocks, [2]uint32{l, r})
		if len(blocks) >= 4 {
			break
		}
	}
	s.bitmapMu.Unlock()
	if err := s.sendSegSACK(FlagACK, nil, blocks); err != nil {
		log.Ctx(ctx).Debug().Msgf("fake_tcp: SACK ACK 发送失败: %v", err)
	}
}

// ---- 保活扫描（由 keepalive 协程周期调用）----

// scanTick 返回 false 表示会话已被回收
func (s *session) scanTick(ctx context.Context, now time.Time) bool {
	if s.isClosed() {
		return false
	}
	st := s.getState()
	idle := now.Sub(time.Unix(0, s.lastActive.Load()))

	switch st {
	case stateSynSent, stateSynReceived:
		// 半开连接超时回收
		if idle > time.Duration(s.cfg.HandshakeRetries+1)*handshakeRetryInterval {
			log.Ctx(ctx).Debug().Msgf("fake_tcp: 半开连接超时回收, peer=%v", s.peer)
			s.teardown()
			return false
		}
	case stateFinWait, stateCloseWait:
		// 关闭流程卡死（对端消失）：一个保活周期后回收
		if idle > s.cfg.Keepalive {
			s.teardown()
			return false
		}
	case stateEstablished:
		if idle > 3*s.cfg.Keepalive {
			// 对端死亡
			log.Ctx(ctx).Info().Msgf("fake_tcp: 对端保活超时，回收会话, peer=%v", s.peer)
			s.reset.Store(true)
			s.teardown()
			return false
		}
		if idle >= s.cfg.Keepalive {
			if err := s.sendKeepalive(); err != nil {
				log.Ctx(ctx).Debug().Msgf("fake_tcp: 保活发送失败: %v", err)
			}
		}
		// delayed ACK 冲刷（仅 RawTCP）
		if s.cfg.Mode == ModeRawTCP && s.ackPending.Load() > 0 &&
			now.Sub(time.Unix(0, s.lastAckAt.Load())) >= ackMaxDelay {
			s.sendAck(ctx)
		}
	}
	return true
}

// closeInitiate 主动关闭（Conn.Close 调用）：发 FIN，异步收尾
func (s *session) closeInitiate(ctx context.Context) {
	st := s.getState()
	if st == stateClosed {
		return
	}
	if st == stateEstablished {
		s.state.Store(int32(stateFinWait))
		if err := s.sendSeg(FlagFIN|FlagACK, nil); err != nil {
			log.Ctx(ctx).Debug().Msgf("fake_tcp: FIN 发送失败: %v", err)
		}
		return
	}
	// 握手未完成或已在关闭流程中：直接收尾（发 RST 让对端立即知道）
	if st == stateSynSent || st == stateSynReceived {
		if err := s.sendSeg(FlagRST, nil); err != nil {
			log.Ctx(ctx).Debug().Msgf("fake_tcp: RST 发送失败: %v", err)
		}
	}
	s.teardown()
}
