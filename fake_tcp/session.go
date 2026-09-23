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

type sessState int32

const (
	stateInit        sessState = iota
	stateSynSent               // 客户端：SYN 已发，等 SYNACK
	stateSynReceived           // 服务端：SYN 已收，SYNACK 已发（等三次握手最后一个 ACK）
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

// seqAfter/seqBefore 序号回绕安全比较（RFC 793 序号空间，int32 差值法）。
// uint32 seq 在 4GB/连接处回绕，直接比大小会误判。
func seqAfter(a, b uint32) bool  { return int32(a-b) > 0 }
func seqBefore(a, b uint32) bool { return int32(a-b) < 0 }

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
	ipID     atomic.Uint32 // 每连接 IP ID 计数器（仿 Linux per-flow 行为）

	sendMu sync.Mutex // 序列化"取 seq → 入队"（sndNxt 分配）

	bitmapMu sync.Mutex
	bitmap   map[uint32]uint32 // 乱序段 seq -> seq+len（去重 / SACK 外观）

	// holeSince 空洞（位图非空）首次出现的时间戳（unixnano）；位图清空时归零。
	// 用于"虚拟重传愈合"（healLocked）：超过 HealDelay 未愈合则直接越过空洞。
	holeSince atomic.Int64

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

// closeErr 读侧关闭时的错误：RST → ErrConnReset；本地关闭 → ErrConnClosed
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
// 纯 ACK 探针之外的控制段都走这里。
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
		Seq:     s.sndNxt.Load(),
		Ack:     s.rcvNxt.Load(),
		TSval:   s.tsNow(),
		TSecr:   s.tsRecent.Load(),
		Win:     defaultWindow,
		SACK:    sack,
		Payload: payload,
		IPID:    uint16(s.ipID.Add(1)),
	}
	n := uint32(len(payload))
	if flags&FlagSYN != 0 || flags&FlagFIN != 0 { // SYN/FIN 各占一个 seq
		n++
	}
	s.sndNxt.Add(n)
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
				log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 发包失败: %v", err)
			}
		case <-s.closeCh:
			for {
				select {
				case seg := <-s.outCh:
					if err := s.link.WriteSegment(seg); err != nil {
						log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 收尾发包失败: %v", err)
					}
				default:
					return
				}
			}
		}
	}
}

// sendKeepalive 发送保活探针（plan.md §4.5：seq=sndNxt-1 的纯 ACK，
// 标准 TCP keepalive 外观；对端回 ACK 应答，双向活性由此维持）。
// 不占用 sndNxt，非阻塞入队。
func (s *session) sendKeepalive() error {
	if s.isClosed() {
		return ErrConnClosed.New()
	}
	seg := &Segment{
		Peer:   s.peer,
		ConnID: s.connID,
		Flags:  FlagACK,
		Seq:    s.sndNxt.Load() - 1, // 窗口前一格
		Ack:    s.rcvNxt.Load(),
		TSval:  s.tsNow(),
		TSecr:  s.tsRecent.Load(),
		Win:    defaultWindow,
		IPID:   uint16(s.ipID.Add(1)),
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
		log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 收到 RST, peer=%v", s.peer)
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

// handleSynSent 客户端握手：等 SYNACK（校验 ack，回 ACK 完成三次握手）
func (s *session) handleSynSent(ctx context.Context, seg *Segment) {
	if seg.Flags&FlagSYN == 0 || seg.Flags&FlagACK == 0 {
		return // 不是 SYNACK，忽略
	}
	if seg.Ack != s.sndNxt.Load() { // SYN 已消耗 isn+1
		log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: SYNACK 的 ack 不符: %d != %d", seg.Ack, s.sndNxt.Load())
		return
	}
	s.rcvNxt.Store(seg.Seq + 1)
	s.rcvMax.Store(seg.Seq + 1)
	s.tsRecent.Store(seg.TSval)
	s.sndUna.Store(seg.Ack)
	s.markEstablished()
	if err := s.sendSeg(FlagACK, nil); err != nil { // 三次握手最后一击
		log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 握手 ACK 发送失败: %v", err)
	}
}

// handleSynReceived 服务端等三次握手最后一个 ACK；
// 重复 SYN 说明 SYNACK 丢失，重发 SYNACK
func (s *session) handleSynReceived(ctx context.Context, seg *Segment) {
	if seg.Flags&FlagSYN != 0 {
		s.resendSynAck(ctx)
		return
	}
	if seg.Flags&FlagACK == 0 || seg.Ack != s.sndNxt.Load() {
		return
	}
	s.sndUna.Store(seg.Ack)
	s.tsRecent.Store(seg.TSval)
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
		log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 重发 SYNACK 失败: %v", err)
	}
}

// handleData Established 及之后的数据/关闭处理
func (s *session) handleData(ctx context.Context, seg *Segment) {
	st := s.getState()

	// Established 状态收到 SYN 说明对端状态错乱（重 SYN 由 SynReceived 兜住），静默忽略
	if seg.Flags&FlagSYN != 0 {
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
	s.handleTCPData(ctx, seg)
}

// handleTCPData 收包规则（plan.md §4.3 表格的代码实现；序号比较回绕安全）
func (s *session) handleTCPData(ctx context.Context, seg *Segment) {
	s.tsRecent.Store(seg.TSval)
	if ack := seg.Ack; seqAfter(ack, s.sndUna.Load()) && !seqAfter(ack, s.sndNxt.Load()) {
		s.sndUna.Store(ack)
	}

	seq := seg.Seq
	end := seq + uint32(len(seg.Payload))
	rcvNxt := s.rcvNxt.Load()
	rcvMax := s.rcvMax.Load()

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
		empty := len(s.bitmap) == 0
		s.bitmapMu.Unlock()
		if empty {
			s.holeSince.Store(0) // 空洞被填补，愈合时钟归零
		}
		s.rcvNxt.Store(cur)
		if seqAfter(cur, rcvMax) {
			s.rcvMax.Store(cur)
		}
		// delayed ACK：凑够 ackEveryPackets 个立即回，否则等 40ms 冲刷（keepalive 扫描）
		if s.ackPending.Add(1) >= ackEveryPackets {
			s.sendAck(ctx)
		}
	case seqAfter(seq, rcvNxt) && !seqAfter(seq, rcvMax+uint32(s.cfg.MTU)*64):
		// 空洞：位图先去重，重复段直接丢；新段照常上交（可靠性是上层的事），
		// 立即回 dup ACK + SACK（仿真实接收端）。空洞逾 HealDelay 未愈合
		// 则执行"虚拟重传愈合"（maybeHeal）。
		s.bitmapMu.Lock()
		if _, dup := s.bitmap[seq]; dup {
			s.bitmapMu.Unlock()
			s.sendAck(ctx)
			return
		}
		if len(s.bitmap) < bitmapCap {
			if len(s.bitmap) == 0 {
				s.holeSince.Store(time.Now().UnixNano()) // 首个空洞：启动愈合时钟
			}
			s.bitmap[seq] = end
		}
		s.bitmapMu.Unlock()
		s.deliver(ctx, seg.Payload)
		if seqAfter(end, rcvMax) {
			s.rcvMax.Store(end)
		}
		s.sendAckWithSACK(ctx)
	default:
		// 完全重复（!seqAfter(end-1, rcvNxt-1) 语义）、半重叠或过老/过新到离谱：
		// 丢弃 payload，照常回 ACK
		s.sendAck(ctx)
	}
}

// handlePureAck 纯 ACK：推进 sndUna（仅统计）；保活探针应答；状态收尾
func (s *session) handlePureAck(ctx context.Context, seg *Segment, st sessState) {
	s.tsRecent.Store(seg.TSval)
	if ack := seg.Ack; seqAfter(ack, s.sndUna.Load()) && !seqAfter(ack, s.sndNxt.Load()) {
		s.sndUna.Store(ack)
	}
	// TCP keepalive 探针（seq = 对端 sndNxt-1，即本端 rcvNxt-1）：回 ACK 应答。
	// 普通 ACK（seq == rcvNxt）不应答，避免乒乓。
	if seg.Seq+1 == s.rcvNxt.Load() && seg.Flags == FlagACK && st == stateEstablished {
		if err := s.sendSeg(FlagACK, nil); err != nil {
			log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: keepalive 应答发送失败: %v", err)
		}
		return
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
		if !seqBefore(seg.Seq, s.rcvNxt.Load()) {
			s.rcvNxt.Store(seg.Seq + 1) // FIN 占一个 seq
		}
		s.signalFIN() // 读侧 EOF
		s.state.Store(int32(stateCloseWait))
		// 回复 FIN|ACK（合并段；真实 TCP 亦常见）
		if err := s.sendSeg(FlagFIN|FlagACK, nil); err != nil {
			log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 回复 FIN|ACK 失败: %v", err)
		}
	case stateFinWait:
		// 本端先关，对端的 FIN|ACK 到达：回最后的 ACK，收尾
		if !seqBefore(seg.Seq, s.rcvNxt.Load()) {
			s.rcvNxt.Store(seg.Seq + 1)
		}
		s.signalFIN()
		if err := s.sendSeg(FlagACK, nil); err != nil {
			log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 回复最终 ACK 失败: %v", err)
		}
		s.teardown()
	case stateCloseWait:
		// 对端重试的 FIN：重发 FIN|ACK
		if err := s.sendSeg(FlagFIN|FlagACK, nil); err != nil {
			log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 重发 FIN|ACK 失败: %v", err)
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
		log.Ctx(ctx).Info().Caller().Msgf("fake_tcp: 接收队列满，丢弃 %d 字节, peer=%v", len(payload), s.peer)
	}
}

func (s *session) sendAck(ctx context.Context) {
	if err := s.sendSeg(FlagACK, nil); err != nil {
		log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: ACK 发送失败: %v", err)
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
		log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: SACK ACK 发送失败: %v", err)
	}
}

// maybeHeal 虚拟重传愈合（移植自 faux_tcp healLocked，plan.md v3）：
// 真实 TCP 此刻应已收到重传并推进累计确认，本层直接越过第一个空洞
// （该数据早已投递给上层 KCP，无需线上重演）：rcvNxt 跳到第一空洞末端，
// 吸收位图中紧随的连续段，然后发推进后的 ACK。
// 线上观察：ack 停顿 HealDelay（≈重传时延）后跳变——与真实快速恢复外观一致。
func (s *session) maybeHeal(ctx context.Context, now time.Time) {
	holeSince := s.holeSince.Load()
	if holeSince == 0 || now.Sub(time.Unix(0, holeSince)) < s.cfg.HealDelay {
		return // 无空洞或未到愈合时刻
	}
	s.bitmapMu.Lock()
	if len(s.bitmap) == 0 {
		s.holeSince.Store(0)
		s.bitmapMu.Unlock()
		return
	}
	// 第一空洞末端 = 位图最小 key（回绕安全比较）
	var minKey uint32
	first := true
	for k := range s.bitmap {
		if first || seqBefore(k, minKey) {
			minKey = k
			first = false
		}
	}
	if !seqAfter(minKey, s.rcvNxt.Load()) {
		// 位图异常（不含超前段）：清空兜底，不后退 rcvNxt
		s.bitmap = make(map[uint32]uint32)
		s.holeSince.Store(0)
		s.bitmapMu.Unlock()
		return
	}
	s.rcvNxt.Store(minKey)
	for {
		end, ok := s.bitmap[minKey]
		if !ok {
			break
		}
		delete(s.bitmap, minKey)
		minKey = end
	}
	s.rcvNxt.Store(minKey)
	if len(s.bitmap) == 0 {
		s.holeSince.Store(0)
	} else {
		s.holeSince.Store(now.UnixNano()) // 仍有空洞：为下一个空洞重新计时
	}
	s.bitmapMu.Unlock()
	// 推进后的累计确认立即发出（对应真实栈收到重传后的 ACK 跳变）
	s.sendAck(ctx)
	log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 空洞虚拟愈合, rcvNxt=%d, peer=%v", minKey, s.peer)
}

// ---- 保活扫描（由 keepalive 协程周期调用）----

// scanTick 返回 false 表示会话已被回收
func (s *session) scanTick(ctx context.Context, now time.Time) bool {
	if s.isClosed() {
		return false
	}
	st := s.getState()
	idle := now.Sub(time.Unix(0, s.lastActive.Load()))

	// 空洞愈合检查（不依赖连接状态——愈合本身只推进接收侧）
	s.maybeHeal(ctx, now)

	switch st {
	case stateSynSent, stateSynReceived:
		// 半开连接超时回收
		if idle > time.Duration(s.cfg.HandshakeRetries+1)*handshakeRetryInterval {
			log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 半开连接超时回收, peer=%v", s.peer)
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
			log.Ctx(ctx).Info().Caller().Msgf("fake_tcp: 对端保活超时，回收会话, peer=%v", s.peer)
			s.reset.Store(true)
			s.teardown()
			return false
		}
		if idle >= s.cfg.Keepalive {
			if err := s.sendKeepalive(); err != nil {
				log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: 保活发送失败: %v", err)
			}
		}
		// delayed ACK 冲刷
		if s.ackPending.Load() > 0 &&
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
			log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: FIN 发送失败: %v", err)
		}
		return
	}
	// 握手未完成或已在关闭流程中：直接收尾（发 RST 让对端立即知道）
	if st == stateSynSent || st == stateSynReceived {
		if err := s.sendSeg(FlagRST, nil); err != nil {
			log.Ctx(ctx).Debug().Caller().Msgf("fake_tcp: RST 发送失败: %v", err)
		}
	}
	s.teardown()
}
