package trunk_kcp

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/utils/log"
	kcp "github.com/xtaci/kcp-go"
)

// stats.go：KCP 线路统计与周期自诊断日志。
//
// 背景（真机教训）：KCP 关掉拥塞控制（NoDelay 的 nc=1）后，发送窗口就是**唯一**的
// 流控手段，窗口该取多大取决于链路 BDP（速率×RTT）与瓶颈排队能力：
//
//	窗口 ≫ BDP + 队列：每轮都把限速/整形队列灌爆 → 丢包 → RTO 重传 → 带宽全变重传
//	                    （实测 1024 段在 24Mbps/40ms 链路上放大 3.2~3.8x、重传 68%）
//	窗口 ≪ BDP：发送端被自己饿死（实测真机把 128 段写死后只有 7Mbps/300kB/s）
//
// 所以窗口**必须由使用者按链路显式设定**（SetWindowSize），这里只负责把"窗口是否合适"
// 变成一行可读的数字：线上/收线/交付速率、放大率、重传率与**重传来源**、RTT、积压。
// （曾试过按放大率自动调窗，真机上把窗口缩到下限、吞吐掉到 1/4，已删除。）

const (
	kcpCmdPush = 0x51 // IKCP_CMD_PUSH
	kcpCmdAck  = 0x52 // IKCP_CMD_ACK

	// defaultSndWnd 默认发送窗口（段）
	defaultSndWnd = 1024
	// defaultRcvWnd 默认接收窗口（段）：缓冲用，保持大值
	defaultRcvWnd = 1024
	// statsLogEvery 自诊断日志间隔
	statsLogEvery = 3 * time.Second
	// statsPruneSpan SN 记账保留跨度（段），超出即清理老条目
	statsPruneSpan = 8192
)

// trunkStats 线路统计：线上字节、PUSH/重传段、交付/已确认字节、RTT。
// 这些是"窗口该多大"的唯一可靠依据（放大率 = 线上字节 / 已确认载荷字节）。
type trunkStats struct {
	segs      atomic.Int64  // 发出的 KCP 段（含 ACK 等控制段）
	push      atomic.Int64  // 发出的 PUSH 段（含重传）
	retrans   atomic.Int64  // 重复 SN 的 PUSH 段（重传）
	wireBytes atomic.Int64  // 写往物理连接的字节（= 本端网卡发出的量）
	rxBytes   atomic.Int64  // 收到的线上字节（= 对端推给本端的量，接收侧判丢包位置用）
	rxSegs    atomic.Int64  // 收到的 PUSH 段（含重复）
	rxDup     atomic.Int64  // 收到的重复 SN PUSH 段（对端重传；接收侧唯一能直接看到的浪费）
	rxReorder atomic.Int64  // 收到的乱序 PUSH 段（SN 小于已见最大 SN，且不是重复）
	delivered atomic.Int64  // 交付给虚拟连接的载荷字节（接收侧用）
	ackedSegs atomic.Int64  // 对端累计确认的段数（取 ACK 里的 una；发送侧算"送达速率"）
	rxAckSegs atomic.Int64  // 收到的 ACK 段总数
	maxUna    atomic.Uint32 // 见过的最大累计确认 SN

	mu      sync.Mutex
	seen    map[uint32]struct{}  // 已发过的 SN（判重传）
	sent    map[uint32]time.Time // SN -> 首次发送时间（算 RTT）
	rxSeen  map[uint32]struct{}  // 收到过的 SN（判对端重传）
	maxSN   uint32               // 已见过的最大发送 SN
	maxRxSN uint32               // 已见过的最大接收 SN
	srtt    time.Duration        // 平滑 RTT（1/8 律）
	minRTT  time.Duration        // 观测到的最小 RTT（原始样本最小值，仅诊断）
}

func newTrunkStats() *trunkStats {
	return &trunkStats{
		seen:   make(map[uint32]struct{}, 1024),
		sent:   make(map[uint32]time.Time, 1024),
		rxSeen: make(map[uint32]struct{}, 1024),
	}
}

// snBefore 判断 a 是否在 b 之前（考虑 uint32 回绕）
func snBefore(a, b uint32) bool { return int32(a-b) < 0 }

// recordSend 记录一批即将写往物理连接的 KCP 数据。
//
// 注意：KCP 一次 flush 会把**多个段拼在同一个缓冲**里交给 output 回调（收包侧也因此
// 要按长度字段重组），所以这里必须按长度遍历每个段——只解析第一个段会把"已发送/重传/
// 已确认"都算成实际的 1/N，放大率随之虚高几十倍（实测踩过，见 TestStatsAckAccounting）。
func (s *trunkStats) recordSend(pkt []byte) {
	s.wireBytes.Add(int64(len(pkt)))
	for off := 0; off+kcpHeaderSize <= len(pkt); {
		seg := pkt[off:]
		payloadLen := int(binary.LittleEndian.Uint32(seg[20:24]))
		if payloadLen < 0 || off+kcpHeaderSize+payloadLen > len(pkt) {
			return // 解析异常：剩余部分放弃统计（不影响数据路径）
		}
		s.recordSegment(seg[:kcpHeaderSize+payloadLen])
		off += kcpHeaderSize + payloadLen
	}
}

// recordSegment 统计单个 KCP 段（判重传、记账发送时间）。
func (s *trunkStats) recordSegment(seg []byte) {
	s.segs.Add(1)
	if seg[4] != kcpCmdPush {
		return
	}
	sn := binary.LittleEndian.Uint32(seg[12:16])
	s.push.Add(1)

	s.mu.Lock()
	if _, ok := s.seen[sn]; ok {
		s.retrans.Add(1)
	}
	s.seen[sn] = struct{}{}
	if _, ok := s.sent[sn]; !ok {
		s.sent[sn] = time.Now()
	}
	if snBefore(s.maxSN, sn) {
		s.maxSN = sn
	}
	if len(s.seen) > statsPruneSpan {
		cut := s.maxSN - statsPruneSpan/2
		for k := range s.seen {
			if snBefore(k, cut) {
				delete(s.seen, k)
			}
		}
		for k := range s.sent {
			if snBefore(k, cut) {
				delete(s.sent, k)
			}
		}
	}
	s.mu.Unlock()
}

// recordAck 用 ACK 段更新 RTT 估计与"累计送达"计数。
//
// ACK 是**合并**发送的（实测 448 段只回了 8 个 ACK），它携带的累计确认号 una 才是
// 真正的送达量（kcp-go 的 snd_nxt 从 0 开始，una 即累计送达段数）；
// 用"ACK 的 sn"逐段计数会严重低估，进而把放大率算成几十倍。
func (s *trunkStats) recordAck(sn, una uint32, now time.Time) {
	if snBefore(s.maxUna.Load(), una) {
		s.maxUna.Store(una)
		s.ackedSegs.Store(int64(una))
	}
	s.mu.Lock()
	t0, ok := s.sent[sn]
	if ok {
		delete(s.sent, sn)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	rtt := now.Sub(t0)
	if rtt <= 0 || rtt > 10*time.Second {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.minRTT == 0 || rtt < s.minRTT {
		s.minRTT = rtt
	}
	if s.srtt == 0 {
		s.srtt = rtt
	} else {
		s.srtt = s.srtt - s.srtt/8 + rtt/8
	}
}

// recordRecvPacket 解析收到的 KCP 数据（可能是一个 flush 缓冲里的多个段）：
// ACK 段用于 RTT 估计与"已确认"计数，PUSH 段用于统计对端重传（线上浪费）。
func (s *trunkStats) recordRecvPacket(pkt []byte, now time.Time) {
	s.rxBytes.Add(int64(len(pkt)))
	for off := 0; off+kcpHeaderSize <= len(pkt); {
		seg := pkt[off:]
		payloadLen := int(binary.LittleEndian.Uint32(seg[20:24]))
		if payloadLen < 0 || off+kcpHeaderSize+payloadLen > len(pkt) {
			return
		}
		switch seg[4] {
		case kcpCmdAck:
			s.rxAckSegs.Add(1)
			s.recordAck(binary.LittleEndian.Uint32(seg[12:16]), binary.LittleEndian.Uint32(seg[16:20]), now)
		case kcpCmdPush:
			s.recordRecvPush(binary.LittleEndian.Uint32(seg[12:16]))
		}
		off += kcpHeaderSize + payloadLen
	}
}

// recordRecvPush 统计收到的 PUSH 段，重复 SN 即"对端重传"，比已见最大 SN 小的新段即"乱序"。
//
// 这是判"浪费发生在哪一段"的关键证据（接收侧唯一能直接看到的浪费）：
//   - 接收侧重复率 ≈ 发送侧重传率：重传的包确实穿过了链路 → 链路有余量，是发送端窗口太大
//     （在途 ≫ BDP+队列）造成的伪重传；
//   - 接收侧重复率 ≈ 0 而发送侧重传率很高：重传包没到 → 发送侧本地队列/链路入口就丢了。
//
// 乱序率则是"多物理连接"的直接代价：一条 trunk 的 KCP 段被发到多条物理连接（发送端多个
// sendLoop 竞争同一个 sendChan、接收端多个 recvLoop 竞争 recvChan），SN 顺序被打乱后，
// kcp-go 的 early retransmit（fastack>0 且本轮无新段可发）会把乱序误判为丢包并重传——
// 实测 4 条物理连接时这一项能吃掉 ~16% 的链路带宽（1 条连接时为 0）。
func (s *trunkStats) recordRecvPush(sn uint32) {
	s.rxSegs.Add(1)
	s.mu.Lock()
	_, dup := s.rxSeen[sn]
	if dup {
		s.rxDup.Add(1)
	} else {
		if snBefore(sn, s.maxRxSN) {
			s.rxReorder.Add(1)
		}
		s.rxSeen[sn] = struct{}{}
	}
	if snBefore(s.maxRxSN, sn) {
		s.maxRxSN = sn
	}
	if len(s.rxSeen) > statsPruneSpan {
		cut := s.maxRxSN - statsPruneSpan/2
		for k := range s.rxSeen {
			if snBefore(k, cut) {
				delete(s.rxSeen, k)
			}
		}
	}
	s.mu.Unlock()
}

// rxDupPct 收到的 PUSH 段里重复（对端重传）的占比（%）。
func (s *trunkStats) rxDupPct() float64 {
	segs := s.rxSegs.Load()
	if segs == 0 {
		return 0
	}
	return 100 * float64(s.rxDup.Load()) / float64(segs)
}

// retransPct 重传段占已发段的比例（%）。
func (s *trunkStats) retransPct() float64 {
	segs := s.segs.Load()
	if segs == 0 {
		return 0
	}
	return 100 * float64(s.retrans.Load()) / float64(segs)
}

func (s *trunkStats) srttValue() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.srtt
}

func (s *trunkStats) minRTTValue() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.minRTT
}

func (s *trunkStats) rtts() (srtt, minRTT time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.srtt, s.minRTT
}

// Stats 暴露给调用方/运维的线路统计快照。
type Stats struct {
	SndWnd     int           // 当前发送窗口（段）
	RcvWnd     int           // 接收窗口（段）
	Backlog    int           // 发送队列积压（段；kcp.WaitSnd，含未发出的）
	InFlight   int64         // 在途（已发出但未被确认的唯一 PUSH 段）≈ 实际占用的窗口
	Segs       int64         // 已发出 KCP 段总数
	Retrans    int64         // 重传段总数
	RetransPct float64       // 重传占比（%）
	WireBytes  int64         // 已写往物理连接的字节
	RxBytes    int64         // 已从物理连接收到的字节
	RxSegs     int64         // 收到的 PUSH 段总数（含重复）
	RxDup      int64         // 收到的重复 PUSH 段数（对端重传）
	RxDupPct   float64       // 收到段里的重复占比（%）
	RxReorder  int64         // 收到的乱序 PUSH 段数（SN 小于已见最大 SN）
	RxReordPct float64       // 收到段里的乱序占比（%）
	Delivered  int64         // 已交付给虚拟连接的载荷字节
	SRTT       time.Duration // 平滑 RTT（原始样本最小值见 MinRTT）
	MinRTT     time.Duration // 最小 RTT（传播时延，仅诊断）
	Push       int64         // 发出的 PUSH 段总数
	Acked      int64         // 被对端确认的 PUSH 段总数
	RxAck      int64         // 收到的 ACK 段总数
	// 重传来源（kcp-go 的进程级全局 snmp，用两次快照相减得区间值）：
	// RTORetrans 超时重传、"窗口太大/包没到"；Fast/EarlyRetrans 由重复 ACK 触发、
	// "包真丢了且后面还在到"。前者占主导 = 在途超过 BDP+队列（伪重传），后者 = 真实丢包。
	RTORetrans   int64
	FastRetrans  int64
	EarlyRetrans int64
}

// Stats 返回当前统计快照（可与上一次快照相减得到区间速率）。
func (t *TrunkKCP) Stats() Stats {
	segs, retrans := t.stats.segs.Load(), t.stats.retrans.Load()
	srtt, minRTT := t.stats.rtts()
	st := Stats{
		Segs:      segs,
		Retrans:   retrans,
		WireBytes: t.stats.wireBytes.Load(),
		RxBytes:   t.stats.rxBytes.Load(),
		RxSegs:    t.stats.rxSegs.Load(),
		RxDup:     t.stats.rxDup.Load(),
		RxReorder: t.stats.rxReorder.Load(),
		Delivered: t.stats.delivered.Load(),
		SRTT:      srtt,
		MinRTT:    minRTT,
		Push:      t.stats.push.Load(),
		Acked:     t.stats.ackedSegs.Load(),
		RxAck:     t.stats.rxAckSegs.Load(),
	}
	if segs > 0 {
		st.RetransPct = 100 * float64(retrans) / float64(segs)
	}
	if n := st.RxSegs; n > 0 {
		st.RxDupPct = 100 * float64(st.RxDup) / float64(n)
		st.RxReordPct = 100 * float64(st.RxReorder) / float64(n)
	}
	// 唯一已发段 = PUSH - 重复；减去被累计确认的段数即"在途"。
	st.InFlight = st.Push - st.Retrans - st.Acked
	if st.InFlight < 0 {
		st.InFlight = 0
	}
	if sm := kcp.DefaultSnmp.Copy(); sm != nil {
		st.RTORetrans, st.FastRetrans, st.EarlyRetrans =
			int64(sm.LostSegs), int64(sm.FastRetransSegs), int64(sm.EarlyRetransSegs)
	}
	t.ctrlMu.Lock()
	st.SndWnd, st.RcvWnd = t.sndWnd, t.rcvWnd
	t.ctrlMu.Unlock()
	t.kcpLock.Lock()
	st.Backlog = t.kcp.WaitSnd()
	t.kcpLock.Unlock()
	return st
}

// logSnapshot statsTick 的区间基准：收线/收段/重复段 + 重传来源（snmp 全局计数）。
type logSnapshot struct {
	rxBytes, rxSegs, rxDup, rxReorder int64
	segs, push, rxAck                 int64
	rto, fast, early                  int64
}

// statsTick 周期性打印线路自诊断（debug 级，每 statsLogEvery 一条）：线上/收线/交付
// 速率、放大率、重传率与**重传来源**、RTT、窗口与积压。
//
// 判读（真机 43.155.182.37 ↔ 家庭宽带，RTT 54ms 的实测口径）：
//   - 线上 ≫ 收线 且 收段重复率高 → 重传包确实过了链路：发送端在途 ≫ BDP+队列（伪重传），
//     调小 kcp_sndwnd 或缩短 RTO；链路本身还有余量。
//   - 线上 ≫ 收线 但 收段重复率≈0 → 重传包在发送端本地/链路入口就丢了（队列溢出），
//     同样是在途过大，但瓶颈在发送侧。
//   - 重传来源 RTO 占主导 → 包其实到了，只是超时判早了（在途 > BDP）；Fast/Early 占主导
//     → 真丢包，靠快速重传恢复，别再调小窗口。
//   - 已确认 ≈ 交付 ≈ 链路速率 且放大≈1 → 窗口合适。
func (t *TrunkKCP) statsTick(ctx context.Context, now time.Time) {
	t.ctrlMu.Lock()
	firstLog := t.lastLogAt.IsZero() // 首行没有"上一次采样"，差值类指标只能按 0 起算
	needLog := now.Sub(t.lastLogAt) >= statsLogEvery
	if needLog {
		t.lastLogAt = now
	}
	t.ctrlMu.Unlock()
	if !needLog {
		return
	}
	wire, acked := t.stats.wireBytes.Load(), t.stats.ackedSegs.Load()
	delivered := t.stats.delivered.Load()
	push, retrans := t.stats.push.Load(), t.stats.retrans.Load()
	payload := t.payloadSize()
	t.ctrlMu.Lock()
	snd, rcv := t.sndWnd, t.rcvWnd
	t.ctrlMu.Unlock()
	snap := logSnapshot{
		rxBytes:   t.stats.rxBytes.Load(),
		rxSegs:    t.stats.rxSegs.Load(),
		rxDup:     t.stats.rxDup.Load(),
		rxReorder: t.stats.rxReorder.Load(),
		segs:      t.stats.segs.Load(),
		push:      push,
		rxAck:     t.stats.rxAckSegs.Load(),
	}
	if sm := kcp.DefaultSnmp.Copy(); sm != nil {
		snap.rto, snap.fast, snap.early =
			int64(sm.LostSegs), int64(sm.FastRetransSegs), int64(sm.EarlyRetransSegs)
	}
	lastSnap := t.lastLogSnap
	if firstLog {
		// snmp 是**进程级**全局计数：首行若直接取差值，会把之前所有 trunk/测试的累计量
		// 当成"这 3 秒的重传"打出来（实测出现过 RTO 29886 这种误导数字）。首行只做基线。
		lastSnap = snap
	}
	t.lastLogSnap = snap

	lastW, lastA, lastD := t.lastLogWire, t.lastLogAcked, t.lastLogDeliv
	lastP, lastR := t.lastLogPush, t.lastLogRetrans
	t.lastLogWire, t.lastLogAcked, t.lastLogDeliv = wire, acked, delivered
	t.lastLogPush, t.lastLogRetrans = push, retrans
	elapsed := statsLogEvery.Seconds()
	wireRate := float64(wire-lastW) / elapsed
	ackedRate := float64(acked-lastA) * float64(payload) / elapsed
	// 本端作为接收方实际交付给上层（虚拟连接）的速率：与服务端的"线上"对比即可判断
	// 丢包发生在"路径/对端"还是"本端收包路径"。
	delivRate := float64(delivered-lastD) / elapsed
	// 本端收到的线上速率（接收侧）：与对端日志的"线上"对齐，判断重传包有没有真的过来。
	rxRate := float64(snap.rxBytes-lastSnap.rxBytes) / elapsed
	rxDupPct, rxReordPct := 0.0, 0.0
	if d := snap.rxSegs - lastSnap.rxSegs; d > 0 {
		rxDupPct = 100 * float64(snap.rxDup-lastSnap.rxDup) / float64(d)
		rxReordPct = 100 * float64(snap.rxReorder-lastSnap.rxReorder) / float64(d)
	}
	// 区间内的 ACK 收发数：发送端"生命线"就是 ACK——ACK 稀疏/丢失会让 snd_una 停滞，
	// KCP 于是把已经到达的段也当丢包重传（真机 2026-09-23 那次的 68% 重传就疑似这个）。
	ackSent := (snap.segs - snap.push) - (lastSnap.segs - lastSnap.push)
	ackRecv := snap.rxAck - lastSnap.rxAck
	// 区间重传率（只看这 3 秒）：会话累计值会被历史拉平，看不出"现在还在不在丢"
	retransPct := 0.0
	if d := push - lastP; d > 0 {
		retransPct = 100 * float64(retrans-lastR) / float64(d)
	}
	amp := 0.0
	if ackedRate > 0 {
		amp = wireRate / ackedRate
	}
	t.kcpLock.Lock()
	backlog := t.kcp.WaitSnd()
	t.kcpLock.Unlock()
	inFlight := push - retrans - acked
	if inFlight < 0 {
		inFlight = 0
	}
	log.Ctx(ctx).Debug().Caller().
		Msgf("trunk_kcp: 窗口 snd=%d rcv=%d(积压 %d 在途 %d) 线上 %.0f 收线 %.0f 已确认 %.0f 交付 %.0f KB/s 放大 %.2f 重传 %.1f%%(累计 %.1f%%; RTO %d 快 %d 提前 %d) 收段 %d(重复 %.1f%% 乱序 %.1f%%) ACK 发%d 收%d RTT srtt=%.0f min=%.0f",
			snd, rcv, backlog, inFlight, wireRate/1024, rxRate/1024, ackedRate/1024, delivRate/1024, amp,
			retransPct, t.stats.retransPct(),
			snap.rto-lastSnap.rto, snap.fast-lastSnap.fast, snap.early-lastSnap.early,
			snap.rxSegs-lastSnap.rxSegs, rxDupPct, rxReordPct, ackSent, ackRecv,
			float64(t.stats.srttValue().Milliseconds()), float64(t.stats.minRTTValue().Milliseconds()))
}

// payloadSize 单段 KCP 载荷字节数（mtu - 24B KCP 头）。
func (t *TrunkKCP) payloadSize() int {
	t.ctrlMu.Lock()
	mtu := t.mtu
	t.ctrlMu.Unlock()
	if mtu <= kcpHeaderSize {
		mtu = KcpMtu
	}
	return mtu - kcpHeaderSize
}
