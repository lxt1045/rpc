package trunk_kcp

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/utils/log"
)

// autownd.go：KCP 发送窗口的自动调节 + 线路自诊断。
//
// 背景（真机教训）：KCP 关掉拥塞控制（NoDelay 的 nc=1）后，发送窗口就是**唯一**的
// 流控手段，而窗口该取多大取决于链路 BDP（速率×RTT）与瓶颈排队能力，写死任何固定值
// 都会踩坑：
//
//	窗口 ≫ BDP + 队列：每轮都把限速/整形队列灌爆 → 丢包 → RTO 重传 → 带宽全变重传
//	                    （实测 1024 段在 24Mbps/40ms 链路上放大 3.2~3.8x、重传 68%）
//	窗口 ≪ BDP：发送端被自己饿死（实测真机把 128 段写死后只有 7Mbps/300kB/s）
//
// 而 BDP 无法预知（RTT 从 20ms 到 300ms、速率千差万别）。这里用 **Vegas 式判据**：
// 只有"RTT 被排队抬升"（srtt > minRTT×1.5）才认为窗口过大而收缩；否则持续放大窗口。
// 好处是**不把随机丢包当拥塞**（有损链路保持大窗口靠重传恢复），也无需配置链路参数。
//
// 接收窗口（rcv）不参与调节，保持大值：它是缓冲而不是限速——调小只会在应用（下游
// TCP/浏览器背压）稍有停顿时就把对端饿死。

const (
	kcpCmdPush = 0x51 // IKCP_CMD_PUSH
	kcpCmdAck  = 0x52 // IKCP_CMD_ACK

	// autoWindowTick 调节周期
	autoWindowTick = 100 * time.Millisecond
	// autoWindowMinWnd 自动调节的下限（段）
	autoWindowMinWnd = 32
	// autoWindowStartWnd 自动调节的起始窗口（段），从小往大爬
	autoWindowStartWnd = 64
	// autoWindowDefaultMax 自动调节的默认上限（段）
	autoWindowDefaultMax = 1024
	// autoWindowDefaultRcv 默认接收窗口（段）：缓冲用，保持大值
	autoWindowDefaultRcv = 1024
	// autoWindowLogEvery 自诊断日志间隔
	autoWindowLogEvery = 3 * time.Second
	// autoWindowAmpSpan 放大率估计的滑动窗口（控制周期数）：100ms×3 = 300ms。
	// 用 ACK 的累计确认 una 计数后，短窗口也足够稳（不再受 ACK 合并到达影响）。
	autoWindowAmpSpan = 10
	// autoWindowAdditiveStep 正常状态下的加性增步长（段/控制周期）。
	// 加性增 + 乘性减 ≈ 窄幅锯齿，稳定在"刚好打满链路"附近，不会像乘性增那样冲过头。
	autoWindowAdditiveStep = 16
	// autoWindowAmpTarget 线上放大率目标（线上字节/交付字节）：超过就认为在把带宽
	// 变成重传（窗口超过 BDP+队列），收回一档。实测 128 段≈1.22x、256≈1.38x、
	// 512≈2.09x、1024≈3.41x，取 1.5 可落在"充满链路且几乎不重传"的区间。
	autoWindowAmpTarget = 1.5
	// autoWindowQueueTarget 目标排队段数：窗口 ≈ BDP + 这个排队量。
	// 太小会浪费链路（BDP 估计略低于真实值时就填不满），太大会造成排队时延与丢包；
	// 32 段 ≈ 45KB（mtu1400）≈ 3Mbps~30Mbps 链路上 10~100ms 的排队，实测折中最佳。
	autoWindowQueueTarget = 32
	// statsPruneSpan SN 记账保留跨度（段），超出即清理老条目
	statsPruneSpan = 8192
)

// trunkStats 线路统计：线上字节、PUSH/重传段、交付/已确认字节、RTT。
// 这些是"窗口该多大"的唯一可靠依据（放大率 = 线上字节 / 已确认载荷字节）。
type trunkStats struct {
	segs      atomic.Int64  // 发出的 KCP 段（含 ACK 等控制段）
	push      atomic.Int64  // 发出的 PUSH 段
	retrans   atomic.Int64  // 重复 SN 的 PUSH 段（重传）
	wireBytes atomic.Int64  // 写往物理连接的字节（= 网卡上的量）
	delivered atomic.Int64  // 交付给虚拟连接的载荷字节（接收侧用）
	ackedSegs atomic.Int64  // 对端累计确认的段数（取 ACK 里的 una；发送侧算"送达速率"）
	rxAckSegs atomic.Int64  // 收到的 ACK 段总数
	maxUna    atomic.Uint32 // 见过的最大累计确认 SN

	mu     sync.Mutex
	seen   map[uint32]struct{}  // 已发过的 SN（判重传）
	sent   map[uint32]time.Time // SN -> 首次发送时间（算 RTT）
	maxSN  uint32               // 已见过的最大 SN
	srtt   time.Duration        // 平滑 RTT（1/8 律）
	minRTT time.Duration        // 观测到的最小 RTT（原始样本最小值，仅诊断）
}

func newTrunkStats() *trunkStats {
	return &trunkStats{
		seen: make(map[uint32]struct{}, 1024),
		sent: make(map[uint32]time.Time, 1024),
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

// recordRecvPacket 解析收到的 KCP 数据（可能是一个 flush 缓冲里的多个段），
// 抽出 ACK 段用于 RTT 估计与"已确认"计数。
func (s *trunkStats) recordRecvPacket(pkt []byte, now time.Time) {
	for off := 0; off+kcpHeaderSize <= len(pkt); {
		seg := pkt[off:]
		payloadLen := int(binary.LittleEndian.Uint32(seg[20:24]))
		if payloadLen < 0 || off+kcpHeaderSize+payloadLen > len(pkt) {
			return
		}
		if seg[4] == kcpCmdAck {
			s.rxAckSegs.Add(1)
			s.recordAck(binary.LittleEndian.Uint32(seg[12:16]), binary.LittleEndian.Uint32(seg[16:20]), now)
		}
		off += kcpHeaderSize + payloadLen
	}
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
	AutoWnd    bool          // 是否处于自动调窗
	Backlog    int           // 发送队列积压（段；kcp.WaitSnd，含未发出的）
	Segs       int64         // 已发出 KCP 段总数
	Retrans    int64         // 重传段总数
	RetransPct float64       // 重传占比（%）
	WireBytes  int64         // 已写往物理连接的字节
	Delivered  int64         // 已交付给虚拟连接的载荷字节
	SRTT       time.Duration // 平滑 RTT（原始样本最小值见 MinRTT）
	MinRTT     time.Duration // 最小 RTT（传播时延，仅诊断）
	Amp        float64       // 线上放大率（线上字节/已确认载荷）的平滑值
	AmpHigh    int           // 放大率连续超标的控制周期数
	Push       int64         // 发出的 PUSH 段总数
	Acked      int64         // 被对端确认的 PUSH 段总数
	RxAck      int64         // 收到的 ACK 段总数
}

// Stats 返回当前统计快照（可与上一次快照相减得到区间速率）。
func (t *TrunkKCP) Stats() Stats {
	segs, retrans := t.stats.segs.Load(), t.stats.retrans.Load()
	srtt, minRTT := t.stats.rtts()
	st := Stats{
		Segs:      segs,
		Retrans:   retrans,
		WireBytes: t.stats.wireBytes.Load(),
		Delivered: t.stats.delivered.Load(),
		SRTT:      srtt,
		MinRTT:    minRTT,
		AutoWnd:   t.autoWnd,
		Push:      t.stats.push.Load(),
		Acked:     t.stats.ackedSegs.Load(),
		RxAck:     t.stats.rxAckSegs.Load(),
	}
	if segs > 0 {
		st.RetransPct = 100 * float64(retrans) / float64(segs)
	}
	t.ctrlMu.Lock()
	st.SndWnd, st.RcvWnd = t.sndWnd, t.rcvWnd
	st.Amp, st.AmpHigh = t.ampEWMA, t.ampHighTicks
	t.ctrlMu.Unlock()
	t.kcpLock.Lock()
	st.Backlog = t.kcp.WaitSnd()
	t.kcpLock.Unlock()
	return st
}

// SetAutoWindow 打开发送窗口自动调节：初始从小往大爬，仅在"RTT 被排队抬高"时收缩，
// 上限 maxWnd 段（<=0 用默认 1024）；接收窗口 rcvWnd 段（<=0 用默认 1024，只做缓冲，
// 不参与调节）。默认即开启，见 NewTrunkKCP。
func (t *TrunkKCP) SetAutoWindow(maxWnd, rcvWnd int) {
	if maxWnd <= 0 {
		maxWnd = autoWindowDefaultMax
	}
	if rcvWnd <= 0 {
		rcvWnd = autoWindowDefaultRcv
	}
	t.ctrlMu.Lock()
	t.autoWnd = true
	t.wndMax = maxWnd
	t.rcvWnd = rcvWnd
	t.sndWnd = autoWindowStartWnd
	if t.sndWnd > maxWnd {
		t.sndWnd = maxWnd
	}
	if t.sndWnd < autoWindowMinWnd {
		t.sndWnd = autoWindowMinWnd
	}
	snd := t.sndWnd
	t.ctrlMu.Unlock()

	t.kcpLock.Lock()
	t.kcp.WndSize(snd, rcvWnd)
	t.kcpLock.Unlock()
}

// autoWindowStep 每 autoWindowTick 调一次：对"线上放大率"做简单 AIMD 调窗。
//
//	amp = 线上字节 / 被对端 ACK 确认的载荷字节 ≈ 1 + 重传占比
//	amp ≤ target（默认 1.5）→ 窗口 ×1.1（每 100ms，约 1s 翻倍）
//	amp > target            → 窗口 ×0.9
//
// 平衡点就是"刚好把链路打满、几乎不重传"的位置。选这个判据的原因：
//   - RTT 不可靠：发送端一次 flush 把整个窗口突发出去，突发自身的串行化时间会被算进
//     后几个包的 RTT（与排队无关），窗口越大越虚高，会把窗口一路估小（真机因此饿死）；
//   - "历史最高速率"不可靠：一旦收缩就再也涨不回来；
//   - 放大率是尺度无关的，40ms 与 150ms 链路都会落到各自的 BDP+队列附近；
//   - 随机丢包的链路 amp 下限本来就高（如 2% 丢包 ≈1.05，20% ≈1.25），只要 ≤1.5 就
//     继续放大窗口，不会被误判成拥塞而缩到饿死。
//
// 只在"应用有数据等着发"（backlog 足够）时调窗——应用自己慢的时候不能怪窗口。
func (t *TrunkKCP) autoWindowStep(ctx context.Context, now time.Time) {
	t.ctrlMu.Lock()
	if !t.autoWnd {
		t.ctrlMu.Unlock()
		return
	}
	dt := autoWindowTick
	if !t.lastCtrlAt.IsZero() {
		if now.Sub(t.lastCtrlAt) < autoWindowTick {
			t.ctrlMu.Unlock()
			return
		}
		dt = now.Sub(t.lastCtrlAt)
	}
	t.lastCtrlAt = now
	snd, rcv, maxW := t.sndWnd, t.rcvWnd, t.wndMax
	t.ctrlMu.Unlock()

	// 发送侧没有"交付给本端应用"的字节，只能用**被对端 ACK 确认的段数**当作
	// "线上真正送达"的量。
	wire, acked := t.stats.wireBytes.Load(), t.stats.ackedSegs.Load()
	payload := t.payloadSize()
	_ = dt

	// ACK 是随对端 flush 批量到达的，逐周期采样会出现"本周期 0 个 ACK"的空档，
	// 使放大率瞬间变成无穷大。这里用**1 秒滑动窗口的累计比**做估计。
	t.ampHistWire[t.ampHistIdx] = wire
	t.ampHistAcked[t.ampHistIdx] = acked
	t.ampHistIdx = (t.ampHistIdx + 1) % autoWindowAmpSpan
	if t.ampHistCnt < autoWindowAmpSpan {
		t.ampHistCnt++
	}
	amp, ampOK := 0.0, false
	if t.ampHistCnt >= autoWindowAmpSpan {
		dW := float64(wire - t.ampHistWire[t.ampHistIdx])
		dA := float64(acked-t.ampHistAcked[t.ampHistIdx]) * float64(payload)
		if dW >= 8*float64(payload) && dA > 0 {
			amp, ampOK = dW/dA, true
		}
	}

	t.kcpLock.Lock()
	backlog := t.kcp.WaitSnd()
	t.kcpLock.Unlock()
	if backlog < snd/2 {
		return // 应用没有积压：不是窗口限速，保持窗口不动
	}

	newWnd, changed := 0, false
	if ampOK {
		if t.ampEWMA == 0 {
			t.ampEWMA = amp
		} else {
			t.ampEWMA = t.ampEWMA*3/4 + amp/4
		}
		if t.ampEWMA > autoWindowAmpTarget {
			t.ampHighTicks++
		} else {
			t.ampHighTicks = 0 // 一旦回到目标以下立即解除"超标"状态，避免长时间停滞
		}
		switch {
		case t.ampHighTicks >= 2 && snd > autoWindowMinWnd:
			newWnd, changed = snd*3/4, true // 超标：乘性减
		case t.ampHighTicks == 0 && snd < maxW:
			newWnd, changed = snd+autoWindowAdditiveStep, true // 正常：加性增（避免冲过头）
		}
	}

	if changed {
		if newWnd < autoWindowMinWnd {
			newWnd = autoWindowMinWnd
		}
		if newWnd > maxW {
			newWnd = maxW
		}
		if newWnd != snd {
			t.ctrlMu.Lock()
			t.sndWnd = newWnd
			t.ctrlMu.Unlock()
			t.kcpLock.Lock()
			t.kcp.WndSize(newWnd, rcv)
			t.kcpLock.Unlock()
			snd = newWnd
		}
	}
}

// statsTick 周期性打印线路自诊断（debug 级，每 autoWindowLogEvery 一条，与是否
// 自动调窗无关）：线上/已确认速率、放大率、重传率、RTT、当前窗口与应用积压。
// 判读：放大≈1 且线上≈链路 = 窗口合适；放大 >1.5 = 窗口超过 BDP+队列（调小）；
// 线上远低于链路且放大≈1 = 窗口太小（调大）。固定窗口模式下也照常打印。
func (t *TrunkKCP) statsTick(ctx context.Context, now time.Time) {
	t.ctrlMu.Lock()
	needLog := now.Sub(t.lastLogAt) >= autoWindowLogEvery
	if needLog {
		t.lastLogAt = now
	}
	t.ctrlMu.Unlock()
	if !needLog {
		return
	}
	wire, acked := t.stats.wireBytes.Load(), t.stats.ackedSegs.Load()
	payload := t.payloadSize()
	t.ctrlMu.Lock()
	snd, rcv := t.sndWnd, t.rcvWnd
	t.ctrlMu.Unlock()
	// 用最近一条日志的计数做区间速率（固定窗口模式下也用它，而不是控制周期增量）
	lastW, lastA := t.lastLogWire, t.lastLogAcked
	t.lastLogWire, t.lastLogAcked = wire, acked
	elapsed := autoWindowLogEvery.Seconds()
	wireRate := float64(wire-lastW) / elapsed
	ackedRate := float64(acked-lastA) * float64(payload) / elapsed
	amp := 0.0
	if ackedRate > 0 {
		amp = wireRate / ackedRate
	}
	t.kcpLock.Lock()
	backlog := t.kcp.WaitSnd()
	t.kcpLock.Unlock()
	log.Ctx(ctx).Debug().
		Msgf("trunk_kcp: 窗口 snd=%d rcv=%d(应用积压 %d 段) 线上 %.0f KB/s 已确认 %.0f KB/s 放大 %.2f 重传 %.1f%% RTT srtt=%.0fms min=%.0fms",
			snd, rcv, backlog, wireRate/1024, ackedRate/1024, amp,
			t.stats.retransPct(), float64(t.stats.srttValue().Milliseconds()),
			float64(t.stats.minRTTValue().Milliseconds()))
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
