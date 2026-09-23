package trunk_kcp

import (
	"context"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// ratelimit_test.go：把"限速瓶颈 + 真实 RTT"固化成自动测试，回答两个问题：
//
//  1. 为什么 socks_faux_trunk_kcp 换掉 TCP 承载后吞吐没改善？——因为瓶颈在两者共用的
//     trunk_kcp KCP 窗口（库默认 1024 段 ≈ 8×BDP 时，限速出口队列被灌爆，带宽全变重传）。
//  2. 窗口到底该多大？——取决于 BDP = 速率×RTT，写死任何值都会踩坑：大了拥塞崩溃，
//     小了把发送端饿死（真机实测 128 段 → 只有 7Mbps 占用、300kB/s 下载）。
//     本文件把"限速"与"有损"两种链路的窗口规律分别固化下来。
//
// 指标：goodput（接收端有效字节/时间）、放大率（发送端推入瓶颈的字节/有效字节，
// 等价于真机上"网卡 30Mbps vs 下载 1MB/s"）、重传率（重复 SN 的 PUSH 段占比）、
// 瓶颈利用率（通过瓶颈的字节 / 理论限速）。

// bucketStats 瓶颈（整形器）统计。offered 是"进入瓶颈前的字节"，也就是真机上
// Task Manager 看到的网卡发送量；passed 是通过瓶颈的量（≈ 限速值）。
type bucketStats struct {
	offered   atomic.Int64
	passed    atomic.Int64
	dropped   atomic.Int64
	drops     atomic.Int64
	forwarded atomic.Int64
}

func (b *bucketStats) snapshot() (offered, passed, dropped, drops, forwarded int64) {
	return b.offered.Load(), b.passed.Load(), b.dropped.Load(), b.drops.Load(), b.forwarded.Load()
}

// linkShaper 模拟"共享限速瓶颈"：速率 rate B/s、排队上限 queueDelay（字节上限 =
// rate×queueDelay），队列满即丢（drop-tail）。**所有物理连接共享同一个桶**——真机上
// 物理连接复用同一条出口，瓶颈是共享的。
type linkShaper struct {
	rate    float64
	burst   float64
	lossPct int // 随机丢包率（%）：模拟有损链路（与"限速队列溢出"是两种不同的丢包）
	st      *bucketStats
	ch      chan shaperPkt
}

type shaperPkt struct {
	dst net.Conn
	b   []byte
}

func newLinkShaper(rate float64, queueDelay time.Duration, lossPct int, st *bucketStats) *linkShaper {
	burst := math.Max(rate*queueDelay.Seconds(), 1500)
	slots := int(burst/1200) + 16
	if slots < 8 {
		slots = 8
	}
	s := &linkShaper{rate: rate, burst: burst, lossPct: lossPct, st: st, ch: make(chan shaperPkt, slots)}
	go s.drain()
	return s
}

// drain 令牌桶发送：令牌不足就等（丢包由入队侧负责），保证长期速率 = rate。
func (s *linkShaper) drain() {
	tokens := s.burst
	last := time.Now()
	for p := range s.ch {
		need := float64(len(p.b))
		now := time.Now()
		tokens = math.Min(s.burst, tokens+s.rate*now.Sub(last).Seconds())
		last = now
		if tokens < need {
			if d := time.Duration((need - tokens) / s.rate * float64(time.Second)); d > 0 {
				time.Sleep(d)
			}
			now = time.Now()
			tokens = math.Min(s.burst, tokens+s.rate*now.Sub(last).Seconds())
			last = now
		}
		tokens -= need
		n, err := p.dst.Write(p.b)
		if err != nil {
			return
		}
		s.st.passed.Add(int64(n))
		s.st.forwarded.Add(1)
	}
}

// forward 把 src 上读到的包计数为 offered 后投入共享队列；队列满则丢弃。
func (s *linkShaper) forward(dst, src net.Conn) {
	buf := make([]byte, 64*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			s.st.offered.Add(int64(n))
			if s.lossPct > 0 && mrand.Intn(100) < s.lossPct {
				s.st.dropped.Add(int64(n)) // 路径随机丢包（不是队列溢出）
				s.st.drops.Add(1)
				continue
			}
			select {
			case s.ch <- shaperPkt{dst: dst, b: append([]byte(nil), buf[:n]...)}:
			default:
				s.st.dropped.Add(int64(n))
				s.st.drops.Add(1)
			}
		}
		if err != nil {
			return
		}
	}
}

// delayCopy 固定单向延迟转发（FIFO，出发时间 = max(到达时间+d, 上一包出发时间)）。
//
// 注意必须用**到达时间**而不是"处理时间"去排程：早先的版本在发送循环里取
// time.Now().Add(d)，于是每发一包都要再等 d，整段突发被压成"1 包 / d"的速率上限
// （单连接 ACK 只有 ~37 包/s），把接收方向的确认通道变成了人为瓶颈。
func delayCopy(dst, src net.Conn, d time.Duration) {
	if d <= 0 {
		_, _ = io.Copy(dst, src)
		return
	}
	type pkt struct {
		b  []byte
		at time.Time
	}
	ch := make(chan pkt, 1<<14)
	go func() {
		next := time.Now()
		for p := range ch {
			due := p.at.Add(d)
			if due.Before(next) {
				due = next
			}
			next = due
			if w := time.Until(due); w > 0 {
				time.Sleep(w)
			}
			if _, err := dst.Write(p.b); err != nil {
				return
			}
		}
	}()
	buf := make([]byte, 64*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			at := time.Now()
			select {
			case ch <- pkt{b: append([]byte(nil), buf[:n]...), at: at}:
			default:
			}
		}
		if err != nil {
			close(ch)
			return
		}
	}
}

type linkMetrics struct {
	goodputMBps float64       // 有效吞吐
	amp         float64       // 放大率 = 推入瓶颈的字节 / 有效字节
	retransPct  float64       // 发送侧重复 SN 段占比（用库自身统计）
	dropPct     float64       // 被瓶颈丢弃的段占比
	linkUsePct  float64       // 瓶颈利用率（passed / 理论限速）
	srtt        time.Duration // 平滑 RTT
	minRtt      time.Duration // 最小 RTT（仅诊断）
	// 下面几项是"浪费发生在哪"的判据，见 trunk_kcp/stats.go 的日志判读
	inFlight   int64   // 发送侧在途（已发未确认）段数
	rxDupPct   float64 // 接收侧收到的重复段占比：重传的包是否真的穿过了链路
	rxReordPct float64 // 接收侧收到的乱序段占比：多物理连接打乱 SN 顺序的程度
	rtoRetrans int64   // 区间内 RTO（超时）触发的重传段数
	fastRetran int64   // 区间内快速重传（重复 ACK 触发）段数
	earlyRetr  int64   // 区间内提前重传段数
}

// runShapedDownload 在"发送端推入瓶颈 → 瓶颈限速 → 接收端"的链路上跑 warmup+measure：
// warmup 用于让重传/窗口进入稳态，指标只统计后 measure 段。
func runShapedDownload(t *testing.T, label string, snd, rcv int,
	rateBps float64, queueDelay, rtt, warmup, measure time.Duration) linkMetrics {
	return runShapedDownloadND(t, label, snd, rcv, []int{1, 10, 32, 1},
		rateBps, queueDelay, rtt, warmup, measure, 4, 0)
}

func runShapedDownloadND(t *testing.T, label string, snd, rcv int, noDelay []int,
	rateBps float64, queueDelay, rtt, warmup, measure time.Duration, nConn int, lossPct int) linkMetrics {
	t.Helper()

	const conv = 0x5a0b0002
	cliTx, svcTx := newWireStats(), newWireStats()
	bs := &bucketStats{}
	shaper := newLinkShaper(rateBps, queueDelay, lossPct, bs)

	var cliRws, svcRws []io.ReadWriteCloser
	for range nConn {
		cliEnd, fwdA := net.Pipe()
		fwdB, svcEnd := net.Pipe()
		// 管道拓扑：svc 写入 svcEnd → 对端 fwdB 可读；cli 写入 cliEnd → 对端 fwdA 可读。
		// 下载方向 svc → cli = fwdB → 共享瓶颈（限速+排队+可选随机丢包）→ fwdA；
		// 反向 cli → svc（ACK）不过瓶颈，但加单向延迟，让发送端测到的 RTT 与真机相当。
		go shaper.forward(fwdA, fwdB)
		go delayCopy(fwdB, fwdA, rtt/2) //nolint // cli → svc：ACK/控制

		cliRws = append(cliRws, &countKCPConn{Conn: cliEnd, tx: cliTx})
		svcRws = append(svcRws, &countKCPConn{Conn: svcEnd, tx: svcTx})
	}

	cliTrunk := NewTrunkKCP(conv, nil, cliRws...)
	svcTrunk := NewTrunkKCP(conv, nil, svcRws...)
	for _, tr := range []*TrunkKCP{cliTrunk, svcTrunk} {
		tr.SetWindowSize(snd, rcv)
		tr.SetNoDelay(noDelay[0], noDelay[1], noDelay[2], noDelay[3])
	}
	ctx := context.Background()
	go cliTrunk.Run(ctx)
	go svcTrunk.Run(ctx)

	cliV := cliTrunk.GetConn(1)
	svcV := svcTrunk.GetConn(1)

	var received atomic.Int64
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		buf := make([]byte, 32*1024)
		for {
			n, err := cliV.Read(buf)
			if n > 0 {
				received.Add(int64(n))
			}
			if err != nil {
				return
			}
		}
	}()

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	payload := make([]byte, 8*1024)
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := svcV.Write(payload); err != nil {
				// KCP 发送队列满（窗口/队列打满）——真实现场就是阻塞在写，这里退让重试
				select {
				case <-stop:
					return
				case <-time.After(2 * time.Millisecond):
				}
			}
		}
	}()

	// 预热（让窗口/重传进入稳态），然后只统计 measure 段
	time.Sleep(warmup)
	start := time.Now()
	recv0 := received.Load()
	off0, passed0 := bs.offered.Load(), bs.passed.Load()
	drop0, fwd0 := bs.drops.Load(), bs.forwarded.Load()
	st0 := svcTrunk.Stats()
	cli0 := cliTrunk.Stats()

	// -v 下每 500ms 采样一次发送端状态，便于观察窗口/重传是否进入稳态
	if testing.Verbose() {
		for i := 0; i < int(measure/(500*time.Millisecond)); i++ {
			time.Sleep(500 * time.Millisecond)
			stx := svcTrunk.Stats()
			t.Logf("  [%s] t=%dms snd=%d 在途=%d 重传=%.1f%% 积压=%d",
				label, i*500, stx.SndWnd, stx.InFlight, stx.RetransPct, stx.Backlog)
		}
	} else {
		time.Sleep(measure)
	}
	elapsed := time.Since(start)
	useful := received.Load() - recv0
	offered := (bs.offered.Load() + cliTx.bytes.Load()) - (off0 + 0) // cliTx 全程未清零，这里只用瓶颈侧
	offered = bs.offered.Load() - off0
	passed := bs.passed.Load() - passed0
	drops := bs.drops.Load() - drop0
	fwd := bs.forwarded.Load() - fwd0
	st1 := svcTrunk.Stats()
	cli1 := cliTrunk.Stats()

	close(stop)
	_ = cliTrunk.Close()
	_ = svcTrunk.Close()
	<-writerDone
	<-readerDone

	m := linkMetrics{
		goodputMBps: float64(useful) / (1 << 20) / elapsed.Seconds(),
		amp:         float64(offered) / float64(max(useful, 1)),
		linkUsePct:  100 * float64(passed) / (rateBps * elapsed.Seconds()),
		dropPct:     100 * float64(drops) / float64(max(fwd+drops, 1)),
		srtt:        st1.SRTT,
		minRtt:      st1.MinRTT,
		inFlight:    st1.InFlight,
		rtoRetrans:  st1.RTORetrans - st0.RTORetrans,
		fastRetran:  st1.FastRetrans - st0.FastRetrans,
		earlyRetr:   st1.EarlyRetrans - st0.EarlyRetrans,
	}
	if segs := st1.Segs - st0.Segs; segs > 0 {
		m.retransPct = 100 * float64(st1.Retrans-st0.Retrans) / float64(segs)
	}
	if d := cli1.RxSegs - cli0.RxSegs; d > 0 {
		m.rxDupPct = 100 * float64(cli1.RxDup-cli0.RxDup) / float64(d)
		m.rxReordPct = 100 * float64(cli1.RxReorder-cli0.RxReorder) / float64(d)
	}
	t.Logf("%-24s goodput=%5.2f MB/s 放大=%5.2fx 重传=%5.1f%%(RTO %d/快 %d/提前 %d) 对端收[重复 %4.1f%% 乱序 %4.1f%%] 在途=%3d 瓶颈丢包=%5.1f%% 利用=%5.1f%%",
		label, m.goodputMBps, m.amp, m.retransPct, m.rtoRetrans, m.fastRetran, m.earlyRetr,
		m.rxDupPct, m.rxReordPct, m.inFlight, m.dropPct, m.linkUsePct)
	return m
}

// TestRateLimitedLinkKCP 固定窗口扫描：证明"窗口 ≫ BDP+队列 ⇒ 拥塞崩溃；窗口 < BDP
// ⇒ 浪费链路"，并给出 40ms RTT 链路上的最优点。接收窗口固定为大值（它是缓冲不是限速）。
func TestRateLimitedLinkKCP(t *testing.T) {
	const (
		rate       = 3.0 << 20 // ≈24 Mbps（真机 30 Mbps 的等价量级）
		queueDelay = 50 * time.Millisecond
		rtt        = 40 * time.Millisecond
		measure    = 2 * time.Second
	)
	type sc struct {
		label string
		snd   int
	}
	scenarios := []sc{
		{"snd1024 (库旧默认)", 1024},
		{"snd512", 512},
		{"snd256", 256},
		{"snd128 (≈BDP)", 128},
		{"snd64", 64},
		{"snd32", 32},
	}
	results := make(map[string]linkMetrics, len(scenarios))
	for _, s := range scenarios {
		results[s.label] = runShapedDownload(t, s.label, s.snd, 1024,
			rate, queueDelay, rtt, 500*time.Millisecond, measure)
	}

	old := results["snd1024 (库旧默认)"]
	bdp := results["snd128 (≈BDP)"]
	small := results["snd32"]

	if old.amp < 2.0 || old.retransPct < 30 {
		t.Errorf("未能复现大窗口的拥塞崩溃：放大=%.2fx 重传=%.1f%%（预期 放大>2x 且 重传>30%%）", old.amp, old.retransPct)
	}
	if bdp.amp > 1.5 {
		t.Errorf("BDP 窗口(128)仍有明显放大：%.2fx（预期 <1.5）", bdp.amp)
	}
	if bdp.linkUsePct < 90 {
		t.Errorf("BDP 窗口(128)没吃满链路：%.1f%%（预期 >90%%）", bdp.linkUsePct)
	}
	if small.linkUsePct > 70 {
		t.Errorf("snd=32 远小于 BDP，本应浪费链路，实测利用 %.1f%%（预期 <70%%）", small.linkUsePct)
	}
}

// TestWindowOvershootSpuriousRTO 用"限速瓶颈 + 真机 RTT"复现真机 A/B/C 三组实验，回答
// "为什么关掉拥塞控制后重传率能到 50%、而打开拥塞控制只有 1/4 吞吐"。
//
// 真机数据（socks_faux_trunk_kcp，RTT 54ms，服务端 30Mbps 网卡）：
//
//	A: snd=96 resend=32 nc=1 → 交付 250kB/s，线上 5Mbps，放大 2.0，重传 50%
//	B: snd=96 resend=32 nc=0 → 交付 180kB/s，放大 1.6，重传 43%
//	C: snd=96 resend=2  nc=0 → 交付  50kB/s，放大 0.97，重传 5%（干净但慢）
//
// 机理（读 kcp-go v5.4.20 的 flush/parse_input 得到）：
//  1. nc=1 时 snd_wnd 是唯一在途上限。snd=96 段 ≈ 132KB，而 512KB/s×54ms 的 BDP 只有
//     28KB：每轮 flush 把窗口整段推入瓶颈队列 → 队尾要等 200ms+ 才发出去，而 RTO 只有
//     ~100~150ms（srtt 尚未被排队抬高）→ 队尾被"超时"重传 → 重传又加剧排队 → 雪崩，
//     于是 50% 的线上字节都是重复段（真机日志里"重传 50%"就是这么来的）。
//  2. nc=0 时 kcp-go 的 cwnd 只有两档：任何 RTO 直接 cwnd=1；任何"提前重传"
//     （fastack>0 且本轮没有新段可发，窗口打满时每轮都会命中）走 rate-halving
//     cwnd=inflight/2+resend。resend=2 → cwnd 被钉在 4 段左右 → 只有 ~60KB/s；
//     resend=32 → cwnd≈inflight/2+32，靠"大 offset"撑着，但一丢包就归 1。**所以
//     kcp-go 的 cwnd 在有丢包/大 BDP 链路上不可用。**
//  3. 结论：保持 nc=1（不要 cwnd），把 snd 设成 ≈BDP，并把 resend 调小（2~3）让丢失段
//     靠快速重传恢复而不是等 RTO。这样在途 ≈ BDP+少量排队，既不饿死也不灌爆。
func TestWindowOvershootSpuriousRTO(t *testing.T) {
	const (
		rate       = 512 << 10 // 512KB/s ≈ 4Mbps，与真机"线上 5~6Mbps、交付 2.5Mbps"同量级
		queueDelay = 40 * time.Millisecond
		rtt        = 54 * time.Millisecond // 真机 ping RTT
		warmup     = 800 * time.Millisecond
		measure    = 2 * time.Second
	)
	// nodelay=0（minRTO=100ms）、interval=10、mtu=1400：与示例默认配置一致。
	// BDP = 512KB/s × 54ms ≈ 28KB ≈ 21 段（1376B 载荷）。
	type sc struct {
		label  string
		snd    int
		resend int
		nc     int
	}
	scenarios := []sc{
		{"snd8_窗口远小于BDP", 8, 2, 1},
		{"snd32_≈BDP_fast_nc1", 32, 2, 1},
		{"snd32_≈BDP_RTO_nc1", 32, 32, 1},
		{"snd96_≫BDP_RTO_nc1(=A)", 96, 32, 1},
		{"snd96_≫BDP_fast_nc1", 96, 2, 1},
		{"snd96_≫BDP_fast_nc0(=C)", 96, 2, 0},
	}
	res := make(map[string]linkMetrics, len(scenarios))
	for _, s := range scenarios {
		res[s.label] = runShapedDownloadND(t, s.label, s.snd, 1024,
			[]int{0, 10, s.resend, s.nc}, rate, queueDelay, rtt, warmup, measure, 4, 0)
	}
	bdp, bigRTO, cwnd := res["snd32_≈BDP_fast_nc1"],
		res["snd96_≫BDP_RTO_nc1(=A)"], res["snd96_≫BDP_fast_nc0(=C)"]

	// 复现 A：窗口 ≫ BDP 且 resend=32（快速重传形同关闭）→ 伪超时重传雪崩。
	// 判据用"放大率/重传率/队列丢包"而不是 goodput：本测试的瓶颈是硬限速的，灌爆队列后
	// passed 仍然是 rate，goodput 反而可能略高；真机上网卡/共享队列被灌爆才是代价。
	if bigRTO.retransPct < 25 || bigRTO.amp < 1.4 {
		t.Errorf("未能复现真机 A 的伪超时雪崩：重传=%.1f%% 放大=%.2fx（预期 重传>25%% 且 放大>1.4）",
			bigRTO.retransPct, bigRTO.amp)
	}
	// A 场景的浪费必须体现在"瓶颈队列溢出"上：丢弃率高，且收到的重复段远少于发出的
	// 重传段（重传的包自己也大多被丢了）——对应真机 NIC 上 5~6Mbps 的现象。
	if bigRTO.dropPct < 20 {
		t.Errorf("A 场景瓶颈丢包只有 %.1f%%：说明窗口并没有灌爆队列（预期 >20%%）", bigRTO.dropPct)
	}
	if bigRTO.rxDupPct >= bigRTO.retransPct*0.8 {
		t.Errorf("A 场景重传的包竟然大多穿过了链路（收重复=%.1f%% vs 重传=%.1f%%）："+
			"与'窗口灌爆队列、重传又加剧排队'的机理不符", bigRTO.rxDupPct, bigRTO.retransPct)
	}
	// 窗口 ≈ BDP + 快速重传：打满链路、队列零丢弃、放大率显著低于大窗口。
	if bdp.linkUsePct < 95 {
		t.Errorf("窗口≈BDP 没吃满链路：利用 %.1f%%（预期 >95%%）", bdp.linkUsePct)
	}
	if bdp.dropPct > 10 {
		t.Errorf("窗口≈BDP 竟然还在丢包：%.1f%%（预期 ≈0，队列未被灌爆）", bdp.dropPct)
	}
	if bdp.amp > 1.5 || bdp.amp >= bigRTO.amp {
		t.Errorf("窗口≈BDP 的放大率没有明显优于大窗口：%.2fx vs %.2fx（预期 <1.5 且低于大窗口）",
			bdp.amp, bigRTO.amp)
	}
	// 复现 C：nc=0 + resend=2 → kcp-go 的 rate-halving 把 cwnd 钉死（在途只剩几段），吞吐塌掉。
	if cwnd.goodputMBps > bdp.goodputMBps*0.8 {
		t.Errorf("未能复现 nc=0 的 cwnd 塌缩：goodput=%.2f MB/s 与 nc=1 的 %.2f MB/s 相当"+
			"（预期明显更低）", cwnd.goodputMBps, bdp.goodputMBps)
	}
	if cwnd.inFlight > 16 {
		t.Errorf("nc=0 时在途仍有 %d 段：kcp-go 的 cwnd 本应被 rate-halving 钉在个位数", cwnd.inFlight)
	}
	t.Logf("结论：nc=1 + snd≈BDP 把放大率从 %.2fx 压到 %.2fx、队列丢包从 %.1f%% 压到 %.1f%%"+
		"（限速瓶颈下 goodput 相近；真机受益在'不再把网卡带宽变成重传'）；"+
		"nc=0 会被 kcp-go 的 cwnd 钉死（在途 %d 段、goodput %.2f MB/s）",
		bigRTO.amp, bdp.amp, bigRTO.dropPct, bdp.dropPct, cwnd.inFlight, cwnd.goodputMBps)
}

// TestTrunkConnCountReorderingCausesRetransmit 隔离"多物理连接乱序"对重传的贡献。
//
// snd=32(≈BDP)、nc=1、resend=32 时瓶颈零丢弃，但重传率仍有 ~15%：因为一条 trunk 的
// KCP 段被**多个 sendLoop 竞争同一个 sendChan**（谁抢到谁发）乱序写到不同物理连接，
// 接收端 4 个 recvLoop 也按调度顺序喂进 recvChan，SN 顺序被打乱，KCP 的
// "early retransmit"（fastack>0 且本轮窗口已满）把乱序当丢包，反复重传已经到达的段。
// 这些重复段会**真的穿过链路**（rxDup ≈ 重传率），直接吃掉可用带宽。
//
// 本测试固定窗口、只改物理连接数：连接越多，乱序越严重、被浪费的带宽越多。
func TestTrunkConnCountReorderingCausesRetransmit(t *testing.T) {
	const (
		rate       = 512 << 10
		queueDelay = 40 * time.Millisecond
		rtt        = 54 * time.Millisecond
		warmup     = 800 * time.Millisecond
		measure    = 2 * time.Second
	)
	res := make(map[int]linkMetrics)
	for _, n := range []int{1, 2, 4, 8} {
		label := fmt.Sprintf("snd32_nc1_RTO_conn%d", n)
		res[n] = runShapedDownloadND(t, label, 32, 1024, []int{0, 10, 32, 1},
			rate, queueDelay, rtt, warmup, measure, n, 0)
	}
	one, many := res[1], res[4]
	if one.dropPct > 5 || many.dropPct > 5 {
		t.Fatalf("瓶颈本不应丢包：conn1=%.1f%% conn4=%.1f%%", one.dropPct, many.dropPct)
	}
	// 单连接：KCP 输入完全按序，实测零重传。
	if one.rxReordPct > 2 || one.retransPct > 2 {
		t.Errorf("单物理连接竟然出现乱序/重传（乱序=%.1f%% 重传=%.1f%%）：零丢包链路上不该有",
			one.rxReordPct, one.retransPct)
	}
	// 多连接：SN 顺序被打乱，kcp-go 的 early retransmit 把乱序误判成丢包。
	if many.rxReordPct < 5 {
		t.Errorf("4 条物理连接只观测到 %.1f%% 乱序：多连接乱序假说被证伪", many.rxReordPct)
	}
	if many.retransPct <= one.retransPct {
		t.Errorf("连接数从 1 增到 4 后重传率没有上升（%.1f%% → %.1f%%）：乱序假说被证伪",
			one.retransPct, many.retransPct)
	}
	// 这些被误判重传的段**真的穿过了链路**（重复率≈重传率），直接吃掉可用带宽。
	if many.rxDupPct < many.retransPct*0.7 {
		t.Errorf("4 连接的重传大多没到对端（收重复=%.1f%% vs 重传=%.1f%%）：与'乱序伪重传会占用带宽'不符",
			many.rxDupPct, many.retransPct)
	}
	if many.goodputMBps >= one.goodputMBps {
		t.Errorf("4 连接 goodput=%.2f 不低于 1 连接 %.2f：乱序浪费没有体现到吞吐上",
			many.goodputMBps, one.goodputMBps)
	}
	t.Logf("物理连接数对重传的影响：conn1 乱序=%.1f%% 重传=%.1f%% goodput=%.2f MB/s → "+
		"conn4 乱序=%.1f%% 重传=%.1f%% goodput=%.2f MB/s（乱序导致的伪重传直接吃掉带宽）",
		one.rxReordPct, one.retransPct, one.goodputMBps,
		many.rxReordPct, many.retransPct, many.goodputMBps)
}

// TestLossyPathTuning 有损链路（随机丢包）下的参数选择。
//
// 真机 2026-09-23 14:55 那次的判据：
//
//	服务端：在途32/32(窗口打满) 线上140~200KB/s 已确认60~85KB/s 放大2.3~2.7
//	        重传58~63%，其中 RTO 40~96、快 0、提前 190~242（即几乎全是重复 ACK 触发的）
//	客户端：收线≈交付≈65~75KB/s（应用侧不是瓶颈），收段重复≈0.5%
//	服务端网卡 1.6Mbps —— 包确实发出去了，只有 ~1/3 到达对端
//
// 结论：这条链路是**有损**（丢包 ~55%）而不是"限速"。两者的调参方向相反：
//   - 限速链路：在途 ≫ BDP 会把队列灌爆 → 窗口要按 BDP 设小（TestRateLimitedLinkKCP）；
//   - 有损链路：丢包与在途无关，吞吐 ≈ 在途 × (1-丢包) / 修复时延，窗口越大越好；
//     关键在于**修复要快**（丢了别等 RTO）与**在途要够**（撑住丢包造成的空洞）。
//
// 所以这里在 45% 随机丢包下扫 resend（快速重传阈值）、nodelay（minRTO）与窗口。
func TestLossyPathTuning(t *testing.T) {
	const (
		rate       = 8 << 20 // 足够大：瓶颈是丢包，不是限速
		queueDelay = 10 * time.Millisecond
		rtt        = 54 * time.Millisecond
		lossPct    = 45
		warmup     = 1 * time.Second
		measure    = 3 * time.Second
	)
	type sc struct {
		label       string
		snd, resend int
		nodelay     int
	}
	scenarios := []sc{
		{"snd32_resend32_minRTO100", 32, 32, 0},
		{"snd96_resend32_minRTO100", 96, 32, 0},
		{"snd96_resend2_minRTO100", 96, 2, 0},
		{"snd96_resend2_minRTO30", 96, 2, 1},
		{"snd192_resend2_minRTO30", 192, 2, 1},
	}
	res := make(map[string]linkMetrics, len(scenarios))
	for _, s := range scenarios {
		res[s.label] = runShapedDownloadND(t, s.label, s.snd, 1024,
			[]int{s.nodelay, 10, s.resend, 1}, rate, queueDelay, rtt, warmup, measure, 4, lossPct)
	}
	rtoOnly := res["snd96_resend32_minRTO100"]
	small, big := res["snd32_resend32_minRTO100"], res["snd192_resend2_minRTO30"]

	// **唯一稳健的结论**：有损链路上吞吐随在途（窗口）单调上升——这是与"限速链路"
	// 完全相反的方向，也是最容易再踩一次的坑，所以只对这一条做断言。
	// （45% 随机丢包下 goodput 抖动很大：同一配置重跑可以差 1.5~2 倍，
	//  resend/minRTO 几项只作观察记录，不作断言，避免 CI 抖动。）
	if big.goodputMBps <= small.goodputMBps*2 {
		t.Errorf("有损链路上窗口 192 未明显优于 32：%.2f vs %.2f MB/s（预期 >2 倍）",
			big.goodputMBps, small.goodputMBps)
	}
	if rtoOnly.goodputMBps <= small.goodputMBps*1.5 {
		t.Errorf("有损链路上窗口 96 未明显优于 32：%.2f vs %.2f MB/s（预期 >1.5 倍）",
			rtoOnly.goodputMBps, small.goodputMBps)
	}
	// 观察项（不作断言）：resend 与 minRTO 的收益远小于窗口，且抖动大。
	t.Logf("观察：resend/minRTO 的收益远小于窗口——snd96 上 resend32/minRTO100=%.2f、resend2=%.2f、resend2+minRTO30=%.2f MB/s",
		rtoOnly.goodputMBps, res["snd96_resend2_minRTO100"].goodputMBps, res["snd96_resend2_minRTO30"].goodputMBps)
	t.Logf("45%% 随机丢包下：snd32=%.2f → snd96=%.2f → snd192=%.2f MB/s（放大都≈%.1fx = 1/(1-丢包率)）",
		small.goodputMBps, rtoOnly.goodputMBps, big.goodputMBps, big.amp)
}

// TestStatsAckAccounting 校验线路统计本身：每个发出去的 PUSH 段都应恰好被 ACK 一次。
// 这条曾经出过错——KCP 一次 flush 会把多个段拼在一个缓冲里，只解析第一个段会把
// "已确认段数"算成实际的 1/N，放大率随之虚高几十倍（真机据此误判过窗口）。
func TestStatsAckAccounting(t *testing.T) {
	const (
		nConn = 2
		conv  = 0x5a0b0003
	)
	var cliRws, svcRws []io.ReadWriteCloser
	for range nConn {
		cliEnd, fwdA := net.Pipe()
		fwdB, svcEnd := net.Pipe()
		go io.Copy(fwdB, fwdA) //nolint
		go io.Copy(fwdA, fwdB) //nolint
		cliRws = append(cliRws, cliEnd)
		svcRws = append(svcRws, svcEnd)
	}
	cliTrunk := NewTrunkKCP(conv, nil, cliRws...)
	svcTrunk := NewTrunkKCP(conv, nil, svcRws...)
	ctx := context.Background()
	go cliTrunk.Run(ctx)
	go svcTrunk.Run(ctx)

	cliV := cliTrunk.GetConn(1)
	svcV := svcTrunk.GetConn(1)

	const total = 512 << 10
	got := make(chan int, 1)
	go func() {
		n := 0
		buf := make([]byte, 32*1024)
		for n < total {
			c, err := cliV.Read(buf)
			n += c
			if err != nil {
				break
			}
		}
		got <- n
	}()

	payload := make([]byte, 8*1024)
	for sent := 0; sent < total; {
		n, err := svcV.Write(payload)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		sent += n
	}
	if n := <-got; n != total {
		t.Fatalf("received %d, want %d", n, total)
	}
	// 等 ACK 处理完（最后一批 ACK 可能在途）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := svcTrunk.Stats()
		if st.Push > 0 && st.Acked >= st.Push {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	st := svcTrunk.Stats()
	t.Logf("push=%d retrans=%d acked=%d rxAck=%d wire=%d segs=%d", st.Push, st.Retrans, st.Acked, st.RxAck, st.WireBytes, st.Segs)
	if st.Push == 0 {
		t.Fatal("no PUSH segment recorded")
	}
	// 注意：ACK 段数是合并的（实测 448 段只回 8 个 ACK），所以不能拿 rxAck 与 push 比，
	// 真正可比的是 ACK 里携带的累计确认 una（= St.Acked）。
	if st.RxAck == 0 {
		t.Error("一个 ACK 段都没解析到：收包路径的统计没接上")
	}
	if st.Acked < st.Push*95/100 {
		t.Errorf("已确认段(%d) 明显少于发出的 PUSH 段(%d)：看 sent 表维护有问题", st.Acked, st.Push)
	}
	// 放大率应接近 1（无重传、只有头部开销）
	amp := float64(st.WireBytes) / float64(st.Acked*1376)
	if amp > 1.2 {
		t.Errorf("无拥塞环回上放大率 %.2fx 偏高（预期 <1.2）", amp)
	}
	_ = cliTrunk.Close()
	_ = svcTrunk.Close()
}
