package fake_tcp

import (
	"context"
	"time"
)

// keepalive.go：保活/超时扫描协程（plan.md §4.5、§6）。
// 每个 Listener/Dialer 一个协程，周期扫描所有 session。

// keepaliveLoop 周期执行 session.scanTick。
// sessions 为遍历回调（Listener 遍历会话表，Dialer 只有一条会话）。
// 周期同时受 Keepalive 与 HealDelay 约束（愈合触发精度 ~HealDelay）。
func keepaliveLoop(ctx context.Context, cfg Config, sessions func(f func(*session) bool)) {
	// 扫描周期：既不能太粗（保活间隔的 1/4），也不要超过 1s
	interval := cfg.Keepalive / 4
	if interval > time.Second {
		interval = time.Second
	}
	// 愈合检查依赖 scanTick：周期收紧到 HealDelay/2，保证 200ms 量级的触发精度
	if d := cfg.HealDelay / 2; d > 0 && d < interval {
		interval = d
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			sessions(func(s *session) bool {
				s.scanTick(ctx, now)
				return true
			})
		}
	}
}
