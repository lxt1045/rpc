package socks_kcp

import (
	"context"
	"io"
	"sync"
)

// relay 双向复制，任一端结束后关闭两端，避免 goroutine 泄漏。
func relay(ctx context.Context, left, right io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)

	cp := func(dst, src io.ReadWriteCloser) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		_ = dst.Close()
		_ = src.Close()
	}
	go cp(left, right)
	go cp(right, left)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		_ = left.Close()
		_ = right.Close()
	}
}
