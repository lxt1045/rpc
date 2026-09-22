//go:build !linux

package fake_tcp

import (
	"context"
	"net/netip"
)

// raw_packetio_other.go：非 Linux 平台桩。RawTCP 模式不可用（plan.md §4.8/§11），
// Listen/Dial 会自动降级为 ModeUDP。

func listenRawTCP(ctx context.Context, cfg Config) (*Listener, error) {
	return nil, ErrUnsupportedPlatform.New()
}

func dialRawTCP(ctx context.Context, cfg Config) (*Conn, error) {
	return nil, ErrUnsupportedPlatform.New()
}

var _ = netip.Addr{} // 占位避免后续实现时遗漏 netip
