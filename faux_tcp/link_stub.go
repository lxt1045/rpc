//go:build !linux

package faux_tcp

import (
	"net/netip"
)

// newRawLink 非 Linux 平台暂不支持（接口已预留，Windows 可接 WinDivert）
func newRawLink(cfg Config, local netip.Addr, localPort uint16) (Link, error) {
	return nil, ErrUnsupportedPlatform.New()
}
