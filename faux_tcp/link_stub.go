//go:build !linux

package faux_tcp

import (
	"net/netip"

	"github.com/lxt1045/errors"
)

// newRawLink 非 Linux 平台暂不支持（接口已预留，Windows 可接 WinDivert）
func newRawLink(local netip.Addr) (Link, error) {
	return nil, errors.New("fauxtcp: only linux is supported (or use mode: udp)")
}
