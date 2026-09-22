//go:build !linux

package faux_tcp

// installRSTDrop 非 Linux 平台无需（也不支持）RST 抑制规则装拆
func installRSTDrop(port uint16) (func(), error) {
	return func() {}, nil
}
