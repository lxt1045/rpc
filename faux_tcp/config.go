package faux_tcp

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// Mode 传输模式
type Mode string

const (
	// ModeUDP 直连 UDP（对照/无权限回退）
	ModeUDP Mode = "udp"
	// ModeFakeTCP UDP 载荷伪装成 TCP 线上特征（无重传/拥塞控制/滑动窗口）
	ModeFakeTCP Mode = "fauxtcp"
)

// Config faux_tcp 配置
type Config struct {
	Mode Mode

	// MSS 单个 TCP 段的最大载荷（默认 1448：1500 MTU - 20 IP - 20 TCP - 12 TS 选项，
	// 与真实 Linux 一致且不分片）。faux_tcp 保留数据报边界：一次 Write = 一个 TCP 段，
	// 超过 MSS 的 Write 直接报错（由上层分段，如 KCP 的 MSS 1376）
	MSS int
	// Window 通告的接收窗口（固定大窗口，默认 65535，配合 WS=7）
	Window uint16
	// WScale 窗口扩大因子（默认 7）
	WScale uint8
	// TTL IPv4 TTL（默认 64，仿 Linux）
	TTL uint8

	// KeepAlive 空闲保活间隔（默认 25s，防 NAT/防火墙表项老化；0 禁用）
	KeepAlive time.Duration
	// HandshakeTimeout 握手单次超时（默认 1s）
	HandshakeTimeout time.Duration
	// HandshakeRetries 握手重试次数（默认 3；SYN 重试是正常 TCP 行为，DPI 可见）
	HandshakeRetries int
	// CloseGrace 挥手宽限时间（默认 500ms；超时直接关闭，不重传 FIN）
	CloseGrace time.Duration

	// PSK 可选预共享密钥：非空时对载荷做 AEAD 封装并防注入/防重放
	PSK []byte
}

func (c *Config) defaults() {
	if c.Mode == "" {
		c.Mode = ModeFakeTCP
	}
	if c.MSS <= 0 || c.MSS > 1448 {
		c.MSS = 1448
	}
	if c.Window == 0 {
		c.Window = 65535
	}
	if c.WScale == 0 {
		c.WScale = 7
	}
	if c.TTL == 0 {
		c.TTL = 64
	}
	if c.KeepAlive <= 0 {
		c.KeepAlive = 25 * time.Second
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = time.Second
	}
	if c.HandshakeRetries <= 0 {
		c.HandshakeRetries = 3
	}
	if c.CloseGrace <= 0 {
		c.CloseGrace = 500 * time.Millisecond
	}
}

// newISN 生成初始序号（RFC 6528：随机）
func newISN() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return binary.LittleEndian.Uint32(b[:])
}

// clockMS 返回单调毫秒时间戳（TSval 用）
func clockMS() uint32 {
	return uint32(time.Now().UnixNano() / int64(time.Millisecond))
}
