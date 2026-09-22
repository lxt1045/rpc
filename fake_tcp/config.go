package fake_tcp

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// Mode 传输模式
type Mode int

const (
	// ModeRawTCP 主模式：线路上是真实 TCP 报文（Linux + CAP_NET_RAW）
	ModeRawTCP Mode = iota + 1
	// ModeUDP 兜底模式：UDP + 私有头（无需特权；线路上是 UDP，可能被运营商 QoS）
	ModeUDP
)

const (
	defaultMTU              = 1400
	defaultWindow           = 64240 // 固定窗口通告值（有字段、无行为）
	defaultKeepalive        = 30 * time.Second
	defaultHandshakeRetries = 5
	handshakeRetryInterval  = time.Second
	// 接收队列容量：本层不做背压，满了直接丢（可靠性由上层 KCP 保证）
	recvQueueCap = 1024
)

// Config 见 plan.md §7
type Config struct {
	Mode       Mode   // RawTCP / UDP
	LocalAddr  string // 服务端监听地址 "0.0.0.0:8443"；客户端本地地址（RawTCP 模式 RST 抑制需要端口）
	RemoteAddr string // 客户端必填，对端地址

	Magic uint32 // payload 私有头魔数；0 时自动随机生成

	MTU              int           // 报文切片上限（含 IP/TCP 头），默认 1400
	Ordered          bool          // 保留字段：当前版本不支持（见 doc.go），置 true 会返回配置错误
	Keepalive        time.Duration // 空闲保活间隔，默认 30s
	HandshakeRetries int           // SYN 重试次数（每次间隔 1s），默认 5
	AutoFirewall     bool          // RawTCP 模式自动装/卸内核 RST 抑制规则，默认 true
	RecvQueue        int           // 每会话接收队列容量（报文数），默认 1024；满则丢（本层不背压）
}

// setDefaults 填充默认值并校验
func (c *Config) setDefaults() error {
	if c.Mode == 0 {
		c.Mode = ModeRawTCP
	}
	if c.Mode != ModeRawTCP && c.Mode != ModeUDP {
		return ErrInvalidConfig.Newf("未知 Mode: %d", c.Mode)
	}
	if c.MTU <= 0 {
		c.MTU = defaultMTU
	}
	if c.MTU < 256 || c.MTU > 9000 {
		return ErrInvalidConfig.Newf("MTU %d 超出合理范围 [256,9000]", c.MTU)
	}
	if c.Ordered {
		return ErrInvalidConfig.New("Ordered 暂不支持：无重传语义下按序上交无法填补 seq 空洞")
	}
	if c.Keepalive <= 0 {
		c.Keepalive = defaultKeepalive
	}
	if c.HandshakeRetries <= 0 {
		c.HandshakeRetries = defaultHandshakeRetries
	}
	if c.RecvQueue <= 0 {
		c.RecvQueue = recvQueueCap
	}
	if c.Magic == 0 {
		var b [4]byte
		if _, err := rand.Read(b[:]); err == nil {
			c.Magic = binary.LittleEndian.Uint32(b[:])
		} else {
			c.Magic = uint32(time.Now().UnixNano())
		}
	}
	return nil
}
