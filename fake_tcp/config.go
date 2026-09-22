package fake_tcp

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

const (
	defaultMTU              = 1400
	defaultWindow           = 64240 // 固定窗口通告值（有字段、无行为）
	defaultKeepalive        = 30 * time.Second
	defaultHandshakeRetries = 5
	handshakeRetryInterval  = time.Second
	defaultHealDelay        = 200 * time.Millisecond
	// 接收队列容量：本层不做线上背压，满了直接丢（可靠性由上层 KCP 保证）
	recvQueueCap = 1024
)

// Config 见 plan.md §7
type Config struct {
	LocalAddr  string // 服务端监听地址 "0.0.0.0:8443"；客户端本地地址（RST 抑制需要端口）
	RemoteAddr string // 客户端必填，对端地址

	Magic uint32 // payload 私有头魔数；0 时自动随机生成

	MTU int // 报文切片上限（含 IP/TCP 头），默认 1400
	// DatagramOnly 数据报模式：一次 Write = 一个 TCP 段，超过 MaxPayload 直接
	// 报错（由上层分段）。叠加 trunk_kcp 等"按长度流式重组"的上层时必须开启——
	// 本层不保序不重传，流式拆分后任一片段丢失都会让上层重组永久错位；
	// 整段投递使"丢包 = 丢整条报文"，与 UDP 语义一致，KCP 可正确重传。
	// 关闭时为流式 Write（内部按 MaxPayload 切片，适合裸用）。
	DatagramOnly     bool
	Keepalive        time.Duration // 空闲保活间隔，默认 30s
	HandshakeRetries int           // SYN 重试次数（每次间隔 1s），默认 5
	AutoFirewall     bool          // 自动装/卸内核 RST 抑制规则，默认 true
	RecvQueue        int           // 每会话接收队列容量（报文数），默认 1024；满则丢（本层不背压）
	// HealDelay 空洞"虚拟重传愈合"等待时间（默认 200ms，约一个 RTT 量级）。
	// 丢包后接收端先按真实 Linux 行为回 dup ACK + SACK；HealDelay 内空洞未被填补
	// （本层永不重传，故永远不会填补），则直接越过空洞推进累计确认——线上呈现
	// "丢包 → dup ACK/SACK → 快速重传恢复"的完整外观，避免 ack 永久冻结在空洞处
	// （长寿连接下那对状态跟踪型 DPI 是显著异常）。移植自 faux_tcp 的 healLocked。
	HealDelay time.Duration

	Ordered bool // 保留字段：当前版本不支持（见 doc.go），置 true 会返回配置错误
}

// setDefaults 填充默认值并校验
func (c *Config) setDefaults() error {
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
	if c.HealDelay <= 0 {
		c.HealDelay = defaultHealDelay
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
