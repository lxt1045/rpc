package faux_tcp

import (
	"crypto/rand"
	"encoding/binary"
	"net/netip"
	"time"
)

// Config faux_tcp 配置
type Config struct {
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

	// KeepAlive 空闲保活间隔（默认 25s，防 NAT/防火墙表项老化；0 禁用）。
	// 保活包为标准 TCP keepalive 探测包形式（seq=sndNxt-1 的纯 ACK）；
	// 连续 3 个周期无任何入站报文判定对端死亡并关闭连接。
	KeepAlive time.Duration
	// HandshakeTimeout 握手单次超时（默认 1s）
	HandshakeTimeout time.Duration
	// HandshakeRetries 握手重试次数（默认 3；SYN 重试是正常 TCP 行为，DPI 可见）
	HandshakeRetries int
	// CloseGrace 挥手宽限时间（默认 500ms；超时直接关闭，不重传 FIN）
	CloseGrace time.Duration
	// HealDelay 空洞"虚拟重传愈合"等待时间（默认 200ms，约一个 RTT 量级）。
	// 丢包后接收端先按真实 Linux 行为回 dup ACK + SACK；若 HealDelay 内空洞未被
	// （不可能发生的）重传填补，则直接越过空洞推进累计确认——线上呈现
	// "丢包 → dup ACK/SACK → 快速重传恢复"的完整外观，避免 ack 永久冻结。
	HealDelay time.Duration

	// AdvMSS SYN/SYN+ACK 里**对外通告**的 MSS（零值 = 用 MSS）。
	//
	// MSS 是本端**发送**单段上限（受路径 MTU 约束，宁可保守），通告值只影响对端
	// 发给我们的大小，两者分开配置更清晰：路径 MTU 受限时把 MSS 压到 1240，
	// 通告值仍可保持 Linux 默认的 1460。接收侧不按通告值校验（读缓冲按最大段预留）。
	AdvMSS int

	// ReplySrc 可选：本端**所有出站报文的源 IP 统一用它**（零值=关闭，按报文目的
	// IP 作为源）。仅服务端有意义，用于"无状态 DNAT/端口映射"环境（Docker 桥接、
	// K8s NodePort 等）：平台只为内核跟踪的流做回程 SNAT，raw socket 发出的
	// SYN+ACK 会以容器私网源地址出去而被丢弃；把源地址直接写成对外服务地址即可
	// 绕过回程转换（等价于本端自己做了 SNAT）。
	//
	// **云主机 1:1 NAT/弹性公网 IP 场景（腾讯云 VPC/轻量、阿里云 ECS 等）不要填**：
	// 这类平台对虚拟网卡做出站源地址校验，源 IP 不是网卡地址的报文会被静默丢弃。
	// 典型现象：内核 TCP 监听同端口能通、faux_tcp 握手超时；服务端 tcpdump 里
	// SYN+ACK 的源地址是公网 IP 而非网卡内网地址，客户端一个包都收不到（且服务端
	// nf_conntrack 里没有该流）。此时留空即可：回包源地址取报文目的 IP（内网地址），
	// 与内核 TCP 完全同路，由平台 NAT 转成公网。
	ReplySrc netip.Addr

	// DebugPackets 收包调试模式：不挂内核 cBPF，改为用户态过滤并统计
	// （rx_total / rx_match / rx_dropped，会随握手失败信息一起打印）。
	// 用于区分"网卡侧一个包都没收到"与"收到了但被过滤掉"；生产勿开
	// （无关流量会全部进用户态）。
	DebugPackets bool

	// ManualFirewall 置 true 表示 RST 抑制规则由用户手工维护（README 有命令），
	// 本包不碰 iptables/nft；默认 false：Listen/Dial 自动安装、Close 自动卸载。
	ManualFirewall bool

	// PSK 预留字段（AEAD 封装/防注入，尚未实现）：当前版本置非空会返回配置错误，
	// 避免"以为加密了其实没有"的静默风险。
	PSK []byte
}

// delayed ACK 参数（仿 Linux：每 2 个数据包或 40ms 回一次 ACK）
const (
	ackEveryPackets = 2
	ackMaxDelay     = 40 * time.Millisecond
)

// 接收位图容量上限（空洞去重与 SACK 外观用，超出时丢弃新条目——不影响投递）
const bitmapCap = 4096

// 出站队列容量（报文数，约 1.4MB）：Write 在此排队等待发出（本地队列背压，
// 非线上限速）；控制段（ACK/FIN/探测）队列满时同步兜底直发，不丢。
const outQueueCap = 1024

// 接收队列容量（报文数，约 1.4MB）：上层来不及读时满则丢包（本层不背压，
// 可靠性由上层 KCP 负责）。容量与出站队列对齐，避免发送侧突发导致
// "非链路原因的"丢包/重传。
const recvQueueCap = 1024

func (c *Config) defaults() {
	if c.MSS <= 0 || c.MSS > 1448 {
		c.MSS = 1448
	}
	if c.AdvMSS <= 0 {
		c.AdvMSS = c.MSS
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
	if c.HealDelay <= 0 {
		c.HealDelay = 200 * time.Millisecond
	}
}

// validate 校验配置（defaults 之后调用）
func (c *Config) validate() error {
	if len(c.PSK) > 0 {
		return ErrInvalidConfig.New("PSK 为预留字段，当前版本未启用 AEAD 封装，请留空")
	}
	if c.AdvMSS > 65495 {
		return ErrInvalidConfig.Newf("AdvMSS 超出 IPv4 段上限: %d", c.AdvMSS)
	}
	return nil
}

// handshakeWindow 半开连接的最大存活时间（超时回收 SYN 扫描留下的半连接）
func (c *Config) handshakeWindow() time.Duration {
	return c.HandshakeTimeout * time.Duration(c.HandshakeRetries+1)
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
