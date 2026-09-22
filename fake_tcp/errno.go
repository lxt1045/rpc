package fake_tcp

import (
	"github.com/lxt1045/errors"
)

// 错误码统一在此定义（moduleCode+n 形式）。
// 500000 为本模块占用段，实现前已确认仓库内未被其它包使用。
const moduleCode = 500000

var (
	ErrUnsupportedPlatform = errors.NewCode(0, moduleCode+1, "fake_tcp: 当前平台不支持 RawTCP 模式（仅 Linux）")
	ErrNeedRoot            = errors.NewCode(0, moduleCode+2, "fake_tcp: RawTCP 模式需要 root 或 CAP_NET_RAW/CAP_NET_ADMIN 权限")
	ErrHandshakeTimeout    = errors.NewCode(0, moduleCode+3, "fake_tcp: 握手超时")
	ErrHandshakeRejected   = errors.NewCode(0, moduleCode+4, "fake_tcp: 握手被对端拒绝")
	ErrBadMagic            = errors.NewCode(0, moduleCode+5, "fake_tcp: Magic 校验失败")
	ErrConnClosed          = errors.NewCode(0, moduleCode+6, "fake_tcp: 连接已关闭")
	ErrConnReset           = errors.NewCode(0, moduleCode+7, "fake_tcp: 连接被对端重置")
	ErrInvalidPacket       = errors.NewCode(0, moduleCode+8, "fake_tcp: 非法报文")
	ErrInvalidConfig       = errors.NewCode(0, moduleCode+9, "fake_tcp: 非法配置")
	ErrTimeout             = errors.NewCode(0, moduleCode+10, "fake_tcp: 操作超时")
	ErrFirewall            = errors.NewCode(0, moduleCode+11, "fake_tcp: RST 抑制规则安装失败")
	ErrAddrRequired        = errors.NewCode(0, moduleCode+12, "fake_tcp: 缺少本地或对端地址")
	ErrPacketTooBig        = errors.NewCode(0, moduleCode+13, "fake_tcp: Write 超过 MaxPayload（数据报模式一次 Write = 一个 TCP 段）")
)
