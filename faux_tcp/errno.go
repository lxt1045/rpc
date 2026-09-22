package faux_tcp

import (
	"github.com/lxt1045/errors"
)

// 错误码统一在此定义（moduleCode+n 形式）。
// 501000 为本模块占用段（fake_tcp 占用 500000 段，codec/conn 等占用低位段，
// 实现前已确认仓库内无冲突）。
//
// 调用方用 errors.AsCode(err).Code() 与下列模板的 Code() 比较来分类；
// 例外：对端正常 FIN 且数据排空后 Read 返回标准 io.EOF（net.Conn 契约）。
const moduleCode = 501000

var (
	ErrUnsupportedPlatform = errors.NewCode(0, moduleCode+1, "fauxtcp: 当前平台不支持（仅 Linux）")
	ErrNeedRoot            = errors.NewCode(0, moduleCode+2, "fauxtcp: 需要 root 或 CAP_NET_RAW/CAP_NET_ADMIN 权限")
	ErrHandshakeTimeout    = errors.NewCode(0, moduleCode+3, "fauxtcp: 握手超时")
	ErrConnReset           = errors.NewCode(0, moduleCode+4, "fauxtcp: 连接被对端重置")
	ErrPeerTimeout         = errors.NewCode(0, moduleCode+5, "fauxtcp: 对端保活超时（判定死亡）")
	ErrConnClosed          = errors.NewCode(0, moduleCode+6, "fauxtcp: 连接已关闭")
	ErrReadTimeout         = errors.NewCode(0, moduleCode+7, "fauxtcp: 读超时")
	ErrPacketTooBig        = errors.NewCode(0, moduleCode+8, "fauxtcp: Write 超过 MSS（一次 Write = 一个 TCP 段，由上层分段）")
	ErrInvalidConfig       = errors.NewCode(0, moduleCode+9, "fauxtcp: 非法配置")
	ErrInvalidAddr         = errors.NewCode(0, moduleCode+10, "fauxtcp: 非法地址（需 IPv4 字面量）")
	ErrRawSocket           = errors.NewCode(0, moduleCode+11, "fauxtcp: 原始套接字操作失败")
	ErrFirewall            = errors.NewCode(0, moduleCode+12, "fauxtcp: RST 抑制规则安装失败")
	ErrInvalidPacket       = errors.NewCode(0, moduleCode+13, "fauxtcp: 非法报文")
	ErrWriteTimeout        = errors.NewCode(0, moduleCode+14, "fauxtcp: 写超时（出站队列持续满，本地背压）")
)
