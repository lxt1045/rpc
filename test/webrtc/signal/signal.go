// Package signal 是 WebRTC 建连用的信令通道: 只中转 offer/answer/candidate,
// 不承载业务数据。数据流量在 ICE 打洞成功后直连, 不经过这里。
//
// 信令不走本项目的 rpc 框架, 原因有两条:
// 一是时序 —— 建连阶段还没有 DataChannel, 而 rpc 需要一条已就绪的 rwc 才能
// Conn, 硬用会形成循环依赖;
// 二是语义 —— 信令要在同一条连接上来回交换 offer/answer/多个 candidate,
// 而 Upgrade 一旦升级就把连接降级成裸管道, 无法再做多次请求响应。
package signal

// 消息类型。
const (
	TypeRegister  = "register"  // 客户端 -> 服务端: 注册自己的 id
	TypeOffer     = "offer"     // 主动侧 -> 被动侧: SDP offer
	TypeAnswer    = "answer"    // 被动侧 -> 主动侧: SDP answer
	TypeCandidate = "candidate" // 双向: ICE candidate
	TypeError     = "error"     // 服务端 -> 客户端: 目标不在线等错误
)

// Msg 是信令的单一信封, JSON 编码。
type Msg struct {
	Type string `json:"type"`
	From string `json:"from"`
	To   string `json:"to"`
	SDP  string `json:"sdp,omitempty"`
	Cand string `json:"cand,omitempty"`
	Err  string `json:"err,omitempty"`
}
