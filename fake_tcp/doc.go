// Package fake_tcp 提供"伪装成 TCP 的 UDP 式传输"：线路上是完整的 TCP 报文
// （三次握手、选项协商、PSH+ACK 数据、FIN 四次挥手等特征齐全），
// 但**不做拥塞控制、丢包重传和滑动窗口限流**——丢包不回填、不降速、不做线上背压，
// 可靠性由上层按需叠加（典型搭配：trunk_kcp 的 KCP）。
//
// 设计文档见同目录 plan.md；与备选实现 faux_tcp 的评审对比与合并决议见 plan.md
// "v2 修订"一节。
//
// 实现形态（RawTCP）：应用层实现极简 TCP 状态机，AF_PACKET + cBPF 收包、
// raw IP socket(IPPROTO_RAW) 发包，内核协议栈被完全旁路。
// 需要 Linux + root/CAP_NET_RAW（装 RST 抑制规则另需 CAP_NET_ADMIN）。
// 无权限环境下请使用外挂 udp2raw 方案（deploy/udp2raw/README.md）——
// 本包**不含 UDP 兜底模式**（v2 起删除：同连接本就不能跨协议号互通，
// 两套会话语义的维护成本高于其价值，无权限场景由外部 udp2raw 覆盖）。
//
// 本层不保证顺序与可靠：接收侧按到达顺序上交、按位图去重（§4.3 规则维护
// ack/SACK 外观）；需要可靠有序请在上层叠 KCP。
// Config.Ordered 为保留字段：无重传语义下"按序上交"无法填补 seq 空洞
// （空洞永远不会被回填），强行等待只会死锁，故置 true 返回配置错误。
//
// 叠加 trunk_kcp 时请开启 Config.DatagramOnly（一次 Write = 一个 TCP 段，
// 超限报错），从构造上杜绝"半帧丢失导致流式重组永久错位"。
package fake_tcp
