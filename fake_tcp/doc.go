// Package fake_tcp 提供"伪装成 TCP 的 UDP 式传输"：线路上是完整的 TCP 报文
// （三次握手、选项协商、PSH+ACK 数据、FIN 四次挥手等特征齐全），
// 但**不做拥塞控制、丢包重传和滑动窗口限流**——丢包不回填、不降速、不背压，
// 可靠性由上层按需叠加（典型搭配：trunk_kcp 的 KCP）。
//
// 设计文档见同目录 plan.md。
//
// 两种模式（Config.Mode）：
//   - ModeRawTCP：主模式。应用层实现极简 TCP 状态机，AF_PACKET 收包、
//     raw IP socket(IP_HDRINCL) 发包，内核协议栈被完全旁路。
//     需要 Linux + root/CAP_NET_RAW（装 RST 抑制规则另需 CAP_NET_ADMIN）。
//     无权限时自动降级 ModeUDP 并打日志。
//   - ModeUDP：兜底模式。UDP + 私有头 [Magic][ConnID][Type]，线路上是 UDP，
//     运营商可能对其 QoS，但功能可用、无需特权。也可外挂 udp2raw 进程
//     完成伪装（plan.md M0 基线方案）。
//
// 注意：两种模式**不能在同一条连接内混用**（NAT/conntrack 按协议号分别建表），
// 但服务端可同时双栈监听，两类客户端各自成会话。
//
// 本层不保证顺序与可靠：接收侧按到达顺序上交、仅去重（RawTCP 模式按 §4.3 规则
// 维护 ack/SACK 外观）；需要可靠有序请在上层叠 KCP。
// Config.Ordered 为保留字段：无重传语义下"按序上交"无法填补 seq 空洞
// （空洞永远不会被回填），强行等待只会死锁，故当前版本不支持、返回配置错误。
package fake_tcp
