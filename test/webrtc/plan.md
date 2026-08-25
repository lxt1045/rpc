# WebRTC 代理服务实现计划

参考 `test/socks/`，在 `test/webrtc/` 下实现基于 WebRTC DataChannel 的代理服务。
三方库使用 pion。

## 1. 目标

客户端在本地监听 SOCKS5，把流量经 WebRTC DataChannel 送到远端出口节点，
由出口节点代为访问目标站点。相对 `test/socks` 的 TLS/QUIC/KCP 传输，
WebRTC 的价值在于 ICE 打洞：两端都在 NAT 后面时仍可直连，
信令服务只在建连阶段参与，不承载数据流量。

明确要求：client 与 service 之间的通信协议用本项目
`github.com/lxt1045/rpc`，不自己另造一套线上格式，也不用 gRPC / HTTP。
至于用框架里哪一种调用形态（`Call` / `Stream` / `Upgrade`），
要求没有规定，由本计划自行选定并给出理由，见 2.8。
信令通道不在此要求范围内（走 WebSocket + JSON，理由见第 3 节末）。

## 2. 调研结论

下面每一条都已在本机核实过，是后续设计的依据。

### 2.1 pion 版本必须锁 v3

模块缓存里只有 `github.com/pion/webrtc/v3@v3.3.6` 带 `.zip`；
`webrtc/v4` 只有一个 `.info`，没有 zip，离线构建会失败。
配套依赖 `datachannel@v1.6.2`、`sctp@v1.11.1`、`ice/v4@v4.4.0`、
`dtls/v3@v3.1.5`、`transport/v4@v4.1.0` 均已缓存。

结论：用 `webrtc/v3@v3.3.6`，不要升 v4。

注意 `go.mod` 里目前**还没有** pion（只有 `gorilla/websocket v1.5.3`，
信令服务用它，不必新增）。步骤 3 首次 import pion 时要落一条
`require github.com/pion/webrtc/v3 v3.3.6`；离线环境下用
`GOFLAGS=-mod=mod go mod tidy` 从上述缓存补全，不要联网拉取，
否则会解析到 v4 而拿不到 zip。

### 2.2 核心矛盾：rpc 编解码是流式的，DataChannel 是消息式的

这是整个实现最容易踩的坑，必须先解决。

rpc 侧 —— `codec/codec.go:447` 的 `ReadPack`：

```go
n, err := io.ReadFull(r, buf[:2])       // 先读 2 字节长度前缀
lenNeed := ParseHeaderLen(buf[:2])
n, err = io.ReadFull(r, buf[2:lenNeed]) // 再读剩余部分
```

它假定 `r` 是字节流，可以先取 2 字节、再取剩下的。

WebRTC 侧 —— `DataChannel.Detach()` 返回的 `datachannel.ReadWriteCloser`，
其 `Read` 走 `stream.ReadSCTP`（`datachannel@v1.6.2/datachannel.go:208`）。
SCTP 是数据报语义：缓冲区装不下整条消息时返回 `io.ErrShortBuffer`，
**并丢弃该消息的剩余部分**（`sctp@v1.11.1/reassembly_queue.go:575`）。

所以把 Detach 出来的 DataChannel 直接传给 `Peer.Conn` 一定坏：
第一次 `io.ReadFull(r, buf[:2])` 会读到 2 字节长度前缀，
然后整帧的剩余内容被 SCTP 丢掉，连接立刻错乱。

结论：必须写一层流式适配（下称 `dcconn`），
内部按整条消息读入缓冲，再按字节数对外提供 `Read`。

### 2.3 帧长度上限是 65535，适配层缓冲取 64 KiB 足够

长度前缀是 uint16（`ParseHeaderLen` 读 `buf[:2]`），
所以 codec 写出的单帧不超过 65535 字节。
`dcconn` 的读缓冲按 64 KiB 分配即可容纳任意单帧，不会触发短缓冲丢弃。

### 2.4 `Peer.Conn` 接受任意 rwc，socks 代理逻辑可原样复用

`peer.go:95`：

```go
func (rpc *Peer) Conn(ctx context.Context, rwc io.ReadWriteCloser) (err error)
```

只要 `dcconn` 实现 `io.ReadWriteCloser`，
`test/socks` 里 `SocksSvc` / `SocksCli` 那套（含 `Conn` 流式方法）
不用改一行就能跑在 WebRTC 上。这是本方案最大的复用点。

### 2.5 protoc 可用

本机 `libprotoc 24.0-rc1`，`$GOPATH/bin` 下有
`protoc-gen-gogofast` / `protoc-gen-gogo` / `protoc-gen-go` 等插件，
信令 proto 可以正常生成。

### 2.6 必须在 SettingEngine 上开启 DetachDataChannels

`settingengine.go:108` 的 `DetachDataChannels()` 不开的话，
`DataChannel.Detach()` 返回 `ErrDetachNotEnabled`；
反之开了却不在 `OnOpen` 里调 `Detach()`，pion 会打警告
（`datachannel.go:205`）并继续走回调模式。两者要配对使用。

### 2.7 证书问题不影响本模块

`test/socks` 依赖 `config.LoadTLSConfig`，而仓库不含 `*.pem`。
WebRTC 的 DTLS 由 pion 自己生成自签证书，不用仓库里的 CA。
信令通道若走 WebSocket，本地联调用明文 `ws://` 即可绕开证书缺失。

### 2.8 选定 Upgrade 承载数据通道；它会独占整条 rpc 连接

要求只规定「用本项目的 rpc」，没有规定调用形态。
框架提供的两种候选在 `test/socks` 里都有现成先例：
`peer.Upgrade(ctx, "ConnUpgrade", req, resp)`（`test/socks/peer_client.go:286`）
与 `peer.Stream(ctx, "Conn")`（`test/socks/peer_client.go:606`）。
本计划选 `Upgrade`，对比见本节末。

先说清它的语义，因为这不是简单换个 API，它改变连接模型，
原因在 codec 的实现里：

- `Codec.Upgrade` 成功后把 `c.status` 置为 2（`codec/upgrade.go:63`）。
- 此后任何 `SendMsg` 只要 ver 不是 Upgrade 类，一律返回
  `ErrHasBeenUpgraded`（`codec/codec.go:659`）。
- `ReadLoop` 处理完 `VerUpgradeReq`/`VerUpgradeResp` 就直接 `return`，
  把连接交出去（`codec/codec.go:427`、`codec/codec.go:436`）。
- 之后 `Upgrade.Read`/`Write` 绕过所有帧格式，
  直接读写 `c.rwc`（`codec/upgrade.go:30-39`）。

也就是说升级之后这条 rpc 连接再也不能承载第二个调用，
它已经退化成一条裸字节管道。

结论：**一条 DataChannel 只能代理一个 TCP 连接**。
`Stream` 方案是「一条 DataChannel 复用 N 个代理连接」，
`Upgrade` 方案必须「每个代理连接开一条新 DataChannel」。
这对 WebRTC 恰好不难：一个 PeerConnection 下可以开很多 DataChannel
（SCTP stream，上限 65535），多路复用由 SCTP 自己做，
反而省掉了 rpc 层再复用一次的开销。

代价是每条 DataChannel 都要独立走一遍 rpc 握手：
`bindCodec` 里客户端会先 `getMethodsFromSvc` 拉 CallID 表
（`peer.go:135-141`），再发 `Upgrade` 请求，
所以每个代理连接建连需要 2 个 RTT。走的是打洞后的直连，可以接受。

选 `Upgrade` 不选 `Stream`，主要理由在 SCTP 这一层。
DataChannel 默认 `ordered: true`，一条通道就是一条有序 SCTP 流。
`Stream` 方案把 N 个代理连接压到同一条通道上，它们共享这条有序流：
某个连接在传大文件时，另一个连接的小请求只能排在它后面，
即队头阻塞。`Upgrade` 方案一连接一通道，各自独立的 SCTP 流、
独立的顺序与流控，天然没有这个问题。
这和 HTTP/2 over TCP 走到 HTTP/3 over QUIC 要解决的是同一件事。

次要好处是升级后收发裸字节，省掉每段数据的帧头，
也不必在 rpc 层重做一遍 SCTP 已经做好的多路复用。

代价两条，都已知且可控：每个代理连接 2 个 RTT（上面那条），
以及 2.9 的写序竞争需要一个 1 字节 ready 标记来规避。
若实现中发现这套时序难维持，退回 `Stream` 是允许的——
要求只说用本项目的 rpc，并没有绑定 `Upgrade`；
届时把队头阻塞记为已知限制即可。

### 2.9 服务端 Upgrade 响应与裸数据之间有写序竞争

这是照搬 `test/socks/peer_service.go` 会踩到的坑，实现时要避开。

`SvcInvoke` 是在 ReadLoop 里同步调用的（`codec/reply.go:25`），
handler 返回之后才发 `VerUpgradeResp`（`codec/reply.go:56-67`）。
而 `test/socks` 的 `ConnUpgrade` 在返回前就把两个方向的 `Copy`
goroutine 起了起来（`test/socks/peer_service.go:244-266`），
其中 `rc -> upgrade` 方向会直接往 `c.rwc` 写裸字节。

于是存在这样的窗口：目标站点先说话（SMTP/SSH/MySQL 这类
server-speaks-first 协议）时，裸载荷可能先于 `VerUpgradeResp`
落到线上，客户端 ReadLoop 会把它当成 rpc 帧解析，连接立刻错乱。
HTTP/HTTPS 经 SOCKS5 时客户端总是先发请求，所以现网跑得通，
但这只是被流量方向掩盖了，不是没有问题。

响应帧一定会发，不能靠「干脆不发响应」绕开：
置空 `respType` 的条件是**类型名以 `.Empty` 结尾**且没有任何带
protobuf tag 的字段（`method.go:114-125`），
而 `ConnUpgradeRsp` 两条都不满足——名字不是 `Empty`，
且有 `Err err = 1`。发送侧的 gate 就是
`if caller.RespType() == nil { return }`（`codec/reply.go:56`）。

已核对过底层：`Upgrade.Write` 直接写 `codec.rwc`
（`codec/upgrade.go:34-39`），没有任何等响应的 gate，
所以这个竞争确实存在，不是靠某处内部同步兜住的。

规避办法：客户端在 `Upgrade()` 返回后（此时它已经消费掉了
`VerUpgradeResp`）先往管道里写 1 字节 ready 标记，
服务端读到这个字节再启动 `rc -> upgrade` 方向的搬运。
每连接多 1 字节，换掉一整类难查的错帧问题。

读侧不用担心，两个理由：
ReadLoop 在处理完 Upgrade 帧的当轮就 `return` 了（见 2.8），
根本不会再进下一轮去抢 `c.rwc`，
每轮开头那个 `status != 0` 退出检查（`codec/codec.go:314-317`）
只是多一道保险；
而 `ReadPack` 按长度精确读取，没有 bufio 之类的预读缓冲，
所以升级瞬间不会有已读进内存却还没交给上层的字节残留。

另外 `test/tcp_upgrade/service/peer_service.go:189-236` 是本仓库里
更贴近本方案的参考：它把整个搬运逻辑放进 goroutine 后立刻 return，
让 handler 不阻塞 ReadLoop。这个形状要照抄，
但它同样没处理上面的写序问题（一上来就 `upgrade.Write`），
所以只借结构，不借时序。

## 3. 架构

三个角色，与 `test/socks` 的 client / service / proxy 三分对应：

```
  client (本地 SOCKS5 :1080)          signal (信令/rendezvous)         service (出口节点)
        |                                      |                              |
        |------- 注册/查询、交换 SDP+ICE ------>|<---- 注册、等待 offer -------|
        |                                      |                              |
        |============ 一个 PeerConnection（ICE 打洞，尽量直连）==============>|
        |                                                                     |
        |--- DataChannel #1 -> rpc 握手 -> ConnUpgrade -> 裸管道 -> 目标A ---->|
        |--- DataChannel #2 -> rpc 握手 -> ConnUpgrade -> 裸管道 -> 目标B ---->|
        |--- DataChannel #n ...                                          ----->|
```

- **signal**：只做 offer/answer/candidate 中转，不碰业务数据。
  客户端与出口节点各自用一条长连接挂在上面。
- **service**：出口节点。每收到一条 DataChannel 就包成 `dcconn`，
  起一个 `rpc.Peer` 跑 `SocksSvc`，在 `ConnUpgrade` 里连目标地址，
  然后把 upgrade 管道与目标连接双向对拷。
- **client**：本地起 SOCKS5 监听，发起 offer 建立 PeerConnection；
  每个入站 SOCKS5 连接新开一条 DataChannel，
  包 `dcconn` 交给 `rpc.Peer`，调
  `Upgrade(ctx, "ConnUpgrade", req, resp)` 换到裸管道，
  再与浏览器侧连接对拷。

client 与 service 之间**全部**用本项目的 rpc 承载：
每条 DataChannel 上先跑一次 rpc 握手，再发 `ConnUpgrade` 调用。
具体选 `Upgrade` 形态（而非 `Stream`）的理由见 2.8，
核心是避免多个代理连接共享一条有序 SCTP 流造成的队头阻塞。
代价是连接模型从「一条通道多路复用」变成「一连接一通道」。

信令交换是唯一的例外，走 WebSocket + JSON，不进 rpc 框架。
第一个理由是时序：建连阶段还没有 DataChannel，
而 rpc 需要一条已就绪的 rwc 才能 `Conn`，硬用会形成循环依赖。
第二个理由是语义：信令要在同一条连接上来回交换
offer / answer / 多个 candidate，
这正是 `Upgrade` 会摧毁的那种「多次请求响应」模式
（升级即降级为裸管道，见 2.8）。
即便换 `Call` 形态绕开第二点，第一点仍然拦着。

## 4. 目录规划

```
test/webrtc/
  plan.md              本文件
  dcconn/              DataChannel -> io.ReadWriteCloser 流式适配（核心）
    dcconn.go
    dcconn_test.go
  signal/              信令协议与内存版 rendezvous
    signal.go          消息结构（JSON）
    server.go          WebSocket 中转服务
    client.go          信令客户端（注册、收发、按对端 id 分派）
  peer/                PeerConnection 建连封装（offer/answer 两侧共用）
    peer.go            建连 + 按需新开 DataChannel
  service/main.go      出口节点
  client/main.go       本地 SOCKS5 入口
  signalsvr/main.go    信令服务
```

`pb` 直接复用 `test/socks/pb`，不再生成一份。
业务语义完全一致，复制 proto 只会带来两份要同步维护的定义。
选用 `Upgrade` 之后这一点更划算了：所需的
`ConnUpgradeReq` / `ConnUpgradeRsp` 与 `SocksSvc.ConnUpgrade`
在那份 proto 里已经有了（`test/socks/pb/service.proto:91-99`、`:126`），
连 proto 都不用改。

`SocksSvc` 有 4 个方法（`Close`/`Auth`/`Conn`/`ConnUpgrade`），
`SocksCli` 只有 1 个（`Close`），本方案只用到 `ConnUpgrade`；
其余方法在出口节点侧写成空壳返回即可满足接口。
注意 CallID 是方法表下标（见 CLAUDE.md 架构一节），
两端必须用同一份 pb，这也是复用 `test/socks/pb` 而非复制的另一个理由。

`ConnUpgradeReq` 还有个 `bytes body = 3` 字段（`:94`），
可以把 SOCKS5 握手之后紧跟的第一段客户端数据一起塞进升级请求，
省掉一次往返；服务端连上目标后先把 `body` 写过去再开始对拷。
（第 2.5 节确认 protoc 可用，是为了确实需要扩展时有退路，
当前设计下信令走 JSON，不需要动 proto。）

## 5. 实现步骤

按依赖顺序推进，每步都可独立验证。

### 步骤 1：dcconn —— 消息流适配层 ✅ 已完成

最关键的一步，先写先测。已实现于 `test/webrtc/dcconn/dcconn.go`，
`go test ./test/webrtc/dcconn/` 通过。

形参取 `io.ReadWriteCloser` 而不是 `datachannel.ReadWriteCloser`：
后者是前者的超集，收窄成 `io` 接口后单测可以直接用 `net.Pipe`，
不必引入 pion 类型。

```go
type Conn struct {
    dc     io.ReadWriteCloser
    rbuf   []byte  // 64 KiB，容纳单条最大帧
    rn, ri int     // 有效长度、已消费位置
    wmu    sync.Mutex
}

func (c *Conn) Read(p []byte) (int, error) {
    if c.ri >= c.rn {          // 缓冲空了，取下一条消息
        n, err := c.dc.Read(c.rbuf)
        if err != nil { return 0, err }
        c.rn, c.ri = n, 0
    }
    n := copy(p, c.rbuf[c.ri:c.rn])   // 按调用方要的字节数吐
    c.ri += n
    return n, nil
}
```

要点：

- `Read` 只从缓冲取，缓冲耗尽才读下一条消息。
  这样 `io.ReadFull(r, buf[:2])` 拿 2 字节后，
  剩余部分仍在 `rbuf` 里等下次 `Read`，不会被 SCTP 丢弃。
- `Write` 单条消息不超过 64 KiB，超长要分片写。
  codec 单帧本就 ≤ 65535，正常路径下一次写完；
  但 `Write` 语义上可能收到更大的切片，仍需按 64 KiB 切分循环写。
- `Close` 关掉底层 DataChannel。
- pion 的 DataChannel 并发写不安全，`Write` 加互斥锁。

单测直接用 `net.Pipe` 造一对假 DataChannel 验证
「大帧分多次小 Read 读出」和「多帧连续读不串味」两个场景，
不依赖真实 ICE，跑得快也稳定。

### 步骤 2：signal —— 信令协议与服务 ✅ 已完成

已实现于 `test/webrtc/signal/{signal,server,client}.go`，`go vet` 干净。

消息结构（JSON，单一信封）：

```go
type Msg struct {
    Type string `json:"type"`  // register | list | offer | answer | candidate | error
    From string `json:"from"`
    To   string `json:"to"`
    SDP  string `json:"sdp,omitempty"`
    Cand string `json:"cand,omitempty"`
    Err  string `json:"err,omitempty"`
}
```

服务端行为：

- 用 `gorilla/websocket`（go.mod 已有 v1.5.3，不新增依赖）。
- `register` 记下 id 与连接的映射；连接断开时清理。
- `offer` / `answer` / `candidate` 按 `To` 查表转发，目标不在线回 `error`。
- 每个连接一个写 goroutine + 写队列，避免并发写 WebSocket panic。

**注意**：这个信令服务默认不带任何鉴权，任何人都能注册 id 并向他人
转发 offer。仅适用于本地联调；对外暴露前必须加接入认证，
否则等于开放任意人借用出口节点。这一点要在 README 和启动日志里写明。

### 步骤 3：peer —— PeerConnection 建连封装

两侧共用，差异只在谁发 offer。

```go
func New(ctx context.Context, cfg Config) (*webrtc.PeerConnection, error)
```

- `SettingEngine.DetachDataChannels()`（见 2.6），
  经 `webrtc.NewAPI(webrtc.WithSettingEngine(se))` 建 API 对象。
- ICE server 默认给一个公共 STUN（`stun:stun.l.google.com:19302`），
  可由命令行覆盖；无外网时留空退化为仅局域网直连。
- 主动侧：`CreateOffer` → `SetLocalDescription` → 经信令发 SDP →
  收 answer → `SetRemoteDescription`。
  注意这里**不再**预先 `CreateDataChannel`：通道改为按需创建。
  （SDP 里只要有 `application` m-line 就能协商出 SCTP，
  所以首次建连时先开一条通道再丢弃，或直接依赖 pion 的
  `CreateDataChannel` 触发重协商——取实现时较简单的一种。）
- 被动侧：`SetRemoteDescription` → `CreateAnswer` → 回发；
  `OnDataChannel` 会被调用多次，每次对应一个新的代理连接。
- 两侧都挂 `OnICECandidate` 往信令发 candidate，
  收到对端 candidate 调 `AddICECandidate`。
- 通道就绪统一走 `OnOpen` 里 `Detach()`，包 `dcconn.New` 后交给上层。

因为改用 `Upgrade`（见 2.8），peer 包要多导出一个按需开通道的方法：

```go
// Open 在已建立的 PeerConnection 上新开一条 DataChannel，
// 用于承载一个代理连接。label 建议带序号，便于抓包时对应。
func Open(ctx context.Context, pc *webrtc.PeerConnection, label string) (io.ReadWriteCloser, error)
```

内部用 `chan io.ReadWriteCloser` 把 `OnOpen` 里 Detach 的结果递出来，
调用方 select 等待或超时。被动侧同理，`OnDataChannel` 收到的通道
逐条投进一个 channel，由 service 主循环取用。

### 步骤 4：service —— 出口节点

对照 `test/socks/service` 的启动流程改写传输层：

- 连信令、`register` 自己的 id，然后等 offer。
- 每来一条 DataChannel（= 一个代理连接），包 `dcconn`，
  各起一个 goroutine 跑
  `rpc.StartPeer(ctx, dc, svc, pb.RegisterSocksSvcServer, pb.NewSocksCliClient)`
  （`StartPeer` = `NewPeer` + `Conn`，见 `peer.go:20-27`；
  两个 fRegisters 分别是「我实现的 server 接口」和
  「我要调对方的 client 接口」，顺序无关，靠函数签名区分——
  对比 `test/socks/proxy/main.go:58` 与
  `test/socks/peer_client.go:921` 两种相反写法）。
  `ConnUpgrade` 声明在 `service SocksSvc` 上
  （`test/socks/pb/service.proto:122-127`，**不是** `service Service`），
  所以出口节点这侧用 `RegisterSocksSvcServer`。
  svc 只需实现 `ConnUpgrade`，其余方法留空壳即可满足接口。
- `ConnUpgrade` 的实现取 `test/socks/peer_service.go:211-268` 的搬运逻辑，
  加上 `test/tcp_upgrade` 的外壳形状：**handler 必须尽快 return**，
  因为它是在 ReadLoop 里同步跑的（见 2.9），
  阻塞住就等于卡死整条连接的收包。连目标、对拷都放进 goroutine。
- 写序按 2.9 处理：goroutine 里先 `net.Dial` 目标、写 `req.Body`，
  然后**只**启动 `upgrade -> rc` 方向（这个方向只读线上数据，安全）；
  反方向 `rc -> upgrade` 会往线上写裸字节，
  必须等读到客户端的 1 字节 ready 标记之后再启动。
  这样裸数据不会抢在 `VerUpgradeResp` 之前上线。
- 两个方向的 `Copy` 仍要同生共死（用 `test/socks` 的 `Copy` 即可），
  否则 reader 会永久阻塞在 `Read` 上泄漏 goroutine。
- 一个出口节点要能同时服务多个客户端，
  所以每条 PeerConnection 各自一套 `OnDataChannel` 循环，互不影响。

### 步骤 5：client —— 本地 SOCKS5 入口

- 连信令、register、向指定出口节点 id 发 offer，建好 PeerConnection。
- 本地 `net.Listen("tcp", ":1080")`，SOCKS5 握手拿到目标地址，
  这段逻辑与 `test/socks/client` 一致，可直接搬。
- 每个入站连接：`peer.Open` 开一条新 DataChannel → 包 `dcconn` →
  `rpc.StartPeer(ctx, dc, cli, pb.RegisterSocksCliServer, pb.NewSocksSvcClient)` →
  ```go
  req := &pb.ConnUpgradeReq{
      Addr:    addr,               // SOCKS5 握出来的目标地址
      Network: pb.Network_TCP,     // enum，见 service.proto:43-46
      Body:    firstPayload,       // 可为 nil；有首包时省一个 RTT
  }
  resp := &pb.ConnUpgradeRsp{}
  upgrade, err := peer.Upgrade(ctx, "ConnUpgrade", req, resp)
  ```
  注意 `Upgrade` 是 `(ctx, method, req, res)` 四参、要自己传响应对象
  （`client.go:146`），不是只传 req；返回的是 `*codec.Upgrade`。
  返回后先检查 `resp.Err`，再立刻写 1 字节 ready 标记（见 2.9），
  然后与本地连接双向对拷。
  参考 `test/tcp_upgrade/client/main.go:104-122`。
- PeerConnection 断开（`OnConnectionStateChange` 到
  `Failed`/`Closed`）时重新走一遍建连，
  避免出口节点重启后客户端僵死。已有的代理连接一并失效，
  由各自的 `Copy` 收尾。

## 6. 验证方式

1. `go vet` + `go build` 三个 main 包，确认编译通过。
2. `go test ./test/webrtc/dcconn/` —— 适配层单测，不需要网络。
3. 端到端手测：起 signalsvr，起 service，起 client，
   然后 `curl -x socks5h://127.0.0.1:1080 http://example.com` 看是否通。
   同机联调 ICE 会直接选中 host candidate，不依赖 STUN 可达。

构建命令要显式列包，不能用 `./...`：
仓库里 `test/webrtc copy/` 目录名带空格，Go 会拒绝这种导入路径，
`go build ./...` 必然失败。这是既有问题，与本次改动无关。

## 7. 风险与取舍

- **仅锁 pion v3**：离线只有 v3 的 zip。若将来要上 v4，
  需先联网拉取依赖，且 `ice`/`dtls` 的主版本号会跟着变。
- **信令无鉴权**：见步骤 2，只作本地联调用。
- **不复制 proto**：复用 `test/socks/pb`。
  好处是不用同步两份定义；代价是 webrtc 模块对 socks 目录有依赖，
  后者的 proto 改动会波及此处。相比双份定义漂移，这个耦合更可控。
- **TURN 未纳入**：只配 STUN。对称 NAT 双方会打洞失败，
  此时需要 TURN 中继，属于后续扩展，不在本次范围。
- **一连接一通道的开销**：选 `Upgrade` 换来无队头阻塞，
  代价是每个代理连接一条 DataChannel、一次 rpc 握手、2 个 RTT。
  浏览器开几十个并发连接时，SCTP 流数量随之上去
  （上限 65535，够用），但握手开销是实打实的。
  若实测握手成本盖过队头阻塞的收益，按 2.8 末尾退回 `Stream`。


