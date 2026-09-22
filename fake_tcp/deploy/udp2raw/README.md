# M0 基线部署：udp2raw 作为底层伪装（零自研）

> 对应 `fake_tcp/plan.md` 的 M0 里程碑。目标：**今天就能**把现有 UDP+KCP 业务
> （如 `test/socks_trunk_kcp`）架到 udp2raw 的 FakeTCP 伪装之上，先拿到实测数据，
> 再决定是否投入 RawTCP 自研（M3）。

## 拓扑

```
客户端机器                                     服务器
┌──────────────────────────────┐             ┌──────────────────────────────┐
│ socks-client                 │             │ socks-service                │
│   │ UDP 127.0.0.1:4001       │             │   ▲ UDP 127.0.0.1:4001       │
│   ▼                          │             │   │                            │
│ udp2raw -c ── FakeTCP ───────┼══ TCP/线上 ═►│ udp2raw -s                   │
└──────────────────────────────┘             └──────────────────────────────┘
        运营商/中间盒看到的是完整 TCP 连接（三次握手/PSH+ACK/FIN 齐全）
```

## 安装

- Linux：从 `wangyu-/udp2raw` releases 取对应架构二进制（amd64/arm64/mips 均有）；
  需 root 或 `setcap 'cap_net_raw,cap_net_admin+ep' udp2raw`。
- Windows：官方压缩包自带 WinDivert 驱动（`WinDivert.dll` + `WinDivert64.sys`，
  与 exe 同目录），管理员权限运行。
- OpenWrt：有现成的 udp2raw 软件包/预编译 ipk。

## 配置模板（关键参数）

服务端：

```bash
udp2raw -s \
  -l 0.0.0.0:4002 -r 127.0.0.1:4001 \
  -k "$PSK" --raw-mode faketcp \
  --cipher-mode none --auth-mode none \
  --lower-level auto --keep-rule
```

客户端：

```bash
udp2raw -c \
  -l 127.0.0.1:4001 -r <server-ip>:4002 \
  -k "$PSK" --raw-mode faketcp \
  --cipher-mode none --auth-mode none \
  --keep-rule
```

参数说明：

- `--cipher-mode none --auth-mode none`：**避免双重加密**——业务侧（socks 链路/TLS）
  已有加密认证，udp2raw 的 AES/HMAC 纯属浪费 CPU。若业务层是明文（不该如此），
  改用默认的 `aes128cbc`/`md5`。老版本若无 `none` 档，用 `xor`/`simple`。
- `-k $PSK`：即便 none 档也要求密码（部分版本校验），两端保持一致。
- `--keep-rule`：iptables 规则常驻（进程异常退出后重启不再重复装）。
- **内层 KCP `mtu` ≤ 1300**：FakeTCP 段 = IP(20)+TCP(20+12 选项)+udp2raw 信封，
  留足余量防 IP 分片。`socks_trunk_kcp` 配置里的 KCP mtu 同步调小。
- 业务侧配置：`socks-client` 的远端地址填 `127.0.0.1:4001`（本机 udp2raw），
  `socks-service` 监听 `127.0.0.1:4001`（或 udp2raw `-r` 指向的地址）。

## 进程守护

见本目录 `udp2raw-server.service` / `udp2raw-client.service`（systemd）。
Windows 端用 `nssm` 或任务计划注册为服务。

## 实测与决策门（M0 验收）

同链路分时打流对比（建议晚高峰）：

| 指标 | 裸 UDP | udp2raw FakeTCP |
|---|---|---|
| 吞吐（Mbps） | | |
| 丢包率 | | |
| 30min 长稳断流次数 | | |

判定：

- FakeTCP **无显著收益** → 重新评估 fake_tcp 自研立项（可能只保留 UDP 模式）；
- FakeTCP 收益显著 → udp2raw 作为生产基线继续用；出现以下信号再启动 M3 自研 RawTCP：
  单二进制交付要求 / udp2raw 吞吐或 CPU 瓶颈 / 聚合链路需要精细化控制 / 上游失效。

## 与 fake_tcp 模块的关系

本方案不走 `fake_tcp` 包的任何代码（业务侧用最普通的 UDP 即可）。
`fake_tcp` 的 UDP 模式（`ModeUDP`）与这里的"裸 UDP 接 udp2raw"等价，
选择后者时 M2 的私有头/会话层用不上——这是有意为之：M2 的价值在于
无 udp2raw 环境下的统一抽象与 RawTCP 模式的对照组。
