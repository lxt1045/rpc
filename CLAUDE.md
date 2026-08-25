# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A gRPC-compatible RPC library that reuses `protoc`-generated gRPC stubs but replaces the entire
transport and dispatch path with its own binary framing. It never links gRPC's runtime: `grpc` is
imported only for the *type signatures* of the generated `RegisterXServer(*grpc.Server, XServer)`
and `NewXClient(*grpc.ClientConn) XClient` functions, which are reflected over to build the method
table. Any `io.ReadWriteCloser` can carry the protocol — TCP, TLS, QUIC, KCP, UDP, WebSocket, or an
in-memory pipe.

Code was moved here from `github.com/lxt1045/utils/rpc`; comments are in Chinese and the style
leans on `unsafe`/`reflect` for dispatch performance.

## Build and test

`go build ./...` **fails** on this repo — `test/webrtc copy/` has a space in its name and Go rejects
the import path. Build explicit package sets instead:

```bash
go build . ./base/... ./codec/... ./conn/... ./socket/... ./trunk/...   # library, clean
go vet .                                                                # also compiles root tests
```

Two pre-existing breaks, unrelated to whatever you are changing: `test/webrtc copy/` (space in
path, and it imports the empty `test/webrtc` package) and `test/socks/service/http`
(`llog.GetStdOutput` undefined — drifted from the `utils` dependency).

Tests live in the root package plus `codec`-adjacent helpers. Run a single one by name:

```bash
go test -run '^TestPipe$' -count=1 -timeout 60s .
go test -run '^TestTrunk$' -count=1 ./trunk
go test -run '^BenchmarkMethod$' -bench . -run '^$' .
```

`go test .` as a whole does **not** pass. These are reliable and are the ones to use when
validating a change: `TestCall`, `TestPipe`, `TestPipeStream`, `TestClient`, `TestClientEm`,
`TestMethod`, `TestTimeoutConn`, `TestConn`, `TestUDPConn`, `TestIps`, `TestCmp`, and in `trunk`,
`TestUint16` and `TestTrunk`.

Known-failing regardless of your change: `TestQuic` and `TestQuicSocket` die on
`timeout: no recent network activity` during the QUIC handshake; `TestKCPConn` gets further — the
service side receives and dispatches `SayHello`, then the client reports `resp timeout` after ~30s;
and `trunk.TestTrunk0` fails. These bind hardcoded ports (18080 for KCP, 18081 for UDP, 18086 from
`default.yml`), so a stale listener or a second test run in parallel breaks them too.

Several tests call `log.Fatal` on failure, which kills the whole test binary — so one broken network
test discards the results of everything scheduled after it. Always narrow with `-run`.

`TestPipe`/`TestPipeStream` are the useful ones for protocol work: they wire a client and service
over `NewFakeConnPipe()` (`test_service_base.go`) with no network at all.

## Dependencies

`go.mod` has `replace github.com/lxt1045/utils => ../utils`, so a sibling checkout of that repo
must exist next to this one. `github.com/lxt1045/utils` supplies logging (`log.Ctx(ctx)`, zerolog),
config loading, TLS helpers, and `delay.Queue`. Errors come from `github.com/lxt1045/errors`
(`errors.Errorf`, and `errors.NewCode(...)` for sentinel codes that survive the wire — see below).
Protobuf runtime is **gogo/protobuf**, not google.golang.org/protobuf.

TLS certs are not in git — `.gitignore` excludes both `*.pem` and `*.yml`, and zero `.pem` files are
tracked. The `conf/default.yml` files were force-added so the `//go:embed static` directives still
compile, but anything calling `config.LoadTLSConfig` fails at runtime on a fresh clone. Generate
certs with `go test -run TestMake ./test/cert`, which writes root/inner/leaf pairs into
`test/cert/ca/`, then
copy them into each example's `filesystem/static/ca/`. Nothing automates that copy, and the leaf
cert is issued for `speedtest.cn` while the configs expect host `lxt1045.com`.

## Architecture

### Peer is symmetric; Client and Service are two halves of one connection

`Peer` (`peer.go`) embeds both `Client` and `Service` and both share a single `*codec.Codec`.
There is no server/client asymmetry in the protocol — either side can call the other over the same
socket, which is what makes the NAT-traversal and reverse-proxy examples work. `NewPeer` sorts the
variadic `fRegisters` by signature: one-in/one-out is a `NewXClient` constructor (outbound calls),
two-in/zero-out is a `RegisterXServer` (inbound handlers).

### Method tables are built once by reflection, then called through unsafe function pointers

`method.go` is the core trick and the most fragile code here. `getSvcMethods` walks the generated
server interface, then for each method swaps the *type word* of an `interface{}` holding
`reflect.Method.Func` with that of a `func(unsafe.Pointer, context.Context, unsafe.Pointer)
(unsafe.Pointer, error)`, keeping the original code pointer. Calls then go straight through that
function pointer with the receiver passed as `unsafe.Pointer`, skipping `reflect.Call` entirely
(`BenchmarkMethod` compares the two paths).

Consequences to respect:

- Methods are addressed on the wire by `CallID`, which is **the index into the method table**, not
  a hash or a name. `Client.getMethodsFromSvc` performs a `CmdReq_CallIDs` handshake right after
  connect to learn the peer's ordering; the two sides must agree on the generated interface.
- Streaming methods from the `.proto` are silently skipped (`mType.NumOut() == 1`) — this library's
  streaming is its own mechanism, not gRPC's.
- A response type named `*.Empty` with no protobuf-tagged fields is compiled to `respType == nil`,
  meaning the handler's return is dropped and no response frame is sent. Renaming such a message
  changes wire behavior.
- Client-side lookup keys are the *short* method name (`"SayHello"`), not the qualified one:
  `Invoke(ctx, "SayHello", req, resp)`. Service-side `MethodIdx` uses the qualified
  `"base.HelloServer.SayHello"`.

### Server pattern: build the table once, Clone per connection

`StartPeer(ctx, nil, svc, ...)` with a nil conn builds the method table without any I/O; then each
accepted connection gets `gPeer.Clone(ctx, conn, svc)`, which rebinds the method table to a new
`Codec` and (for `CloneMethods`) a new service receiver pointer. See `test/tcp/service/main.go:54`
and `:82`. Do not re-reflect per connection.

`Peer.Conn`/`Clone` call `Handshake()` first if the conn exposes it, because the read loop and
writes would otherwise race into a concurrent-handshake deadlock on TLS.

### codec: one framing, several message kinds

`codec/header.go` defines a 16-byte little-endian header (6-byte short form when `Len == 0`) whose
`Ver` field multiplexes everything: heartbeat, call req/resp, error resp, close, cmd req/resp,
stream req/resp, and upgrade req/resp. `codec/codec.go` runs one `ReadLoop` goroutine per
connection that dispatches on `Ver`; each inbound request is handled in its own goroutine
(`go c.Handler(...)`).

Things worth knowing before touching this layer:

- **Frames are capped at 64 KiB.** `Send` splits anything larger across frames using
  `SegmentCount`/`SegmentIdx`, and the reader reassembles them in `c.segments` keyed by `CallSN`.
  The split path deliberately rewrites the header in place over the previous frame's tail.
- **Responses are matched by `CallSN`**, registered in `c.resps` *before* the request is written,
  with a `delay.Queue` entry as the timeout. Handlers reply on a `chan error` of capacity 1.
- **Errors round-trip structurally.** `codec/reply.go` marshals an error into `base.Err` including
  `Code()`, `Msg()`, and `Stack()` if the error implements them, and the client rebuilds it via
  `errors.NewCodeWithStack`. So returning an `errors.NewCode(...)`-derived error from a handler
  preserves the code and stack across the network.
- **`ctx` values are propagated selectively.** `Peer.ClientPassKey(keys...)` names the string-valued
  ctx keys to serialize into the `base.Ctx` block; the peer reinstalls them only if the count
  matches what it advertised at handshake. A log ID travels this way on every frame.

### Three ways to move data beyond request/response

- **Stream** (`codec/stream.go`): a `CmdReq_Stream` handshake allocates a `Stream` on both sides
  keyed by `CallSN`, multiplexed over the same connection. The first inbound frame invokes the
  handler; subsequent ones are queued for `Recv`. Handlers reach it with `codec.GetStream(ctx)`.
- **Upgrade** (`codec/upgrade.go`): hands the raw conn to the caller and abandons RPC framing
  entirely — `Upgrade.Read`/`Write` go straight to `rwc`. `Codec.status` moves 0 → 1 → 2 and the
  read loop exits without closing. Handlers use `codec.GetUpgrade(ctx)`. This is one-way and
  terminal for that connection; see `test/tcp_upgrade`.
- **Trunk** (`trunk/`): link aggregation below the RPC layer. `NewTrunk(rws...)` presents N real
  conns as virtual `*Conn`s addressed by `ConnID`, round-robining writes and reordering by
  per-conn `Idx` on read. It has its own header and its own `base.Err`-encoded event channel,
  independent of `codec`.

### Transports

`conn/` wraps concrete transports into `io.ReadWriteCloser`: `NewZip` (klauspost/compress framing,
used by the pipe tests), `WrapQuic`/`WrapQuicClient`, KCP (`NewKcpCli`, plus a shared update loop
and addr↔conn registry), and UDP pseudo-connections. `socket/` handles listen/dial with
`SO_REUSEADDR`/`SO_REUSEPORT` set via `syscall.RawConn.Control`, with the fd type abstracted across
`base_linux.go`/`base_windows.go`.

### Middleware

`middleware.go` gives Gin-style chains: `ClientUse(func(*CliParam))` on the outbound path,
`ServiceUse(func(*SvcParam))` on the inbound. Call `p.Next()` to continue; the terminal step
performs the real invoke. Registering service middleware wraps each `SvcMethod` in a
`WrapSvcMethod`, so `Service.Methods()` must be re-read after `Use`.

## Protobuf generation

Protos live beside their generated code (`option go_package = "./;pb"`). The library's own types
are in `base/`; each example has its own `test/*/pb/`. Windows, from `base/README.md`:

```ps1
$env:dir="D:/project/go/src/github.com/lxt1045"
protoc -I="$env:dir" $env:dir/rpc/base/*.proto --gogofast_out=plugins=grpc:"$env:dir/rpc/base/"
```

Use `--gogofast_out=plugins=grpc` (gogo), not `--go_out`. Several `test/*/pb/README.md` files are
stale copy-paste and regenerate into `test/socks/pb/` no matter which directory they sit in — check
the path before running one.

## Examples

`test/` holds runnable `main` packages, not tests. Two layouts appear: shared peer logic in the
example root with thin `main.go` binaries under `service/`, `client/`, `proxy/` (`socks`,
`socks_stream`, `socks_trunk`, `socks_quic2`), or peer logic inline in each command directory
(`tcp`, `tcp_upgrade`, `socks_kcp`, `socks_quic`, `socks_udp`, `socks_nat`). Run them from their own
directory — config is read from an embed-relative `static/conf/default.yml`.

Read `test/tcp` first for the minimal shape, `test/tcp_upgrade` for hijacking, `test/socks_stream`
for streaming, and `test/socks` for the full remote SOCKS5/HTTP proxy (including uTLS browser
fingerprints under `chrome/`).
