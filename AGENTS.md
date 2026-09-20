# Repository Guidelines

## Project Structure & Module Organization

This repository is the `github.com/lxt1045/rpc` Go module, a gRPC-stub-compatible RPC library with its own framing and dispatch layer. Root-level files implement peer, client, service, middleware, and methods. `base/` contains protobuf definitions and generated types, `codec/` owns wire framing, `conn/` wraps transports, `socket/` provides listener/dial helpers, and `trunk/` implements connection aggregation. `trunk_kcp/` is a KCP trunk variant.

Root `*_test.go` files cover the library. `test/` contains runnable examples and integration programs; `test/proxy`, `test/socks_trunk`, and `test/socks_trunk_kcp` also carry real unit tests that pass offline. Keep generated `*.pb.go` files beside their matching `*.proto` source. `plan.md`/`TODO.md` at the root are trunk/trunk_kcp design notes.

## Build, Test, and Development Commands

Run commands from the repository root. The module replaces `github.com/lxt1045/utils` with `../utils`, so a sibling checkout is required.

```powershell
go build . ./base/... ./codec/... ./conn/... ./socket/... ./trunk/... ./trunk_kcp/... # library packages
go build ./...                                                                      # all packages and examples
go vet .                                                                            # root package checks
go test -run '^TestPipe$' -count=1 -timeout 60s .
go test -run '^TestUint16$' -count=1 ./trunk
go test -count=1 ./codec ./trunk ./test/proxy ./test/socks_trunk ./test/socks_trunk_kcp # offline suites
```

Use targeted tests during development. `go test ./trunk` is fully green; `go test ./trunk_kcp` has one known failure (`TestReviewHeaderID/ParseHeader2`, an experimental header-codec pair unused in production); `go test ./socket` hangs forever in `TestListen`'s infinite accept loop — run its tests by name. Some full-suite tests depend on fixed ports, local certificates, or QUIC/KCP timing; interpret `go test ./...` failures accordingly.

## Coding Style & Naming Conventions

Format Go changes with `gofmt`; use tabs and standard Go naming (`NewPeer`, `CallID`, `svcMethods`). Document exported APIs when needed. Follow package boundaries and avoid changing framing, reflection, or `unsafe` code without focused coverage. Keep protocol fields and generated protobuf names compatible: method ordering and message shapes are wire-visible.

## Testing Guidelines

Add tests in the changed package, named `TestFeature` and `BenchmarkFeature`. Prefer deterministic in-memory or loopback tests such as `TestPipe` and `TestPipeStream`; run them with `-count=1`. Narrow benchmarks with `go test -run '^$' -bench . .`. Build relevant examples individually when changing transport or configuration code.

## Commit & Pull Request Guidelines

Recent history uses short prefixes, primarily `feat:` and `fix:`. Write imperative, scoped subjects such as `fix: preserve call ID during reconnect`. Keep commits focused. Pull requests should explain the behavior change, list commands run, note tests not run, and link related issues. Include configuration or protocol compatibility notes when applicable; screenshots are only needed for example UI changes.

## Generated Code and Configuration

Regenerate protobufs with the gogo toolchain (`protoc --gogofast_out=plugins=grpc`), not standard `--go_out`. Do not commit private certificates, keys, or local YAML secrets; example TLS assets are intentionally absent from Git.
