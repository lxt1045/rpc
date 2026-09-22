package fake_tcp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lxt1045/errors"
)

// fake_tcp_test.go：UDP 模式端到端集成测试（127.0.0.1 回环，无机权限制，CI 可跑）。
// 回环 UDP 可靠且有序，因此可做强校验（哈希比对）；乱序/丢包语义见 session_test.go。

func udpTestConfig(t *testing.T, local, remote string) Config {
	t.Helper()
	return Config{
		Mode:             ModeUDP,
		LocalAddr:        local,
		RemoteAddr:       remote,
		Magic:            0x5a5a1234,
		Keepalive:        time.Hour,
		HandshakeRetries: 3,
	}
}

// listenUDPTest 起 UDP 模式服务端，返回监听器与实际地址
func listenUDPTest(t *testing.T, ctx context.Context) (*Listener, string) {
	t.Helper()
	l, err := Listen(ctx, udpTestConfig(t, "127.0.0.1:0", ""))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", l.Addr().Port)
	return l, addr
}

// writeAll 循环写满
func writeAll(c *Conn, data []byte) error {
	for len(data) > 0 {
		n, err := c.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// readAll 读满 len 或到 EOF
func readAll(c *Conn, total int) ([]byte, error) {
	buf := make([]byte, 0, total)
	tmp := make([]byte, 64*1024)
	for len(buf) < total {
		n, err := c.Read(tmp)
		if err != nil {
			return buf, err
		}
		buf = append(buf, tmp[:n]...)
	}
	return buf, nil
}

// seqStats 序号块传输统计
type seqStats struct {
	loss, dup, reorder, corrupt int
	chunks                      int
}

// writeSeqChunks 写入 n 个序号块：[uint32(base+序号)][序号派生填充...]
func writeSeqChunks(c *Conn, chunk, n int, base uint32) error {
	buf := make([]byte, chunk)
	for i := 0; i < n; i++ {
		seq := base + uint32(i)
		binary.LittleEndian.PutUint32(buf[:4], seq)
		for j := 4; j < chunk; j++ {
			buf[j] = byte(int(seq) + j)
		}
		if _, err := c.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

// checkSeqChunks 读出序号为 [base, base+n) 的 n 个块并统计。
// 本层不保证可靠/有序（plan.md §1.3），
// 故断言"无重复、无损坏"，loss/reorder 仅统计上报（交由上层 KCP 补）。
func checkSeqChunks(c *Conn, chunk, n int, base uint32) (seqStats, error) {
	var st seqStats
	seen := make(map[uint32]bool)
	expect := base
	lastIdx := int64(-1)
	tmp := make([]byte, 64*1024)
	for expect < base+uint32(n) {
		m, err := c.Read(tmp)
		if err != nil {
			return st, err
		}
		if m%chunk != 0 {
			st.corrupt++
			continue
		}
		for off := 0; off+chunk <= m; off += chunk {
			b := tmp[off : off+chunk]
			idx := binary.LittleEndian.Uint32(b[:4])
			good := true
			for j := 4; j < chunk; j++ {
				if b[j] != byte(int(idx)+j) {
					good = false
					break
				}
			}
			if !good {
				st.corrupt++
				continue
			}
			if seen[idx] {
				st.dup++
				continue
			}
			seen[idx] = true
			if int64(idx) < lastIdx {
				st.reorder++
			}
			lastIdx = int64(idx)
			if idx == expect {
				expect++
			} else if idx > expect {
				st.loss += int(idx - expect)
				expect = idx + 1
			}
			st.chunks++
		}
	}
	return st, nil
}

// TestUDPEndToEnd 双向各 10MB 序号块。回环高并发下内核 UDP 允许少量丢包/乱序，
// 本层语义即"不重传"（plan.md §1.3），故断言：无重复、无损坏、低丢包率。
// 字节级完整性锚点由 TestPipeEndToEnd（有序无损管道）承担。
func TestUDPEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, addr := listenUDPTest(t, ctx)
	defer l.Close()

	const chunk = 1387 // MaxPayload
	const total = 10 << 20
	const nChunks = total / chunk

	var wg sync.WaitGroup
	wg.Add(1)
	var srvStats seqStats
	go func() { // 服务端：收 10MB、发 10MB
		defer wg.Done()
		conn, err := l.Accept()
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		var sWG sync.WaitGroup
		sWG.Add(2)
		go func() {
			defer sWG.Done()
			st, err := checkSeqChunks(conn, chunk, nChunks, 0)
			if err != nil {
				t.Errorf("服务端读: %v", err)
				return
			}
			srvStats = st
		}()
		go func() {
			defer sWG.Done()
			if err := writeSeqChunks(conn, chunk, nChunks, 0); err != nil {
				t.Errorf("服务端写: %v", err)
			}
		}()
		sWG.Wait()
	}()

	cli, err := Dial(ctx, udpTestConfig(t, "", addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = cli.SetReadDeadline(time.Now().Add(60 * time.Second))
	var cWG sync.WaitGroup
	cWG.Add(2)
	var cliStats seqStats
	go func() {
		defer cWG.Done()
		if err := writeSeqChunks(cli, chunk, nChunks, 0); err != nil {
			t.Errorf("客户端写: %v", err)
		}
	}()
	go func() {
		defer cWG.Done()
		st, err := checkSeqChunks(cli, chunk, nChunks, 0)
		if err != nil {
			t.Errorf("客户端读: %v", err)
			return
		}
		cliStats = st
	}()
	cWG.Wait()
	wg.Wait()

	for name, st := range map[string]seqStats{"上行": srvStats, "下行": cliStats} {
		t.Logf("%s: chunks=%d loss=%d dup=%d reorder=%d corrupt=%d", name, st.chunks, st.loss, st.dup, st.reorder, st.corrupt)
		if st.dup != 0 || st.corrupt != 0 {
			t.Errorf("%s 出现重复或损坏: %+v", name, st)
		}
		// 容忍 5% 内核侧丢包（回环在全量测试 CPU 竞争下可能飙升；
		// 字节级完整性锚点是 TestPipeEndToEnd，这里只验证"不重复、不损坏"）
		if st.loss > nChunks/20 {
			t.Errorf("%s 丢包率过高: %+v", name, st)
		}
	}
}

// TestUDPMultiConn 多连接并发（服务端会话路由）。每连接 32KB 序号块回显，
// 断言各连接数据不串流、无重复、无损坏（容忍少量丢包/乱序）
func TestUDPMultiConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, addr := listenUDPTest(t, ctx)
	defer l.Close()

	const conns = 8
	const chunk = 1387
	const nChunks = 24 // 约 32KB/连接

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 服务端：接受 8 条连接，每条回显（回显协程由 l.Close 兜底退出）
		defer wg.Done()
		for i := 0; i < conns; i++ {
			conn, err := l.Accept()
			if err != nil {
				t.Errorf("Accept #%d: %v", i, err)
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			go func(c *Conn) {
				buf := make([]byte, 64*1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return // 对端关闭或读超时即结束回显
					}
					if _, err := c.Write(buf[:n]); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	var cWG sync.WaitGroup
	for i := 0; i < conns; i++ {
		cWG.Add(1)
		go func(idx int) {
			defer cWG.Done()
			cli, err := Dial(ctx, udpTestConfig(t, "", addr))
			if err != nil {
				t.Errorf("Dial #%d: %v", idx, err)
				return
			}
			defer cli.Close()
			_ = cli.SetReadDeadline(time.Now().Add(30 * time.Second))
			// 序号以 idx*nChunks 起步：串流会被序号区间检查当场抓获
			base := uint32(idx * nChunks)
			go writeSeqChunks(cli, chunk, nChunks, base)
			st, err := func() (seqStats, error) {
				var st seqStats
				seen := make(map[uint32]bool)
				tmp := make([]byte, 64*1024)
				got := 0
				for got < nChunks*9/10 { // 收够 90% 即认为路由正确
					m, err := cli.Read(tmp)
					if err != nil {
						return st, err
					}
					for off := 0; off+chunk <= m; off += chunk {
						b := tmp[off : off+chunk]
						seq := binary.LittleEndian.Uint32(b[:4])
						good := seq >= base && seq < base+uint32(nChunks)
						for j := 4; good && j < chunk; j++ {
							if b[j] != byte(int(seq)+j) {
								good = false
							}
						}
						if !good {
							st.corrupt++
							continue
						}
						if seen[seq] {
							st.dup++
							continue
						}
						seen[seq] = true
						got++
					}
				}
				return st, nil
			}()
			if err != nil {
				t.Errorf("客户端 #%d 读: %v", idx, err)
				return
			}
			if st.dup != 0 || st.corrupt != 0 {
				t.Errorf("客户端 #%d 重复/损坏: %+v", idx, st)
			}
		}(i)
	}
	cWG.Wait()
	cancel() // 通知服务端回显协程退出（读超时兜底）
	wg.Wait()
}

// TestUDPCloseSemantics 关闭语义：客户端 Close → 服务端读 EOF
func TestUDPCloseSemantics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, addr := listenUDPTest(t, ctx)
	defer l.Close()

	cli, err := Dial(ctx, udpTestConfig(t, "", addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	srv, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	if err := writeAll(cli, []byte("bye")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // 等数据到达后再关，避免 FIN 先于数据被处理
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := readAll(srv, 3)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("服务端读: %v", err)
	}
	if string(got) != "bye" {
		t.Fatalf("数据不符: %q", got)
	}
	// 再读应为 EOF
	if _, err := srv.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("应为 EOF: %v", err)
	}
}

// TestUDPDialTimeout 对端不应答时握手失败：回环下关闭端口会回 ICMP unreachable（拒绝），
// 过滤丢包的网络则走超时——两种结果都合法
func TestUDPDialTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := udpTestConfig(t, "", "127.0.0.1:1") // port 1 无人应答
	cfg.HandshakeRetries = 1
	start := time.Now()
	_, err := Dial(ctx, cfg)
	if err == nil {
		t.Fatalf("应握手失败")
	}
	code := errors.AsCode(err)
	if code == nil || (code.Code() != ErrHandshakeTimeout.Code() && code.Code() != ErrHandshakeRejected.Code()) {
		t.Fatalf("应为 ErrHandshakeTimeout/ErrHandshakeRejected: %v", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("失败耗时异常: %v", d)
	}
}

// TestUDPBadMagic 垃圾报文被静默丢弃，不产生会话
func TestUDPBadMagic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, addr := listenUDPTest(t, ctx)
	defer l.Close()

	// 直接发垃圾 UDP 报文（Magic 不符）
	raw, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("net.Dial: %v", err)
	}
	defer raw.Close()
	junk := []byte("this is garbage, no magic header at all......")
	for i := 0; i < 5; i++ {
		if _, err := raw.Write(junk); err != nil {
			t.Fatalf("写垃圾报文: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	cnt := 0
	l.sessions.Range(func(_, _ any) bool { cnt++; return true })
	if cnt != 0 {
		t.Fatalf("垃圾报文不应产生会话，实际 %d 个", cnt)
	}

	// 正常客户端仍能建立（垃圾报文不影响监听）
	cli, err := Dial(ctx, udpTestConfig(t, "", addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()
	srv, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := writeAll(cli, []byte("ok")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := readAll(srv, 2)
	if err != nil || string(got) != "ok" {
		t.Fatalf("数据不符: %q %v", got, err)
	}
}

// TestUDPKeepalive 空闲时保活维持会话不被回收
func TestUDPKeepalive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := udpTestConfig(t, "127.0.0.1:0", "")
	cfg.Keepalive = 100 * time.Millisecond
	l, err := Listen(ctx, cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", l.Addr().Port)

	ccfg := udpTestConfig(t, "", addr)
	ccfg.Keepalive = 100 * time.Millisecond
	cli, err := Dial(ctx, ccfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()
	srv, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	// 空闲 450ms（>3×Keepalive 的话若保活失效会被回收），期间保活应刷新 lastActive
	time.Sleep(450 * time.Millisecond)
	if cli.sess.isClosed() || srv.sess.isClosed() {
		t.Fatalf("空闲期会话被误回收")
	}

	// 空闲后仍可通信
	if err := writeAll(cli, []byte("alive")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := readAll(srv, 5)
	if err != nil || string(got) != "alive" {
		t.Fatalf("数据不符: %q %v", got, err)
	}
}

// TestUDPWriteChunking 大包按 MaxPayload 切片
func TestUDPWriteChunking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, addr := listenUDPTest(t, ctx)
	defer l.Close()

	cli, err := Dial(ctx, udpTestConfig(t, "", addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()
	srv, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	data := make([]byte, 5000)
	_, _ = rand.Read(data)
	if n, err := cli.Write(data); err != nil || n != 5000 {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	got, err := readAll(srv, 5000)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatalf("数据哈希不符")
	}
}

// TestUDPConnIDIsolation 同一客户端地址、不同 ConnID 的会话互不影响
// （UDP 模式以 ConnID+四元组为键；同一进程两条 Conn 复用同一 UDP 端口时验证路由正确性）
func TestUDPConnIDIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, addr := listenUDPTest(t, ctx)
	defer l.Close()

	cli1, err := Dial(ctx, udpTestConfig(t, "", addr))
	if err != nil {
		t.Fatalf("Dial1: %v", err)
	}
	defer cli1.Close()
	cli2, err := Dial(ctx, udpTestConfig(t, "", addr))
	if err != nil {
		t.Fatalf("Dial2: %v", err)
	}
	defer cli2.Close()
	if cli1.sess.connID == cli2.sess.connID {
		t.Fatalf("ConnID 碰撞")
	}
	srv1, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept1: %v", err)
	}
	srv2, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept2: %v", err)
	}

	// 两条连接各发不同数据，服务端各自读到的应是各自客户端的数据
	m1 := []byte("conn-1-data")
	m2 := []byte("conn-2-data")
	if err := writeAll(cli1, m1); err != nil {
		t.Fatalf("cli1.Write: %v", err)
	}
	if err := writeAll(cli2, m2); err != nil {
		t.Fatalf("cli2.Write: %v", err)
	}
	got1, err := readAll(srv1, len(m1))
	if err != nil || string(got1) != string(m1) {
		t.Fatalf("srv1 数据不符: %q %v", got1, err)
	}
	got2, err := readAll(srv2, len(m2))
	if err != nil || string(got2) != string(m2) {
		t.Fatalf("srv2 数据不符: %q %v", got2, err)
	}
}
