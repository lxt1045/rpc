package trunk_kcp

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/errors"
)

// VirtualConn 虚拟连接，通过 TrunkKCP 复用物理连接
type VirtualConn struct {
	*TrunkKCP
	connID uint16

	// 读缓冲
	readBuf  []byte
	readChan chan []byte
	readDone chan struct{}
	readLock sync.Mutex

	// 写缓冲
	writeLock sync.Mutex

	// 状态
	closed       atomic.Bool
	closeSent    atomic.Bool
	remoteClosed atomic.Bool
}

var _ io.ReadWriteCloser = &VirtualConn{}

func (vc *VirtualConn) ConnID() (connID uint16) {
	return vc.connID
}

// Write 写入数据到虚拟连接
func (vc *VirtualConn) Write(p []byte) (n int, err error) {
	if vc.closed.Load() {
		return 0, errors.New("connection closed")
	}

	vc.writeLock.Lock()
	defer vc.writeLock.Unlock()
	if vc.closed.Load() || vc.TrunkKCP.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	// 由于 Header.Len 是 uint16，单次最大只能发送 65535 - HeaderSize 字节
	// 对于大数据需要分块发送
	const maxChunkSize = 8*1024 - HeaderSize //math.MaxUint16 - HeaderSize
	totalWritten := 0

	// 背压：kcp-go 的 Send 只拒绝"单次 >255 段"，**不限制发送队列长度**。
	// 不在这里限流的话，窗口被卡住时应用会把数据无限堆进 snd_queue（内存涨、
	// 而且上层误以为发送成功、看不到真实速率）。这里把积压控制在软上限内，
	// 让阻塞沿 relay 传回上游（等价于 TCP 的发送窗口背压）。
	if err := vc.waitSendBacklog(); err != nil {
		return 0, err
	}

	for totalWritten < len(p) {
		chunkSize := len(p) - totalWritten
		if chunkSize > maxChunkSize {
			chunkSize = maxChunkSize
		}

		chunk := p[totalWritten : totalWritten+chunkSize]

		// 添加 ConnID 头部
		header := Header{
			ConnID: vc.connID,
			Len:    uint16(chunkSize),
		}

		buf := make([]byte, HeaderSize+chunkSize)
		header.Format(buf)
		copy(buf[HeaderSize:], chunk)

		// 发送到 KCP（需要加锁）
		vc.TrunkKCP.kcpLock.Lock()
		if vc.closed.Load() || vc.TrunkKCP.closed.Load() {
			vc.TrunkKCP.kcpLock.Unlock()
			return totalWritten, io.ErrClosedPipe
		}
		ret := vc.TrunkKCP.kcp.Send(buf)
		vc.TrunkKCP.kcpLock.Unlock()

		if ret < 0 {
			return totalWritten, errors.Errorf("kcp send failed: %d", ret)
		}

		totalWritten += chunkSize
	}
	vc.TrunkKCP.kcpLock.Lock()
	vc.TrunkKCP.kcp.Update() // 只 Update 一次，保持高吞吐量
	vc.TrunkKCP.kcpLock.Unlock()

	return totalWritten, nil
}

// Read 从虚拟连接读取数据
func (vc *VirtualConn) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	vc.readLock.Lock()
	defer vc.readLock.Unlock()

	for {
		if len(vc.readBuf) > 0 {
			n = copy(p, vc.readBuf)
			vc.readBuf = vc.readBuf[n:]
			return n, nil
		}

		// Drain data queued before the ordered remote close.
		select {
		case vc.readBuf = <-vc.readChan:
			continue
		default:
		}
		select {
		case vc.readBuf = <-vc.readChan:
		case <-vc.readDone:
			return 0, io.EOF
		}
	}
}

// sendBacklogSoftLimit 发送队列积压软上限（段）。取 2×自动调窗上限，留一个窗口的
// 应用缓冲；超过就等（背压），避免无限缓冲。单段载荷 ≈ mtu-24 字节。
const sendBacklogSoftLimit = 2048

// sendBacklogWait 背压等待超时（防止对端彻底无响应时永久阻塞）
const sendBacklogWait = 60 * time.Second

// waitSendBacklog 等发送队列积压降到软上限以下；连接关闭/超时返回错误。
func (vc *VirtualConn) waitSendBacklog() error {
	deadline := time.Now().Add(sendBacklogWait)
	for {
		vc.TrunkKCP.kcpLock.Lock()
		backlog := vc.TrunkKCP.kcp.WaitSnd()
		vc.TrunkKCP.kcpLock.Unlock()
		if backlog < sendBacklogSoftLimit {
			return nil
		}
		if vc.closed.Load() || vc.TrunkKCP.closed.Load() {
			return io.ErrClosedPipe
		}
		if time.Now().After(deadline) {
			return errors.Errorf("kcp send backlog full: %d segs", backlog)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Close 关闭虚拟连接
func (vc *VirtualConn) Close() error {
	if !vc.closeLocal() {
		return errors.New("already closed")
	}
	// Serialize the close frame after any in-flight Write.
	vc.writeLock.Lock()
	defer vc.writeLock.Unlock()
	vc.kcpLock.Lock()
	defer vc.kcpLock.Unlock()
	if vc.TrunkKCP.closed.Load() {
		return nil
	}
	header := Header{
		ConnID: vc.connID,
		Cmd:    CmdCloseConn,
	}
	buf := make([]byte, CmdHeaderSize)
	header.Format(buf)
	if ret := vc.kcp.Send(buf); ret < 0 {
		return errors.Errorf("kcp send failed: %d", ret)
	}
	vc.closeSent.Store(true)
	vc.kcp.Update()
	return nil
}

func (vc *VirtualConn) closeLocal() bool {
	if !vc.closed.CompareAndSwap(false, true) {
		return false
	}
	close(vc.readDone)
	return true
}

func (vc *VirtualConn) handleCmd(header Header, data []byte) {
	switch header.Cmd {
	case CmdCloseConn:
		// Reply once, even when the application has not closed its end yet.
		_ = vc.Close()
		vc.remoteClosed.Store(true)
	}
}

func (vc *VirtualConn) reusable() bool {
	// KCP orders each direction independently. Both close frames must pass
	// before a new stream can safely reuse this ID without receiving old data.
	return vc.closeSent.Load() && vc.remoteClosed.Load()
}
