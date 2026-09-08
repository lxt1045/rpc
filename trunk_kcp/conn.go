package trunk_kcp

import (
	"io"
	"math"
	"sync"
	"sync/atomic"

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
	closed atomic.Bool
}

var _ io.ReadWriteCloser = &VirtualConn{}

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
	const maxChunkSize = math.MaxUint16 - HeaderSize
	totalWritten := 0

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
		if vc.TrunkKCP.closed.Load() {
			vc.TrunkKCP.kcpLock.Unlock()
			return totalWritten, io.ErrClosedPipe
		}
		ret := vc.TrunkKCP.kcp.Send(buf)
		vc.TrunkKCP.kcp.Update() // 立即更新触发发送，保持低延迟
		vc.TrunkKCP.kcpLock.Unlock()

		if ret < 0 {
			return totalWritten, errors.Errorf("kcp send failed: %d", ret)
		}

		totalWritten += chunkSize
	}

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
	header := Header{ConnID: vc.connID, Cmd: CmdCloseConn}
	buf := make([]byte, CmdHeaderSize)
	header.Format(buf)
	if ret := vc.kcp.Send(buf); ret < 0 {
		return errors.Errorf("kcp send failed: %d", ret)
	}
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
		vc.closeLocal()
	}
}
