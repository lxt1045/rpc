package trunk_kcp

import (
	"io"
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

	// 由于 Header.Len 是 uint16，单次最大只能发送 65535 - HeaderSize 字节
	// 对于大数据需要分块发送
	const maxChunkSize = 65535 - HeaderSize
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
			Len:    uint16(chunkSize + HeaderSize),
		}

		buf := make([]byte, HeaderSize+chunkSize)
		header.Format(buf)
		copy(buf[HeaderSize:], chunk)

		// 发送到 KCP（需要加锁）
		vc.TrunkKCP.kcpLock.Lock()
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
	vc.readLock.Lock()
	defer vc.readLock.Unlock()

	for {
		if len(vc.readBuf) > 0 {
			n = copy(p, vc.readBuf)
			vc.readBuf = vc.readBuf[n:]
			return n, nil
		}

		data, ok := <-vc.readChan
		if !ok {
			return 0, io.EOF
		}
		vc.readBuf = data
	}
}

// Close 关闭虚拟连接
func (vc *VirtualConn) Close() error {
	if !vc.closed.CompareAndSwap(false, true) {
		return errors.New("already closed")
	}

	// 发送关闭命令到对端
	header := Header{
		ConnID: vc.connID,
		Cmd:    CmdCloseConn,
		Len:    CmdHeaderSize,
	}

	buf := make([]byte, CmdHeaderSize)
	header.Format(buf)

	vc.TrunkKCP.kcpLock.Lock()
	vc.TrunkKCP.kcp.Send(buf)
	vc.TrunkKCP.kcp.Update()
	vc.TrunkKCP.kcpLock.Unlock()

	// 不关闭本地的 readChan，等待对端的关闭命令
	// 当对端收到我们的关闭命令并回复关闭命令时，handleCmd 会关闭本地的 readChan
	return nil
}

// handleCmd 处理命令
func (vc *VirtualConn) handleCmd(header Header, data []byte) {
	switch header.Cmd {
	case CmdCloseConn:
		// 对端关闭连接
		if vc.closed.CompareAndSwap(false, true) {
			close(vc.readChan)
		}
	case CmdAddConn:
		// 添加连接命令（保留用于扩展）
	default:
		// 未知命令，忽略
	}
}
