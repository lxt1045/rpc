package codec

import (
	"context"
	stderr "errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/errors"
)

/*
TODO:
  切换stream后，放弃此连接，全力供应stream，类似 websocket那种方式！
*/

var (
	ErrUpgradeInProgress = errors.NewCode(0, -1, "upgrade is in progress")
	ErrUpgradeFailed     = errors.NewCode(0, -1, "upgrade failed")
	ErrUpgradeClosed     = errors.NewCode(0, -1, "upgrade closed")
)

type Upgrade struct {
	codec  *Codec
	rwc    io.ReadWriteCloser
	callSN uint32
	callID uint16

	ready     chan struct{}
	readyOnce sync.Once
	closeOnce sync.Once
	closeErr  error
	closed    uint32
}

func (s *Upgrade) Read(p []byte) (n int, err error) {
	if s == nil || atomic.LoadUint32(&s.closed) != 0 || s.rwc == nil {
		return 0, ErrUpgradeClosed.Clone()
	}
	return s.rwc.Read(p)
}

func (s *Upgrade) Write(p []byte) (n int, err error) {
	if s == nil || atomic.LoadUint32(&s.closed) != 0 || s.rwc == nil {
		return 0, ErrUpgradeClosed.Clone()
	}
	return writeFull(s.rwc, p)
}

func (s *Upgrade) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		atomic.StoreUint32(&s.closed, 1)
		s.readyOnce.Do(func() { close(s.ready) })
		if dl, ok := s.rwc.(interface{ SetDeadline(time.Time) error }); ok {
			// Wake blocked tunnel reads and prevent TLS close_notify from
			// waiting indefinitely on a peer that stopped reading.
			_ = dl.SetDeadline(time.Now())
		}
		if s.codec != nil {
			s.closeErr = s.codec.Close()
		}
	})
	return s.closeErr
}

func (s *Upgrade) markReady() {
	if s != nil {
		s.readyOnce.Do(func() { close(s.ready) })
	}
}

// WaitReady blocks until the upgrade response has been written. Raw tunnel
// bytes must not be sent before that response, otherwise the peer's frame
// decoder can interpret them as an RPC header.
func (s *Upgrade) WaitReady(ctx context.Context) error {
	if s == nil {
		return ErrUpgradeClosed.Clone()
	}
	select {
	case <-s.ready:
		if atomic.LoadUint32(&s.closed) != 0 {
			return ErrUpgradeClosed.Clone()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newUpgrade(c *Codec, callID uint16, callSN uint32) *Upgrade {
	return &Upgrade{
		codec:  c,
		rwc:    c.currentRWC(),
		callID: callID,
		callSN: callSN,
		ready:  make(chan struct{}),
	}
}

// client 端发起升级请求，服务端收到升级请求后，切换到 stream 模式，之后 client 和 server 都可以在 stream 上读写数据
func (c *Codec) Upgrade(ctx context.Context, callID uint16, req, res Msg) (upgrade *Upgrade, err error) {
	upgrade = newUpgrade(c, callID, atomic.AddUint32(&c.tmpCallSN, 1))
	defer func() {
		if err != nil {
			upgrade.Close()
		} else {
			atomic.StoreUint32(&c.status, 2)
		}
	}()
	func() {
		c.upgradeLock.Lock()
		defer c.upgradeLock.Unlock()
		if atomic.LoadUint32(&c.status) > 1 {
			err = ErrUpgradeInProgress.Clonef("upgrade is in progress: %d", c.status)
			return
		}
		swapped := atomic.CompareAndSwapUint32(&c.status, 0, 1)
		if !swapped {
			err = ErrUpgradeInProgress.Clonef("upgrade is in progress: %d", c.status)
			return
		}
		c.upgrade = upgrade
	}()
	done, err := c.clientCall(ctx, VerUpgradeReq, callID, upgrade.callSN, req, res)
	if err != nil {
		return
	}
	if done != nil {
		select {
		case err = <-done:
		case <-c.Done():
			err = stderr.New("resp timeout")
		case <-ctx.Done():
			err = stderr.New("resp timeout")
		}
	}
	return
}

// 调用 Upgrade() 后的返回值
func (c *Codec) VerUpgradeResp(ctx context.Context, header Header, bsBody []byte) (err error) {
	upgrade := c.upgrade
	if upgrade != nil {
		ctx = context.WithValue(ctx, ctxUpgradeKey{}, upgrade)
		atomic.StoreUint32(&c.status, 2)
	}
	err = c.VerCallResp(ctx, header, bsBody)
	if upgrade != nil {
		if err == nil {
			upgrade.markReady()
		} else {
			_ = upgrade.Close()
		}
	}
	return err
}

// 服务端收到 Upgrade() 请求后调用此函数处理逻辑
func (c *Codec) VerUpgradeReq(ctx context.Context, header Header, bsBody []byte) (err error) {
	upgrade := newUpgrade(c, header.CallID, header.CallSN)
	defer func() {
		if err != nil {
			upgrade.Close()
		}
	}()
	func() {
		c.upgradeLock.Lock()
		defer c.upgradeLock.Unlock()
		if atomic.LoadUint32(&c.status) > 1 {
			err = ErrUpgradeInProgress.Clonef("upgrade is in progress: %d", c.status)
			return
		}
		swapped := atomic.CompareAndSwapUint32(&c.status, 0, 1)
		if !swapped {
			err = ErrUpgradeInProgress.Clonef("upgrade is in progress: %d", c.status)
			return
		}
		c.upgrade = upgrade
	}()

	ctx = context.WithValue(ctx, ctxUpgradeKey{}, upgrade)
	return c.VerCallReq(ctx, header, bsBody)
}

// 服务端通过 GetUpgrade() 获取 Upgrade 对象
func (c *Codec) GetUpgrade(ctx context.Context, callID uint16, caller Method) (upgrade *Upgrade, err error) {
	c.upgradeLock.Lock()
	defer c.upgradeLock.Unlock()
	upgrade = c.upgrade
	if upgrade == nil {
		err = ErrUpgradeFailed.Clonef("upgrade failed: %d", atomic.LoadUint32(&c.status))
		return
	}

	return
}
