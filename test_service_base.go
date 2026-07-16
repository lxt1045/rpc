package rpc

import (
	"net"
	"sync"
)

type fakeConn struct {
	w      chan struct{}
	r      chan struct{}
	wCache *[]byte
	rCache *[]byte
	wl     *sync.Mutex
	rl     *sync.Mutex

	name string

	net.Conn
}

func NewFakeConnPipe() (svc, cli *fakeConn) {
	svc = &fakeConn{
		w:      make(chan struct{}),
		r:      make(chan struct{}),
		wCache: &[]byte{},
		rCache: &[]byte{},
		wl:     &sync.Mutex{},
		rl:     &sync.Mutex{},
		name:   "svc",
	}
	cli = &fakeConn{
		w:      svc.r,
		r:      svc.w,
		wCache: svc.rCache,
		rCache: svc.wCache,
		wl:     svc.rl,
		rl:     svc.wl,
		name:   "cli",
	}
	return
}

func (f *fakeConn) Read(data []byte) (n int, err error) {
	read := func(data []byte) (n int, err error) {
		f.rl.Lock()
		defer f.rl.Unlock()
		if l := len(*f.rCache); l > 0 {
			if l >= len(data) {
				n = len(data)
				copy(data, *f.rCache)
				*f.rCache = (*f.rCache)[n:]
				select {
				case f.r <- struct{}{}:
				default:
				}
				return
			}

			n = l
			copy(data, *f.rCache)
			*f.rCache = (*f.rCache)[:0]
			return
		}
		return
	}

	for {
		n, err = read(data)
		if err != nil || n > 0 {
			return
		}
		<-f.r
	}
}

func (f *fakeConn) Write(data []byte) (n int, err error) {
	f.wl.Lock()
	defer f.wl.Unlock()
	*f.wCache = append(*f.wCache, data...)
	select {
	case f.w <- struct{}{}:
	default:
	}
	return len(data), nil
}

func (f *fakeConn) Close() (err error) {
	return
}
