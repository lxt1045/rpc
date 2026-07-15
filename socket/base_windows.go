package socket

import "syscall"

const (
	SO_REUSEPORT = 0x0F
	SO_REUSEADDR = syscall.SO_REUSEADDR
	TCP_SYNCNT   = 0
)

type Handle struct {
	H syscall.Handle
}

func newHandle(h uintptr) Handle {
	return Handle{
		H: syscall.Handle(h),
	}
}

