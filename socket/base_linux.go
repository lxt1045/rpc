package socket

import "syscall"

const (
	SO_REUSEPORT = 0x0F
	SO_REUSEADDR = syscall.SO_REUSEADDR
	TCP_SYNCNT   = syscall.TCP_SYNCNT
)

type Handle struct {
	H int
}

func newHandle(h uintptr) Handle {
	return Handle{
		H: int(h),
	}
}
