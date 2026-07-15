package conn

import (
	"context"
	"io"
	"net"

	"github.com/lxt1045/utils/log"
)

// SetReadWriteBuff 设置 conn 读写缓存的大小
func SetReadWriteBuff(ctx context.Context, rwc io.ReadWriteCloser, read, write int) (err error) {
	netConn, ok := rwc.(interface {
		NetConn() net.Conn
	})
	if ok {
		connTCP := netConn.NetConn()
		setBuffer, ok := connTCP.(interface {
			SetReadBuffer(bytes int) error
			SetWriteBuffer(bytes int) error
		})
		if ok {
			if read != 0 {
				err = setBuffer.SetReadBuffer(read)
				if err != nil {
					log.Ctx(ctx).Error().Err(err).Send()
				}
			}
			if write != 0 {
				err = setBuffer.SetWriteBuffer(write)
				if err != nil {
					log.Ctx(ctx).Error().Err(err).Send()
				}
			}
		}
	}
	return
}
