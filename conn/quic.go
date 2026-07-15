package conn

import (
	"context"

	"github.com/lxt1045/errors"
	"github.com/quic-go/quic-go"
)

type QuicConn struct {
	*quic.Conn
	*quic.Stream
}

// func (c QuicConn) Close() (err error) {
// 	 c.Stream.SetDeadline(time.Now())
// return c.Stream.Close()
// }

func WrapQuic(ctx context.Context, c *quic.Conn) (qc *QuicConn, err error) {
	stream, err := c.AcceptStream(ctx)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	qc = &QuicConn{
		Conn:   c,
		Stream: stream,
	}

	// qc.ConnectionState()
	return
}

func WrapQuicClient(ctx context.Context, c *quic.Conn) (qc *QuicConn, err error) {
	stream, err := c.OpenStreamSync(ctx)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	qc = &QuicConn{
		Conn:   c,
		Stream: stream,
	}

	// qc.ConnectionState()
	return
}
