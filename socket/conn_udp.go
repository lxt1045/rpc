package socket

import (
	"context"
	"fmt"
	"net"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/utils/log"
)

func ListenPacket(ctx context.Context, network, addr string) (conn net.PacketConn, err error) {
	if network != "udp" && network != "udp4" {
		err = errors.Errorf("network error: %s", network)
		return
	}
	cfg := net.ListenConfig{
		Control: control,
	}
	conn, err = cfg.ListenPacket(ctx, network, addr)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	return
}

func DialPacket(ctx context.Context, network, addr string) (conn *net.UDPConn, err error) {
	if network != "udp" && network != "udp4" {
		err = errors.Errorf("network error: %s", network)
		return
	}
	cfg := net.Dialer{
		Control: control,
	}
	// net.DialUDP("udp", nil, addr)
	c, err := cfg.DialContext(ctx, network, addr)
	if err != nil {
		err = errors.Errorf(err.Error())
		return
	}
	conn, ok := c.(*net.UDPConn)
	if !ok {
		err = errors.Errorf("conn type error: %T", c)
		return
	}
	return
}

func ListenPacketAndRead(ctx context.Context, network, addr string) {
	udp, err := ListenPacket(ctx, network, addr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
		return
	}

	buf := make([]byte, 1024)
	for {
		n, caddr, err := udp.ReadFrom(buf)
		if err != nil {
			fmt.Println("read failed", err)
			continue
		}

		fmt.Println(addr, caddr, string(buf[:n]))
	}
}

func ConnectPacketAndSend(ctx context.Context, network, addr string) {
	// cfg := net.Dialer{
	// 	Control: control,
	// }
	// // net.DialUDP("udp", nil, addr)
	// conn, err := cfg.DialContext(ctx, network, addr)
	// if err != nil {
	// 	log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
	// 	return
	// }

	udpconn, err := DialPacket(ctx, network, addr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
		return
	}
	buf := make([]byte, 1024)
	for {
		n, caddr, err := udpconn.ReadFrom(buf)
		if err != nil {
			fmt.Println("read failed", err)
			break
		}

		fmt.Println(addr, caddr, string(buf[:n]))
	}
}
