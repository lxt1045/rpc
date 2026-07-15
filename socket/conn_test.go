package socket

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lxt1045/utils/log"
)

func TestPipe(t *testing.T) {
	ctx := context.Background()
	go Listen(ctx, "tcp", ":1234")

	go Listen(ctx, "tcp", "127.0.0.1:1234")

	go Dial(ctx, "tcp", "127.0.0.1:1234", "127.0.0.1:1234")
	go Dial(ctx, "tcp", "localhost:1234", "localhost:1234")

	time.Sleep(time.Second * 3)
}

func TestListen(t *testing.T) {
	ctx := context.Background()
	addr := ":1234"

	ln, err := Listen(ctx, "tcp", addr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
		return
	}

	buf := make([]byte, 1024)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("accept failed", err)
			continue
		}
		for {
			n, err := conn.Read(buf)
			if err != nil {
				fmt.Println("read failed", err)
				break
			}

			log.Ctx(ctx).Error().Caller().Str("read", string(buf[:n])).
				Str("local", addr).Str("remote", conn.RemoteAddr().String()).Send()
		}
	}
}

func TestConnect(t *testing.T) {
	ctx := context.Background()
	lnaddr, addr := ":1234", "localhost:1234"
	ch := make(chan bool, 1)
	go func(addr string) {
		ln, err := Listen(ctx, "tcp4", addr)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
			return
		}
		ch <- true
		buf := make([]byte, 1024)
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("accept failed", err)
				continue
			}
			for {
				n, err := conn.Read(buf)
				if err != nil {
					fmt.Println("read failed", err)
					break
				}
				log.Ctx(ctx).Error().Caller().Str("read", string(buf[:n])).
					Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()
			}
		}
	}(lnaddr)

	<-ch

	conn, err := Dial(ctx, "tcp4", "10088", addr)
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
		return
	}
	go func() {
		buf := make([]byte, 1024)
		for {
			buf1 := []byte("hello")
			n, err := conn.Write(buf1)
			if err != nil {
				fmt.Println("read failed", err)
				break
			}
			_ = n
			log.Ctx(ctx).Error().Caller().Str("write", string(buf1)).
				Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()

			n, err = conn.Read(buf)
			if err != nil {
				fmt.Println("read failed", err)
				break
			}

			log.Ctx(ctx).Error().Caller().Str("read", string(buf[:n])).
				Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()
		}
	}()

	laddr := conn.LocalAddr()
	log.Ctx(ctx).Error().Caller().Str("laddr", laddr.String()).Msg("listen failed")

	addrs := strings.Split(laddr.String(), ":")
	go func(addr string) {
		ln, err := Listen(ctx, "tcp4", addr)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
			return
		}
		ch <- true
		buf := make([]byte, 1024)
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("accept failed", err)
				continue
			}
			for {
				n, err := conn.Read(buf)
				if err != nil {
					fmt.Println("read failed", err)
					break
				}
				log.Ctx(ctx).Error().Caller().Str("read", string(buf[:n])).
					Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()
			}
		}
	}(":" + addrs[len(addrs)-1])
	<-ch
	go func(addr string) {
		conn, err := Dial(ctx, "tcp4", "10088", addr)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Msg("listen failed")
			return
		}
		buf := make([]byte, 1024)
		for {
			buf1 := []byte("hello222")
			n, err := conn.Write(buf1)
			if err != nil {
				fmt.Println("read failed", err)
				break
			}
			_ = n
			log.Ctx(ctx).Error().Caller().Str("write", string(buf1)).
				Str("local", conn.LocalAddr().String()).Str("remote", conn.RemoteAddr().String()).Send()

			n, err = conn.Read(buf)
			if err != nil {
				fmt.Println("read failed", err)
				break
			}

			log.Ctx(ctx).Error().Caller().Str("read", string(buf[:n])).
				Str("local", addr).Str("remote", conn.RemoteAddr().String()).Send()
		}
	}(laddr.String())

	time.Sleep(time.Second * 1)
}
