package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

func TestConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	addr := testService(ctx, t, wg)

	time.Sleep(time.Millisecond * 10)
	testClient(t, addr)

	// wg.Wait()
}

func testService(ctx context.Context, t *testing.T, wg *sync.WaitGroup) (addr string) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			// 接收输入流
			conn, err := ln.Accept()
			if err != nil {
				t.Errorf("ln.Accept: %v", err)
				return
			}
			wg.Add(1)
			// 处理流程尽量要和Listen流程分开，避免相互影响
			go func(conn net.Conn) {
				defer conn.Close()
				defer wg.Done()

				buf := make([]byte, 0xff)
				n, err := conn.Read(buf)
				if err != nil {
					if errors.Is(err, io.EOF) {
						log.Printf("testService read:%s", err)
						return
					}
					panic(err)
				}

				// log.Printf("testService read:%s", string(buf[:n]))
				r := rand.Int31n(1000)
				time.Sleep(time.Millisecond * time.Duration(10+r))
				log.Printf("testService write:%s", string(buf[:n]))
				_, err = conn.Write(buf[:n])
				if err != nil {
					log.Printf("testService write:%s", err)
					return
				}
			}(conn)
		}
	}()
	addr = ln.Addr().String()
	return
}

func testClient(t *testing.T, addr string) {
	conns := make([]net.Conn, 0, 100)
	defer func() {
		for _, c := range conns {
			err := c.Close()
			if err != nil {
				log.Printf("testClient Close:%s", err)
			}
		}
	}()
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		str := fmt.Sprintf("hello %d", i)
		_, err = conn.Write([]byte(str))
		if err != nil {
			log.Printf("testClient write:%s", err)
			continue
		}
		conns = append(conns, conn)
	}
	buf := make([]byte, 0xff)
	for {
		for _, conn := range conns {
			// SetReadDeadline 可以设置读超时，如果 Read 读超时会报 "i/o timeout" 错误。
			// 这样可以实现用一个 goroutine 读多条 conn 的数据
			conn.SetReadDeadline(time.Now().Add(1))
			n, err := conn.Read(buf)
			if err != nil {
				if t, ok := err.(interface{ Timeout() bool }); ok && t.Timeout() {
					// log.Printf("testClient read:%s", "timeout")
					continue
				}
				if errors.Is(err, io.EOF) {
					log.Printf("testClient read:%s", err)
					continue
				}
				log.Printf("testClient read:%s", err)
				continue
			}
			log.Printf("testClient read:%s", string(buf[:n]))
			return
		}
	}
}
