package trunk

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/lxt1045/rpc"
	"golang.org/x/sync/errgroup"
)

func TestUint16(t *testing.T) {

	x := uint16(100)
	y := x - 200
	t.Logf("%d", y)
}

func TestTrunk0(t *testing.T) {
	ctx := t.Context()

	s0, c0 := rpc.NewFakeConnPipe()

	peer1, peer2 := NewTrunk(s0), NewTrunk(c0)
	go peer1.Run(ctx)
	go peer2.Run(ctx)

	payload11, payload12 := peer1.GetConn(1), peer2.GetConn(1)

	g := errgroup.Group{}
	g.Go(func() (err error) {
		go func() {
			for i := range 3 {
				str := fmt.Sprintf("msg:%d. ", i)
				payload11.Write([]byte(str))
			}
			payload11.Close()
		}()
		bs := make([]byte, math.MaxUint16)
		for i := range 100 {
			n, err := payload12.Read(bs)
			if err != nil {
				t.Error(err)
				break
			}
			t.Logf("connID: %d, i:%d, msg: %s\n", payload11.connID, i, bs[:n])
		}

		return
	})

	err := g.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTrunk(t *testing.T) {
	ctx := t.Context()

	s0, c0 := rpc.NewFakeConnPipe()
	s1, c1 := rpc.NewFakeConnPipe()
	s2, c2 := rpc.NewFakeConnPipe()

	peer1, peer2 := NewTrunk(s0, s1, s2), NewTrunk(c0, c1, c2)
	go peer1.Run(ctx)
	go peer2.Run(ctx)

	payload11, payload12 := peer1.GetConn(1), peer2.GetConn(1)
	payload21, payload22 := peer1.GetConn(2), peer2.GetConn(2)

	nLinePrint := 256
	g := errgroup.Group{}
	g.Go(func() (err error) {
		go func() {
			for i := range 65532 + 100 {
				if i != 0 && i%1000 == 0 {
					time.Sleep(time.Millisecond * time.Duration(rand.Int31n(20)))
				}
				payload11.Write([]byte(fmt.Sprintf("%d ", i)))
			}
			payload11.Close()
		}()
		bs := make([]byte, math.MaxUint16)
		all := []byte{}
		for {
			n, err := payload12.Read(bs)
			if err != nil {
				t.Log(err)
				break
			}
			all = append(all, bs[:n]...)
		}
		for i := 0; i < len(all); i += nLinePrint {
			bs := all[i:]
			if len(bs) > nLinePrint {
				bs = bs[:nLinePrint]
			}
			t.Logf("connID: %d, msg: %s\n", payload11.connID, bs)
		}
		return
	})
	g.Go(func() (err error) {
		go func() {
			for i := range 65532 + 100 {
				if i != 0 && i%1000 == 0 {
					time.Sleep(time.Millisecond * time.Duration(rand.Int31n(20)))
				}
				payload22.Write([]byte(fmt.Sprintf("%d ", i)))
			}
			payload22.Close()
		}()
		bs := make([]byte, math.MaxUint16)
		all := []byte{}
		for {
			n, err := payload21.Read(bs)
			if err != nil {
				t.Log(err)
				break
			}
			all = append(all, bs[:n]...)
		}
		for i := 0; i < len(all); i += nLinePrint {
			bs := all[i:]
			if len(bs) > nLinePrint {
				bs = bs[:nLinePrint]
			}
			t.Logf("connID: %d, msg: %s\n", payload21.connID, bs)
		}
		return
	})

	err := g.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
