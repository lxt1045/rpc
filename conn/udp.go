package conn

import (
	"context"
	"net"
	"sync"
)

var (
	//connsMap 用于保存连接相关的上下文
	udpConns sync.Map //map[connID uint64]*Conn  //用于保存网络连接
)

func consistentHash(addr *net.UDPAddr) uint64 {
	//binary.LittleEndian.PutUint16(ret[4:6], uint16(addr.Port))
	b := addr.IP.To4()

	//最好加上serviceID，以保证在所有Services中保持唯一！！！
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
		uint64(b[3])<<24 | uint64(uint16(addr.Port))<<32
}

// UdpConn 每条连接建立一个Conn
type UdpConn struct {
	Addr *net.UDPAddr //只在创建的时候写入，其他时候只读，所以不加锁读
	ln   *net.UDPConn

	cache []byte

	ChRead     chan []byte
	sync.Mutex            //kcp操作锁,因为有可能并发创建kcp,所以必须在Conn创建之初创建锁
	W          sync.Mutex //kcp操作锁,因为有可能并发创建kcp,所以必须在Conn创建之初创建锁
}

func ToUdpConn(ctx context.Context, addr *net.UDPAddr, ln *net.UDPConn) (c *UdpConn, isFirstTime bool) {
	connID := consistentHash(addr)
	I, ok := udpConns.Load(connID) //先用Load，而不是用LoadOrStore()，因为后者每次都生成一个数据结构成本有点高
	if ok {
		if p, ok := I.(*UdpConn); ok {
			return p, false
		}
	}
	p := &UdpConn{
		Addr:   addr,
		ln:     ln,
		ChRead: make(chan []byte, 256),
	}
	I, loaded := connsMap.LoadOrStore(connID, p) //为了强一致性，用LoadOrStore()
	if loaded {
		if p, ok := I.(*UdpConn); ok {
			return p, false
		}
	}
	return p, true
}

func (c *UdpConn) Read(p []byte) (n int, err error) {
	c.Lock()
	defer c.Unlock()

	if len(c.cache) > 0 {
		n = copy(p, c.cache)
		c.cache = c.cache[n:]
		return
	}

	buf := <-c.ChRead

	n = copy(p, buf)
	c.cache = append(c.cache, buf[n:]...)
	return
}
func (c *UdpConn) Write(p []byte) (n int, err error) {
	c.W.Lock()
	defer c.W.Unlock()

	// n, err = c.Conn.Write(p)
	n, err = c.ln.WriteToUDP(p, c.Addr)
	if err != nil {
	}
	return
}
func (c *UdpConn) Close() error {
	return c.ln.Close()
}

// UdpConn 每条连接建立一个Conn
type UdpConnCli struct {
	Conn       net.Conn
	cache      []byte
	sync.Mutex            //kcp操作锁,因为有可能并发创建kcp,所以必须在Conn创建之初创建锁
	W          sync.Mutex //kcp操作锁,因为有可能并发创建kcp,所以必须在Conn创建之初创建锁
}

func (c *UdpConnCli) Read(p []byte) (n int, err error) {
	c.Lock()
	defer c.Unlock()

	if len(c.cache) > 0 {
		n = copy(p, c.cache)
		c.cache = append(c.cache[:0], c.cache[n:]...)
		return
	}

	if c.cache == nil {
		c.cache = make([]byte, UdpBufLen)
	}
	c.cache = c.cache[:cap(c.cache)]
	n, err = c.Conn.Read(c.cache)
	if err != nil || n <= 0 {
		return
	}
	c.cache = c.cache[:n]
	n = copy(p, c.cache)
	c.cache = append(c.cache[:0], c.cache[n:]...)
	return
}
func (c *UdpConnCli) Write(p []byte) (n int, err error) {
	c.W.Lock()
	defer c.W.Unlock()

	// n, err = c.Conn.Write(p)
	n, err = c.Conn.Write(p)
	if err != nil {
	}
	return
}
func (c *UdpConnCli) Close() error {
	return c.Conn.Close()
}
