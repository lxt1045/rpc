package conn

import (
	"container/list"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/utils/log"
	"github.com/xtaci/kcp-go"
)

// IP协议规定路由器最少能转发：512数据+60IP首部最大+4预留=576字节,即最少可以转发512-8=504字节UDP数据
// 内网一般1500字节,UDP：1500-IP(20)-UDP(8)=1472字节数据
const (
	UdpBufLen        = 1472  //UDP发送、接收，kcp发送、接收缓冲的大小
	ioChanLenDefault = 10240 //当config没有设置时，使用此默认值
)

var (
	//connsMap 用于保存连接相关的上下文
	connsMap = &sync.Map{} //map[connID uint64]*Conn  //用于保存网络连接

	once        sync.Once
	updataList  = list.New()                         //用于存放kcp，然后循环调用kcp.Updata(); updata()处理较快，单goroutine即可
	updataAddCh = make(chan *Conn, ioChanLenDefault) //单协程操作list，不需要加锁，通过chan同步(加入、删除)
	updataDelCh = make(chan *list.Element, ioChanLenDefault)
)

var (
	ErrBufferTooSmall = errors.NewCode(0, 10001, "buffer is too small")
)

func UpdataAdd(c *Conn) {
	updataAddCh <- c
}
func UpdataDel(c *Conn) {
	// updataDelCh <- c
}

// Conn 每条连接建立一个Conn
type Conn struct {
	Addr   *net.UDPAddr //只在创建的时候写入，其他时候只读，所以不加锁读
	connID uint64

	KCP         *kcp.KCP      //kcp线程不安全，所以必须加锁
	listE       *list.Element //kcp updata()的list,delete的时候要用到，在加入list的时候更新
	RefleshTime int64         //每次收到数据包，就更新这个时间，在kcp_updata()的时候检查这个时间，超时则删除
	M           int           // KCP 是否又可接收的完整数据

	buf []byte

	*sync.Mutex //kcp操作锁,因为有可能并发创建kcp,所以必须在Conn创建之初创建锁
}

func (c Conn) Read(p []byte) (n int, err error) {
	// c.Lock()
	// defer c.Unlock()
	// n = c.KCP.Recv(p)
	// if c.M < 0 {
	// 	return
	// }
	// if n <= 0 {
	// 	if n == -3 {
	// 		err = ErrBufferTooSmall
	// 	}
	// 	return
	// }
	// c.M = n

	//
	buf := make([]byte, UdpBufLen)
	for n <= 0 {
		if len(c.buf) > 0 {
			n = copy(p, c.buf)
			c.buf = c.buf[n:]
			return
		}

		n = func() (n int) {
			c.Lock()
			defer c.Unlock()
			if c.M < 0 {
				return
			}
			for n >= 0 {
				buf = buf[:cap(buf)]
				n = c.KCP.Recv(buf)
				if n == -3 || n == -2 {
					buf = make([]byte, len(buf)*2)
					n = 0
					continue
				}
				break
			}
			if n <= 0 {
				return
			}
			buf = buf[:n]
			n = copy(p, buf)
			c.buf = buf[n:]
			buf = make([]byte, UdpBufLen)
			return
		}()
		if n > 0 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	c.M = n
	return
}
func (c Conn) Write(p []byte) (n int, err error) {
	n = c.KCP.Send(p)
	return
}
func (c Conn) Close() error {

	return nil
}

type KcpConfig struct {
	KcpUpdataTime int64
	MaxIdleTime   int64
}

func KcpUpdata(ctx context.Context, conf KcpConfig) {
	once.Do(func() {
		kcpUpdata(ctx, conf)
	})
}

// kcpUpdataRun保证kcpUpdata()全局以goroutine形式启动一次
func kcpUpdata(ctx context.Context, conf KcpConfig) {
	if conf.KcpUpdataTime == 0 {
		conf.KcpUpdataTime = 10
	}
	if conf.MaxIdleTime == 0 { //< 10 {
		conf.MaxIdleTime = 10 //最少10s
	}
	for {
		deadTime := time.Now().Unix() - conf.MaxIdleTime //认定僵死状态的时间
		select {
		case <-time.After(time.Second * time.Duration(conf.KcpUpdataTime)):
			for e := updataList.Front(); e != nil; e = e.Next() {
				if e.Value == nil {
					continue
				}
				conn, ok := e.Value.(*Conn)
				if !ok {
					continue
				}
				if atomic.LoadInt64(&conn.RefleshTime) < deadTime {
					removeConn(conn) //该连接已经凉了
					continue
				}
				conn.Lock() //kcp.Check() 和 kcp.Update() 线程不安全，，，
				if conn.KCP != nil && conn.KCP.Check() <= uint32(time.Now().UnixMilli()) {
					conn.KCP.Update() //kcp.Check()比kcp.Update()轻量
				}
				conn.Unlock()
			}
		case conn := <-updataAddCh:
			conn.Lock()
			if conn.listE != nil {
				log.Ctx(ctx).Error().Msgf("conn.listE is not nil, remove before PushBack, conn:%v", conn)
				updataList.Remove(conn.listE)
			}
			conn.listE = updataList.PushBack(conn)
			conn.Unlock()
		case ekcp := <-updataDelCh:
			updataList.Remove(ekcp)
		}
	}
	log.Ctx(ctx).Fatal().Caller().Msg("udp.kcpUpdata exit")
}

// kcp send()之后,会在flush()的时候调用这里的函数,将数据包通过udp 发送出去
func KcpOutoput(ctx context.Context, ln *net.UDPConn, addr *net.UDPAddr) (f func([]byte, int)) {
	f = func(buf []byte, size int) {
		//发送数据成熟时的异步回调函数，成熟数据为：buf[:size]，在这里把数据通过UDP发送出去
		n, err := ln.WriteToUDP(buf[:size+1], addr)
		if err != nil || n != size+1 {
			log.Ctx(ctx).Error().Err(err).Msgf("error during kcp send:%v,n:%d\n", err, n)
		}
	}
	return
}

// 通过*net.UDPAddr查找或生成一个*Conn，保证返回可用结果
func Addr2Conn(addr *net.UDPAddr) (conn *Conn, isFirstTime bool) {
	connID := consistentHash(addr)
	I, ok := connsMap.Load(connID) //先用Load，而不是用LoadOrStore()，因为后者每次都生成一个数据结构成本有点高
	if ok {
		if p, ok := I.(*Conn); ok {
			return p, false
		}
	}
	p := &Conn{
		Addr:   addr,
		connID: connID,
		KCP:    nil,             //kcp.NewKCP(),//在第一次收到可靠连接的时候创建
		Mutex:  new(sync.Mutex), //kcp操作锁,因为有可能并发创建kcp,所以必须在Conn创建之初创建锁
	}
	I, loaded := connsMap.LoadOrStore(connID, p) //为了强一致性，用LoadOrStore()
	if loaded {
		if p, ok := I.(*Conn); ok {
			return p, false
		}
	}
	return p, true
}

// 通过*net.UDPAddr查找*Conn，找不到则返回nil
func ConnID2Conn(connID uint64) (conn *Conn) {
	I, ok := connsMap.Load(connID)
	if !ok {
		return nil
	}

	conn, ok = I.(*Conn)
	if !ok {
		connsMap.Delete(connID)
		return nil
	}
	return
}

// 通过*net.UDPAddr查找*Conn，找不到则返回nil
func connIDRemoveConn(connID uint64) {
	I, ok := connsMap.Load(connID)
	if !ok {
		return
	}
	conn, ok := I.(*Conn)
	if !ok {
		connsMap.Delete(connID)
		return
	}
	removeConn(conn)
	return
}
func removeConn(conn *Conn) {
	conn.Lock()
	updataDelCh <- conn.listE //通过更新list删除自己
	conn.listE = nil
	conn.KCP = nil //清空kcp
	connsMap.Delete(conn.connID)
	conn.Unlock()
	return
}
func addrRemoveConn(addr *net.UDPAddr) {
	connID := consistentHash(addr)
	connIDRemoveConn(connID)
}

// client

// KcpCli 每条连接建立一个Conn
type KcpCli struct {
	net.Conn
	ID          int
	KCP         *kcp.KCP //kcp线程不安全，所以必须加锁
	RefleshTime int64    //每次收到数据包，就更新这个时间，在kcp_updata()的时候检查这个时间，超时则删除
	M           int      // KCP 是否又可接收的完整数据

	buf []byte

	*sync.Mutex //kcp操作锁,因为有可能并发创建kcp,所以必须在Conn创建之初创建锁
}

func KcpOutoputByConn(ctx context.Context, conn net.Conn) (f func([]byte, int)) {
	f = func(buf []byte, size int) {
		//发送数据成熟时的异步回调函数，成熟数据为：buf[:size]，在这里把数据通过UDP发送出去
		//func copy(dst, src []Type) int //The source and destination may overlap.
		n, err := conn.Write(buf[:size+1])
		if err != nil || n != size+1 {
			log.Ctx(ctx).Error().Err(err).Caller().Msgf("error during kcp send:%v,n:%d\n", err, n)
		}
	}
	return
}
func NewKcpCli(ctx context.Context, conv uint32, conn net.Conn) (c *KcpCli) {
	c = &KcpCli{
		Conn:  conn,
		KCP:   kcp.NewKCP(conv, KcpOutoputByConn(ctx, conn)),
		Mutex: new(sync.Mutex),
	}
	c.KCP.WndSize(128, 128) //设置最大收发窗口为128
	// 第二个参数 nodelay-启用以后若干常规加速将启动
	// 第三个参数 interval为内部处理时钟，默认设置为 10ms
	// 第四个参数 resend为快速重传指标，设置为2
	// 第五个参数 为是否禁用常规流控，这里禁止
	// c.KCP.NoDelay(0, 10, 0, 0) // 默认模式
	// c.KCP.NoDelay(0, 10, 0, 1) // 普通模式，关闭流控等
	c.KCP.NoDelay(1, 10, 2, 1) // 启动快速模式

	c.KCP.Update() //要调用一次？？？

	go c.update(ctx) //要调用一次？？？
	return
}
func (c KcpCli) update(ctx context.Context) (n int, err error) {
	for {
		func() {
			c.Lock()
			defer c.Unlock()

			checkT := c.KCP.Check()
			nowMs := uint32(time.Now().UnixMilli())
			if checkT <= nowMs {
				c.KCP.Update()
			}
		}()
		time.Sleep(time.Millisecond * 10)
	}

}

func (c KcpCli) Read(p []byte) (n int, err error) {
	buf := make([]byte, UdpBufLen)
	for n <= 0 {
		if len(c.buf) > 0 {
			n = copy(p, c.buf)
			c.buf = c.buf[n:]
			return
		}
		n, err = c.Conn.Read(buf)
		if err != nil || n <= 0 {
			if n == -3 {
				err = ErrBufferTooSmall
			}
			return
		}

		n = func() (n int) {
			c.Lock()
			defer c.Unlock()
			n = c.KCP.Input(buf[:n], true, false)
			for n >= 0 {
				n = c.KCP.Recv(buf)
				if n == -3 || n == -2 {
					buf = make([]byte, len(buf)*2)
					n = 0
					continue
				}
				break
			}
			if n <= 0 {
				return
			}
			buf = buf[:n]
			n = 0
			if len(c.buf) > 0 {
				n = copy(p, c.buf)
				c.buf = nil
			}
			m := copy(p, buf)
			c.buf = buf[m:]
			n += m
			buf = make([]byte, UdpBufLen)
			return
		}()
		if n > 0 {
			return
		}
	}
	return
}
func (c KcpCli) Write(p []byte) (n int, err error) {
	n = c.KCP.Send(p)
	// c.KCP.Update() //要调用一次？？？
	return
}
func (c KcpCli) Close() error {

	return nil
}
