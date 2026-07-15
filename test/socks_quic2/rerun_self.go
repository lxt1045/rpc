package socks

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"time"

	"github.com/lxt1045/utils/log"
	"github.com/shirou/gopsutil/process"
)

func RerunAfter(pid int) (err error) {
	exePath, err := os.Executable()
	if err != nil {
		return
	}

	p := path.Dir(exePath)
	logf := path.Join(p, "proxy.log")

	// nohup ./proxy > proxy.log 2>&1 &
	c := exec.Command(exePath, fmt.Sprintf("rerun=%d", pid))
	// if runtime.GOOS != "windows" {
	// 	// c = exec.Command("nohup", exePath, fmt.Sprintf("rerun=%d", pid), ">", logf, "2>&1", "&")
	// 	c = exec.Command("nohup", exePath, ">", logf, "2>&1", "&")
	// }

	// c.Stdout = os.Stdout
	// c.Stderr = os.Stderr
	c.Dir = p
	log.Ctx(context.Background()).Info().Caller().Msgf("path:%s", logf)
	return c.Start() // c.Run()
}

func CheckMemLoop(mb uint64) {
	const MB = 1024 * 1024
	for {
		time.Sleep(time.Second * 10)

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		if ms.Alloc > mb*MB {
			log.Ctx(context.Background()).Error().Caller().Msgf(
				"exit(); Alloc:%d(bytes) HeapIdle:%d(bytes) HeapReleased:%d(bytes)",
				ms.Alloc, ms.HeapIdle, ms.HeapReleased)

			pid := os.Getpid()
			err := RerunAfter(pid)
			if err != nil {
				log.Ctx(context.Background()).Error().Caller().Err(err).Send()
				continue
			}
			log.Ctx(context.Background()).Info().Caller().Err(err).Msg("os.Exit(0)")
			os.Exit(0)
		}
		log.Ctx(context.Background()).Info().Caller().Msgf(
			"CheckMemLoop; Alloc:%d(bytes) HeapIdle:%d(bytes) HeapReleased:%d(bytes)",
			ms.Alloc, ms.HeapIdle, ms.HeapReleased)
	}
}
func CheckProcess(ctx context.Context, pidOld int) {
	// exePath, err := os.Executable()
	// if err != nil {
	// 	log.Ctx(ctx).Error().Caller().Err(err).Send()
	// 	return
	// }
	pid := os.Getpid()
	p0, err := process.NewProcessWithContext(ctx, int32(pid))
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	name, err := p0.Name()
	if err != nil {
		log.Ctx(ctx).Error().Caller().Err(err).Send()
		return
	}
	log.Ctx(ctx).Info().Caller().Msgf("started! pid:%d, name:%s", pid, name)

	for i := 0; i < 100; i++ {
		processes, err := process.ProcessesWithContext(ctx)
		if err != nil {
			log.Ctx(ctx).Error().Caller().Err(err).Send()
			continue
		}
		found := false
		for _, p := range processes {
			if pidOld != 0 && p.Pid == int32(pidOld) {
				found = true
				break
			}
			pName, err := p.Name()
			if err != nil {
				log.Ctx(ctx).Error().Caller().Err(err).Send()
				continue
			}
			if p.Pid != p0.Pid && name == pName {
				found = true
				break
			}
		}
		if !found {
			break
		}
		time.Sleep(time.Millisecond * 300)
	}
}

func traceMemStats() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	log.Ctx(context.Background()).Error().Caller().Msgf("Alloc:%d(bytes) HeapIdle:%d(bytes) HeapReleased:%d(bytes)", ms.Alloc, ms.HeapIdle, ms.HeapReleased)
}

/*
type MemStats struct {

	// 当前堆上对象的内存分配大小, 同HeapAlloc字段, 单位 bytes
	Alloc uint64

	// 历史总的累计分配内存大小
	TotalAlloc uint64

	// 从操作系统分配的内存大小
	Sys uint64

	// 记录指针索引性能, go 语言内部使用
	Lookups uint64

	// 堆上分配的对象数量
	Mallocs uint64

	// 堆上剩余的内存大小
	Frees uint64

	HeapAlloc uint64

	// 从操作系统分配的 堆 内存大小
	HeapSys uint64

	// 未使用的空闲内存分片大小 spans
	HeapIdle uint64

	// 使用中的内存分片大小
	HeapInuse uint64

	// 回退的内存大小
	HeapReleased uint64

	// 堆上分配的对象数量
	HeapObjects uint64

	// 栈上使用的内存片大小
	StackInuse uint64

	// 从操作系统分配的栈的内存大小
	StackSys uint64

	// MSpanInuse is bytes of allocated mspan structures.
	MSpanInuse uint64

	// MSpanSys is bytes of memory obtained from the OS for mspan
	// structures.
	MSpanSys uint64

	// MCacheInuse is bytes of allocated mcache structures.
	MCacheInuse uint64

	// MCacheSys is bytes of memory obtained from the OS for
	// mcache structures.
	MCacheSys uint64

	// BuckHashSys is bytes of memory in profiling bucket hash tables.
	BuckHashSys uint64

	// GCSys is bytes of memory in garbage collection metadata.
	GCSys uint64

	// OtherSys is bytes of memory in miscellaneous off-heap
	// runtime allocations.
	OtherSys uint64

	// 在多大的堆内存时, 触发GC
	NextGC uint64

	// 上次GC 时间
	LastGC uint64

	// PauseTotalNs is the cumulative nanoseconds in GC
	// stop-the-world pauses since the program started.
	//
	// During a stop-the-world pause, all goroutines are paused
	// and only the garbage collector can run.
	PauseTotalNs uint64

	// PauseNs is a circular buffer of recent GC stop-the-world
	// pause times in nanoseconds.
	//
	// The most recent pause is at PauseNs[(NumGC+255)%256]. In
	// general, PauseNs[N%256] records the time paused in the most
	// recent N%256th GC cycle. There may be multiple pauses per
	// GC cycle; this is the sum of all pauses during a cycle.
	PauseNs [256]uint64

	// PauseEnd is a circular buffer of recent GC pause end times,
	// as nanoseconds since 1970 (the UNIX epoch).
	//
	// This buffer is filled the same way as PauseNs. There may be
	// multiple pauses per GC cycle; this records the end of the
	// last pause in a cycle.
	PauseEnd [256]uint64

	// GC 次数
	NumGC uint32

	// 手动调用 GC 的次数
	NumForcedGC uint32

	// GC 使用的 CPU 时间
	GCCPUFraction float64

	// 可以GC,一直是true
	EnableGC bool

	// BySize reports per-size class allocation statistics.
	//
	// BySize[N] gives statistics for allocations of size S where
	// BySize[N-1].Size < S ≤ BySize[N].Size.
	//
	// This does not report allocations larger than BySize[60].Size.
	BySize [61]struct {
		// Size is the maximum byte size of an object in this
		// size class.
		Size uint32

		// Mallocs is the cumulative count of heap objects
		// allocated in this size class. The cumulative bytes
		// of allocation is Size*Mallocs. The number of live
		// objects in this size class is Mallocs - Frees.
		Mallocs uint64

		// Frees is the cumulative count of heap objects freed
		// in this size class.
		Frees uint64
	}
}
//*/
