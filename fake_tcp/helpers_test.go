package fake_tcp

import (
	"encoding/binary"
)

// helpers_test.go：各测试文件共享的序号块传输辅助。
// 语义约定：本层不保证可靠/有序（plan.md §1.3），checker 统计 loss/dup/reorder/corrupt，
// 由调用方按场景断言（管道场景要求全零；真实链路场景容忍少量 loss/reorder）。

// seqStats 序号块传输统计
type seqStats struct {
	loss, dup, reorder, corrupt int
	chunks                      int
}

// writeSeqChunks 写入 n 个序号块：[uint32(base+序号)][序号派生填充...]
func writeSeqChunks(c *Conn, chunk, n int, base uint32) error {
	buf := make([]byte, chunk)
	for i := 0; i < n; i++ {
		seq := base + uint32(i)
		binary.LittleEndian.PutUint32(buf[:4], seq)
		for j := 4; j < chunk; j++ {
			buf[j] = byte(int(seq) + j)
		}
		if _, err := c.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

// checkSeqChunks 读出序号为 [base, base+n) 的 n 个块并统计。
func checkSeqChunks(c *Conn, chunk, n int, base uint32) (seqStats, error) {
	var st seqStats
	seen := make(map[uint32]bool)
	expect := base
	lastIdx := int64(-1)
	tmp := make([]byte, 64*1024)
	for expect < base+uint32(n) {
		m, err := c.Read(tmp)
		if err != nil {
			return st, err
		}
		if m%chunk != 0 {
			st.corrupt++
			continue
		}
		for off := 0; off+chunk <= m; off += chunk {
			b := tmp[off : off+chunk]
			idx := binary.LittleEndian.Uint32(b[:4])
			good := true
			for j := 4; j < chunk; j++ {
				if b[j] != byte(int(idx)+j) {
					good = false
					break
				}
			}
			if !good {
				st.corrupt++
				continue
			}
			if seen[idx] {
				st.dup++
				continue
			}
			seen[idx] = true
			if int64(idx) < lastIdx {
				st.reorder++
			}
			lastIdx = int64(idx)
			if idx == expect {
				expect++
			} else if idx > expect {
				st.loss += int(idx - expect)
				expect = idx + 1
			}
			st.chunks++
		}
	}
	return st, nil
}
