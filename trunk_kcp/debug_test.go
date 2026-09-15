package trunk_kcp

import (
	"fmt"
	"testing"
)

func TestDebugFormat(t *testing.T) {
	h := Header{ConnID: 0, Cmd: 1, Len: 5}
	bs := h.Format(make([]byte, CmdHeaderSize))
	fmt.Printf("Format output len: %d, bytes: %#v\n", len(bs), bs)

	// 计算 ConnIDMask
	ConnIDMask := uint16(0x8000 << (0x8000 << h.Cmd))
	fmt.Printf("ConnIDMask for Cmd=%d: 0x%04X\n", h.Cmd, ConnIDMask)

	got, l := ParseHeader(bs)
	fmt.Printf("ParseHeader result: ConnID=%d, Cmd=%d, len=%d\n", got.ConnID, got.Cmd, l)
}
