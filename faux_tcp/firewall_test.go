//go:build linux

package faux_tcp

import (
	"strings"
	"testing"
)

// TestRSTDropRuleUsesRawTable RST 抑制规则必须装在 raw 表（conntrack 之前）。
// 若装到 filter 表：内核 RST 虽被丢弃，但 conntrack 已把它记为 RST/CLOSED，
// 之后用户态发出的 SYN+ACK 会被判 INVALID，NAT 不再为其做地址转换，
// 客户端表现为"服务端发了 SYN+ACK 却收不到"（真机踩过）。
func TestRSTDropRuleUsesRawTable(t *testing.T) {
	if rstDropTable != "raw" {
		t.Fatalf("rstDropTable = %q, want raw", rstDropTable)
	}
	rule := iptablesRSTRule(18099)
	joined := strings.Join(rule, " ")
	if !strings.Contains(joined, "--sport 18099") ||
		!strings.Contains(joined, "--tcp-flags RST RST") ||
		!strings.Contains(joined, "-j DROP") {
		t.Fatalf("unexpected rule: %v", rule)
	}
}
