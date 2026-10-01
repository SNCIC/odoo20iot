package cluster

import (
	"testing"
	"time"
)

// TestDedup_MarkThenSeen 验证 mark 后 seen 命中，且不同键互不干扰。
func TestDedup_MarkThenSeen(t *testing.T) {
	d := newDedupCache()

	if d.seen("n1", 1) {
		t.Fatal("未 mark 的键不应命中")
	}

	d.mark("n1", 1)
	if !d.seen("n1", 1) {
		t.Fatal("mark 后应命中")
	}
	if d.seen("n1", 2) {
		t.Fatal("同 origin 不同 seq 不应命中")
	}
	if d.seen("n2", 1) {
		t.Fatal("不同 origin 同 seq 不应命中")
	}
}

// TestDedup_Expiry 验证 TTL 到期后不再命中。
func TestDedup_Expiry(t *testing.T) {
	d := newDedupCache()
	d.ttl = 20 * time.Millisecond

	d.mark("n1", 1)
	if !d.seen("n1", 1) {
		t.Fatal("TTL 内应命中")
	}

	time.Sleep(40 * time.Millisecond)
	if d.seen("n1", 1) {
		t.Fatal("TTL 到期后不应再命中")
	}
}

// TestDedup_OverflowClears 验证超限时整体清空（best-effort，不阻塞、不无限增长）。
func TestDedup_OverflowClears(t *testing.T) {
	d := newDedupCache()
	d.max = 2

	d.mark("n1", 1)
	d.mark("n1", 2)
	d.mark("n1", 3) // 触发超限清空

	// 清空后旧键不应再命中，但新的 mark 仍可用。
	if d.seen("n1", 1) {
		t.Fatal("超限清空后旧键不应命中")
	}
	d.mark("n2", 1)
	if !d.seen("n2", 1) {
		t.Fatal("清空后缓存应仍可写入新键")
	}
}
