package rules

import (
	"testing"
	"time"
)

func TestWindowBufferAggregatesAndExpires(t *testing.T) {
	base := time.Unix(1000, 0)
	w := NewWindowBuffer(time.Minute, 10)
	w.Observe("p1:d1:t", base.Add(-30*time.Second), 10)
	w.Observe("p1:d1:t", base.Add(-10*time.Second), 20)
	got := w.Values("p1:d1:t", base)
	if got["avg"] != float64(15) || got["max"] != float64(20) || got["min"] != float64(10) || got["count"] != int64(2) {
		t.Fatalf("聚合结果错误: %#v", got)
	}
	got = w.Values("p1:d1:t", base.Add(2*time.Minute))
	if got["count"] != int64(0) {
		t.Fatalf("过期样本未清理: %#v", got)
	}
}
