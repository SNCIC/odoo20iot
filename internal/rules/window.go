package rules

import (
	"sort"
	"sync"
	"time"
)

// WindowBuffer 是规则层的滑动窗口聚合器。
// 它只保存数值样本，不持久化；重启后的窗口由上游重新填充，避免把缓存误当事实数据。
type WindowBuffer struct {
	mu       sync.Mutex
	maxAge   time.Duration
	maxItems int
	items    map[string][]windowPoint
}

type windowPoint struct {
	ts    time.Time
	value float64
}

func NewWindowBuffer(maxAge time.Duration, maxItems int) *WindowBuffer {
	if maxAge <= 0 {
		maxAge = time.Minute
	}
	if maxItems <= 0 {
		maxItems = 4096
	}
	return &WindowBuffer{maxAge: maxAge, maxItems: maxItems, items: make(map[string][]windowPoint)}
}

func (w *WindowBuffer) Observe(key string, ts time.Time, value float64) {
	if key == "" || ts.IsZero() {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	points := append(w.items[key], windowPoint{ts: ts, value: value})
	cutoff := ts.Add(-w.maxAge)
	start := 0
	for start < len(points) && points[start].ts.Before(cutoff) {
		start++
	}
	if start > 0 {
		points = append([]windowPoint(nil), points[start:]...)
	}
	if len(points) > w.maxItems {
		points = points[len(points)-w.maxItems:]
	}
	w.items[key] = points
}

// Values 返回可直接绑定到规则 env.window 的聚合值。
func (w *WindowBuffer) Values(key string, now time.Time) map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	points := w.items[key]
	cutoff := now.Add(-w.maxAge)
	start := 0
	for start < len(points) && points[start].ts.Before(cutoff) {
		start++
	}
	points = points[start:]
	if len(points) == 0 {
		return map[string]any{"count": int64(0)}
	}
	values := make([]float64, 0, len(points))
	var sum, max, min float64
	for i, p := range points {
		if i == 0 || p.value > max {
			max = p.value
		}
		if i == 0 || p.value < min {
			min = p.value
		}
		sum += p.value
		values = append(values, p.value)
	}
	sort.Float64s(values)
	return map[string]any{"avg": sum / float64(len(values)), "max": max, "min": min, "count": int64(len(values)), "last": points[len(points)-1].value, "p50": values[len(values)/2]}
}
