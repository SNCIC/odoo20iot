// Package rollup 负责把原始时序**增量物化**成预聚合表（B1 补充项（2）· 02 §7）。
//
// 职责划分：
//   - 本包只管「什么时候物化哪个窗口」与「水位怎么走」；
//   - 真正的 INSERT…SELECT 由 Materializer 执行（生产上是 internal/tsdb/greptimedb.Store）；
//   - 水位落 PG（见 internal/pg/migrations/0005_rollup_watermark.sql）。
//
// 幂等性的两层保证（缺一不可）：
//  1. 预聚合表不设 append_mode，同键重写=覆盖（实测见 greptimedb/capability_test.go 探针 P-b）；
//  2. 水位**只在写入成功后**推进；若「写成功但推进失败」，下轮原地重放，靠第 1 层兜住。
package rollup

import (
	"context"
	"time"
)

// Watermark 是某个粒度的物化进度。
type Watermark struct {
	Granularity string
	WindowStart time.Time
	UpdatedAt   time.Time
	LastError   string
}

// Store 是水位账本的持久化契约。
type Store interface {
	// Get 读取水位；found=false 表示账本里还没有该粒度的记录（首次运行）。
	Get(ctx context.Context, granularity string) (Watermark, bool, error)
	// Advance 推进水位。**只进不退**（并发/乱序调用时取较大值）。
	// 只允许在窗口写入成功之后调用 —— 反过来会让窗口被永久跳过。
	Advance(ctx context.Context, granularity string, windowStart time.Time) error
	// RecordError 记录失败原因，**不得**触碰 window_start。
	RecordError(ctx context.Context, granularity, errText string) error
}
