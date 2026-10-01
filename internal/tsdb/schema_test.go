package tsdb

import (
	"errors"
	"testing"
	"time"
)

// TestAdaptiveBucket 锁定自适应桶宽的核心不变量：
// **设备数 × ceil(span/bucket) ≤ rowCap**，且桶宽落在可读阶梯上。
func TestAdaptiveBucket(t *testing.T) {
	cases := []struct {
		name    string
		span    time.Duration
		devices int
		rowCap  int
		want    time.Duration
	}{
		{"Q3 形态：10 设备 1h", time.Hour, 10, MaxDetailRows, 10 * time.Second},
		{"10 设备 24h", 24 * time.Hour, 10, MaxDetailRows, 5 * time.Minute},
		{"单设备 1h：0.72s 兜底到 1s 下限", time.Hour, 1, MaxDetailRows, 1 * time.Second},
		{"50 设备 90d：阶梯顶到整天", 90 * 24 * time.Hour, 50, MaxDetailRows, 24 * time.Hour},
		{"跨度小于设备数：兜底 1s", 5 * time.Second, 10, MaxDetailRows, 1 * time.Second},
		{"单设备 6h", 6 * time.Hour, 1, MaxDetailRows, 5 * time.Second},
		{"50 设备 1h", time.Hour, 50, MaxDetailRows, 1 * time.Minute},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := AdaptiveBucket(c.span, c.devices, c.rowCap)
			if err != nil {
				t.Fatalf("不应报错: %v", err)
			}
			if got != c.want {
				t.Fatalf("桶宽期望 %s，得到 %s", c.want, got)
			}

			// 不变量：返回行数 = 设备数 × 桶数 ≤ rowCap。
			buckets := int64((int64(c.span) + int64(got) - 1) / int64(got))
			if rows := int64(c.devices) * buckets; rows > int64(c.rowCap) {
				t.Fatalf("行预算被破坏：设备 %d × 桶数 %d = %d > %d", c.devices, buckets, rows, c.rowCap)
			}
			if got < MinBucketWidth {
				t.Fatalf("桶宽 %s 小于 date_bin 下限 %s", got, MinBucketWidth)
			}
		})
	}
}

// TestAdaptiveBucket_Errors 校验非法入参必须报 *PolicyError，而不是返回一个会放行的桶宽。
func TestAdaptiveBucket_Errors(t *testing.T) {
	cases := []struct {
		name    string
		span    time.Duration
		devices int
		rowCap  int
		rule    string
	}{
		{"设备数 ≥ 行预算", time.Hour, 6000, MaxDetailRows, "device_budget"},
		{"跨度为 0", 0, 10, MaxDetailRows, "bucket"},
		{"设备数为 0", time.Hour, 0, MaxDetailRows, "bucket"},
		{"行预算为 0", time.Hour, 10, 0, "bucket"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := AdaptiveBucket(c.span, c.devices, c.rowCap)
			var pe *PolicyError
			if !errors.As(err, &pe) {
				t.Fatalf("期望 *PolicyError，得到 %v", err)
			}
			if pe.Rule != c.rule {
				t.Fatalf("规则名期望 %q，得到 %q", c.rule, pe.Rule)
			}
		})
	}
}

// TestSeriesQueryNormalize_Defaults 校验默认值补全与合法查询原样通过。
func TestSeriesQueryNormalize_Defaults(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	q := SeriesQuery{ProjectID: 1, DeviceIDs: []int64{2}, Metric: "temperature"}

	got, err := q.Normalize(now)
	if err != nil {
		t.Fatalf("合法查询不应报错: %v", err)
	}
	if !got.Until.Equal(now) {
		t.Errorf("Until 应补为 now，得到 %s", got.Until)
	}
	if want := now.Add(-DefaultLookback); !got.Since.Equal(want) {
		t.Errorf("Since 应补为 now-24h（%s），得到 %s", want, got.Since)
	}
	if got.Limit != MaxDetailRows {
		t.Errorf("Limit 应补为 %d，得到 %d", MaxDetailRows, got.Limit)
	}

	// 显式 Limit 不得超过硬上限，等于上限应放行。
	q.Limit = MaxDetailRows
	if _, err := q.Normalize(now); err != nil {
		t.Fatalf("Limit=%d 应放行: %v", MaxDetailRows, err)
	}
}

// TestSeriesQueryNormalize_Guards 逐条校验保护规则，按 Rule 断言（不匹配文案）。
func TestSeriesQueryNormalize_Guards(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	base := SeriesQuery{ProjectID: 1, DeviceIDs: []int64{2}, Metric: "temperature"}

	manyDevices := make([]int64, MaxDetailDevices+1)
	for i := range manyDevices {
		manyDevices[i] = int64(i)
	}

	cases := []struct {
		name string
		mut  func(*SeriesQuery)
		rule string
	}{
		{"project_id 缺失", func(q *SeriesQuery) { q.ProjectID = 0 }, "project_id"},
		{"设备列表为空", func(q *SeriesQuery) { q.DeviceIDs = nil }, "devices"},
		{"设备数超上限", func(q *SeriesQuery) { q.DeviceIDs = manyDevices }, "devices"},
		{"指标不在白名单", func(q *SeriesQuery) { q.Metric = "not_a_metric" }, "metric"},
		{"指标为注入串", func(q *SeriesQuery) { q.Metric = "temperature' OR '1'='1" }, "metric"},
		{"Since 不早于 Until", func(q *SeriesQuery) { q.Since = now; q.Until = now }, "range"},
		{"回溯超 90 天", func(q *SeriesQuery) {
			q.Until = now
			q.Since = now.Add(-MaxLookback - time.Hour)
		}, "lookback"},
		{"Limit 超上限", func(q *SeriesQuery) { q.Limit = MaxDetailRows + 1 }, "limit"},
		{"Limit 为负", func(q *SeriesQuery) { q.Limit = -1 }, "limit"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := base
			c.mut(&q)
			_, err := q.Normalize(now)
			var pe *PolicyError
			if !errors.As(err, &pe) {
				t.Fatalf("期望 *PolicyError，得到 %v", err)
			}
			if pe.Rule != c.rule {
				t.Fatalf("规则名期望 %q，得到 %q（%v）", c.rule, pe.Rule, err)
			}
		})
	}
}

// TestSeriesQueryNormalize_ExplicitWindowKept 校验显式窗口不被默认值覆盖。
func TestSeriesQueryNormalize_ExplicitWindowKept(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	since := now.Add(-2 * time.Hour)
	q := SeriesQuery{ProjectID: 1, DeviceIDs: []int64{2}, Since: since, Metric: "temperature"}

	got, err := q.Normalize(now)
	if err != nil {
		t.Fatalf("不应报错: %v", err)
	}
	if !got.Since.Equal(since) {
		t.Errorf("显式 Since 被改写：期望 %s，得到 %s", since, got.Since)
	}
	if !got.Until.Equal(now) {
		t.Errorf("Until 应补为 now，得到 %s", got.Until)
	}
}
