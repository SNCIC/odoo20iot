package greptimedb

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// 本文件是 B1 补充项（1）· 明细查询限行的**真实库**验证（02 §4.3.1）。
//
// ⚠️ 会 DROP/重建 `telemetry` 表 —— 与 `make b1-bench` 同一张表、同一份注意事项：
// 只在可重建的开发栈上跑，且不要与压测并行。
//
//	IOT_GREPTIMEDB_DSN='postgres://greptime:greptime@100.64.0.3:28403/public' \
//	IOT_PERF_ASSERT=1 go test ./internal/tsdb/greptimedb -run TestQuerySeries -count=1 -v
//
// 未设 IOT_GREPTIMEDB_DSN 时整体跳过；未设 IOT_PERF_ASSERT 时只测量、不断言 P95
// （对齐 internal/rules 的先例：共享宿主上的绝对延迟断言需显式开启）。

const (
	queryFixtureDevices = 10
	queryFixtureSpan    = time.Hour
	queryFixtureEvery   = time.Second
	// targetQueryP95 对齐 06 §3.4「API 延迟 95% 请求 < 200 ms」。
	targetQueryP95 = 200 * time.Millisecond
)

// queryFixture 描述一份确定性数据：N 台设备 × 1h × 1Hz。
type queryFixture struct {
	ProjectID    int64
	DeviceTypeID int64
	DeviceIDs    []int64
	Since        time.Time
	Until        time.Time
}

// loadQueryFixture 重建 telemetry 并写入 10 设备 × 1h × 1Hz（= 36000 行）。
func loadQueryFixture(t *testing.T, store *Store, ctx context.Context) queryFixture {
	t.Helper()

	if err := store.DropTable(ctx, tsdb.PlanJSON); err != nil {
		t.Fatalf("删表失败: %v", err)
	}
	if err := store.CreateTable(ctx, tsdb.PlanJSON); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_ = store.DropTable(context.Background(), tsdb.PlanJSON)
	})

	until := time.Now().UTC().Truncate(time.Second)
	since := until.Add(-queryFixtureSpan)
	f := queryFixture{
		ProjectID:    1,
		DeviceTypeID: 55,
		Since:        since,
		Until:        until,
	}
	for i := 0; i < queryFixtureDevices; i++ {
		f.DeviceIDs = append(f.DeviceIDs, int64(2_000_001+i))
	}

	points := int(queryFixtureSpan / queryFixtureEvery)
	rows := make([]tsdb.Row, 0, points)
	written := 0
	for _, dev := range f.DeviceIDs {
		for p := 0; p < points; p++ {
			// 与查询窗口严格对齐：ts ∈ (Since, Until]，共 points 个点。
			ts := since.Add(time.Duration(p+1) * queryFixtureEvery)
			rows = append(rows, tsdb.Row{
				TS:           ts,
				ProjectID:    f.ProjectID,
				DeviceID:     dev,
				DeviceTypeID: f.DeviceTypeID,
				Values: []tsdb.Value{
					tsdb.Number(float64(p%50) + float64(dev%7)),
					tsdb.Number(60), tsdb.Number(1013), tsdb.Number(3.7), tsdb.Bool(true),
				},
			})
			// A 方案 5 列，协议上限 13107 行；取 5000 作保守分批。
			if len(rows) == 5000 {
				if err := store.InsertRows(ctx, tsdb.PlanJSON, rows); err != nil {
					t.Fatalf("写入失败: %v", err)
				}
				written += len(rows)
				rows = rows[:0]
			}
		}
	}
	if len(rows) > 0 {
		if err := store.InsertRows(ctx, tsdb.PlanJSON, rows); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
		written += len(rows)
	}
	t.Logf("fixture：%d 设备 × %s × %s = %d 行（窗口 %s ~ %s）",
		queryFixtureDevices, queryFixtureSpan, queryFixtureEvery, written,
		since.Format(time.RFC3339), until.Format(time.RFC3339))
	return f
}

func openQueryStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("IOT_GREPTIMEDB_DSN")
	if dsn == "" {
		t.Skip("未设置 IOT_GREPTIMEDB_DSN，跳过")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	store, err := Open(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	t.Cleanup(store.Close)
	return store, ctx
}

func perfAssert() bool { return os.Getenv("IOT_PERF_ASSERT") != "" }

// TestQuerySeries_DownsamplesAndMeetsSLO 是本次交付的判定用例：
// Q3 形态（10 设备 × 1h × 1Hz = 36000 行）经受保护入口后必须
// 自动降采样、行数 ≤ 上限、P95 落回 200ms 预算内。
func TestQuerySeries_DownsamplesAndMeetsSLO(t *testing.T) {
	store, ctx := openQueryStore(t)
	f := loadQueryFixture(t, store, ctx)

	const iters = 30
	lats := make([]time.Duration, 0, iters)
	var res tsdb.SeriesResult

	for i := 0; i < iters; i++ {
		t0 := time.Now()
		var err error
		res, err = store.QuerySeries(ctx, tsdb.PlanJSON, tsdb.SeriesQuery{
			ProjectID: f.ProjectID,
			DeviceIDs: f.DeviceIDs,
			Since:     f.Since,
			Until:     f.Until,
			Metric:    "temperature",
		})
		if err != nil {
			t.Fatalf("受保护查询失败: %v", err)
		}
		lats = append(lats, time.Since(t0))
	}

	if res.Granularity != tsdb.GranularityDownsampled {
		t.Fatalf("36000 行明细必须自动降采样，得到粒度 %q", res.Granularity)
	}
	if !res.CapHit {
		t.Error("CapHit 应为 true（明细行数超过上限）")
	}
	if res.Bucket != 10*time.Second {
		t.Errorf("桶宽期望 10s（1h/500 向上取整到阶梯），得到 %s", res.Bucket)
	}
	if n := len(res.Buckets); n == 0 || n > tsdb.MaxDetailRows {
		t.Errorf("降采样行数应在 (0, %d]，得到 %d", tsdb.MaxDetailRows, n)
	}
	if len(res.Points) != 0 {
		t.Errorf("降采样结果不应带原始点，得到 %d 个", len(res.Points))
	}

	// 每个桶应聚集全部 10 台设备（10s 桶 × 10 设备 = 每时间桶 10 行）。
	bucketDevices := map[time.Time]int{}
	for _, b := range res.Buckets {
		bucketDevices[b.Bucket]++
	}
	for ts, n := range bucketDevices {
		if n != len(f.DeviceIDs) {
			t.Fatalf("时间桶 %s 只含 %d 台设备，期望 %d", ts, n, len(f.DeviceIDs))
		}
	}

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p95 := percentile(lats, 0.95)
	t.Logf("受保护查询：粒度=%s 桶宽=%s 返回 %d 行（原始 36000 行）｜ P50=%s P95=%s P99=%s",
		res.Granularity, res.Bucket, len(res.Buckets),
		percentile(lats, 0.50).Round(time.Microsecond), p95.Round(time.Microsecond),
		percentile(lats, 0.99).Round(time.Microsecond))

	// R1 证据：单独量「廉价探测」本身的成本（只取常量、不解析、不排序）。
	// 实测它必须远低于预算，否则探测本身就会吃掉 SLO 余量。
	probe := tsdb.RangeQuery{
		ProjectID: f.ProjectID, DeviceIDs: f.DeviceIDs,
		Since: f.Since, Until: f.Until, Metric: "temperature",
	}
	plats := make([]time.Duration, 0, iters)
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		if _, err := store.probeRows(ctx, tsdb.PlanJSON, probe, tsdb.MaxDetailRows+1); err != nil {
			t.Fatalf("探测失败: %v", err)
		}
		plats = append(plats, time.Since(t0))
	}
	sort.Slice(plats, func(i, j int) bool { return plats[i] < plats[j] })
	t.Logf("廉价探测（SELECT 1 ... LIMIT %d）单独成本：P50=%s P95=%s", tsdb.MaxDetailRows+1,
		percentile(plats, 0.50).Round(time.Microsecond), percentile(plats, 0.95).Round(time.Microsecond))

	if perfAssert() {
		if p95 >= targetQueryP95 {
			t.Fatalf("P95 %s 未达 SLO（<%s）—— 明细限行未把该形态拉回预算", p95.Round(time.Microsecond), targetQueryP95)
		}
	} else {
		t.Logf("未设 IOT_PERF_ASSERT，仅记录 P95=%s（不做断言）", p95.Round(time.Microsecond))
	}
}

// TestQuerySeries_SmallStaysRaw 校验未超限的查询仍返回原始明细、不触发降采样。
func TestQuerySeries_SmallStaysRaw(t *testing.T) {
	store, ctx := openQueryStore(t)
	f := loadQueryFixture(t, store, ctx)

	res, err := store.QuerySeries(ctx, tsdb.PlanJSON, tsdb.SeriesQuery{
		ProjectID: f.ProjectID,
		DeviceIDs: f.DeviceIDs[:1],
		Since:     f.Until.Add(-10 * time.Minute),
		Until:     f.Until,
		Metric:    "temperature",
	})
	if err != nil {
		t.Fatalf("受保护查询失败: %v", err)
	}

	if res.Granularity != tsdb.GranularityRaw {
		t.Fatalf("600 行明细不应降采样，得到粒度 %q", res.Granularity)
	}
	if res.CapHit {
		t.Error("CapHit 应为 false")
	}
	if res.Bucket != 0 {
		t.Errorf("raw 结果桶宽应为 0，得到 %s", res.Bucket)
	}
	if len(res.Buckets) != 0 {
		t.Errorf("raw 结果不应带桶，得到 %d 行", len(res.Buckets))
	}
	if len(res.Points) != 600 {
		t.Errorf("10min × 1Hz 期望 600 点，得到 %d", len(res.Points))
	}
}

// TestQuerySeries_UnprotectedBaseline 对照：同一 Q3 形态走未保护路径的原始成本，
// 用于在日志里给出「限行前」的数字（这正是 02 §4.3.1 记录的超线根因）。
func TestQuerySeries_UnprotectedBaseline(t *testing.T) {
	store, ctx := openQueryStore(t)
	f := loadQueryFixture(t, store, ctx)

	const iters = 10
	lats := make([]time.Duration, 0, iters)
	var rows int
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		pts, err := store.SelectRangeUnprotected(ctx, tsdb.PlanJSON, tsdb.RangeQuery{
			ProjectID: f.ProjectID,
			DeviceIDs: f.DeviceIDs,
			Since:     f.Since,
			Until:     f.Until,
			Metric:    "temperature",
			Limit:     100000,
		})
		if err != nil {
			t.Fatalf("未保护查询失败: %v", err)
		}
		lats = append(lats, time.Since(t0))
		rows = len(pts)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	t.Logf("未保护基线：返回 %d 行 ｜ P50=%s P95=%s P99=%s",
		rows,
		percentile(lats, 0.50).Round(time.Microsecond),
		percentile(lats, 0.95).Round(time.Microsecond),
		percentile(lats, 0.99).Round(time.Microsecond))
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}
