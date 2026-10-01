package greptimedb

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// 本文件是 B1 补充项（2）· 预聚合的**真实库**验证（02 §7）。
//
// ⚠️ 会 DROP/重建 `telemetry` 与 `telemetry_1m` —— 与 `make b1-bench` 同一张表，
// 只在可重建的开发栈上跑，且不要与压测并行。
//
//	IOT_GREPTIMEDB_DSN='postgres://greptime:greptime@100.64.0.3:28403/public' \
//	IOT_PERF_ASSERT=1 go test ./internal/tsdb/greptimedb -run TestRollup -count=1 -v
const (
	rollupTestDevices = 10
	rollupTestSpan    = 6 * time.Hour
	rollupTestEvery   = time.Second
)

type rollupFixture struct {
	ProjectID    int64
	DeviceTypeID int64
	DeviceIDs    []int64
	Start        time.Time // 已对齐到整分钟
	End          time.Time
}

// loadRollupFixture 重建 telemetry + telemetry_1m，并写入 10 设备 × 6h × 1Hz（= 216000 行，
// 与 B1 Q4 的量级一致）。
func loadRollupFixture(t *testing.T, store *Store, ctx context.Context) rollupFixture {
	t.Helper()

	if err := store.DropTable(ctx, tsdb.PlanJSON); err != nil {
		t.Fatalf("删 telemetry 失败: %v", err)
	}
	if err := store.DropRollupTable(ctx, tsdb.Rollup1m); err != nil {
		t.Fatalf("删 telemetry_1m 失败: %v", err)
	}
	if err := store.CreateTable(ctx, tsdb.PlanJSON); err != nil {
		t.Fatalf("建 telemetry 失败: %v", err)
	}
	if err := store.EnsureRollupTable(ctx, tsdb.Rollup1m); err != nil {
		t.Fatalf("建 telemetry_1m 失败: %v", err)
	}
	t.Cleanup(func() {
		_ = store.DropTable(context.Background(), tsdb.PlanJSON)
		_ = store.DropRollupTable(context.Background(), tsdb.Rollup1m)
	})

	end := time.Now().UTC().Truncate(time.Minute)
	start := end.Add(-rollupTestSpan)
	f := rollupFixture{ProjectID: 1, DeviceTypeID: 55, Start: start, End: end}
	for i := 0; i < rollupTestDevices; i++ {
		f.DeviceIDs = append(f.DeviceIDs, int64(3_000_001+i))
	}

	points := int(rollupTestSpan / rollupTestEvery)
	rows := make([]tsdb.Row, 0, 5000)
	written := 0
	for _, dev := range f.DeviceIDs {
		for p := 0; p < points; p++ {
			ts := start.Add(time.Duration(p) * rollupTestEvery)
			rows = append(rows, tsdb.Row{
				TS:           ts,
				ProjectID:    f.ProjectID,
				DeviceID:     dev,
				DeviceTypeID: f.DeviceTypeID,
				Values: []tsdb.Value{
					tsdb.Number(float64(p%50) + float64(dev%7)),
					tsdb.Number(60), tsdb.Number(1013), tsdb.Number(3.7), tsdb.Bool(p%2 == 0),
				},
			})
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
	t.Logf("fixture：%d 设备 × %s × %s = %d 行（%s ~ %s）",
		rollupTestDevices, rollupTestSpan, rollupTestEvery, written,
		start.Format(time.RFC3339), end.Format(time.RFC3339))
	return f
}

// TestRollup_Integration 一次加载 fixture，覆盖守恒 / 幂等 / 路由提速三件事。
func TestRollup_Integration(t *testing.T) {
	store, ctx := openQueryStore(t)
	f := loadRollupFixture(t, store, ctx)

	// 物化全部 6h @1m == 360 个窗口（每窗口 5 条 INSERT）。
	windows := int(rollupTestSpan / time.Minute)
	t0 := time.Now()
	for w := f.Start; w.Before(f.End); w = w.Add(time.Minute) {
		if _, err := store.RollupWindow(ctx, tsdb.Rollup1m, w, w.Add(time.Minute)); err != nil {
			t.Fatalf("物化窗口 %s 失败: %v", w.Format(time.RFC3339), err)
		}
	}
	t.Logf("物化 %d 个 1m 窗口（%d 条 INSERT）耗时 %s", windows, windows*len(tsdb.RollupableMetrics()), time.Since(t0).Round(time.Millisecond))

	t.Run("Conservation", func(t *testing.T) { testRollupConservation(t, ctx, store, f) })
	t.Run("Idempotent", func(t *testing.T) { testRollupIdempotent(t, ctx, store, f) })
	t.Run("RouteIsFaster", func(t *testing.T) { testRollupRouteIsFaster(t, ctx, store, f) })
}

// testRollupConservation 校验「再聚合无损」：预聚合的 sum/max/count 必须与
// 直接在原始数据上聚合**逐设备逐指标**相等（否则粗桶查询就会失真）。
func testRollupConservation(t *testing.T, ctx context.Context, store *Store, f rollupFixture) {
	win := f.Start.Add(90 * time.Minute) // 取中段一个整分钟窗口
	for _, metric := range []string{"temperature", "running"} {
		raw := directAgg(t, ctx, store, f, win, metric)
		if len(raw) == 0 {
			t.Fatalf("%s：原始聚合为空，fixture 有问题", metric)
		}

		rows, err := store.Pool().Query(ctx,
			`SELECT device_id, "sum", "max", "count" FROM telemetry_1m WHERE ts = $1 AND "metric" = $2`,
			win, metric)
		if err != nil {
			t.Fatalf("读预聚合失败: %v", err)
		}
		got := map[int64][3]float64{}
		for rows.Next() {
			var dev int64
			var sum, max *float64
			var cnt int64
			if err := rows.Scan(&dev, &sum, &max, &cnt); err != nil {
				rows.Close()
				t.Fatalf("扫描失败: %v", err)
			}
			var s, m float64
			if sum != nil {
				s = *sum
			}
			if max != nil {
				m = *max
			}
			got[dev] = [3]float64{s, m, float64(cnt)}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("迭代失败: %v", err)
		}

		if len(got) != len(raw) {
			t.Fatalf("%s：预聚合 %d 台设备，原始 %d 台", metric, len(got), len(raw))
		}
		for dev, want := range raw {
			g, ok := got[dev]
			if !ok {
				t.Fatalf("%s：设备 %d 缺预聚合行", metric, dev)
			}
			for i, label := range []string{"sum", "max", "count"} {
				if d := g[i] - want[i]; d > 1e-9 || d < -1e-9 {
					t.Fatalf("%s 设备 %d：%s 不守恒（预聚合 %v vs 原始 %v）", metric, dev, label, g[i], want[i])
				}
			}
		}
		t.Logf("✓ 守恒：%s 在窗口 %s 上 %d 台设备的 sum/max/count 与原始聚合逐项相等", metric, win.Format(time.RFC3339), len(raw))
	}
}

// directAgg 直接在原始表上算单窗口单指标的 (sum,max,count)。
func directAgg(t *testing.T, ctx context.Context, store *Store, f rollupFixture, win time.Time, metric string) map[int64][3]float64 {
	t.Helper()
	expr := fmt.Sprintf(`json_get("metrics", '%s', 0.0)`, metric)
	sql := fmt.Sprintf(
		`SELECT device_id, SUM(%s), MAX(%s), COUNT(%s) FROM telemetry `+
			`WHERE project_id = $1 AND device_id IN (%s) AND ts >= $2 AND ts < $3 GROUP BY device_id`,
		expr, expr, expr, int64List(f.DeviceIDs))

	rows, err := store.Pool().Query(ctx, sql, f.ProjectID, win, win.Add(time.Minute))
	if err != nil {
		t.Fatalf("原始聚合失败: %v", err)
	}
	defer rows.Close()

	out := map[int64][3]float64{}
	for rows.Next() {
		var dev int64
		var sum, max *float64
		var cnt int64
		if err := rows.Scan(&dev, &sum, &max, &cnt); err != nil {
			t.Fatalf("扫描失败: %v", err)
		}
		var s, m float64
		if sum != nil {
			s = *sum
		}
		if max != nil {
			m = *max
		}
		out[dev] = [3]float64{s, m, float64(cnt)}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("迭代失败: %v", err)
	}
	return out
}

// testRollupIdempotent 校验重跑同一窗口不产生新行、取值不变（库内覆盖写的直接证据）。
func testRollupIdempotent(t *testing.T, ctx context.Context, store *Store, f rollupFixture) {
	win := f.Start.Add(30 * time.Minute)

	countRows := func() int64 {
		var n int64
		if err := store.Pool().QueryRow(ctx,
			`SELECT count(1) FROM telemetry_1m WHERE ts = $1`, win).Scan(&n); err != nil {
			t.Fatalf("计数失败: %v", err)
		}
		return n
	}
	valueOf := func() float64 {
		var v float64
		if err := store.Pool().QueryRow(ctx,
			`SELECT "sum" FROM telemetry_1m WHERE ts = $1 AND "metric" = 'temperature' AND device_id = $2`,
			win, f.DeviceIDs[0]).Scan(&v); err != nil {
			t.Fatalf("读 sum 失败: %v", err)
		}
		return v
	}

	before, sumBefore := countRows(), valueOf()
	if _, err := store.RollupWindow(ctx, tsdb.Rollup1m, win, win.Add(time.Minute)); err != nil {
		t.Fatalf("重跑物化失败: %v", err)
	}
	after, sumAfter := countRows(), valueOf()

	if after != before {
		t.Fatalf("重跑后行数变化（%d → %d）：预聚合表不是覆盖写，幂等性不成立", before, after)
	}
	if sumAfter != sumBefore {
		t.Fatalf("重跑后取值变化（%v → %v）", sumBefore, sumAfter)
	}
	t.Logf("✓ 幂等：重跑窗口 %s 后行数仍 %d、sum 仍 %v", win.Format(time.RFC3339), after, sumAfter)
}

// testRollupRouteIsFaster 校验跨度路由生效，且预聚合路径优于原始路径。
func testRollupRouteIsFaster(t *testing.T, ctx context.Context, store *Store, f rollupFixture) {
	const iters = 20
	since := f.End.Add(-24 * time.Hour) // 24h > 6h ⇒ 路由到 1m

	// 原始路径：直接对 telemetry 做 5min 分桶（与路由后的桶宽一致，保证比的是同一件事）。
	rawLats := make([]time.Duration, 0, iters)
	rawRows := 0
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		rows, err := store.SelectBucketsUnprotected(ctx, tsdb.PlanJSON, tsdb.BucketQuery{
			ProjectID: f.ProjectID, DeviceIDs: f.DeviceIDs,
			Since: since, Bucket: 5 * time.Minute, Metric: "temperature",
		})
		if err != nil {
			t.Fatalf("原始聚合失败: %v", err)
		}
		rawLats = append(rawLats, time.Since(t0))
		rawRows = len(rows)
	}

	// 受保护路径：QuerySeries（跨度 24h 应自动路由到预聚合表）。
	routedLats := make([]time.Duration, 0, iters)
	var res tsdb.SeriesResult
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		var err error
		res, err = store.QuerySeries(ctx, tsdb.PlanJSON, tsdb.SeriesQuery{
			ProjectID: f.ProjectID, DeviceIDs: f.DeviceIDs,
			Since: since, Until: f.End, Metric: "temperature",
		})
		if err != nil {
			t.Fatalf("受保护查询失败: %v", err)
		}
		routedLats = append(routedLats, time.Since(t0))
	}

	if res.Source != tsdb.SourceRollup1m {
		t.Fatalf("24h 跨度应路由到 1m 表，得到 Source=%q", res.Source)
	}
	if res.Granularity != tsdb.GranularityDownsampled {
		t.Errorf("预聚合查询粒度应为 downsampled，得到 %q", res.Granularity)
	}
	if res.Bucket != 5*time.Minute {
		t.Errorf("桶宽期望 5m（AdaptiveBucket(24h,10,5000)），得到 %s", res.Bucket)
	}
	if res.CapHit {
		t.Error("跨度路由不算 CapHit")
	}
	if n := len(res.Buckets); n == 0 || n > tsdb.MaxDetailRows {
		t.Errorf("返回行数应在 (0, %d]，得到 %d", tsdb.MaxDetailRows, n)
	}
	if len(res.Buckets) != rawRows {
		t.Errorf("路由与原始应返回同样的桶数：原始 %d，路由 %d", rawRows, len(res.Buckets))
	}

	sort.Slice(rawLats, func(i, j int) bool { return rawLats[i] < rawLats[j] })
	sort.Slice(routedLats, func(i, j int) bool { return routedLats[i] < routedLats[j] })
	rawP95, routedP95 := percentile(rawLats, 0.95), percentile(routedLats, 0.95)
	t.Logf("Q4 形态（10 设备 × 24h 窗口，5min 桶）：原始 %d 行 P95=%s ｜ 预聚合路由 %d 行 P95=%s",
		rawRows, rawP95.Round(time.Microsecond), len(res.Buckets), routedP95.Round(time.Microsecond))

	if routedP95 >= rawP95 {
		t.Errorf("预聚合路径应快于原始路径：路由 P95=%s ≥ 原始 P95=%s", routedP95, rawP95)
	}
	if perfAssert() && routedP95 >= targetQueryP95 {
		t.Fatalf("预聚合路由 P95 %s 未达 SLO（<%s）", routedP95.Round(time.Microsecond), targetQueryP95)
	}
}
