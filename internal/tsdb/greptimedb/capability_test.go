package greptimedb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestRollupCapabilities 是 B1 补充项（2）· 预聚合的**开工探针**（02 §7）。
//
// 它回答计划里 P-a…P-f 六个未知，全部在**临时表 probe_*** 上做，不碰 telemetry。
// 结论固化进 store.go/sql.go，并记录在 docs/reports/b1-rollup.md。
// 其中 P-b（非 append 表同键重写是否覆盖）是**最关键的一条**：rollup 的幂等性押在它上面，
// 故本用例对它做硬断言，其余只记录。
//
// 需要 IOT_GREPTIMEDB_DSN；`go test ./...` 默认跳过。
func TestRollupCapabilities(t *testing.T) {
	dsn := os.Getenv("IOT_GREPTIMEDB_DSN")
	if dsn == "" {
		t.Skip("未设置 IOT_GREPTIMEDB_DSN，跳过")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	store, err := Open(ctx, dsn, 2)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	defer store.Close()
	pool := store.Pool()

	tables := []string{"probe_src", "probe_rollup", "probe_metric_bare"}
	for _, tb := range tables {
		if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+tb); err != nil {
			t.Fatalf("清理 %s 失败: %v", tb, err)
		}
	}
	t.Cleanup(func() {
		for _, tb := range tables {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+tb)
		}
	})

	// 与 telemetry 同形的源表（append_mode=true，允许乱序/重复写入）。
	if _, err := pool.Exec(ctx, `CREATE TABLE probe_src (
		ts TIMESTAMP TIME INDEX, project_id BIGINT, device_id BIGINT, device_type_id BIGINT,
		"metrics" JSON, PRIMARY KEY (project_id, device_id)
	) WITH ('append_mode' = 'true')`); err != nil {
		t.Fatalf("建 probe_src 失败: %v", err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insertSrc := func(dev int64, ts time.Time, doc string) {
		t.Helper()
		sql := fmt.Sprintf(
			`INSERT INTO probe_src (ts, project_id, device_id, device_type_id, "metrics") VALUES ('%s', 1, %d, 55, '%s')`,
			ts.UTC().Format("2006-01-02 15:04:05.000"), dev, doc)
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("写 probe_src 失败: %v", err)
		}
	}

	// 同一 60s 桶（00:00:00~00:01:00）里放两个温度样本 + 一个缺 temperature 键的样本，
	// 外加一条 JSON 布尔，用于 P-c / P-f。
	insertSrc(1, base.Add(10*time.Second), `{"temperature":10,"running":true}`)
	insertSrc(1, base.Add(20*time.Second), `{"temperature":20,"running":false}`)
	insertSrc(2, base.Add(30*time.Second), `{"humidity":60}`) // 缺 temperature

	// ---------- P-d / P-e：rollup 表 DDL 形态 ----------
	if _, err := pool.Exec(ctx, `CREATE TABLE probe_rollup (
		ts TIMESTAMP TIME INDEX, project_id BIGINT, device_id BIGINT, device_type_id BIGINT,
		"metric" STRING, "sum" DOUBLE, "max" DOUBLE, "count" BIGINT,
		PRIMARY KEY (project_id, device_id, "metric")
	) WITH ('ttl' = '35d')`); err != nil {
		t.Fatalf("P-d/P-e 失败：rollup 表 DDL（带 ttl、无 append_mode、引号列名）不被接受: %v", err)
	}
	t.Log("✓ P-d/P-e 带 TTL 的 rollup 表 DDL 接受；列名 metric/sum/max/count 用双引号")

	if _, err := pool.Exec(ctx, `CREATE TABLE probe_metric_bare (
		ts TIMESTAMP TIME INDEX, device_id BIGINT, metric STRING, PRIMARY KEY (device_id)
	) WITH ('ttl' = '1d')`); err != nil {
		t.Logf("⚠ P-e metric 未加引号建表失败（属保留字）: %v", err)
	} else {
		t.Log("✓ P-e metric 未加引号也可用（仍统一加引号以求稳）")
	}

	// ---------- P-a：INSERT…SELECT + date_bin + 绑定参数 ----------
	rollupOne := func(start, end time.Time) error {
		const sql = `INSERT INTO probe_rollup
SELECT date_bin(INTERVAL '60 seconds', ts) AS ts, project_id, device_id, device_type_id,
       'temperature',
       SUM(json_get("metrics", 'temperature', 0.0)),
       MAX(json_get("metrics", 'temperature', 0.0)),
       COUNT(json_get("metrics", 'temperature', 0.0))
FROM probe_src
WHERE ts >= $1 AND ts < $2
GROUP BY date_bin(INTERVAL '60 seconds', ts), project_id, device_id, device_type_id`
		_, err := pool.Exec(ctx, sql, start, end)
		return err
	}

	start := base
	end := base.Add(time.Minute)
	if err := rollupOne(start, end); err != nil {
		t.Fatalf("P-a 失败：INSERT…SELECT 聚合（含绑定参数）不被接受: %v", err)
	}
	t.Log("✓ P-a INSERT…SELECT + date_bin + SUM/MAX/COUNT + 绑定参数（$1/$2）可用")

	// ---------- P-b：非 append 表同键重写是否「覆盖」 ----------
	// 注意 P-f：缺该指标的分组会写出 sum=NULL 的行（下面是 device2），故用指针扫描。
	type agg struct {
		dev      int64
		sum, max *float64
		cnt      int64
	}
	rows, err := pool.Query(ctx, `SELECT device_id, "sum", "max", "count" FROM probe_rollup ORDER BY device_id`)
	if err != nil {
		t.Fatalf("读 probe_rollup 失败: %v", err)
	}
	var first []agg
	for rows.Next() {
		var a agg
		if err := rows.Scan(&a.dev, &a.sum, &a.max, &a.cnt); err != nil {
			rows.Close()
			t.Fatalf("扫描失败: %v", err)
		}
		first = append(first, a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("迭代失败: %v", err)
	}
	for _, a := range first {
		t.Logf("  首轮 rollup 行: device=%d sum=%v max=%v count=%d", a.dev, a.sum, a.max, a.cnt)
	}
	if len(first) != 2 {
		t.Fatalf("期望 2 行（device1 有值 / device2 全 NULL），得到 %d 行", len(first))
	}
	if first[1].sum != nil {
		t.Errorf("P-f 预期：缺该指标的分组 sum 应为 NULL，得到 %v", *first[1].sum)
	} else {
		t.Log("✓ P-f 缺该指标的分组写出 sum=NULL 的行、count=0（窗口完整性得以保留）")
	}

	// 往同一桶再加一个温度样本（30s 处），重跑同一窗口。
	insertSrc(1, base.Add(30*time.Second), `{"temperature":50}`)
	if err := rollupOne(start, end); err != nil {
		t.Fatalf("P-b 重跑失败: %v", err)
	}

	var rowCount int64
	if err := pool.QueryRow(ctx, `SELECT count(1) FROM probe_rollup`).Scan(&rowCount); err != nil {
		t.Fatalf("计数失败: %v", err)
	}
	var dev1Sum float64
	var dev1Cnt int64
	if err := pool.QueryRow(ctx,
		`SELECT "sum", "count" FROM probe_rollup WHERE device_id = 1`).Scan(&dev1Sum, &dev1Cnt); err != nil {
		t.Fatalf("读 device1 失败: %v", err)
	}

	// 期望：device1 只更新（sum=80, count=3），device2 与首轮一致；总行数不变。
	if rowCount != int64(len(first)) {
		t.Fatalf("P-b 不成立：非 append 表重写同键产生了新行（首轮 %d 行 → %d 行）—— rollup 幂等不能靠库内覆盖，需改 DELETE-then-INSERT", len(first), rowCount)
	}
	if dev1Sum != 80 || dev1Cnt != 3 {
		t.Fatalf("P-b 语义不符：重写后 device1 期望 sum=80 count=3，得到 sum=%v count=%d", dev1Sum, dev1Cnt)
	}
	t.Logf("✓ P-b 非 append 表同键重写=覆盖（行数仍 %d，device1 sum=80 count=3）", rowCount)

	// ---------- P-c：json_get 对 JSON 布尔 ----------
	var boolNum *float64
	if err := pool.QueryRow(ctx,
		`SELECT json_get("metrics", 'running', 0.0) FROM probe_src WHERE device_id = 1 LIMIT 1`).Scan(&boolNum); err != nil {
		t.Logf("✗ P-c json_get 取布尔值报错: %v", err)
	} else if boolNum == nil {
		t.Logf("⚠ P-c json_get 取布尔值返回 NULL（布尔指标不能按数值聚合）")
	} else {
		t.Logf("✓ P-c json_get 取布尔值返回数值 %v", *boolNum)
	}

	// ---------- P-f：全 NULL 分组 / NULLIF ----------
	var nullSum, nullCnt *float64
	if err := pool.QueryRow(ctx,
		`SELECT SUM(json_get("metrics", 'nope', 0.0)), COUNT(json_get("metrics", 'nope', 0.0)) FROM probe_src`).Scan(&nullSum, &nullCnt); err != nil {
		t.Fatalf("P-f 失败：全 NULL 分组聚合报错: %v", err)
	}
	if nullSum == nil {
		t.Log("✓ P-f 全 NULL 分组 SUM 返回 NULL")
	} else {
		t.Logf("⚠ P-f 全 NULL 分组 SUM 返回 %v（应视作 NULL）", *nullSum)
	}
	t.Logf("  P-f 全 NULL 分组 COUNT = %v", nullCnt)

	var nullif *float64
	if err := pool.QueryRow(ctx, `SELECT NULLIF(0, 0)`).Scan(&nullif); err != nil {
		t.Fatalf("P-f 失败：NULLIF 不可用: %v", err)
	}
	if nullif != nil {
		t.Fatalf("P-f 预期 NULLIF(0,0) 为 NULL，得到 %v", *nullif)
	}
	t.Log("✓ P-f NULLIF 可用 → agg 源 avg 表达式 SUM(\"sum\") / NULLIF(SUM(\"count\"), 0)")
}
