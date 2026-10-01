package greptimedb

import (
	"strings"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// TestCreateTableSQL 锁定 A 方案 DDL 的关键细节：**`metrics` 必须双引号转义**。
// 该词是 GreptimeDB 保留关键字，漏掉引号会直接建表失败
// （02 §4.1.1 记录了这个坑，本用例防止它被改回去）。
func TestCreateTableSQL(t *testing.T) {
	ddl, err := CreateTableSQL(tsdb.PlanJSON)
	if err != nil {
		t.Fatalf("构造 A 方案 DDL 失败: %v", err)
	}

	if !strings.Contains(ddl, `"metrics"`) {
		t.Errorf("A 方案 DDL 必须把保留字列名写成 \"metrics\"，实际:\n%s", ddl)
	}
	if strings.Contains(ddl, "\n  metrics ") {
		t.Errorf("A 方案 DDL 出现了未转义的 metrics 列名:\n%s", ddl)
	}
	for _, want := range []string{
		"TIME INDEX",
		"PRIMARY KEY (project_id, device_id)",
		"'append_mode' = 'true'",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("A 方案 DDL 缺少 %q:\n%s", want, ddl)
		}
	}
}

// TestCreateWideTableSQL 校验宽表列来自物模型定义，且与 A 方案写入的键集合一致。
func TestCreateWideTableSQL(t *testing.T) {
	ddl, err := CreateTableSQL(tsdb.PlanWide)
	if err != nil {
		t.Fatalf("构造 B 方案 DDL 失败: %v", err)
	}

	for _, m := range tsdb.BenchMetrics {
		if !strings.Contains(ddl, m.Key) {
			t.Errorf("宽表 DDL 缺少指标列 %q:\n%s", m.Key, ddl)
		}
	}
	if !strings.Contains(ddl, "temperature     DOUBLE") {
		t.Errorf("体温列未映射为 DOUBLE:\n%s", ddl)
	}
	if !strings.Contains(ddl, "running         BOOLEAN") {
		t.Errorf("布尔指标未映射为 BOOLEAN:\n%s", ddl)
	}
}

// TestInsertSQL_BindParamLimit 校验绑定参数上限保护：
// PostgreSQL 协议单语句 65535 个参数，超限必须**提前报错**而不是让服务端拒绝。
func TestInsertSQL_BindParamLimit(t *testing.T) {
	// A 方案 5 列 → 上限 13107 行
	if _, err := insertSQL(tsdb.PlanJSON, 13107); err != nil {
		t.Fatalf("13107 行应在上限内: %v", err)
	}
	if _, err := insertSQL(tsdb.PlanJSON, 13108); err == nil {
		t.Fatal("13108 行应超限报错")
	}

	// B 方案 8 列 → 上限 8191 行
	if _, err := insertSQL(tsdb.PlanWide, 8191); err != nil {
		t.Fatalf("8191 行应在上限内: %v", err)
	}
	if _, err := insertSQL(tsdb.PlanWide, 8192); err == nil {
		t.Fatal("8192 行应超限报错")
	}
}

// TestValueExpr 校验取值表达式：JSON 走 json_get，宽表走原生列，
// 且未知指标被白名单拦下（02 §4.3 的查询保护）。
func TestValueExpr(t *testing.T) {
	jsonExpr, err := valueExpr(tsdb.PlanJSON, "temperature")
	if err != nil {
		t.Fatalf("JSON 取值表达式失败: %v", err)
	}
	if jsonExpr != `json_get("metrics", 'temperature', 0.0)` {
		t.Errorf("JSON 取值表达式不符合实测语法: %s", jsonExpr)
	}

	wideExpr, err := valueExpr(tsdb.PlanWide, "temperature")
	if err != nil {
		t.Fatalf("宽表取值表达式失败: %v", err)
	}
	if wideExpr != "temperature" {
		t.Errorf("宽表应直接引用原生列，得到 %s", wideExpr)
	}

	if _, err := valueExpr(tsdb.PlanJSON, "not_a_metric"); err == nil {
		t.Fatal("白名单外的指标必须被拒绝")
	}
	if _, err := valueExpr(tsdb.PlanJSON, "temperature' OR '1'='1"); err == nil {
		t.Fatal("疑似注入的指标名必须被拒绝")
	}
}

// TestEscapeSQLLiteral 校验内联字面量的转义规则与安全边界。
//
// 这条边界不是学术问题：内联字面量是 A 方案上**唯一**吞吐达标的写入路径
// （见 02 §4.1.1），它的安全性完全落在这个函数上。
func TestEscapeSQLLiteral(t *testing.T) {
	t.Run("反斜杠与单引号被转义", func(t *testing.T) {
		got, err := escapeSQLLiteral(`{"a":"it's\ok"}`)
		if err != nil {
			t.Fatalf("不应报错: %v", err)
		}
		want := `{"a":"it''s\\ok"}`
		if got != want {
			t.Fatalf("期望 %s，得到 %s", want, got)
		}
	})

	t.Run("数值型 JSON 原样通过", func(t *testing.T) {
		doc := `{"temperature":25.3,"humidity":62.1,"running":true}`
		got, err := escapeSQLLiteral(doc)
		if err != nil {
			t.Fatalf("不应报错: %v", err)
		}
		if got != doc {
			t.Fatalf("数值型 JSON 不应被改写：%s", got)
		}
	})

	t.Run("拒绝不可打印字符", func(t *testing.T) {
		if _, err := escapeSQLLiteral("a\x01b"); err == nil {
			t.Fatal("含控制字节的字面量必须被拒绝，而不是静默拼进 SQL")
		}
		if _, err := escapeSQLLiteral("a\nb"); err == nil {
			t.Fatal("含真实换行的字面量必须被拒绝")
		}
	})
}

// TestBucketSQLRequiresPositiveInterval 校验分桶间隔保护。
func TestBucketSQLRequiresPositiveInterval(t *testing.T) {
	_, err := bucketSQL(tsdb.PlanJSON, tsdb.BucketQuery{Metric: "temperature", Bucket: 0})
	if err == nil {
		t.Fatal("分桶间隔为 0 必须报错")
	}
}

// TestRangeSQLContent 锁定范围查询的关键约束：
// 显式 limit、确定性排序 `ORDER BY ts, device_id`、Until 存在时多一个占位符。
func TestRangeSQLContent(t *testing.T) {
	t.Run("无 Until：limit 是 $3", func(t *testing.T) {
		sql, err := rangeSQL(tsdb.PlanJSON, tsdb.RangeQuery{
			ProjectID: 1, DeviceIDs: []int64{2, 3}, Metric: "temperature",
		}, 5001)
		if err != nil {
			t.Fatalf("构造失败: %v", err)
		}
		for _, want := range []string{
			"project_id = $1", "ts > $2", "ORDER BY ts, device_id", "LIMIT $3",
		} {
			if !strings.Contains(sql, want) {
				t.Errorf("缺少 %q:\n%s", want, sql)
			}
		}
		if strings.Contains(sql, "ts <= $3") {
			t.Errorf("未设 Until 不应出现上界条件:\n%s", sql)
		}
	})

	t.Run("有 Until：上界占 $3，limit 顺延为 $4", func(t *testing.T) {
		sql, err := rangeSQL(tsdb.PlanJSON, tsdb.RangeQuery{
			ProjectID: 1, DeviceIDs: []int64{2}, Metric: "temperature",
			Until: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
		}, 5001)
		if err != nil {
			t.Fatalf("构造失败: %v", err)
		}
		for _, want := range []string{"ts <= $3", "LIMIT $4"} {
			if !strings.Contains(sql, want) {
				t.Errorf("缺少 %q:\n%s", want, sql)
			}
		}
	})

	t.Run("limit 非正直接报错", func(t *testing.T) {
		if _, err := rangeSQL(tsdb.PlanJSON, tsdb.RangeQuery{Metric: "temperature"}, 0); err == nil {
			t.Fatal("limit=0 必须报错，不能静默退化成全量扫描")
		}
	})
}

// TestCreateRollupTableSQL 锁定预聚合表 DDL 的三个实测细节：
// **无 `append_mode`**（幂等靠库内覆盖，探针 P-b）、保留字列名加引号（P-e）、TTL 分级。
func TestCreateRollupTableSQL(t *testing.T) {
	ddl, err := CreateRollupTableSQL(tsdb.Rollup1m)
	if err != nil {
		t.Fatalf("构造 1m DDL 失败: %v", err)
	}
	for _, want := range []string{
		"telemetry_1m", "TIME INDEX", `"metric"`, `"sum"`, `"max"`, `"count"`,
		`PRIMARY KEY (project_id, device_id, "metric")`, "'ttl' = '35d'",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("1m DDL 缺少 %q:\n%s", want, ddl)
		}
	}
	if strings.Contains(ddl, "append_mode") {
		t.Errorf("预聚合表**不得**设 append_mode（会使重跑追加而非覆盖）:\n%s", ddl)
	}

	ddl1h, err := CreateRollupTableSQL(tsdb.Rollup1h)
	if err != nil {
		t.Fatalf("构造 1h DDL 失败: %v", err)
	}
	if !strings.Contains(ddl1h, "telemetry_1h") || !strings.Contains(ddl1h, "'ttl' = '365d'") {
		t.Errorf("1h DDL 表名/TTL 不符:\n%s", ddl1h)
	}

	if _, err := CreateRollupTableSQL("5m"); err == nil {
		t.Fatal("未知粒度必须报错")
	}
}

// TestRollupInsertSQL 锁定 INSERT…SELECT 的形状：半开窗口绑定、单指标、SUM/MAX/COUNT。
func TestRollupInsertSQL(t *testing.T) {
	sql, err := rollupInsertSQL(tsdb.Rollup1m, "temperature")
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	for _, want := range []string{
		"INSERT INTO telemetry_1m", "date_bin(INTERVAL '60 seconds', ts)",
		"'temperature'", `json_get("metrics", 'temperature', 0.0)`, "SUM(", "MAX(", "COUNT(",
		"ts >= $1 AND ts < $2", "GROUP BY date_bin(INTERVAL '60 seconds', ts), project_id, device_id, device_type_id",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("缺少 %q:\n%s", want, sql)
		}
	}

	sql1h, err := rollupInsertSQL(tsdb.Rollup1h, "humidity")
	if err != nil {
		t.Fatalf("构造 1h 失败: %v", err)
	}
	if !strings.Contains(sql1h, "telemetry_1h") || !strings.Contains(sql1h, "'3600 seconds'") {
		t.Errorf("1h INSERT 表名/窗口不符:\n%s", sql1h)
	}

	if _, err := rollupInsertSQL(tsdb.Rollup1m, "not_a_metric"); err == nil {
		t.Fatal("白名单外指标必须被拒绝")
	}
}

// TestBucketSQLForRollupSource 锁定预聚合源的再聚合公式：
// avg 必须是 SUM(sum)/SUM(count)（而非对 avg 再平均），并带 metric 过滤、不加 LIMIT。
func TestBucketSQLForRollupSource(t *testing.T) {
	sql, err := bucketSQLFor(tsdb.SourceRollup1m, tsdb.BucketQuery{
		ProjectID: 1, DeviceIDs: []int64{2}, Metric: "temperature", Bucket: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	for _, want := range []string{
		"telemetry_1m", `SUM("sum") / NULLIF(SUM("count"), 0)`, `MAX("max")`, `SUM("count")`,
		`"metric" = 'temperature'`, "GROUP BY bucket, device_id",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("缺少 %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "LIMIT") {
		t.Errorf("再聚合查询不得加 LIMIT:\n%s", sql)
	}
	if strings.Contains(sql, "json_get") {
		t.Errorf("预聚合源不应再解析 JSON:\n%s", sql)
	}

	// Until 存在时多一个上界占位符。
	sqlUntil, err := bucketSQLFor(tsdb.SourceRollup1h, tsdb.BucketQuery{
		ProjectID: 1, DeviceIDs: []int64{2}, Metric: "temperature", Bucket: time.Hour,
		Until: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	if !strings.Contains(sqlUntil, "telemetry_1h") || !strings.Contains(sqlUntil, "ts <= $3") {
		t.Errorf("1h + Until 不符:\n%s", sqlUntil)
	}

	if _, err := bucketSQLFor(tsdb.SourceRollup1m, tsdb.BucketQuery{Metric: "not_a_metric", Bucket: time.Minute}); err == nil {
		t.Fatal("白名单外指标必须被拒绝")
	}

	// raw 源应委托给原 bucketSQL（JSON 走 json_get）。
	raw, err := bucketSQLFor(tsdb.SourceRawJSON, tsdb.BucketQuery{
		ProjectID: 1, DeviceIDs: []int64{2}, Metric: "temperature", Bucket: time.Minute,
	})
	if err != nil {
		t.Fatalf("构造 raw 失败: %v", err)
	}
	if !strings.Contains(raw, "telemetry") || !strings.Contains(raw, "json_get") {
		t.Errorf("raw 源应走原始表 + json_get:\n%s", raw)
	}
}

// TestProbeSQLContent 锁定廉价探测查询：
// **只取常量 1、不出现指标表达式、不排序**（这三点正是它比取数查询快的全部原因）。
func TestProbeSQLContent(t *testing.T) {
	sql, err := probeSQL(tsdb.PlanJSON, tsdb.RangeQuery{
		ProjectID: 1, DeviceIDs: []int64{2, 3}, Metric: "temperature",
	}, 5001)
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	for _, want := range []string{"SELECT 1 FROM telemetry", "project_id = $1", "ts > $2", "LIMIT $3"} {
		if !strings.Contains(sql, want) {
			t.Errorf("缺少 %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "json_get") {
		t.Errorf("探测不得解析指标:\n%s", sql)
	}
	if strings.Contains(sql, "ORDER BY") {
		t.Errorf("探测不得排序:\n%s", sql)
	}

	// 有 Until 时多一个上界占位符。
	sql, err = probeSQL(tsdb.PlanWide, tsdb.RangeQuery{
		ProjectID: 1, DeviceIDs: []int64{2},
		Until: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
	}, 5001)
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	if !strings.Contains(sql, "ts <= $3") || !strings.Contains(sql, "LIMIT $4") {
		t.Errorf("有 Until 时占位符应顺延:\n%s", sql)
	}

	if _, err := probeSQL(tsdb.PlanJSON, tsdb.RangeQuery{Metric: "temperature"}, 0); err == nil {
		t.Fatal("limit=0 必须报错")
	}
}

// TestBucketSQLUntil 校验聚合查询的上界条件与「刻意不加 LIMIT」。
func TestBucketSQLUntil(t *testing.T) {
	sql, err := bucketSQL(tsdb.PlanJSON, tsdb.BucketQuery{
		ProjectID: 1, DeviceIDs: []int64{2}, Metric: "temperature", Bucket: 10 * time.Second,
		Until: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	if !strings.Contains(sql, "ts <= $3") {
		t.Errorf("缺少上界条件:\n%s", sql)
	}
	if !strings.Contains(sql, "ORDER BY bucket, device_id") {
		t.Errorf("聚合应按桶与设备排序:\n%s", sql)
	}
	if strings.Contains(sql, "LIMIT") {
		t.Errorf("聚合查询不得加 LIMIT（行预算由桶数学保证，加 LIMIT 等于静默截断）:\n%s", sql)
	}
}
