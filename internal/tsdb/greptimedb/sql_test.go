package greptimedb

import (
	"strings"
	"testing"

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
