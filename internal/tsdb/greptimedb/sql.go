// Package greptimedb 是 GreptimeDB 的方言适配层。
//
// 02 §4.0 要求：**业务代码中不得出现 GreptimeDB 专有语法**，
// 方言集中在本包。本包因此对外只暴露契约类型（tsdb.*），
// SQL 文本一律不导出。
//
// 全部语法结论均在 GreptimeDB 1.2.1 实测得到（见 02 §4.1.1），
// 不要凭其他版本或文档推断：
//   - `metrics` 是保留字，JSON 列名必须双引号转义，否则建表直接失败；
//   - JSON 取值只有 `json_get(col, 'key')`（返回 String）
//     与 `json_get(col, 'key', 0.0)`（返回 Float64）两种形式；
//   - `->>` / `col['key']` / `json_path_query` / `json_keys` / `json_length` 均不可用。
package greptimedb

import (
	"fmt"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// createJSONTable 是 02 §4.2 的 A 方案 DDL。
//
// 与文档的唯一差异：列名 `metrics` 加了双引号 —— 该词是 GreptimeDB 保留关键字，
// 不加引号会报 `Cannot use keyword 'metrics' as column name`。
const createJSONTable = `CREATE TABLE IF NOT EXISTS ` + tsdb.PlanJSONTable + ` (
  ts              TIMESTAMP TIME INDEX,
  project_id      BIGINT,
  device_id       BIGINT,
  device_type_id  BIGINT,
  "metrics"       JSON,
  PRIMARY KEY (project_id, device_id)
) WITH ('ttl' = '365d', 'append_mode' = 'true')`

// createWideTable 是 02 §4.1 的 B 方案 DDL 展开。
// 宽表列名来自物模型，`benchMetrics` 与 A 方案写入的键集合完全一致，
// 保证两个方案承载的是同一份数据。
func createWideTable() (string, error) {
	var cols []string
	for _, m := range tsdb.BenchMetrics {
		col, err := wideColumnType(m)
		if err != nil {
			return "", err
		}
		cols = append(cols, fmt.Sprintf("  %-15s %s", m.Key, col))
	}

	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  ts              TIMESTAMP TIME INDEX,
  project_id      BIGINT,
  device_id       BIGINT,
%s,
  PRIMARY KEY (project_id, device_id)
) WITH ('ttl' = '365d', 'append_mode' = 'true')`,
		tsdb.PlanWideTable, strings.Join(cols, ",\n")), nil
}

// CreateTableSQL 返回方案的建表语句。
func CreateTableSQL(p tsdb.Plan) (string, error) {
	switch p {
	case tsdb.PlanJSON:
		return createJSONTable, nil
	case tsdb.PlanWide:
		return createWideTable()
	default:
		return "", fmt.Errorf("未知的表模型方案 %q", p)
	}
}

// DropTableSQL 返回删表语句。
func DropTableSQL(p tsdb.Plan) string {
	return "DROP TABLE IF EXISTS " + tsdb.TableName(p)
}

func wideColumnType(m tsdb.Metric) (string, error) {
	switch m.Kind {
	case tsdb.KindNumber:
		return "DOUBLE", nil
	case tsdb.KindBool:
		return "BOOLEAN", nil
	default:
		return "", fmt.Errorf("指标 %q 的类型 %d 未映射到宽表列类型", m.Key, m.Kind)
	}
}

// columns 返回方案的表列顺序；插入参数的顺序必须与此严格一致。
func columns(p tsdb.Plan) ([]string, error) {
	switch p {
	case tsdb.PlanJSON:
		return []string{"ts", "project_id", "device_id", "device_type_id", `"metrics"`}, nil
	case tsdb.PlanWide:
		cols := []string{"ts", "project_id", "device_id"}
		for _, m := range tsdb.BenchMetrics {
			if _, err := wideColumnType(m); err != nil {
				return nil, err
			}
			cols = append(cols, m.Key)
		}
		return cols, nil
	default:
		return nil, fmt.Errorf("未知的表模型方案 %q", p)
	}
}

// Columns 导出列顺序，供压测工具组装参数。
func Columns(p tsdb.Plan) ([]string, error) { return columns(p) }

// insertSQL 为指定行数构造多值 INSERT。
//
// 单语句行数受 PostgreSQL 协议 65535 个绑定参数的限制：A 方案 5 列、B 方案 8 列，
// 因此上限分别为 13107 / 8191 行。攒批阈值（02 §4.2 的 1000 行）远低于该上限。
func insertSQL(p tsdb.Plan, rows int) (string, error) {
	cols, err := columns(p)
	if err != nil {
		return "", err
	}
	perRow := len(cols)

	if rows*perRow > 65535 {
		return "", fmt.Errorf("单批 %d 行超过 PostgreSQL 协议绑定参数上限（本方案 %d 行上限）", rows, 65535/perRow)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES ", tsdb.TableName(p), strings.Join(cols, ", "))
	for r := 0; r < rows; r++ {
		if r > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for c := 0; c < perRow; c++ {
			if c > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "$%d", r*perRow+c+1)
		}
		b.WriteByte(')')
	}
	return b.String(), nil
}

// rangeSQL 构造曲线查询。
//
// metric 必须先经 tsdb.Metric() 白名单校验，绝不把原始用户输入拼进 SQL
// （02 §4.3 的查询保护要求）。
func rangeSQL(p tsdb.Plan, q tsdb.RangeQuery) (string, error) {
	valueExpr, err := valueExpr(p, q.Metric)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"SELECT ts, device_id, %s FROM %s WHERE project_id = $1 AND device_id IN (%s) AND ts > $2 ORDER BY ts LIMIT $3",
		valueExpr, tsdb.TableName(p), int64List(q.DeviceIDs)), nil
}

// bucketSQL 构造分桶聚合查询。
//
// 分桶间隔必须是 SQL 字面量（不能绑定参数），因此这里用秒数格式化；
// 调用方只能传 time.Duration，不存在注入面。
func bucketSQL(p tsdb.Plan, q tsdb.BucketQuery) (string, error) {
	valueExpr, err := valueExpr(p, q.Metric)
	if err != nil {
		return "", err
	}

	secs := int64(q.Bucket / time.Second)
	if secs <= 0 {
		return "", fmt.Errorf("分桶间隔必须 ≥ 1s，得到 %s", q.Bucket)
	}

	return fmt.Sprintf(
		"SELECT date_bin(INTERVAL '%d seconds', ts) AS bucket, device_id, AVG(%s), MAX(%s), COUNT(1) "+
			"FROM %s WHERE project_id = $1 AND device_id IN (%s) AND ts > $2 GROUP BY bucket, device_id ORDER BY bucket",
		secs, valueExpr, valueExpr, tsdb.TableName(p), int64List(q.DeviceIDs)), nil
}

// int64List 把设备 ID 渲染成内联列表。
//
// 不用 `= ANY($n)` 数组绑定：在 QueryExecModeExec（不做 Describe）下 pgx 无法确定
// 数组参数类型，服务端报 `unknown_parameter_type`（实测）。设备 ID 是 int64，
// 白名单式内联不存在注入面，且与 02 §4.3 的 `device_id IN (...)` 写法一致。
func int64List(ids []int64) string {
	if len(ids) == 0 {
		return "NULL"
	}
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%d", id)
	}
	return b.String()
}

// valueExpr 返回指标在方案下的取值表达式。
func valueExpr(p tsdb.Plan, key string) (string, error) {
	m, ok := tsdb.LookupMetric(key)
	if !ok {
		return "", fmt.Errorf("指标 %q 不在物模型白名单内", key)
	}

	switch p {
	case tsdb.PlanJSON:
		// 三参形式的返回类型是 Float64，省掉字符串→数值的逐行转换；
		// 注意：**默认值不生效于缺失键** —— 键不存在时仍返回 NULL（实测）。
		switch m.Kind {
		case tsdb.KindNumber:
			return fmt.Sprintf(`json_get("metrics", '%s', 0.0)`, m.Key), nil
		case tsdb.KindBool:
			return fmt.Sprintf(`json_get("metrics", '%s', 0.0) <> 0.0`, m.Key), nil
		}
		return "", fmt.Errorf("指标 %q 的类型未映射到 JSON 取值表达式", key)
	case tsdb.PlanWide:
		if m.Kind == tsdb.KindBool {
			return fmt.Sprintf("CAST(%s AS BIGINT)", m.Key), nil
		}
		return m.Key, nil
	default:
		return "", fmt.Errorf("未知的表模型方案 %q", p)
	}
}
