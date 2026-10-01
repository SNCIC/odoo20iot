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

// ---------- 预聚合（02 §7） ----------

// CreateRollupTableSQL 返回预聚合表的建表语句。
//
// 与原始表的三处关键差异（均由 capability_test.go 探针实测确认）：
//   - **不设 `append_mode`**：非 append 表按 (PRIMARY KEY, TIME INDEX) 合并，
//     同键重写=覆盖 —— 这是 rollup 幂等（重跑不重复）的**唯一依据**（探针 P-b）；
//   - 列名 `metric`/`sum`/`max`/`count` 一律双引号：`metric` 是保留字，
//     不引号会直接 `Cannot use keyword 'metric' as column name`（探针 P-e）；
//   - 长表结构（每桶每设备每指标一行），故物模型加指标**不需要 DDL**。
func CreateRollupTableSQL(r tsdb.Rollup) (string, error) {
	var ttl time.Duration
	switch r {
	case tsdb.Rollup1m:
		ttl = tsdb.Rollup1mTTL
	case tsdb.Rollup1h:
		ttl = tsdb.Rollup1hTTL
	default:
		return "", fmt.Errorf("未知的预聚合粒度 %q", r)
	}

	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  ts              TIMESTAMP TIME INDEX,
  project_id      BIGINT,
  device_id       BIGINT,
  device_type_id  BIGINT,
  "metric"        STRING,
  "sum"           DOUBLE,
  "max"           DOUBLE,
  "count"         BIGINT,
  PRIMARY KEY (project_id, device_id, "metric")
) WITH ('ttl' = '%dd')`, tsdb.RollupTableName(r), int(ttl.Hours()/24)), nil
}

// DropRollupTableSQL 返回删除预聚合表的语句。
func DropRollupTableSQL(r tsdb.Rollup) string {
	return "DROP TABLE IF EXISTS " + tsdb.RollupTableName(r)
}

// rollupInsertSQL 生成「单指标 × 单窗口」的 INSERT…SELECT。
//
// 窗口是**半开** [start, end)，由 $1/$2 绑定（探针 P-a 实测 INSERT…SELECT 接受绑定参数），
// 因此相邻窗口天然平铺、不重不漏。metric 先过物模型白名单再内联为字面量（无注入面）。
func rollupInsertSQL(r tsdb.Rollup, metricKey string) (string, error) {
	expr, err := rollupValueExpr(metricKey)
	if err != nil {
		return "", err
	}
	width, err := tsdb.RollupWidth(r)
	if err != nil {
		return "", err
	}
	secs := int64(width / time.Second)
	if secs <= 0 {
		return "", fmt.Errorf("预聚合窗口必须 ≥ 1s，得到 %s", width)
	}

	return fmt.Sprintf(`INSERT INTO %s
SELECT date_bin(INTERVAL '%d seconds', ts) AS ts, project_id, device_id, device_type_id, '%s',
       SUM(%s), MAX(%s), COUNT(%s)
FROM %s
WHERE ts >= $1 AND ts < $2
GROUP BY date_bin(INTERVAL '%d seconds', ts), project_id, device_id, device_type_id`,
		tsdb.RollupTableName(r), secs, metricKey, expr, expr, expr, tsdb.PlanJSONTable, secs), nil
}

// rollupValueExpr 返回预聚合使用的**数值**取值表达式。
//
// 布尔指标也走数值形式（探针 P-c 实测 json_get 对 JSON 布尔返回 1/0）：
// 这样 SUM/COUNT 得到「真值占比」、MAX 得到「是否出现过真」，与数值指标同一套语义。
// 注意与 valueExpr 的区别 —— 后者对布尔返回 `<> 0.0`（布尔），不能直接喂给 SUM。
func rollupValueExpr(key string) (string, error) {
	m, ok := tsdb.LookupMetric(key)
	if !ok {
		return "", fmt.Errorf("指标 %q 不在物模型白名单内", key)
	}
	return fmt.Sprintf(`json_get("metrics", '%s', 0.0)`, m.Key), nil
}

// bucketSQLFor 按数据源构造分桶聚合查询（02 §7 路由的落点）。
func bucketSQLFor(source tsdb.Source, q tsdb.BucketQuery) (string, error) {
	switch source {
	case tsdb.SourceRawJSON:
		return bucketSQL(tsdb.PlanJSON, q)
	case tsdb.SourceRawWide:
		return bucketSQL(tsdb.PlanWide, q)
	case tsdb.SourceRollup1m:
		return rollupBucketSQL(tsdb.Rollup1m, q)
	case tsdb.SourceRollup1h:
		return rollupBucketSQL(tsdb.Rollup1h, q)
	default:
		return "", fmt.Errorf("未知的数据源 %q", source)
	}
}

// rollupBucketSQL 构造预聚合表上的再聚合查询。
//
// 关键点：预聚合存的是 sum/max/count，**不是** avg。粗桶要么直接取（1m 桶对 1m 表），
// 要么把细桶再聚合 —— 而「avg 再平均」是错的，必须 `SUM(sum)/SUM(count)` 才能保证
// 与直接在原始数据上聚合逐桶相等（capability 探针 P-f 确认 NULLIF 可用）。
// 同样**刻意不加 LIMIT**：行预算由调用方的桶宽数学保证。
func rollupBucketSQL(r tsdb.Rollup, q tsdb.BucketQuery) (string, error) {
	if _, ok := tsdb.LookupMetric(q.Metric); !ok {
		return "", fmt.Errorf("指标 %q 不在物模型白名单内", q.Metric)
	}
	secs := int64(q.Bucket / time.Second)
	if secs <= 0 {
		return "", fmt.Errorf("分桶间隔必须 ≥ 1s，得到 %s", q.Bucket)
	}

	where := fmt.Sprintf(`project_id = $1 AND device_id IN (%s) AND ts > $2 AND "metric" = '%s'`,
		int64List(q.DeviceIDs), q.Metric)
	if !q.Until.IsZero() {
		where += " AND ts <= $3"
	}

	return fmt.Sprintf(
		`SELECT date_bin(INTERVAL '%d seconds', ts) AS bucket, device_id, `+
			`SUM("sum") / NULLIF(SUM("count"), 0), MAX("max"), SUM("count") `+
			`FROM %s WHERE %s GROUP BY bucket, device_id ORDER BY bucket, device_id`,
		secs, tsdb.RollupTableName(r), where), nil
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
// metric 必须先经 tsdb.LookupMetric 白名单校验，绝不把原始用户输入拼进 SQL
// （02 §4.3 的查询保护要求）。
//
// limit 由调用方显式给出 —— 不在这里补默认值，
// 避免「忘了设上限」时静默退化成全量扫描。
// `ORDER BY ts, device_id` 让多设备结果在时间戳相同时仍有确定顺序（可稳定分页）。
func rangeSQL(p tsdb.Plan, q tsdb.RangeQuery, limit int) (string, error) {
	valueExpr, err := valueExpr(p, q.Metric)
	if err != nil {
		return "", err
	}
	if limit <= 0 {
		return "", fmt.Errorf("范围查询必须给定正的 limit，得到 %d", limit)
	}

	where := fmt.Sprintf("project_id = $1 AND device_id IN (%s) AND ts > $2", int64List(q.DeviceIDs))
	limitPlaceholder := "$3"
	if !q.Until.IsZero() {
		where += " AND ts <= $3"
		limitPlaceholder = "$4"
	}

	return fmt.Sprintf(
		"SELECT ts, device_id, %s FROM %s WHERE %s ORDER BY ts, device_id LIMIT %s",
		valueExpr, tsdb.TableName(p), where, limitPlaceholder), nil
}

// probeSQL 构造「是否超限」的**廉价探测**查询。
//
// 只取常量 1，既不解析 JSON 指标、也不排序：探测的职责仅是判断行数是否超过上限，
// 超限时在第 limit+1 行处尽早终止。取数与排序交给后续的 rangeSQL。
// 实测这一步把探测成本从「值+ORDER BY」的 P95 96ms 降到 20ms（见 store.go QuerySeries）。
func probeSQL(p tsdb.Plan, q tsdb.RangeQuery, limit int) (string, error) {
	if limit <= 0 {
		return "", fmt.Errorf("探测查询必须给定正的 limit，得到 %d", limit)
	}

	where := fmt.Sprintf("project_id = $1 AND device_id IN (%s) AND ts > $2", int64List(q.DeviceIDs))
	limitPlaceholder := "$3"
	if !q.Until.IsZero() {
		where += " AND ts <= $3"
		limitPlaceholder = "$4"
	}

	return fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT %s",
		tsdb.TableName(p), where, limitPlaceholder), nil
}

// bucketSQL 构造分桶聚合查询。
//
// 分桶间隔必须是 SQL 字面量（不能绑定参数），因此这里用秒数格式化；
// 调用方只能传 time.Duration，不存在注入面。
//
// **刻意不加 LIMIT**：行预算由 tsdb.AdaptiveBucket 的桶数学保证（设备数 × 桶数 ≤ 上限）。
// 在这里再加一个 LIMIT 等于重新引入「静默截断」—— 那正是本批次要修掉的问题。
func bucketSQL(p tsdb.Plan, q tsdb.BucketQuery) (string, error) {
	valueExpr, err := valueExpr(p, q.Metric)
	if err != nil {
		return "", err
	}

	secs := int64(q.Bucket / time.Second)
	if secs <= 0 {
		return "", fmt.Errorf("分桶间隔必须 ≥ 1s，得到 %s", q.Bucket)
	}

	where := fmt.Sprintf("project_id = $1 AND device_id IN (%s) AND ts > $2", int64List(q.DeviceIDs))
	if !q.Until.IsZero() {
		where += " AND ts <= $3"
	}

	return fmt.Sprintf(
		"SELECT date_bin(INTERVAL '%d seconds', ts) AS bucket, device_id, AVG(%s), MAX(%s), COUNT(1) "+
			"FROM %s WHERE %s GROUP BY bucket, device_id ORDER BY bucket, device_id",
		secs, valueExpr, valueExpr, tsdb.TableName(p), where), nil
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
