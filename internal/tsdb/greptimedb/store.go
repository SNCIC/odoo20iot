package greptimedb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// Store 通过 PostgreSQL wire protocol 访问 GreptimeDB。
//
// 02 §4.0：只允许网络协议接入；`pgx` 是纯 Go 实现（ADR-010 禁 cgo）。
type Store struct {
	pool *pgxpool.Pool
	// insertSQL 按「方案 + 行数」缓存 SQL 文本 —— 攒批时行数高度重复，
	// 避免每批都重新拼接上千个占位符。
	insertSQL sync.Map
}

// Open 建立连接池。
func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析 DSN: %w", err)
	}
	cfg.MaxConns = maxConns
	// 必须用 simple protocol：GreptimeDB 在扩展协议下把 JSON 列声明为 bytea，
	// 而 pgx 按该声明以二进制格式编码字符串参数，服务端随即报
	// `\x prefix expected for bytea`（实测）。simple protocol 由 pgx 在客户端
	// 完成字面量转义，绕开类型协商 —— 这也是 JSON 列在当前版本唯一可用的写入路径。
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建连接池: %w", err)
	}
	// 不用 pgx 的 Ping：它发送空语句，而 GreptimeDB 会以
	// `Invalid SQL, error: empty statements` 拒绝（实测）。
	if err := probe(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("连接 GreptimeDB: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close 关闭连接池。
func (s *Store) Close() { s.pool.Close() }

// Pool 暴露底层连接池，供压测工具读取版本等诊断信息。
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Ping 探活。
func (s *Store) Ping(ctx context.Context) error { return probe(ctx, s.pool) }

// probe 用一条真实查询确认链路可用（GreptimeDB 拒绝空语句）。
func probe(ctx context.Context, pool *pgxpool.Pool) error {
	var one int
	return pool.QueryRow(ctx, "SELECT 1").Scan(&one)
}

// CreateTable 幂等建表。
func (s *Store) CreateTable(ctx context.Context, p tsdb.Plan) error {
	ddl, err := CreateTableSQL(p)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("建表 %s: %w", tsdb.TableName(p), err)
	}
	return nil
}

// FlushTable 强制把 memtable 落盘。
//
// 必要：写入刚结束时数据几乎全在 memtable，此时读到的 disk_size 只是残余，
// 压缩比会严重失真（实测：未 flush 时 A/B 为 72.8 / 29.6 字节/行，
// flush 后为 13.8 / 7.5 字节/行 —— 结论方向不变但量级差 5 倍）。
func (s *Store) FlushTable(ctx context.Context, p tsdb.Plan) error {
	if _, err := s.pool.Exec(ctx, fmt.Sprintf("ADMIN FLUSH_TABLE('%s')", tsdb.TableName(p))); err != nil {
		return fmt.Errorf("flush %s: %w", tsdb.TableName(p), err)
	}
	return nil
}

// DropTable 删表（压测前清理）。
func (s *Store) DropTable(ctx context.Context, p tsdb.Plan) error {
	if _, err := s.pool.Exec(ctx, DropTableSQL(p)); err != nil {
		return fmt.Errorf("删表 %s: %w", tsdb.TableName(p), err)
	}
	return nil
}

// EnsureRollupTable 幂等建预聚合表（02 §7）。
func (s *Store) EnsureRollupTable(ctx context.Context, r tsdb.Rollup) error {
	ddl, err := CreateRollupTableSQL(r)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("建预聚合表 %s: %w", tsdb.RollupTableName(r), err)
	}
	return nil
}

// DropRollupTable 删预聚合表（测试/重建用）。
func (s *Store) DropRollupTable(ctx context.Context, r tsdb.Rollup) error {
	if _, err := s.pool.Exec(ctx, DropRollupTableSQL(r)); err != nil {
		return fmt.Errorf("删预聚合表 %s: %w", tsdb.RollupTableName(r), err)
	}
	return nil
}

// RollupWindow 物化一个半开窗口 [start, end)：对每个可聚合指标执行一条 INSERT…SELECT。
//
// 幂等性由**表结构**保证：预聚合表不设 append_mode，同 (project_id, device_id, metric, ts)
// 重写即覆盖（capability_test.go · 探针 P-b 实测），因此「写成功但水位没推进」的重放是安全的。
// 返回实际执行的 INSERT 条数（GreptimeDB 不回传受影响行数，故不谎报「写入行数」）。
func (s *Store) RollupWindow(ctx context.Context, r tsdb.Rollup, start, end time.Time) (int, error) {
	if !start.Before(end) {
		return 0, fmt.Errorf("预聚合窗口必须 start < end，得到 [%s, %s)", start, end)
	}

	metrics := tsdb.RollupableMetrics()
	for _, m := range metrics {
		sqlText, err := rollupInsertSQL(r, m.Key)
		if err != nil {
			return 0, err
		}
		if _, err := s.pool.Exec(ctx, sqlText, start.UTC(), end.UTC()); err != nil {
			return 0, fmt.Errorf("物化 %s 窗口 [%s, %s) 指标 %s: %w",
				tsdb.RollupTableName(r), start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), m.Key, err)
		}
	}
	return len(metrics), nil
}

// JSONEncoding 是 JSON 列的写入编码方式。
//
// GreptimeDB 1.2.1 在扩展协议下把 JSON 列声明为 bytea，**普通字符串参数会被拒绝**
// （`\x prefix expected for bytea`），`CAST($1 AS JSON)` 同样不支持
// （`Unsupported SQL type JSON`）。实测只有下面两条路径可用，二者都有代价，
// 因此必须显式选择而不是默默用其中一条（02 §4.1.1）。
type JSONEncoding string

const (
	// JSONHexParam 把 JSON 文档按 bytea 的文本表示（`\x` + 十六进制）作为绑定参数。
	// 安全（无注入面），但**线上体积翻倍**。
	JSONHexParam JSONEncoding = "hex"
	// JSONInlineLiteral 把 JSON 作为内联字面量拼进 SQL。
	// 体积不膨胀，但 SQL 文本每批都变（无法复用解析计划），且需自行转义。
	JSONInlineLiteral JSONEncoding = "literal"
)

// InsertRows 单语句多值批量写入（JSON 列使用 JSONHexParam 编码）。
func (s *Store) InsertRows(ctx context.Context, p tsdb.Plan, rows []tsdb.Row) error {
	return s.InsertRowsWith(ctx, p, JSONHexParam, rows)
}

// InsertRowsWith 单语句多值批量写入，可指定 JSON 列的编码方式。
func (s *Store) InsertRowsWith(ctx context.Context, p tsdb.Plan, enc JSONEncoding, rows []tsdb.Row) error {
	if len(rows) == 0 {
		return nil
	}

	if p == tsdb.PlanJSON && enc == JSONInlineLiteral {
		return s.insertInlineJSON(ctx, rows)
	}

	sqlText, err := s.cachedInsertSQL(p, len(rows))
	if err != nil {
		return err
	}
	args, err := insertArgs(p, rows)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, sqlText, args...); err != nil {
		return fmt.Errorf("批量写 %d 行: %w", len(rows), err)
	}
	return nil
}

// insertInlineJSON 把 JSON 文档内联进 SQL 文本。
//
// 不走参数绑定是因为服务端把 JSON 列声明为 bytea，普通文本参数不可用（见 JSONEncoding）。
// 转义规则按 `standard_conforming_strings=off`（GreptimeDB 实测值）处理：
// 单引号双写、反斜杠双写。
func (s *Store) insertInlineJSON(ctx context.Context, rows []tsdb.Row) error {
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO %s (ts, project_id, device_id, device_type_id, "metrics") VALUES `,
		tsdb.PlanJSONTable)

	for i, r := range rows {
		doc, err := jsonDoc(r)
		if err != nil {
			return err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		lit, err := escapeSQLLiteral(doc)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "('%s', %d, %d, %d, '%s')",
			r.TS.UTC().Format("2006-01-02 15:04:05.000"), r.ProjectID, r.DeviceID, r.DeviceTypeID, lit)
	}

	if _, err := s.pool.Exec(ctx, b.String()); err != nil {
		return fmt.Errorf("内联字面量批量写 %d 行: %w", len(rows), err)
	}
	return nil
}

// escapeSQLLiteral 把文本转义为 SQL 字面量内容。
//
// 按 `standard_conforming_strings=off`（GreptimeDB 实测值）处理：反斜杠与单引号都要转义。
//
// **同时拒绝不可打印字符**：SQL 文本字面量无法可靠表达任意控制字节，
// 而当前物模型只包含数值/布尔指标，序列化后的 JSON 必定是可打印 ASCII。
// 一旦引入字符串指标，这条断言会立刻失败 —— 这是刻意的：
// 届时必须改回 `\x` 参数或改走 COPY，而不是让一个静默的注入面溜进生产。
func escapeSQLLiteral(s string) (string, error) {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c == 0x7f {
			return "", fmt.Errorf("字面量含不可打印字节 0x%02x（位置 %d）：内联字面量不适用于该取值", c, i)
		}
	}
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	return s, nil
}

func (s *Store) cachedInsertSQL(p tsdb.Plan, rows int) (string, error) {
	key := fmt.Sprintf("%s/%d", p, rows)
	if v, ok := s.insertSQL.Load(key); ok {
		return v.(string), nil
	}
	text, err := insertSQL(p, rows)
	if err != nil {
		return "", err
	}
	s.insertSQL.Store(key, text)
	return text, nil
}

// insertArgs 按 columns() 的顺序展开绑定参数。
func insertArgs(p tsdb.Plan, rows []tsdb.Row) ([]any, error) {
	cols, err := columns(p)
	if err != nil {
		return nil, err
	}

	args := make([]any, 0, len(rows)*len(cols))
	for _, r := range rows {
		args = append(args, r.TS.UTC(), r.ProjectID, r.DeviceID)

		if p == tsdb.PlanJSON {
			doc, err := jsonDoc(r)
			if err != nil {
				return nil, err
			}
			// 必须以 bytea 的文本表示（`\x` + 十六进制）传参，否则服务端报
			// `\x prefix expected for bytea`（见 JSONEncoding 的说明）。
			args = append(args, r.DeviceTypeID, hexEncoded(doc))
			continue
		}

		for i, def := range tsdb.BenchMetrics {
			if i >= len(r.Values) {
				return nil, fmt.Errorf("行 %s 的取值少于物模型定义的 %d 个指标", r, len(tsdb.BenchMetrics))
			}
			v := r.Values[i]
			switch {
			case v.Null:
				args = append(args, nil)
			case def.Kind == tsdb.KindBool:
				args = append(args, v.Bool)
			default:
				args = append(args, v.Number)
			}
		}
	}
	return args, nil
}

// hexEncoded 把 JSON 文档转成 PostgreSQL bytea 的文本表示（`\x` + 十六进制）。
func hexEncoded(doc string) string {
	return `\x` + hex.EncodeToString([]byte(doc))
}

// jsonDoc 把取值序列化为 JSON 文档；空值以「缺键」表达
// （A 方案下「未上报」= 键不存在，与 B 方案的列 NULL 语义对齐）。
func jsonDoc(r tsdb.Row) (string, error) {
	doc := make(map[string]any, len(tsdb.BenchMetrics))
	for i, def := range tsdb.BenchMetrics {
		if i >= len(r.Values) || r.Values[i].Null {
			continue
		}
		if def.Kind == tsdb.KindBool {
			doc[def.Key] = r.Values[i].Bool
		} else {
			doc[def.Key] = r.Values[i].Number
		}
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("序列化 metrics JSON: %w", err)
	}
	return string(b), nil
}

// SelectRangeUnprotected 执行**未经保护**的曲线查询。
//
// ⚠️ 刻意绕过 02 §4.3.1 的明细限行：仅压测 / 诊断可用，
// 业务代码必须走 QuerySeries（否则等于把「静默截断」重新放进生产）。
// limit ≤ 0 时沿用历史默认 100000 —— 压测正是要量「未保护的超线成本」。
func (s *Store) SelectRangeUnprotected(ctx context.Context, p tsdb.Plan, q tsdb.RangeQuery) ([]tsdb.SeriesPoint, error) {
	if q.Limit <= 0 {
		q.Limit = 100000
	}
	return s.selectRange(ctx, p, q, q.Limit)
}

// SelectBucketsUnprotected 执行**未经保护**的分桶聚合查询（仅压测 / 诊断）。
func (s *Store) SelectBucketsUnprotected(ctx context.Context, p tsdb.Plan, q tsdb.BucketQuery) ([]tsdb.BucketRow, error) {
	return s.selectBuckets(ctx, tsdb.SourceOfPlan(p), q)
}

// QuerySeries 是受保护的曲线查询入口（02 §4.3.1 + §7 跨度路由）。
//
// 流程：Normalize 校验 → RouteSource 选源 ——
//   - **原始源**（≤6h，或宽表）：廉价探测（只取常量、不解析指标、不排序，超限时尽早终止）
//     → 未超限返回原始明细；超限按自适应桶宽降采样（**绝不静默截断**）。
//   - **预聚合源**（6h–30d 走 1m，>30d 走 1h）：跳过原始探测（扫原始正是要避开的成本），
//     直接对预聚合表再聚合，输出桶宽不小于源粒度。
//
// 探测刻意不取指标值：实测（1.12M 行表、10 设备 × 1h）「值+ORDER BY」探测 P95 96ms，
// 而常量探测 P95 20ms —— 探测的职责只是「超没超」，取数和排序是下一步的事。
func (s *Store) QuerySeries(ctx context.Context, p tsdb.Plan, q tsdb.SeriesQuery) (tsdb.SeriesResult, error) {
	nq, err := q.Normalize(time.Now())
	if err != nil {
		return tsdb.SeriesResult{}, err
	}

	span := nq.Until.Sub(nq.Since)
	source := tsdb.RouteSource(p, span)
	if r, ok := tsdb.RollupOfSource(source); ok {
		return s.querySeriesRollup(ctx, r, source, nq, span)
	}
	return s.querySeriesRaw(ctx, p, source, nq)
}

// querySeriesRaw 在原始表上执行受保护查询。
func (s *Store) querySeriesRaw(ctx context.Context, p tsdb.Plan, source tsdb.Source, nq tsdb.SeriesQuery) (tsdb.SeriesResult, error) {
	rq := tsdb.RangeQuery{
		ProjectID: nq.ProjectID,
		DeviceIDs: nq.DeviceIDs,
		Since:     nq.Since,
		Until:     nq.Until,
		Metric:    nq.Metric,
	}

	n, err := s.probeRows(ctx, p, rq, nq.Limit+1)
	if err != nil {
		return tsdb.SeriesResult{}, err
	}
	if n <= nq.Limit {
		pts, err := s.selectRange(ctx, p, rq, nq.Limit+1)
		if err != nil {
			return tsdb.SeriesResult{}, err
		}
		// 探测与取数之间可能有并发写入（append_mode 允许），取数仍超限就落到降采样，
		// 保证返回行数上限不被突破。
		if len(pts) <= nq.Limit {
			return tsdb.SeriesResult{Granularity: tsdb.GranularityRaw, Source: source, Points: pts}, nil
		}
	}

	bucket, err := tsdb.AdaptiveBucket(nq.Until.Sub(nq.Since), len(nq.DeviceIDs), nq.Limit)
	if err != nil {
		return tsdb.SeriesResult{}, err
	}
	rows, err := s.selectBuckets(ctx, source, tsdb.BucketQuery{
		ProjectID: nq.ProjectID,
		DeviceIDs: nq.DeviceIDs,
		Since:     nq.Since,
		Until:     nq.Until,
		Bucket:    bucket,
		Metric:    nq.Metric,
	})
	if err != nil {
		return tsdb.SeriesResult{}, err
	}
	// 安全网：桶数学若被破坏，宁可报错也不能把超限结果当成「正常」返回。
	if len(rows) > nq.Limit {
		return tsdb.SeriesResult{}, fmt.Errorf(
			"降采样返回 %d 行超过上限 %d（桶宽 %s，设备 %d 台）：桶宽计算有缺陷",
			len(rows), nq.Limit, bucket, len(nq.DeviceIDs))
	}
	return tsdb.SeriesResult{
		Granularity: tsdb.GranularityDownsampled,
		Source:      source,
		Bucket:      bucket,
		CapHit:      true,
		Buckets:     rows,
	}, nil
}

// querySeriesRollup 在预聚合表上执行受保护查询。
//
// `CapHit` 保持 false：按跨度路由到预聚合表**不是**「明细超上限」，
// 来源由 `Source` 表达（见 SeriesResult 的注释）。
func (s *Store) querySeriesRollup(
	ctx context.Context, r tsdb.Rollup, source tsdb.Source, nq tsdb.SeriesQuery, span time.Duration,
) (tsdb.SeriesResult, error) {
	width, err := tsdb.RollupWidth(r)
	if err != nil {
		return tsdb.SeriesResult{}, err
	}

	bucket, err := tsdb.AdaptiveBucket(span, len(nq.DeviceIDs), nq.Limit)
	if err != nil {
		return tsdb.SeriesResult{}, err
	}
	// 输出粒度不得比源更细：1m 表上问不出 1s 的曲线。
	if bucket < width {
		bucket = width
	}

	rows, err := s.selectBuckets(ctx, source, tsdb.BucketQuery{
		ProjectID: nq.ProjectID,
		DeviceIDs: nq.DeviceIDs,
		Since:     nq.Since,
		Until:     nq.Until,
		Bucket:    bucket,
		Metric:    nq.Metric,
	})
	if err != nil {
		return tsdb.SeriesResult{}, err
	}
	if len(rows) > nq.Limit {
		return tsdb.SeriesResult{}, fmt.Errorf(
			"预聚合查询返回 %d 行超过上限 %d（桶宽 %s，设备 %d 台）",
			len(rows), nq.Limit, bucket, len(nq.DeviceIDs))
	}
	return tsdb.SeriesResult{
		Granularity: tsdb.GranularityDownsampled,
		Source:      source,
		Bucket:      bucket,
		Buckets:     rows,
	}, nil
}

// probeRows 以 LIMIT limit 探测匹配行数（最多 limit）：只取常量、不解析指标、不排序。
// 超限时可在第 limit+1 行处尽早终止，而不必物化整窗数据。
func (s *Store) probeRows(ctx context.Context, p tsdb.Plan, q tsdb.RangeQuery, limit int) (int, error) {
	sqlText, err := probeSQL(p, q, limit)
	if err != nil {
		return 0, err
	}

	args := []any{q.ProjectID, q.Since.UTC()}
	if !q.Until.IsZero() {
		args = append(args, q.Until.UTC())
	}
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, sqlText, args...)
	if err != nil {
		return 0, fmt.Errorf("探测查询: %w", err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var one int
		if err := rows.Scan(&one); err != nil {
			return 0, fmt.Errorf("扫描探测行: %w", err)
		}
		n++
	}
	return n, rows.Err()
}

// selectRange 是曲线查询的非导出核心：limit 必须由调用方显式给出。
func (s *Store) selectRange(ctx context.Context, p tsdb.Plan, q tsdb.RangeQuery, limit int) ([]tsdb.SeriesPoint, error) {
	sqlText, err := rangeSQL(p, q, limit)
	if err != nil {
		return nil, err
	}

	args := []any{q.ProjectID, q.Since.UTC()}
	if !q.Until.IsZero() {
		args = append(args, q.Until.UTC())
	}
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, sqlText, args...)
	if err != nil {
		return nil, fmt.Errorf("曲线查询: %w", err)
	}
	defer rows.Close()

	var out []tsdb.SeriesPoint
	for rows.Next() {
		var (
			pt  tsdb.SeriesPoint
			val *float64
		)
		if err := rows.Scan(&pt.TS, &pt.DeviceID, &val); err != nil {
			return nil, fmt.Errorf("扫描曲线行: %w", err)
		}
		if val != nil {
			pt.Value = *val
		} else {
			pt.Null = true
		}
		out = append(out, pt)
	}
	return out, rows.Err()
}

// selectBuckets 是分桶聚合查询的非导出核心，按数据源构造 SQL（原始表或预聚合表）。
func (s *Store) selectBuckets(ctx context.Context, source tsdb.Source, q tsdb.BucketQuery) ([]tsdb.BucketRow, error) {
	sqlText, err := bucketSQLFor(source, q)
	if err != nil {
		return nil, err
	}

	args := []any{q.ProjectID, q.Since.UTC()}
	if !q.Until.IsZero() {
		args = append(args, q.Until.UTC())
	}

	rows, err := s.pool.Query(ctx, sqlText, args...)
	if err != nil {
		return nil, fmt.Errorf("分桶聚合: %w", err)
	}
	defer rows.Close()

	var out []tsdb.BucketRow
	for rows.Next() {
		var (
			br  tsdb.BucketRow
			avg *float64
			max *float64
		)
		if err := rows.Scan(&br.Bucket, &br.DeviceID, &avg, &max, &br.Count); err != nil {
			return nil, fmt.Errorf("扫描聚合行: %w", err)
		}
		if avg != nil {
			br.Avg = *avg
		}
		if max != nil {
			br.Max = *max
		}
		out = append(out, br)
	}
	return out, rows.Err()
}

// TableStat 是单表的存储占用（用于压缩比对照）。
type TableStat struct {
	Table      string
	Rows       int64
	DiskSize   int64
	SSTSize    int64
	IndexSize  int64
	MemtableSz int64
}

// TableStats 读取 region 级存储统计。
//
// 该查询依赖 information_schema 的内部字段，版本间可能变动 ——
// 因此调用方应把失败当作「统计不可用」，而不是压测失败。
func (s *Store) TableStats(ctx context.Context) (map[string]TableStat, error) {
	const q = `SELECT t.table_name,
       SUM(r.region_rows),
       SUM(r.disk_size),
       SUM(r.sst_size),
       SUM(r.index_size),
       SUM(r.memtable_size)
FROM information_schema.region_statistics r
JOIN information_schema.tables t ON r.table_id = t.table_id
GROUP BY t.table_name`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("查询 region 统计: %w", err)
	}
	defer rows.Close()

	out := make(map[string]TableStat)
	for rows.Next() {
		var st TableStat
		var regionRows, disk, sst, idx, mem *int64
		if err := rows.Scan(&st.Table, &regionRows, &disk, &sst, &idx, &mem); err != nil {
			return nil, fmt.Errorf("扫描统计行: %w", err)
		}
		st.Rows = deref(regionRows)
		st.DiskSize = deref(disk)
		st.SSTSize = deref(sst)
		st.IndexSize = deref(idx)
		st.MemtableSz = deref(mem)
		out[st.Table] = st
	}
	return out, rows.Err()
}

func deref(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
