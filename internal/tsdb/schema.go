// Package tsdb 定义时序存储的访问契约与表模型。
//
// 当前服务于 Phase 0 · **B1（时序表模型选型，02 §4.1）**：
// 用同一份口径对比「A. 通用表 + JSON 字段」与「B. 按设备类型宽表」。
// 本包刻意**不含任何方言** —— DDL/SQL 全部集中在 internal/tsdb/greptimedb，
// 以满足 02 §4.0「业务代码中不得出现 GreptimeDB 专有语法」。
package tsdb

import (
	"fmt"
	"time"
)

// Plan 是时序表模型（02 §4.1）。
type Plan string

const (
	// PlanJSON = A. 通用表 + JSON 字段：单表承载全租户，物模型变更零成本。
	PlanJSON Plan = "json"
	// PlanWide = B. 按设备类型宽表：每设备类型一张表，聚合下推快、压缩比高。
	PlanWide Plan = "wide"

	// PlanJSONTable 是 A 方案的表名（02 §4.2）。
	PlanJSONTable = "telemetry"
	// PlanWideTable 是 B 方案的宽表名；真实环境为 telemetry_{device_type}，
	// 压测固定用一个设备类型，故取固定后缀。
	PlanWideTable = "telemetry_wide_dt55"
)

// TableName 返回方案对应的表名。
func TableName(p Plan) string {
	if p == PlanWide {
		return PlanWideTable
	}
	return PlanJSONTable
}

// Plans 返回全部方案（固定顺序，保证报告可复现）。
func Plans() []Plan { return []Plan{PlanJSON, PlanWide} }

// ---------- 预聚合（降采样物化，02 §7） ----------

// Rollup 是预聚合粒度。刻意与 Plan 分开：
// Plan 是「原始表模型」的封闭枚举，Rollup 是「派生表」的维度，两者正交。
// 当前只从 JSON 方案的 telemetry 物化（宽表聚合已达标，不路由，见 RouteSource）。
type Rollup string

const (
	// Rollup1m / Rollup1h 是两级预聚合粒度。
	Rollup1m Rollup = "1m"
	Rollup1h Rollup = "1h"

	// Rollup1mTable / Rollup1hTable 是预聚合表名。
	Rollup1mTable = "telemetry_1m"
	Rollup1hTable = "telemetry_1h"

	// Rollup1mTTL 覆盖路由阈值 30d 再留 5d 余量；1h 与原始表一致取 365d。
	Rollup1mTTL = 35 * 24 * time.Hour
	Rollup1hTTL = 365 * 24 * time.Hour
)

// RollupTableName 返回粒度对应的表名。
func RollupTableName(r Rollup) string {
	switch r {
	case Rollup1m:
		return Rollup1mTable
	case Rollup1h:
		return Rollup1hTable
	default:
		return ""
	}
}

// RollupWidth 返回粒度对应的窗口宽度（date_bin 间隔）。
func RollupWidth(r Rollup) (time.Duration, error) {
	switch r {
	case Rollup1m:
		return time.Minute, nil
	case Rollup1h:
		return time.Hour, nil
	default:
		return 0, &PolicyError{"rollup", fmt.Sprintf("未知的预聚合粒度 %q", r)}
	}
}

// Rollups 返回全部粒度（固定顺序，保证报告与调度可复现）。
func Rollups() []Rollup { return []Rollup{Rollup1m, Rollup1h} }

// ParseRollup 解析字符串粒度（CLI 用），非法值报 *PolicyError。
func ParseRollup(s string) (Rollup, error) {
	r := Rollup(s)
	if _, err := RollupWidth(r); err != nil {
		return "", err
	}
	return r, nil
}

// RollupableMetrics 返回可预聚合的指标集合。
//
// 探针（capability_test.go · P-c）实测 `json_get("metrics",'running',0.0)` 对 JSON 布尔
// 返回数值 1，故布尔指标与数值指标走同一条路径 —— 本函数当前即全部 BenchMetrics。
// 保留为函数而非直接引用切片，是为了让「某类指标不可聚合」时只需改这一处。
func RollupableMetrics() []Metric {
	out := make([]Metric, len(BenchMetrics))
	copy(out, BenchMetrics)
	return out
}

// Kind 是指标的数据类型。
type Kind int

const (
	// KindNumber 对应 DOUBLE。
	KindNumber Kind = iota
	// KindBool 对应 BOOLEAN。
	KindBool
)

// Metric 是一个指标的物模型定义。
type Metric struct {
	Key  string // 物模型属性键，同时是 JSON 字段名 / 宽表列名
	Kind Kind
}

// BenchMetrics 是 B1 压测使用的指标集，模拟 02 §4.2 注释里的
// 「每消息 5 个指标」（06 §5 的容量测算即按每消息 5 个指标）。
var BenchMetrics = []Metric{
	{Key: "temperature", Kind: KindNumber},
	{Key: "humidity", Kind: KindNumber},
	{Key: "pressure", Kind: KindNumber},
	{Key: "voltage", Kind: KindNumber},
	{Key: "running", Kind: KindBool},
}

// LookupMetric 按键名查找指标定义（白名单校验，杜绝把用户输入拼进 SQL）。
func LookupMetric(key string) (Metric, bool) {
	for _, m := range BenchMetrics {
		if m.Key == key {
			return m, true
		}
	}
	return Metric{}, false
}

// Value 是一个指标的取值。Null 表示设备本次未上报该指标
// （A 方案下表现为 JSON 里缺少键，B 方案下表现为列值为 NULL）。
type Value struct {
	Number float64
	Bool   bool
	Null   bool
}

// Number 构造一个数值取值。
func Number(v float64) Value { return Value{Number: v} }

// Bool 构造一个布尔取值。
func Bool(v bool) Value { return Value{Bool: v} }

// Null 构造一个空值。
func Null() Value { return Value{Null: true} }

// Row 是一次设备上报展开后的时序行：一个时间戳 + 一组指标值。
type Row struct {
	TS           time.Time
	ProjectID    int64
	DeviceID     int64
	DeviceTypeID int64
	// Values 与 BenchMetrics 一一对应。
	Values []Value
}

// String 便于在断言失败时定位问题。
func (r Row) String() string {
	return fmt.Sprintf("Row{ts=%s,pid=%d,did=%d,vals=%v}",
		r.TS.UTC().Format(time.RFC3339Nano), r.ProjectID, r.DeviceID, r.Values)
}

// RangeQuery 对应 02 §4.3 的「单设备近 N 曲线」与「多设备对比」
// （两者是同一个查询，只是设备数不同；§4.3 要求多设备强制 ≤ 50）。
//
// Until 为零值时不设上界（`ts > Since`）；非零时加 `ts <= Until`。
type RangeQuery struct {
	ProjectID int64
	DeviceIDs []int64
	Since     time.Time
	Until     time.Time
	Metric    string
	Limit     int
}

// BucketQuery 对应 02 §4.3 的「单设备/多设备区间聚合 + 时间分桶降采样」。
//
// Until 语义同 RangeQuery。
type BucketQuery struct {
	ProjectID int64
	DeviceIDs []int64
	Since     time.Time
	Until     time.Time
	Bucket    time.Duration
	Metric    string // AVG/MAX 的目标指标
}

// SeriesPoint 是曲线查询返回的一行。
type SeriesPoint struct {
	TS       time.Time
	DeviceID int64
	Value    float64
	Null     bool
}

// BucketRow 是分桶聚合返回的一行。
type BucketRow struct {
	Bucket   time.Time
	DeviceID int64
	Avg      float64
	Max      float64
	Count    int64
}

// ---------- 查询保护（02 §4.3 / §4.3.1） ----------
//
// 以下策略**全部在适配层强制**，业务代码不可绕过（02 §4.3 的「查询保护」）。
// 本批次只落地「明细限行 + 自适应降采样」及几条廉价校验；
// 并发上限（20）、慢查询降级（>3s）、预聚合表路由是后续批次，尚未实现。

const (
	// MaxDetailRows 是明细查询单次返回行数的硬上限（02 §4.3.1，默认 5000）。
	// 依据：实测 3.6 万行 ≈ 240ms，按 P95<200ms 预算留 2× 余量反推。
	MaxDetailRows = 5000
	// MaxDetailDevices 是单次查询的设备数上限（02 §4.3）。
	MaxDetailDevices = 50
	// MaxProjectedMetrics 是明细查询单次投影的指标数上限（02 §4.3.1）。
	//
	// 当前 SeriesQuery 是单指标模型，天然 ≤ 本上限；保留该常量作为契约上限，
	// 未来多指标端点组合 ≤ MaxProjectedMetrics 次单指标查询即可（见 §4.3.1 决策）。
	MaxProjectedMetrics = 4
	// DefaultLookback 是未显式给 Since 时的默认回溯（02 §4.3）。
	DefaultLookback = 24 * time.Hour
	// MaxLookback 是单次查询允许的最大回溯，超限引导走异步导出（02 §4.3）。
	MaxLookback = 90 * 24 * time.Hour
	// MinBucketWidth 是分桶间隔下限：`date_bin` 不接受小于 1s 的间隔。
	MinBucketWidth = time.Second
)

// Granularity 标注返回数据的粒度，回答「这是原始明细还是降采样」。
type Granularity string

const (
	// GranularityRaw 表示逐点明细。
	GranularityRaw Granularity = "raw"
	// GranularityDownsampled 表示**命中行数上限后自动**按时间桶聚合的结果。
	GranularityDownsampled Granularity = "downsampled"
	// GranularityAggregated 表示**调用方显式要求**分桶聚合的结果（见 SeriesQuery.Bucket）。
	GranularityAggregated Granularity = "aggregated"
)

// Source 标注一次查询实际从哪张表取数（02 §7 的跨度路由）。
//
// 与 Granularity 分工：Granularity 说「是明细还是聚合」，Source 说「数据来自哪张表」。
type Source string

const (
	// SourceRawJSON / SourceRawWide 是原始表。
	SourceRawJSON Source = "raw_json"
	SourceRawWide Source = "raw_wide"
	// SourceRollup1m / SourceRollup1h 是预聚合表。
	SourceRollup1m Source = "rollup_1m"
	SourceRollup1h Source = "rollup_1h"
)

// 路由阈值（02 §7）：≤6h 原始；6h–30d 1m；>30d 1h。
const (
	// RawMaxSpan 是走原始表的最大跨度。
	RawMaxSpan = 6 * time.Hour
	// Rollup1mMaxSpan 是走 1m 表的最大跨度，超过走 1h 表。
	Rollup1mMaxSpan = 30 * 24 * time.Hour
)

// RouteSource 按表模型与查询跨度选数据源（纯函数，可确定性单测）。
//
// **只对 JSON 方案路由**：宽表的聚合本就达标（B1 实测 Q4 P95 91ms），
// 且预聚合只从 telemetry 物化，故 PlanWide 恒走原始表。
func RouteSource(p Plan, span time.Duration) Source {
	if p == PlanWide {
		return SourceRawWide
	}
	switch {
	case span <= RawMaxSpan:
		return SourceRawJSON
	case span <= Rollup1mMaxSpan:
		return SourceRollup1m
	default:
		return SourceRollup1h
	}
}

// RollupOfSource 返回预聚合数据源对应的粒度；原始源返回 false。
func RollupOfSource(s Source) (Rollup, bool) {
	switch s {
	case SourceRollup1m:
		return Rollup1m, true
	case SourceRollup1h:
		return Rollup1h, true
	default:
		return "", false
	}
}

// SourceOfPlan 返回原始表模型对应的数据源（不通路由，纯粹是「哪张原始表」）。
func SourceOfPlan(p Plan) Source {
	if p == PlanWide {
		return SourceRawWide
	}
	return SourceRawJSON
}

// PolicyError 是查询保护规则拒绝请求时返回的错误。
//
// Rule 是机器可判定的规则名（便于上层区分「引导导出」与「参数错误」），
// 单测按 Rule 断言而不是匹配文案。
type PolicyError struct {
	Rule string
	Msg  string
}

func (e *PolicyError) Error() string { return "查询保护[" + e.Rule + "]: " + e.Msg }

// SeriesQuery 是受保护的曲线查询参数（业务代码的唯一读入口，见 QuerySeries）。
type SeriesQuery struct {
	ProjectID int64
	DeviceIDs []int64
	Since     time.Time
	Until     time.Time // 零值 = now
	Metric    string    // 物模型白名单内的单指标
	Limit     int       // 0 = MaxDetailRows；不允许超过 MaxDetailRows
	// Bucket > 0 时**显式要求分桶聚合**（返回 GranularityAggregated）；
	// 0 表示沿用自适应行为：未超限返回原始明细，超限才自动降采样。
	//
	// 实际生效的桶宽是 max(Bucket, AdaptiveBucket(...))：用户指定的桶宽不得细于
	// 行数预算所允许的下限，否则返回行数会突破上限（见 QuerySeries）。
	Bucket time.Duration
}

// SeriesResult 携带粒度元数据。
//
// Granularity == raw 时 Points 有效；== downsampled 时 Buckets 有效（恰好其一非 nil）。
type SeriesResult struct {
	Granularity Granularity
	// Source 是本次数据实际来自的表（跨度路由的结果，见 RouteSource）。
	Source Source
	// Bucket 是 downsampled 时的有效桶宽；raw 时为 0。
	Bucket time.Duration
	// CapHit 表示明细行数超过上限、已自动降采样（调用方可据此引导异步导出）。
	// 注意：按跨度路由到预聚合表**不算** CapHit（来源由 Source 表达）。
	CapHit  bool
	Points  []SeriesPoint
	Buckets []BucketRow
}

// Normalize 校验保护规则并补默认值，返回规范化副本。
//
// now 由调用方注入（而非内部取 time.Now），使单测可确定性地断言默认回溯。
// 任一条不满足返回 *PolicyError。
func (q SeriesQuery) Normalize(now time.Time) (SeriesQuery, error) {
	if q.ProjectID <= 0 {
		return SeriesQuery{}, &PolicyError{"project_id", "必须提供正的 project_id（强制租户等值条件）"}
	}
	if len(q.DeviceIDs) == 0 {
		return SeriesQuery{}, &PolicyError{"devices", "设备列表为空"}
	}
	if len(q.DeviceIDs) > MaxDetailDevices {
		return SeriesQuery{}, &PolicyError{"devices", fmt.Sprintf("设备数 %d 超过上限 %d", len(q.DeviceIDs), MaxDetailDevices)}
	}
	if _, ok := LookupMetric(q.Metric); !ok {
		return SeriesQuery{}, &PolicyError{"metric", fmt.Sprintf("指标 %q 不在物模型白名单内", q.Metric)}
	}

	if q.Until.IsZero() {
		q.Until = now
	}
	if q.Since.IsZero() {
		q.Since = q.Until.Add(-DefaultLookback)
	}
	if !q.Since.Before(q.Until) {
		return SeriesQuery{}, &PolicyError{"range", "Since 必须早于 Until"}
	}
	span := q.Until.Sub(q.Since)
	if span > MaxLookback {
		return SeriesQuery{}, &PolicyError{"lookback", fmt.Sprintf("回溯 %s 超过上限 %s，请走异步导出", span, MaxLookback)}
	}

	if q.Limit == 0 {
		q.Limit = MaxDetailRows
	}
	if q.Limit < 0 || q.Limit > MaxDetailRows {
		return SeriesQuery{}, &PolicyError{"limit", fmt.Sprintf("行数上限 %d 非法，允许区间 [1, %d]", q.Limit, MaxDetailRows)}
	}

	if q.Bucket < 0 {
		return SeriesQuery{}, &PolicyError{"bucket", fmt.Sprintf("桶宽不能为负，得到 %s", q.Bucket)}
	}
	if q.Bucket > 0 {
		if q.Bucket < MinBucketWidth {
			return SeriesQuery{}, &PolicyError{"bucket", fmt.Sprintf("桶宽 %s 小于下限 %s", q.Bucket, MinBucketWidth)}
		}
		if q.Bucket > span {
			return SeriesQuery{}, &PolicyError{"bucket", fmt.Sprintf("桶宽 %s 大于查询跨度 %s", q.Bucket, span)}
		}
	}
	return q, nil
}

// bucketSteps 是自适应桶宽的可读阶梯。
//
// 向上取整到阶梯值既保证行预算，也让返回的时间桶落在人能读的刻度上
// （10s / 5m / 15m / 24h …），而不是 7.2s 这种由除法凑出来的数。
var bucketSteps = []time.Duration{
	1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second,
	15 * time.Second, 30 * time.Second,
	1 * time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute,
	15 * time.Minute, 30 * time.Minute,
	1 * time.Hour, 2 * time.Hour, 3 * time.Hour, 6 * time.Hour,
	12 * time.Hour, 24 * time.Hour,
}

// AdaptiveBucket 在「行 = 设备数 × 桶数 ≤ rowCap」约束下求最小可用桶宽。
//
// 取整方向：raw = ceil(span / maxBuckets)，再向上取到阶梯值，因此
// buckets = ceil(span / bucket) ≤ maxBuckets，返回行数必然 ≤ rowCap。
// 这是「不静默截断」的另一半保证 —— 超限时宁可降采样，也不丢点。
func AdaptiveBucket(span time.Duration, devices, rowCap int) (time.Duration, error) {
	if span <= 0 {
		return 0, &PolicyError{"bucket", fmt.Sprintf("跨度必须为正，得到 %s", span)}
	}
	if devices < 1 {
		return 0, &PolicyError{"bucket", fmt.Sprintf("设备数必须 ≥1，得到 %d", devices)}
	}
	if rowCap < 1 {
		return 0, &PolicyError{"bucket", fmt.Sprintf("行预算必须 ≥1，得到 %d", rowCap)}
	}

	maxBuckets := rowCap / devices
	if maxBuckets < 1 {
		return 0, &PolicyError{"device_budget", fmt.Sprintf("%d 台设备超过 %d 行的行预算", devices, rowCap)}
	}

	raw := time.Duration((int64(span) + int64(maxBuckets) - 1) / int64(maxBuckets))
	if raw < MinBucketWidth {
		raw = MinBucketWidth
	}
	for _, s := range bucketSteps {
		if s >= raw {
			return s, nil
		}
	}

	// 超出阶梯最大档（>24h）：按整天向上取整。
	day := 24 * time.Hour
	return ((raw + day - 1) / day) * day, nil
}
