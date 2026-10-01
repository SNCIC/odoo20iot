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
type RangeQuery struct {
	ProjectID int64
	DeviceIDs []int64
	Since     time.Time
	Metric    string
	Limit     int
}

// BucketQuery 对应 02 §4.3 的「单设备/多设备区间聚合 + 时间分桶降采样」。
type BucketQuery struct {
	ProjectID int64
	DeviceIDs []int64
	Since     time.Time
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
