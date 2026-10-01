package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// ErrPermanent 标记**不可重试**的报文错误（毒消息）：非法 JSON、缺 ts、
// 类型不符等。调用方应计数后 ACK 释放（03 §4.3「毒消息不重试」），
// 而不是无限重投 —— 那只会堵住队列。
var ErrPermanent = errors.New("不可重试的报文错误")

// Parser 把设备上报的信封解析成时序行。
//
// ⚠️ Phase 0 的物模型是**单一份静态指标表**（当前用 tsdb.BenchMetrics），
// 与 tsdb.Row.Values 的「按序一一对应」约定一致。真实系统中物模型随设备类型
// 而异、且由 svc-device 管理并按设备类型加载 —— 那是后续工作。
type Parser struct {
	metrics []tsdb.Metric
}

// NewParser 构造解析器。metrics 的顺序即 tsdb.Row.Values 的顺序。
func NewParser(metrics []tsdb.Metric) (*Parser, error) {
	if len(metrics) == 0 {
		return nil, errors.New("物模型指标不能为空")
	}
	return &Parser{metrics: metrics}, nil
}

// Metrics 返回解析器使用的物模型指标（供测试与观测）。
func (p *Parser) Metrics() []tsdb.Metric { return p.metrics }

// telemetryBody 是端侧遥测/属性报文的公共结构（03 §5.2）。
type telemetryBody struct {
	Ts   string         `json:"ts"`
	Seq  int64          `json:"seq"`
	Data map[string]any `json:"data"`
}

// Record 是一条解析后的记录：时序行 + 幂等所需的端侧元数据。
type Record struct {
	Row tsdb.Row
	// Seq 是端侧报文里的单调序号（03 §5.2），与 project_id / device_id / ts
	// 一起构成幂等键（见 IdempotencyKey）。
	Seq int64
}

// Parse 把一条信封解析为记录。
//
// 返回 ErrPermanent 包裹的错误表示毒消息（不可重试）；其他错误视为可重试。
func (p *Parser) Parse(env envelope.Envelope) (Record, error) {
	var body telemetryBody
	if err := json.Unmarshal(env.Payload, &body); err != nil {
		return Record{}, fmt.Errorf("%w: payload 不是合法 JSON: %v", ErrPermanent, err)
	}
	if body.Ts == "" {
		return Record{}, fmt.Errorf("%w: 缺少 ts（device_key=%s）", ErrPermanent, env.DeviceKey)
	}

	// 03 §5.2：ts 必须是 ISO 8601 UTC（带毫秒），拒绝 Unix 时间戳。
	ts, err := time.Parse(time.RFC3339Nano, body.Ts)
	if err != nil {
		return Record{}, fmt.Errorf("%w: ts %q 不是 ISO 8601: %v", ErrPermanent, body.Ts, err)
	}

	values := make([]tsdb.Value, len(p.metrics))
	for i, m := range p.metrics {
		raw, ok := body.Data[m.Key]
		if !ok {
			// 设备本次未上报该指标：如实记空值（不是错误）。
			values[i] = tsdb.Null()
			continue
		}
		v, err := toValue(raw, m.Kind)
		if err != nil {
			return Record{}, fmt.Errorf("%w: 指标 %s: %v", ErrPermanent, m.Key, err)
		}
		values[i] = v
	}

	return Record{
		Row: tsdb.Row{
			TS:           ts,
			ProjectID:    env.ProjectID,
			DeviceID:     env.DeviceID,
			DeviceTypeID: env.DeviceTypeID,
			Values:       values,
		},
		Seq: body.Seq,
	}, nil
}

func toValue(raw any, kind tsdb.Kind) (tsdb.Value, error) {
	switch kind {
	case tsdb.KindNumber:
		f, ok := raw.(float64) // encoding/json 把 JSON number 解为 float64
		if !ok {
			return tsdb.Value{}, fmt.Errorf("期望数值，得到 %T", raw)
		}
		return tsdb.Number(f), nil
	case tsdb.KindBool:
		b, ok := raw.(bool)
		if !ok {
			return tsdb.Value{}, fmt.Errorf("期望布尔，得到 %T", raw)
		}
		return tsdb.Bool(b), nil
	default:
		return tsdb.Value{}, fmt.Errorf("不支持的指标类型 %v", kind)
	}
}
