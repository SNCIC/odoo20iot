package connector

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// OdooSubjectPrefix 是「Odoo → IoT」事件在总线上的 subject 前缀（07 §4.4）。
//
// ⚠️ **实现决策**：07 §4.4 只写了「连接器消费后翻译为 `iot.odoo.*` 发布到 NATS」，
// 未定义末级结构。本实现取 `iot.odoo.{aggregate_model}`，并把模型名里的 `.`
// 归一化为 `_` —— NATS subject 以 `.` 分层，`mrp.workorder` 会被切成两个 token，
// 下游就无法用 `iot.odoo.mrp.>` 精确订阅了。
//
// 若将来要按事件类型细分，扩成 `iot.odoo.{model}.{event}` 即可：
// 下游现有的 `iot.odoo.>` 订阅不受影响，属向后兼容的加细。
const OdooSubjectPrefix = "iot.odoo"

// OutboxStream 是 Odoo cron 投递 Outbox 事件的 Redis Stream 键（C-1）。
//
// ⚠️ 同样是**实现决策**：07 §4.4 只说「投递到 Redis Streams」未给键名。
// 两侧（Odoo 的 `sn_edge_integration` 与 connector）必须一致。
const OutboxStream = "odoo:outbox"

// DefaultConsumerGroup 是连接器的消费组名。
const DefaultConsumerGroup = "odoo-connector"

// OdooEvent 是「Odoo → IoT」事件的统一信封。
//
// 字段与 Odoo 侧 `edge.outbox` 一一对应（`event_id` / `company_id` /
// `aggregate_model` / `aggregate_id` / `version` / `occurred_at` / `payload`），
// 另加连接器翻译时刻与 trace。
type OdooEvent struct {
	EventID        string          `json:"event_id"`
	TenantID       string          `json:"tenant_id,omitempty"`
	CompanyID      int64           `json:"company_id"`
	AggregateModel string          `json:"aggregate_model"`
	AggregateID    int64           `json:"aggregate_id"`
	Version        int32           `json:"version"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
	// PublishedAt 是连接器翻译/发布的时刻（平台侧时间轴）。
	PublishedAt time.Time `json:"published_at"`
	// TraceID 贯穿 IoT 侧日志与 `t_integration_log`。
	TraceID string `json:"trace_id,omitempty"`
}

// Subject 返回该事件应发布的 NATS subject。
func (e OdooEvent) Subject() string {
	return OdooSubjectPrefix + "." + NormalizeModel(e.AggregateModel)
}

// Encode 序列化事件。
func (e OdooEvent) Encode() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("序列化 Odoo 事件（event_id=%s）: %w", e.EventID, err)
	}
	return b, nil
}

// NormalizeModel 把 Odoo 模型名转成可用的 subject token。
func NormalizeModel(model string) string {
	return strings.ReplaceAll(strings.TrimSpace(model), ".", "_")
}

// odooDatetimeLayout 是 Odoo Datetime 字段的字符串格式（naive UTC）。
//
// 对账的 domain 条件必须用这个格式，否则 Odoo 侧的字符串比较会出错。
const odooDatetimeLayout = "2006-01-02 15:04:05"

// StreamEntry 是 Redis Stream 的一条记录。
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// parseOdooEvent 把 Outbox 流记录翻译成事件信封。
//
// 这里的错误分两类，调用方必须区别对待：
//   - **字段缺失/格式错**：这条记录永远不可能处理成功，重投只是浪费 —— 应 ACK 丢弃并告警；
//   - **瞬时错误**：不应 ACK，留给消费组重投。
func parseOdooEvent(e StreamEntry, now time.Time) (OdooEvent, error) {
	ev := OdooEvent{
		TenantID:       strings.TrimSpace(e.Fields["tenant_id"]),
		EventID:        strings.TrimSpace(e.Fields["event_id"]),
		AggregateModel: strings.TrimSpace(e.Fields["aggregate_model"]),
		TraceID:        strings.TrimSpace(e.Fields["trace_id"]),
		PublishedAt:    now,
	}

	if ev.EventID == "" {
		return OdooEvent{}, fmt.Errorf("outbox 记录 %s 缺少 event_id", e.ID)
	}
	if ev.AggregateModel == "" {
		return OdooEvent{}, fmt.Errorf("事件 %s 缺少 aggregate_model", ev.EventID)
	}

	var err error
	if ev.CompanyID, err = parseIntField(e.Fields["company_id"], 0); err != nil {
		return OdooEvent{}, fmt.Errorf("事件 %s 的 company_id 非法: %w", ev.EventID, err)
	}
	if ev.AggregateID, err = parseIntField(e.Fields["aggregate_id"], 0); err != nil {
		return OdooEvent{}, fmt.Errorf("事件 %s 的 aggregate_id 非法: %w", ev.EventID, err)
	}
	if v, err := parseIntField(e.Fields["version"], 1); err == nil {
		ev.Version = int32(v)
	}

	// occurred_at 由 Odoo 生成（07 §4.3「依赖 Odoo 的 write_date，不信任本地时钟」）。
	// 解析不出来时**不**回退到本地时间 —— 那会把服务端时间轴污染成客户端时间。
	if raw := strings.TrimSpace(e.Fields["occurred_at"]); raw != "" {
		if ts, err := parseOdooTime(raw); err == nil {
			ev.OccurredAt = ts
		}
	}

	raw := strings.TrimSpace(e.Fields["payload"])
	switch {
	case raw == "":
		ev.Payload = json.RawMessage("{}")
	case !json.Valid([]byte(raw)):
		return OdooEvent{}, fmt.Errorf("事件 %s 的 payload 不是合法 JSON", ev.EventID)
	default:
		ev.Payload = json.RawMessage(raw)
	}

	return ev, nil
}

func parseIntField(raw string, fallback int64) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	return strconv.ParseInt(raw, 10, 64)
}

// parseOdooTime 解析 Odoo 的时间字段。
//
// Odoo 的 Datetime 存储为 UTC 的 `YYYY-MM-DD HH:MM:SS`，JSON-2 / Redis 里
// 可能带 T 与小数秒，故按几种常见形式依次尝试。
func parseOdooTime(raw string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	}
	// Odoo 的 naive datetime 按 UTC 解释（其内部即存 UTC）。
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC(), nil
		}
		if ts, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			return ts.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("无法解析 Odoo 时间: %q", raw)
}
