package alarm

import (
	"encoding/json"
	"time"
)

// AlarmSubjectPrefix 是告警事件的 subject 前缀。
//
// 依据 07 §6 S3：`IoT 告警进入 active ▼ NATS: iot.alarm.{project}` ——
// 这是**下游契约**（odoo-connector 消费它建 maintenance.request），
// 不是本包自选的命名；改名会静默切断 S3，而且两边都看不出错。
const AlarmSubjectPrefix = "iot.alarm"

// AlarmSubject 按租户构造 subject。
//
// projectID 为空时用 `_` 占位，而不是拼出末尾带空 token 的 `iot.alarm.`：
// 后者在 NATS 里仍能匹配 `iot.alarm.>`，于是「租户丢了」这件事
// 只会在下游查 t_external_ref 时以「未绑定告警」的形式出现，很难倒查到这里。
func AlarmSubject(projectID string) string {
	if projectID == "" {
		projectID = "_"
	}
	return AlarmSubjectPrefix + "." + projectID
}

// Event 是告警事件的线上载荷（07 §6 S3 的契约起点）。
//
// 字段名刻意对齐下游要用的东西：`alarm_id` → Odoo 的 `iot_alarm_id`、
// `level` → `iot_severity`、`metric_snapshot` → `iot_metric_snapshot`，
// 而 `dedup_key` 是 S3 幂等键 `idem:{tenant}:maintenance_req:eq-{eq}-{alarm_dedup_key}`
// 的原料 —— 少给一个，下游就只能靠猜或干脆建重复工单。
type Event struct {
	AlarmID      string `json:"alarm_id"`
	DedupKey     string `json:"dedup_key"`
	ProjectID    string `json:"project_id"`
	DeviceID     string `json:"device_id"`
	DeviceTypeID int64  `json:"device_type_id,omitempty"`
	RuleID       string `json:"rule_id"`
	RuleName     string `json:"rule_name,omitempty"`
	Level        string `json:"level"`

	State  State  `json:"state"`
	Action Action `json:"action"`

	// 四个时间戳不加 omitempty：time.Time 是结构体，omitempty 对它无效，
	// 反而会序列化成 "0001-01-01T00:00:00Z" 这种下游必须特判的脏值。
	// 能走到这里（进入 active）的告警，这四个时间戳必然都有值。
	FirstTS     time.Time `json:"first_ts"`
	LastTS      time.Time `json:"last_ts"`
	ConfirmedTS time.Time `json:"confirmed_ts"`
	NotifiedTS  time.Time `json:"notified_ts"`

	NotifyCount int    `json:"notify_count"`
	BatchID     string `json:"batch_id,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Escalated   bool   `json:"escalated,omitempty"`

	// MetricSnapshot 是触发时的取值快照（07 §6 S3 的 iot_metric_snapshot）。
	MetricSnapshot json.RawMessage `json:"metric_snapshot,omitempty"`
}

// NewEvent 把一次推进结果翻译成线上事件；ok 为 false 表示这次推进不该对外发事件。
//
// **只在「进入 active」时发** —— 07 §6 S3 的原话就是「IoT 告警进入 active」。
// 把 observed / suppressed 这类中间推进也发出去，等于让下游按事件数计费与去重；
// 而 S3 的容量不等式是按「**有效**告警数 ×（1 - 合并率）」算的，
// 多发会把工单量放大一个数量级，且超载时的降级策略会失效。
func NewEvent(d Decision) (Event, bool) {
	if !d.Notify || d.Alarm == nil {
		return Event{}, false
	}
	a := d.Alarm
	return Event{
		AlarmID:        a.ID,
		DedupKey:       a.DedupKey,
		ProjectID:      a.ProjectID,
		DeviceID:       a.DeviceID,
		DeviceTypeID:   a.DeviceTypeID,
		RuleID:         a.RuleID,
		RuleName:       a.RuleName,
		Level:          a.Level,
		State:          a.State,
		Action:         d.Action,
		FirstTS:        a.FirstTS,
		LastTS:         a.LastTS,
		ConfirmedTS:    a.ConfirmedTS,
		NotifiedTS:     a.NotifiedTS,
		NotifyCount:    a.NotifyCount,
		BatchID:        a.BatchID,
		Reason:         d.Reason,
		Escalated:      d.Escalate,
		MetricSnapshot: normalizeValue(a.TriggerValue),
	}, true
}
