package alarm

import (
	"encoding/json"
	"fmt"
	"time"
)

// TriggerSubject 是规则层产出告警的 subject。
const TriggerSubject = "iot.rule.alarm"

// TriggerPayload 是规则层产出告警的线上格式（04 §2.4 的 emit("alarm")）。
//
// ⚠️ **实现决策**：04 只写了「复杂逻辑通过引用 svc-rule 的规则输出
// （emit("alarm")）实现」，既没给字段名也没给 subject。这里按 §2.4 的规则定义
// （timing 三段 + level + scope）取字段名，subject 沿用既有的 `iot.*` 风格
// （对齐 `iot.quota.usage` / `iot.odoo.*`）。定在 alarm 包里而不是某个 cmd：
// 它是跨服务契约，散落在服务里会让「改一处忘一处」变成常态。
type TriggerPayload struct {
	ProjectID    string `json:"project_id"`
	DeviceID     string `json:"device_id"`
	DeviceTypeID int64  `json:"device_type_id"`
	RuleID       string `json:"rule_id"`
	RuleName     string `json:"rule_name"`
	// Level 是 info|warn|critical（02 §3.3 的 t_alarm.level）。
	Level string `json:"level"`
	// ParentID 是父告警 id，用于根因抑制（04 §2.2）；通常留空。
	ParentID string `json:"parent_id"`

	// 时序三段随规则走（04 §2.4 的 timing）；零值取 §2.1 的默认。
	DetectWindowS float64 `json:"detect_window_s"`
	SuppressS     float64 `json:"suppress_s"`
	AutoCloseS    float64 `json:"auto_close_s"`

	// At 是触发时刻；零值取引擎时钟。
	At time.Time `json:"at"`
	// Value 是触发时的取值快照，原样带给下游（07 §6 S3 的 iot_metric_snapshot）。
	Value json.RawMessage `json:"value"`
}

// ToTrigger 转成引擎输入。
func (p TriggerPayload) ToTrigger() Trigger {
	return Trigger{
		ProjectID:    p.ProjectID,
		DeviceID:     p.DeviceID,
		DeviceTypeID: p.DeviceTypeID,
		RuleID:       p.RuleID,
		RuleName:     p.RuleName,
		Level:        p.Level,
		ParentID:     p.ParentID,
		At:           p.At,
		Value:        p.Value,
		Timing: Timing{
			DetectWindow: time.Duration(p.DetectWindowS * float64(time.Second)),
			Suppress:     time.Duration(p.SuppressS * float64(time.Second)),
			AutoClose:    time.Duration(p.AutoCloseS * float64(time.Second)),
		},
	}
}

// ParseTrigger 解析并校验一条触发消息。
//
// 四个必填字段（租户 / 设备 / 规则 / 级别）缺一不可，且**不设默认值**：
//   - 缺租户 → 去重键跨租户串味，两个客户的告警互相压制；
//   - 缺级别 → 通知无法分级路由，一个「不知道多严重」的告警比没有告警更难处理；
//
// 这类消息属于**配置/上游错误**，重投多少次都不会变对，故由调用方 ACK 掉并告警
// （而不是 NAK 让它一直转）。
func ParseTrigger(data []byte) (Trigger, error) {
	var p TriggerPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return Trigger{}, fmt.Errorf("告警触发消息不是合法 JSON: %w", err)
	}
	switch {
	case p.ProjectID == "":
		return Trigger{}, fmt.Errorf("告警触发消息缺少 project_id")
	case p.DeviceID == "":
		return Trigger{}, fmt.Errorf("告警触发消息缺少 device_id")
	case p.RuleID == "":
		return Trigger{}, fmt.Errorf("告警触发消息缺少 rule_id")
	case p.Level == "":
		return Trigger{}, fmt.Errorf("告警触发消息缺少 level（通知靠它分级路由，不能默认）")
	}
	return p.ToTrigger(), nil
}
