package alarm

import (
	"sync/atomic"
	"time"
)

// Metrics 是 04 §2.2.1 告警质量指标的原始计数。
//
// 为什么要单独做一层业务口径：§2.2.1 明确要求「暴露业务口径指标，而不是只有
// 技术指标」——「只统计 P99 延迟而不统计误报率，会出现『系统很快但没人看告警』
// 的失败形态」。
type Metrics struct {
	Total      atomic.Int64 // 新建告警总数（合并率的分母）
	Notified   atomic.Int64 // 已通知
	Suppressed atomic.Int64 // 被抑制（静默 / 根因 / 风暴）
	Resolved   atomic.Int64 // 已恢复
	Closed     atomic.Int64 // 已自动关闭
	Batched    atomic.Int64 // 被聚合
	Reopened   atomic.Int64 // 抖动重开
	Storms     atomic.Int64 // 风暴熔断次数（目标 0）

	// ManualConfirmed / ConvertedToTicket 本包观察不到，需外部调用方上报
	// （人工确认来自控制台，转工单发生在 Odoo 侧）。
	ManualConfirmed   atomic.Int64
	ConvertedToTicket atomic.Int64

	confirmDelaySumMicros atomic.Int64
	confirmDelayCount     atomic.Int64
}

// RecordConfirmed 由调用方上报一次人工确认（effective=true）或转 Odoo 工单。
func (m *Metrics) RecordConfirmed(ticket bool) {
	m.ManualConfirmed.Add(1)
	if ticket {
		m.ConvertedToTicket.Add(1)
	}
}

// RecordTicket 由调用方上报一次「已生成 Odoo 工单」。
func (m *Metrics) RecordTicket() { m.ConvertedToTicket.Add(1) }

// record 按 Decision 累加计数。
func (m *Metrics) record(d Decision, at time.Time) {
	switch d.Action {
	case ActionCreated:
		m.Total.Add(1)
	case ActionConfirmed:
		// 平均确认时长 = confirmed_ts - first_ts（04 §2.2.1）。
		if d.Alarm != nil && !d.Alarm.FirstTS.IsZero() {
			m.confirmDelaySumMicros.Add(at.Sub(d.Alarm.FirstTS).Microseconds())
			m.confirmDelayCount.Add(1)
		}
	case ActionNotified:
		m.Notified.Add(1)
	case ActionBatched:
		// 聚合告警也是一次通知（合并后的那一条）。
		m.Notified.Add(1)
		m.Batched.Add(1)
	case ActionSuppressed:
		m.Suppressed.Add(1)
	case ActionResolved:
		m.Resolved.Add(1)
	case ActionClosed:
		m.Closed.Add(1)
	case ActionReopened:
		m.Reopened.Add(1)
	}
}

// Effective 是「有效告警数」= 人工确认 或 转化为 Odoo 工单（04 §2.2.1）。
//
// 两者取并集而不是相加：一条告警既被确认又转了工单，不该被算成两条有效告警。
// 本包只拿得到各自的计数，无法判重，故取**较大者**作为保守下界 ——
// 宁可低估有效占比，也不要把指标做成好看的数字。
func (m *Metrics) Effective() int64 {
	c, t := m.ManualConfirmed.Load(), m.ConvertedToTicket.Load()
	if c > t {
		return c
	}
	return t
}

// EffectiveRate 是有效告警占比（04 §2.2.1，首期目标 ≥ 60%）。
func (m *Metrics) EffectiveRate() float64 {
	total := m.Total.Load()
	if total <= 0 {
		return 0
	}
	return float64(m.Effective()) / float64(total)
}

// TicketConversion 是告警到工单转化率 = 生成工单数 / 有效告警数（目标 ≥ 90%）。
func (m *Metrics) TicketConversion() float64 {
	eff := m.Effective()
	if eff <= 0 {
		return 0
	}
	return float64(m.ConvertedToTicket.Load()) / float64(eff)
}

// MergeRate 是告警合并率 = 1 - (生成工单数 / 原始告警数)。
//
// 它直接决定 Odoo 写入量（07 §6 S3 的容量不等式），所以是**记录基线**的指标，
// 没有目标值 —— 但必须能看到它随规则调整怎么变。
func (m *Metrics) MergeRate() float64 {
	total := m.Total.Load()
	if total <= 0 {
		return 0
	}
	rate := 1 - float64(m.ConvertedToTicket.Load())/float64(total)
	if rate < 0 {
		return 0
	}
	return rate
}

// AvgConfirmDelay 是平均确认时长（confirmed_ts - first_ts）。
//
// ⚠️ §2.2.1 要的是**中位数**，中位数需要完整样本。本包只维护和与计数，
// 故这里给出的是均值 —— 需要中位数时请从 PG 的 t_alarm 计算。
// 如实留白，而不是用均值冒充中位数。
func (m *Metrics) AvgConfirmDelay() time.Duration {
	n := m.confirmDelayCount.Load()
	if n <= 0 {
		return 0
	}
	return time.Duration(m.confirmDelaySumMicros.Load()/n) * time.Microsecond
}
