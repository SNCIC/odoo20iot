package alarm

import (
	"math"
	"testing"
	"time"
)

func TestMetricsBusinessRatios(t *testing.T) {
	var m Metrics
	m.Total.Store(10)
	m.ManualConfirmed.Store(6)
	m.ConvertedToTicket.Store(5)

	if got := m.Effective(); got != 6 {
		t.Fatalf("有效告警应取两者较大者（本包无法判重），得 %d", got)
	}
	if got := m.EffectiveRate(); math.Abs(got-0.6) > 1e-9 {
		t.Fatalf("有效告警占比应 0.6（04 §2.2.1 目标 ≥60%%），得 %v", got)
	}
	if got := m.TicketConversion(); math.Abs(got-5.0/6.0) > 1e-9 {
		t.Fatalf("告警到工单转化率（目标 ≥90%%）得 %v", got)
	}
	if got := m.MergeRate(); math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("告警合并率应 0.5，得 %v", got)
	}
}

func TestMetricsEmptyIsZero(t *testing.T) {
	var m Metrics
	if m.EffectiveRate() != 0 || m.TicketConversion() != 0 || m.MergeRate() != 0 || m.AvgConfirmDelay() != 0 {
		t.Fatal("无数据时各项应为 0：既不 panic，也不给假数字")
	}
}

func TestMetricsRecordConfirmed(t *testing.T) {
	var m Metrics
	m.RecordConfirmed(true)  // 人工确认并转了工单
	m.RecordConfirmed(false) // 仅人工确认
	m.RecordTicket()         // Odoo 侧上报转工单

	if m.ManualConfirmed.Load() != 2 {
		t.Fatalf("人工确认应为 2，得 %d", m.ManualConfirmed.Load())
	}
	if m.ConvertedToTicket.Load() != 2 {
		t.Fatalf("转工单应为 2，得 %d", m.ConvertedToTicket.Load())
	}
}

func TestMetricsRecordsActions(t *testing.T) {
	var m Metrics
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := &Alarm{FirstTS: at.Add(-90 * time.Second)}

	m.record(Decision{Action: ActionCreated, Alarm: a}, at)
	m.record(Decision{Action: ActionConfirmed, Alarm: a}, at)
	m.record(Decision{Action: ActionBatched, Alarm: a}, at) // 聚合后那一条也是通知
	m.record(Decision{Action: ActionSuppressed, Alarm: a}, at)
	m.record(Decision{Action: ActionResolved, Alarm: a}, at)
	m.record(Decision{Action: ActionClosed, Alarm: a}, at)
	m.record(Decision{Action: ActionReopened, Alarm: a}, at)

	if m.Total.Load() != 1 {
		t.Fatalf("Total=%d", m.Total.Load())
	}
	if m.Notified.Load() != 1 {
		t.Fatalf("聚合应计入一次通知，Notified=%d", m.Notified.Load())
	}
	if m.Batched.Load() != 1 {
		t.Fatalf("Batched=%d", m.Batched.Load())
	}
	if m.Suppressed.Load() != 1 || m.Resolved.Load() != 1 || m.Closed.Load() != 1 || m.Reopened.Load() != 1 {
		t.Fatal("各动作计数不符")
	}
	if got := m.AvgConfirmDelay(); got != 90*time.Second {
		t.Fatalf("平均确认时长应为 90s，得 %v", got)
	}
}
