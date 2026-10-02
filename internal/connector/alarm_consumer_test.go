package connector

import (
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/alarm"
)

func TestAlarmMaintenancePayloadUsesFirstTimestampAndEquipmentID(t *testing.T) {
	first := time.Date(2026, 10, 2, 4, 0, 0, 0, time.UTC)
	last := first.Add(5 * time.Minute)
	payload := AlarmMaintenancePayload(alarm.Event{
		AlarmID: "alarm-1", DedupKey: "dedup-1", ProjectID: "7", RuleName: "温度超限",
		Level: "warning", Reason: "temperature > 80", FirstTS: first, LastTS: last,
	}, 42)
	if payload["equipment_id"] != int64(42) {
		t.Fatalf("equipment_id = %#v", payload["equipment_id"])
	}
	if payload["severity"] != "warn" {
		t.Fatalf("severity = %#v", payload["severity"])
	}
	if payload["alarm_ts"] != "2026-10-02 04:00:00" {
		t.Fatalf("alarm_ts = %#v", payload["alarm_ts"])
	}
	if payload["idempotency_key"] != "idem:7:maintenance_req:eq-42-dedup-1" {
		t.Fatalf("idempotency_key = %#v", payload["idempotency_key"])
	}
}

func TestAlarmMaintenancePayloadFallsBackToLastTimestamp(t *testing.T) {
	last := time.Date(2026, 10, 2, 5, 0, 0, 0, time.UTC)
	payload := AlarmMaintenancePayload(alarm.Event{LastTS: last, Level: "unknown"}, 1)
	if payload["severity"] != "warn" {
		t.Fatalf("severity = %#v", payload["severity"])
	}
	if payload["alarm_ts"] != "2026-10-02 05:00:00" {
		t.Fatalf("alarm_ts = %#v", payload["alarm_ts"])
	}
}
