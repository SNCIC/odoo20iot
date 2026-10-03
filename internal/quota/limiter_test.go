package quota

import (
	"context"
	"testing"
	"time"
)

func TestReserveWithoutLimitAllows(t *testing.T) {
	var counter RedisCounter
	decision, err := counter.Reserve(context.Background(), 1, "msg_count", 0, 1, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Allowed {
		t.Fatal("未配置硬限额时应放行")
	}
}

func TestQuotaWindowBounds(t *testing.T) {
	at := time.Date(2026, 10, 3, 15, 30, 0, 0, time.FixedZone("local", 8*60*60))
	dayStart, dayEnd := quotaWindow(at, "day")
	if dayStart.Format(time.RFC3339) != "2026-10-03T00:00:00Z" || !dayEnd.Equal(dayStart.Add(24*time.Hour)) {
		t.Fatalf("day window=%s..%s", dayStart, dayEnd)
	}
	monthStart, monthEnd := quotaWindow(at, "month")
	if monthStart.Format(time.RFC3339) != "2026-10-01T00:00:00Z" || monthEnd.Format(time.RFC3339) != "2026-11-01T00:00:00Z" {
		t.Fatalf("month window=%s..%s", monthStart, monthEnd)
	}
	if start, _ := quotaWindow(at, "unknown"); !start.IsZero() {
		t.Fatalf("unknown window should be rejected: %s", start)
	}
}

func TestQuotaAlertLevelThreeStages(t *testing.T) {
	p := Policy{SoftLimit: 80, WarningLimit: 90, HardLimit: 100}
	for _, tc := range []struct {
		total int64
		level string
		limit int64
	}{
		{79, "", 0}, {80, "info", 80}, {90, "warning", 90}, {100, "critical", 100},
	} {
		level, limit := quotaAlertLevel(tc.total, p)
		if level != tc.level || limit != tc.limit {
			t.Fatalf("total=%d got %s/%d want %s/%d", tc.total, level, limit, tc.level, tc.limit)
		}
	}
}
