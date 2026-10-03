package quota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/metering"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeStore 累计增量，可注入错误。
type fakeStore struct {
	mu     sync.Mutex
	totals map[string]int64
	err    error
}

type fakeDurableStore struct {
	records []string
}

func (f *fakeDurableStore) Record(_ context.Context, report metering.UsageReport, metric string, delta int64) error {
	f.records = append(f.records, fmt.Sprintf("%s:%s:%d", report.ReportID, metric, delta))
	return nil
}

func (f *fakeStore) IncrBy(_ context.Context, projectID int64, metric string, delta int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	if f.totals == nil {
		f.totals = make(map[string]int64)
	}
	key := fmt.Sprintf("%d:%s", projectID, metric)
	f.totals[key] += delta
	return f.totals[key], nil
}

func report(t *testing.T, r metering.UsageReport) []byte {
	t.Helper()
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("序列化上报失败: %v", err)
	}
	return data
}

// TestAggregator_ApplyAccumulates 验证多个窗口的上报被累加到同一天桶。
func TestAggregator_ApplyAccumulates(t *testing.T) {
	store := new(fakeStore)
	agg := NewAggregator(store, nil, testLogger())

	// 同一租户、同一指标，两个窗口。
	for index, v := range []int64{100, 50} {
		data := report(t, metering.UsageReport{
			ProjectID: 7,
			ReportID:  fmt.Sprintf("report-%d", index),
			Counters:  map[string]int64{metering.MetricMsgCount: v},
		})
		if err := agg.Apply(context.Background(), data); err != nil {
			t.Fatalf("Apply 失败: %v", err)
		}
	}

	if got := store.totals["7:"+metering.MetricMsgCount]; got != 150 {
		t.Fatalf("消息数应累加为 150，得到 %d", got)
	}
	if got := agg.Metrics().ReportsTotal.Load(); got != 2 {
		t.Fatalf("应处理 2 批，得到 %d", got)
	}
}

// TestAggregator_SkipsZeroDelta 验证零增量不产生计数器写入（避免无谓的 Redis 往返）。
func TestAggregator_SkipsZeroDelta(t *testing.T) {
	store := new(fakeStore)
	agg := NewAggregator(store, nil, testLogger())

	data := report(t, metering.UsageReport{
		ProjectID: 1,
		ReportID:  "report-zero",
		Counters:  map[string]int64{metering.MetricMsgCount: 0},
	})
	if err := agg.Apply(context.Background(), data); err != nil {
		t.Fatalf("Apply 失败: %v", err)
	}
	if len(store.totals) != 0 {
		t.Fatalf("零增量不应写入计数器，得到 %v", store.totals)
	}
	if got := agg.Metrics().CountersTotal.Load(); got != 0 {
		t.Fatalf("零增量不应计入 CountersTotal，得到 %d", got)
	}
}

// TestAggregator_PoisonIsPermanent 验证非法 JSON 被标记为不可重试。
func TestAggregator_PoisonIsPermanent(t *testing.T) {
	agg := NewAggregator(new(fakeStore), nil, testLogger())

	err := agg.Apply(context.Background(), []byte("{not-json"))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("应为 ErrPermanent，得到 %v", err)
	}
	if got := agg.Metrics().PermanentTotal.Load(); got != 1 {
		t.Fatalf("应记录 1 条毒消息，得到 %d", got)
	}
}

// TestAggregator_StoreErrorIsRetryable 验证存储失败**不是**毒消息（应重投）。
func TestAggregator_StoreErrorIsRetryable(t *testing.T) {
	store := &fakeStore{err: errors.New("Redis 不可用（测试注入）")}
	agg := NewAggregator(store, nil, testLogger())

	data := report(t, metering.UsageReport{
		ProjectID: 1,
		ReportID:  "report-error",
		Counters:  map[string]int64{metering.MetricMsgCount: 10},
	})
	err := agg.Apply(context.Background(), data)
	if err == nil {
		t.Fatal("存储失败应返回错误")
	}
	if errors.Is(err, ErrPermanent) {
		t.Fatal("存储失败不应被标记为毒消息（重投后 Redis 恢复即可补上计数）")
	}
	if got := agg.Metrics().Errors.Load(); got != 1 {
		t.Fatalf("应记录 1 次错误，得到 %d", got)
	}
}

func TestAggregator_DurableStoreReceivesStableReportID(t *testing.T) {
	store := new(fakeStore)
	durable := new(fakeDurableStore)
	agg := NewAggregator(store, nil, testLogger()).WithDurableStore(durable)
	data := report(t, metering.UsageReport{
		ProjectID: 7,
		NodeID:    "gw-1",
		Window:    time.Date(2026, 10, 3, 1, 2, 0, 0, time.UTC),
		ReportID:  "report-1",
		Counters:  map[string]int64{metering.MetricMsgCount: 10},
	})
	if err := agg.Apply(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if err := agg.Apply(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if len(durable.records) != 2 || durable.records[0] != durable.records[1] {
		t.Fatalf("report id must remain stable: %v", durable.records)
	}
}

// TestCounterKey 验证键格式与按天分桶（02 §5.2）。
func TestCounterKey(t *testing.T) {
	day := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	got := CounterKey(7, metering.MetricMsgCount, day)
	if want := "quota:7:msg_count:20261001"; got != want {
		t.Fatalf("键格式错误：期望 %s，得到 %s", want, got)
	}

	// 同一天内不同时刻应落在同一桶。
	if CounterKey(7, metering.MetricMsgCount, day.Add(time.Hour)) != got {
		t.Error("同一天应落同一桶")
	}
	// 跨天应换桶。
	if CounterKey(7, metering.MetricMsgCount, day.Add(24*time.Hour)) == got {
		t.Error("跨天应换桶")
	}
}
