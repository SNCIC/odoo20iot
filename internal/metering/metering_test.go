package metering

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakePublisher 记录上报内容，可注入错误。
type fakePublisher struct {
	mu       sync.Mutex
	reports  []UsageReport
	subjects []string
	err      error
}

func (f *fakePublisher) Publish(_ context.Context, subject string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	var r UsageReport
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}
	f.reports = append(f.reports, r)
	f.subjects = append(f.subjects, subject)
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reports)
}

// TestAccumulator_DrainAndSnapshot 验证按 project 隔离，且 Drain 取出即清零。
func TestAccumulator_DrainAndSnapshot(t *testing.T) {
	a := NewAccumulator()
	a.Add(1, MetricMsgCount, 5)
	a.Add(1, MetricMsgCount, 3)
	a.Add(2, MetricMsgCount, 7)
	a.Add(1, MetricConnPeak, 10)
	a.Add(1, MetricMsgCount, 0) // 0 不应产生副作用

	// Snapshot 不清零。
	s := a.Snapshot()
	if s[1][MetricMsgCount] != 8 {
		t.Errorf("project 1 消息数应为 8，得到 %d", s[1][MetricMsgCount])
	}
	if s[2][MetricMsgCount] != 7 {
		t.Errorf("project 2 消息数应为 7，得到 %d", s[2][MetricMsgCount])
	}
	if s[1][MetricConnPeak] != 10 {
		t.Errorf("project 1 连接峰值应为 10，得到 %d", s[1][MetricConnPeak])
	}
	a.Add(1, MetricConnPeak, 7)
	if got := a.Snapshot()[1][MetricConnPeak]; got != 10 {
		t.Fatalf("连接峰值不应累加，期望保留 10，得到 %d", got)
	}
	a.Add(1, MetricConnPeak, 12)
	if got := a.Snapshot()[1][MetricConnPeak]; got != 12 {
		t.Fatalf("连接峰值应取窗口最大值，得到 %d", got)
	}

	// Drain 取出并清零。
	d := a.Drain()
	if d[1][MetricMsgCount] != 8 || d[2][MetricMsgCount] != 7 {
		t.Fatalf("Drain 内容不符: %+v", d)
	}
	if len(a.Drain()) != 0 {
		t.Fatal("Drain 后应清零")
	}
}

// TestReporter_FlushReportsThenClears 验证上报后计数被清空（不会重复计入）。
func TestReporter_FlushReportsThenClears(t *testing.T) {
	a := NewAccumulator()
	pub := new(fakePublisher)
	r, err := NewReporter(ReporterOptions{
		Accumulator: a, Publisher: pub, NodeID: "gw-1",
		Window: time.Second, Logger: testLogger(),
	})
	if err != nil {
		t.Fatalf("构造上报器失败: %v", err)
	}

	a.Add(7, MetricMsgCount, 100)

	if n := r.Flush(context.Background()); n != 1 {
		t.Fatalf("应上报 1 个租户，得到 %d", n)
	}
	if pub.count() != 1 {
		t.Fatalf("应发出 1 条上报，得到 %d", pub.count())
	}
	if got := pub.reports[0].ProjectID; got != 7 {
		t.Errorf("project_id 应为 7，得到 %d", got)
	}
	if got := pub.reports[0].Counters[MetricMsgCount]; got != 100 {
		t.Errorf("消息数应为 100，得到 %d", got)
	}
	if got := r.Metrics().CountersReported.Load(); got != 100 {
		t.Errorf("CountersReported 应为 100，得到 %d", got)
	}

	// 第二次 Flush 应无内容（已 Drain）。
	if n := r.Flush(context.Background()); n != 0 {
		t.Fatalf("已上报的计数不应重复上报，得到 %d", n)
	}
	if pub.count() != 1 {
		t.Fatalf("不应产生第二条上报，得到 %d", pub.count())
	}
}

func TestReporter_FailedReportIsRetriedWithStableReportID(t *testing.T) {
	a := NewAccumulator()
	pub := &fakePublisher{err: errors.New("NATS 不可用（测试注入）")}
	r, err := NewReporter(ReporterOptions{
		Accumulator: a, Publisher: pub, Window: time.Second, Logger: testLogger(),
	})
	if err != nil {
		t.Fatalf("构造上报器失败: %v", err)
	}

	a.Add(1, MetricMsgCount, 50)
	if n := r.Flush(context.Background()); n != 0 {
		t.Fatalf("上报失败时不应计为成功，得到 %d", n)
	}
	if r.Metrics().ReportErrors.Load() != 1 {
		t.Fatalf("应记录 1 次上报失败，得到 %d", r.Metrics().ReportErrors.Load())
	}

	pub.err = nil
	if n := r.Flush(context.Background()); n != 1 {
		t.Fatalf("失败的计数应在下次重试，得到 %d", n)
	}
	if pub.count() != 1 || pub.reports[0].Counters[MetricMsgCount] != 50 {
		t.Fatalf("重试批次内容不符: %+v", pub.reports)
	}
}

// TestReporter_UsesUsageSubject 确认上报落在文档约定的 subject 上。
func TestReporter_UsesUsageSubject(t *testing.T) {
	a := NewAccumulator()
	pub := new(fakePublisher)
	r, err := NewReporter(ReporterOptions{Accumulator: a, Publisher: pub, Logger: testLogger()})
	if err != nil {
		t.Fatalf("构造上报器失败: %v", err)
	}

	a.Add(1, MetricMsgCount, 1)
	r.Flush(context.Background())

	if len(pub.subjects) != 1 || pub.subjects[0] != UsageSubject {
		t.Fatalf("上报 subject 应为 %s，得到 %v", UsageSubject, pub.subjects)
	}
}

// TestReporter_RunFlushesOnShutdown 验证优雅退出前会补发最后一个窗口。
func TestReporter_RunFlushesOnShutdown(t *testing.T) {
	a := NewAccumulator()
	pub := new(fakePublisher)
	r, err := NewReporter(ReporterOptions{
		Accumulator: a, Publisher: pub,
		Window: time.Hour, // 不靠 ticker，只靠退出时的补发
		Logger: testLogger(),
	})
	if err != nil {
		t.Fatalf("构造上报器失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	a.Add(3, MetricMsgCount, 9)
	cancel()
	<-done

	if pub.count() != 1 {
		t.Fatalf("退出前应补发最后一个窗口，得到 %d 条", pub.count())
	}
}
