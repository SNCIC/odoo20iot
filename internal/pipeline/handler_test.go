package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// rowSink 收集 flush 到的行；err 非空时模拟落库失败。
type rowSink struct {
	mu   sync.Mutex
	rows []tsdb.Row
	err  error
}

func (s *rowSink) flush(_ context.Context, rows []tsdb.Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.rows = append(s.rows, rows...)
	return nil
}

func (s *rowSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

// newTestHandler 用真实 Parser/Batcher + 内存幂等存储组装处理器。
func newTestHandler(t *testing.T, sink *rowSink, batchSize int) *Handler {
	t.Helper()
	parser, err := NewParser(tsdb.BenchMetrics)
	if err != nil {
		t.Fatalf("构造解析器失败: %v", err)
	}
	b := NewBatcher[tsdb.Row](batchSize, 20*time.Millisecond, sink.flush)
	b.Start(context.Background())
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	return NewHandler(parser, b, NewMemIdempotency(), new(Metrics), testLogger())
}

// testEnvelope 的归属固定为 project=1 / device=99，供幂等键推断复用。
const (
	testProjectID = 1
	testDeviceID  = 99
)

func mustEnvelope(t *testing.T, payload string) []byte {
	t.Helper()
	env := envelope.Envelope{
		SchemaVersion: envelope.CurrentSchemaVersion,
		TraceID:       "0123456789abcdef0123456789abcdef",
		ProjectID:     testProjectID,
		DeviceKey:     "dev-A",
		DeviceID:      testDeviceID,
		DeviceTypeID:  55,
		Stream:        "telemetry",
		ReceivedAt:    time.Now(),
		Payload:       json.RawMessage(payload),
	}
	data, err := env.Encode()
	if err != nil {
		t.Fatalf("编码信封失败: %v", err)
	}
	return data
}

// TestHandler_HandleAndWait 覆盖正常路径：入批 → 落库 → done。
func TestHandler_HandleAndWait(t *testing.T) {
	sink := new(rowSink)
	h := newTestHandler(t, sink, 2)

	res, err := h.Handle(context.Background(),
		mustEnvelope(t, `{"ts":"2026-10-01T00:00:00Z","seq":1,"data":{"temperature":25.3}}`))
	if err != nil {
		t.Fatalf("Handle 失败: %v", err)
	}
	if res != ResultPersisted {
		t.Fatalf("应落库（ResultPersisted），得到 %s", res)
	}
	if sink.count() != 1 {
		t.Fatalf("应有 1 行落库，实际 %d", sink.count())
	}

	m := h.Metrics()
	if m.ConsumedTotal.Load() != 1 || m.ParsedTotal.Load() != 1 || m.RowsWritten.Load() != 1 {
		t.Fatalf("计数不符: consumed=%d parsed=%d written=%d",
			m.ConsumedTotal.Load(), m.ParsedTotal.Load(), m.RowsWritten.Load())
	}
}

// TestHandler_DuplicateIsSkipped 是幂等的核心判据：
// 同一条 (ts, seq) 的重复交付必须跳过，**不得重复入库**。
func TestHandler_DuplicateIsSkipped(t *testing.T) {
	sink := new(rowSink)
	h := newTestHandler(t, sink, 10)

	data := mustEnvelope(t, `{"ts":"2026-10-01T00:00:00Z","seq":7,"data":{"temperature":1}}`)

	if res, err := h.Handle(context.Background(), data); err != nil || res != ResultPersisted {
		t.Fatalf("首次处理应落库，得到 %s / %v", res, err)
	}
	// 重复交付（NATS redelivery 场景）。
	res, err := h.Handle(context.Background(), data)
	if err != nil {
		t.Fatalf("重复处理不应报错: %v", err)
	}
	if res != ResultDuplicate {
		t.Fatalf("重复消息应判为 ResultDuplicate，得到 %s", res)
	}

	if sink.count() != 1 {
		t.Fatalf("重复消息不应重复入库，实际 %d 行", sink.count())
	}
	if got := h.Metrics().DuplicateTotal.Load(); got != 1 {
		t.Fatalf("应记录 1 次重复交付，得到 %d", got)
	}
}

// TestHandler_ContendedRetries 验证 processing 占用时**绝不 ACK**（必须重试）。
//
// 这是 03 §4.3 修正的第二个数据丢失缺陷：若此处放行 ACK，而上一次处理
// 尚未落库，数据就会永久丢失。
func TestHandler_ContendedRetries(t *testing.T) {
	sink := new(rowSink)
	h := newTestHandler(t, sink, 10)

	ts := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	data := mustEnvelope(t, `{"ts":"2026-10-01T00:00:00Z","seq":7,"data":{"temperature":1}}`)

	// 预置 processing 标记：模拟「同一消息正被（本实例或他实例）处理」。
	key := IdempotencyKey(testProjectID, testDeviceID, ts, 7)
	if ok, err := h.idem.AcquireProcessing(context.Background(), key, time.Minute); err != nil || !ok {
		t.Fatalf("预置 processing 失败: ok=%v err=%v", ok, err)
	}

	res, err := h.Handle(context.Background(), data)
	if err != nil {
		t.Fatalf("Handle 不应报错（重试即可）: %v", err)
	}
	if res != ResultRetry {
		t.Fatalf("processing 占用时应返回 ResultRetry（绝不 ACK），得到 %s", res)
	}
	if sink.count() != 0 {
		t.Fatal("被占用的消息不应入库")
	}
	if got := h.Metrics().ContendedTotal.Load(); got != 1 {
		t.Fatalf("应记录 1 次互斥退避，得到 %d", got)
	}
}

// TestHandler_FlushErrorRetries 验证落库失败 → 重试（不是丢弃）。
func TestHandler_FlushErrorRetries(t *testing.T) {
	sink := &rowSink{err: errors.New("GreptimeDB 不可用（测试注入）")}
	h := newTestHandler(t, sink, 1) // batchSize=1：立即 flush

	res, err := h.Handle(context.Background(),
		mustEnvelope(t, `{"ts":"2026-10-01T00:00:00Z","seq":1,"data":{"temperature":1}}`))
	if err == nil {
		t.Fatal("落库失败应返回错误")
	}
	if res != ResultRetry {
		t.Fatalf("落库失败应为 ResultRetry，得到 %s", res)
	}
	if h.Metrics().FlushErrorTotal.Load() != 1 {
		t.Fatalf("应记录 1 次落库失败，得到 %d", h.Metrics().FlushErrorTotal.Load())
	}
}

// TestHandler_PoisonMessage 验证毒消息被标记为不可重试并计数。
func TestHandler_PoisonMessage(t *testing.T) {
	h := newTestHandler(t, new(rowSink), 2)

	cases := map[string]string{
		"缺 ts":     `{"data":{"temperature":1}}`,
		"ts 非 ISO": `{"ts":"1759300000","data":{}}`,
		"指标类型不符":   `{"ts":"2026-10-01T00:00:00Z","data":{"temperature":"hot"}}`,
	}
	for name, payload := range cases {
		res, err := h.Handle(context.Background(), mustEnvelope(t, payload))
		if !errors.Is(err, ErrPermanent) {
			t.Errorf("%s：应返回 ErrPermanent，得到 %v", name, err)
		}
		if res != ResultPoison {
			t.Errorf("%s：应为 ResultPoison，得到 %s", name, res)
		}
	}

	// 信封本身损坏（不是合法信封）。
	if res, err := h.Handle(context.Background(), []byte("not-an-envelope")); !errors.Is(err, ErrPermanent) || res != ResultPoison {
		t.Errorf("损坏信封应为 ResultPoison/ErrPermanent，得到 %s / %v", res, err)
	}

	if got := h.Metrics().PermanentTotal.Load(); got != 4 {
		t.Fatalf("毒消息计数应为 4，实际 %d", got)
	}
	if h.Metrics().RowsWritten.Load() != 0 {
		t.Fatal("毒消息不应计入 RowsWritten")
	}
}

// TestIdempotencyKey_Stable 保证同一份报文恒定同一键（重投才能被识别）。
func TestIdempotencyKey_Stable(t *testing.T) {
	ts := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	first := IdempotencyKey(1, 99, ts, 7)
	if got := IdempotencyKey(1, 99, ts, 7); got != first {
		t.Fatalf("同一报文的幂等键应稳定，%s != %s", got, first)
	}
	// 不同 seq / ts / 设备应得到不同键，否则会把不同消息误判为重复。
	if IdempotencyKey(1, 99, ts, 8) == first {
		t.Error("不同 seq 应得到不同键")
	}
	if IdempotencyKey(1, 99, ts.Add(time.Second), 7) == first {
		t.Error("不同 ts 应得到不同键")
	}
	if IdempotencyKey(1, 100, ts, 7) == first {
		t.Error("不同设备应得到不同键")
	}
}
