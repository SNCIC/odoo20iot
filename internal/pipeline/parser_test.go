package pipeline

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

func newTestParser(t *testing.T) *Parser {
	t.Helper()
	p, err := NewParser([]tsdb.Metric{
		{Key: "temperature", Kind: tsdb.KindNumber},
		{Key: "running", Kind: tsdb.KindBool},
	})
	if err != nil {
		t.Fatalf("构造解析器失败: %v", err)
	}
	return p
}

func envWith(payload string) envelope.Envelope {
	return envelope.Envelope{
		SchemaVersion: envelope.CurrentSchemaVersion,
		TraceID:       "0123456789abcdef0123456789abcdef",
		ProjectID:     7,
		DeviceKey:     "dev-A",
		DeviceID:      12345,
		DeviceTypeID:  55,
		Stream:        "telemetry",
		ReceivedAt:    time.Now(),
		Payload:       json.RawMessage(payload),
	}
}

// TestParser_Parse 覆盖正常路径：ts + 指标 → 时序行，归属元数据来自信封。
func TestParser_Parse(t *testing.T) {
	p := newTestParser(t)

	rec, err := p.Parse(envWith(`{"ts":"2026-10-01T08:12:33.421Z","seq":1042,"data":{"temperature":25.3,"running":true}}`))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	row := rec.Row

	if row.ProjectID != 7 || row.DeviceID != 12345 || row.DeviceTypeID != 55 {
		t.Fatalf("归属元数据未从信封带入: %+v", row)
	}
	if rec.Seq != 1042 {
		t.Errorf("seq 应随记录带出（幂等键需要它），得到 %d", rec.Seq)
	}
	if want := time.Date(2026, 10, 1, 8, 12, 33, 421_000_000, time.UTC); !row.TS.Equal(want) {
		t.Fatalf("ts 解析错误：期望 %s，得到 %s", want, row.TS)
	}
	if len(row.Values) != 2 {
		t.Fatalf("指标数应与物模型一致（2），得到 %d", len(row.Values))
	}
	if row.Values[0].Null || row.Values[0].Number != 25.3 {
		t.Errorf("temperature 解析错误: %+v", row.Values[0])
	}
	if row.Values[1].Null || !row.Values[1].Bool {
		t.Errorf("running 解析错误: %+v", row.Values[1])
	}
}

// TestParser_MissingMetricIsNull 验证未上报的指标记为 Null（不是错误）。
func TestParser_MissingMetricIsNull(t *testing.T) {
	p := newTestParser(t)

	rec, err := p.Parse(envWith(`{"ts":"2026-10-01T00:00:00Z","data":{"temperature":20}}`))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if rec.Row.Values[0].Null {
		t.Error("temperature 应已上报，不应为 Null")
	}
	if !rec.Row.Values[1].Null {
		t.Error("running 未上报，应记为 Null")
	}
}

// TestParser_PermanentErrors 覆盖各类毒消息：必须是 ErrPermanent（不可重试）。
func TestParser_PermanentErrors(t *testing.T) {
	p := newTestParser(t)

	cases := map[string]string{
		"非法 JSON":      `{"ts":`,
		"缺 ts":         `{"data":{"temperature":1}}`,
		"ts 非 ISO8601": `{"ts":"1759300000","data":{}}`,
		"数值位置给字符串":     `{"ts":"2026-10-01T00:00:00Z","data":{"temperature":"hot"}}`,
		"布尔位置给数值":      `{"ts":"2026-10-01T00:00:00Z","data":{"running":1}}`,
	}
	for name, payload := range cases {
		_, err := p.Parse(envWith(payload))
		if err == nil {
			t.Errorf("%s：应报错但通过了", name)
			continue
		}
		if !errors.Is(err, ErrPermanent) {
			t.Errorf("%s：应被标记为 ErrPermanent（毒消息），得到 %v", name, err)
		}
	}
}

// TestNewParser_RejectsEmpty 验证空物模型被拒绝（否则每条消息都会产出空行）。
func TestNewParser_RejectsEmpty(t *testing.T) {
	if _, err := NewParser(nil); err == nil {
		t.Fatal("空物模型应被拒绝")
	}
}
