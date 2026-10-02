package envelope

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func sample() Envelope {
	return Envelope{
		SchemaVersion: CurrentSchemaVersion,
		TraceID:       "0123456789abcdef0123456789abcdef",
		ProjectID:     1,
		DeviceKey:     "dev-A",
		DeviceID:      PlaceholderDeviceID("dev-A"),
		DeviceTypeID:  55,
		Stream:        "telemetry",
		ReceivedAt:    time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		Payload:       json.RawMessage(`{"ts":"2026-10-01T08:12:33.421Z","seq":1042,"data":{"temperature":25.3}}`),
	}
}

// TestEnvelope_RoundTrip 验证编解码往返后归属与原始报文都无损。
func TestEnvelope_RoundTrip(t *testing.T) {
	orig := sample()

	data, err := orig.Encode()
	if err != nil {
		t.Fatalf("Encode 失败: %v", err)
	}

	got, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}

	if got.DeviceKey != orig.DeviceKey {
		t.Errorf("device_key 丢失: %q", got.DeviceKey)
	}
	if got.SchemaVersion != CurrentSchemaVersion || got.TraceID != orig.TraceID {
		t.Errorf("信封追踪元数据不一致: %+v", got)
	}
	if got.Stream != "telemetry" {
		t.Errorf("stream 应为 telemetry，得到 %q", got.Stream)
	}
	if got.DeviceID != orig.DeviceID || got.DeviceTypeID != orig.DeviceTypeID || got.ProjectID != orig.ProjectID {
		t.Errorf("归属元数据不一致: %+v", got)
	}
	if !bytes.Equal(got.Payload, orig.Payload) {
		t.Errorf("payload 被改写:\n 期望 %s\n 实际 %s", orig.Payload, got.Payload)
	}
}

func TestNewTraceID(t *testing.T) {
	traceID, err := NewTraceID()
	if err != nil || len(traceID) != 32 {
		t.Fatalf("trace_id=%q err=%v", traceID, err)
	}
}

// TestEnvelope_PayloadNotBase64 验证 payload 以 JSON 内联而不是 base64 编码。
//
// 这直接关系总线体积：`[]byte` 字段会被 base64 膨胀 33%，而 03 §2.4 已要求
// 进入管道的报文是合法 JSON，故必须内联。
func TestEnvelope_PayloadNotBase64(t *testing.T) {
	data, err := sample().Encode()
	if err != nil {
		t.Fatalf("Encode 失败: %v", err)
	}

	// 内联时原始 JSON 片段应原样出现；base64 编码时不会。
	if !strings.Contains(string(data), `"temperature":25.3`) {
		t.Fatalf("payload 未内联（疑似被 base64 编码）:\n%s", data)
	}
}

// TestDecode_RejectsInvalid 验证入口校验：缺关键字段 / 非法 JSON 一律拒绝。
func TestDecode_RejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"缺 device_key": `{"schema_version":"1","trace_id":"0123456789abcdef0123456789abcdef","payload":{"a":1}}`,
		"缺 payload":    `{"schema_version":"1","trace_id":"0123456789abcdef0123456789abcdef","device_key":"dev-A"}`,
		"缺 schema":     `{"trace_id":"0123456789abcdef0123456789abcdef","device_key":"dev-A","payload":{"a":1}}`,
		"非法 trace":     `{"schema_version":"1","trace_id":"bad","device_key":"dev-A","payload":{"a":1}}`,
		"非法 JSON":      `{"device_key":"dev-A",`,
	}
	for name, raw := range cases {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Errorf("%s：应报错但通过了", name)
		}
	}
}

// TestPlaceholderDeviceID_Stable 保证占位 ID 对同一设备恒定（分片与落库的前提）。
func TestPlaceholderDeviceID_Stable(t *testing.T) {
	first := PlaceholderDeviceID("dev-A")
	for i := 0; i < 100; i++ {
		if got := PlaceholderDeviceID("dev-A"); got != first {
			t.Fatalf("同一设备的占位 ID 应稳定，%d != %d", got, first)
		}
	}
	if first < 0 {
		t.Fatalf("占位 ID 应为正数，得到 %d", first)
	}
	if PlaceholderDeviceID("dev-B") == first {
		t.Fatal("不同设备应得到不同占位 ID")
	}
}
