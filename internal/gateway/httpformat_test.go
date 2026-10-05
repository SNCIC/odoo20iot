package gateway

import (
	"encoding/json"
	"testing"
)

func TestNormalizeHTTPPayloadScanner(t *testing.T) {
	got, err := normalizeHTTPPayload(HTTPFormatScanner, "text/plain; charset=utf-8", []byte(" 6901234567890\n"))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(got, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["barcode"] != "6901234567890" || payload["raw"] != "6901234567890" {
		t.Fatalf("payload=%v", payload)
	}
}

func TestNormalizeHTTPPayloadDTUKV(t *testing.T) {
	got, err := normalizeHTTPPayload(HTTPFormatDTUKV, "text/plain", []byte("temperature=25.3&count=7&online=true"))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(got, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["temperature"] != 25.3 || payload["count"] != float64(7) || payload["online"] != true {
		t.Fatalf("payload=%v", payload)
	}
}

func TestNormalizeHTTPPayloadRejectsUnsafeDTUField(t *testing.T) {
	if _, err := normalizeHTTPPayload(HTTPFormatDTUKV, "text/plain", []byte(`{"x":1}=bad`)); err == nil {
		t.Fatal("应拒绝非法字段")
	}
	if _, err := normalizeHTTPPayload(HTTPFormatScanner, "text/plain", []byte("a\nb")); err == nil {
		t.Fatal("应拒绝多行扫码值")
	}
}
