package odoo

import (
	"strings"
	"testing"
)

// TestSanitizeErrorBody_剥离Python堆栈 是技术方案 p.7 的直接要求：
// 禁止把 Python 堆栈或 ORM 异常原文暴露给外部。
func TestSanitizeErrorBody_剥离Python堆栈(t *testing.T) {
	raw := []byte(`{"name":"werkzeug.exceptions.Unauthorized","message":"Invalid apikey",` +
		`"arguments":["Invalid apikey",401],` +
		`"debug":"Traceback (most recent call last):\n  File \"/x/router.py\", line 446"}`)

	got := sanitizeErrorBody(raw)
	if strings.Contains(got, "Traceback") || strings.Contains(got, "router.py") {
		t.Fatalf("不应保留 Python 堆栈: %s", got)
	}
	if !strings.Contains(got, "Invalid apikey") {
		t.Fatalf("应保留可读的 message: %s", got)
	}
}

// TestSanitizeErrorBody_非JSON整体截断 覆盖 HTML 错误页、网关纯文本等场景。
func TestSanitizeErrorBody_非JSON整体截断(t *testing.T) {
	got := sanitizeErrorBody([]byte("<html><body>" + strings.Repeat("x", 4096) + "</body></html>"))
	if len(got) > 600 {
		t.Fatalf("非 JSON 错误体应被截断，实际 %d 字节", len(got))
	}
}

// TestAPIError_可读且不含堆栈 验证错误文案对外可用且不泄露实现细节。
func TestAPIError_可读且不含堆栈(t *testing.T) {
	err := newAPIError(500, "res.partner", "search_read",
		[]byte(`{"name":"x","message":"boom","debug":"Traceback: secret-internal-path"}`))

	msg := err.Error()
	if strings.Contains(msg, "Traceback") || strings.Contains(msg, "secret-internal-path") {
		t.Fatalf("错误文案不应包含堆栈: %s", msg)
	}
	if !strings.Contains(msg, "res.partner.search_read") || !strings.Contains(msg, "500") {
		t.Fatalf("错误文案应含模型方法名与状态码: %s", msg)
	}
}
