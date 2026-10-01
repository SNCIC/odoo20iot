package odoo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestClient 起一个模拟 Odoo 的 httptest 服务器。
func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := New(Config{BaseURL: srv.URL, Database: "odoo20", APIKey: "test-key"})
	if err != nil {
		t.Fatalf("构造客户端失败: %v", err)
	}
	return c
}

// TestClient_CallSendsRequiredHeaders 是 D2 的核心契约：
// 路径、Bearer 鉴权、**多库路由头**、命名参数，缺一不可。
func TestClient_CallSendsRequiredHeaders(t *testing.T) {
	var (
		gotPath, gotAuth, gotDB, gotCT string
		gotBody                        map[string]any
	)
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotDB = r.Header.Get("X-Odoo-Database")
		gotCT = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	var out struct {
		OK bool `json:"ok"`
	}
	err := c.Call(context.Background(), "maintenance.equipment", "search_read",
		map[string]any{"domain": []any{}, "limit": 10}, &out)
	if err != nil {
		t.Fatalf("Call 失败: %v", err)
	}

	if gotPath != "/json/2/maintenance.equipment/search_read" {
		t.Errorf("路径错误: %s", gotPath)
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("鉴权头错误: %s", gotAuth)
	}
	// 多库部署下缺这个头会静默路由到错误库（07 §2.2），必须强制注入。
	if gotDB != "odoo20" {
		t.Errorf("X-Odoo-Database 错误: %s", gotDB)
	}
	if !strings.HasPrefix(gotCT, "application/json") {
		t.Errorf("Content-Type 错误: %s", gotCT)
	}
	// JSON-2 只支持命名参数：参数必须以 JSON 对象形式发出。
	if gotBody["limit"] != float64(10) {
		t.Errorf("参数未按命名参数发出: %v", gotBody)
	}
	if !out.OK {
		t.Error("响应未解析到 out")
	}
}

// TestClient_ErrorMapping 验证状态码 → 哨兵错误 → 可重试判定（07 §4.3）。
func TestClient_ErrorMapping(t *testing.T) {
	cases := []struct {
		status    int
		sentinel  error
		retryable bool
	}{
		{http.StatusUnauthorized, ErrUnauthorized, false},
		{http.StatusForbidden, ErrForbidden, false},
		{http.StatusConflict, ErrConflict, false},
		{http.StatusUnprocessableEntity, ErrValidation, false},
		{http.StatusBadRequest, nil, false},
		{http.StatusInternalServerError, nil, true},
		{http.StatusServiceUnavailable, nil, true},
		{http.StatusTooManyRequests, nil, true},
	}

	for _, tc := range cases {
		c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(`{"error":"boom"}`))
		})

		err := c.Call(context.Background(), "res.partner", "search_read", nil, nil)
		if err == nil {
			t.Fatalf("%d：应报错", tc.status)
		}
		if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
			t.Errorf("%d：应映射到 %v，得到 %v", tc.status, tc.sentinel, err)
		}
		if got := IsRetryable(err); got != tc.retryable {
			t.Errorf("%d：IsRetryable 应为 %v，得到 %v", tc.status, tc.retryable, got)
		}
	}
}

// TestClient_SearchReadDefaultLimit 验证不设上限时会给安全页大小。
func TestClient_SearchReadDefaultLimit(t *testing.T) {
	var gotBody map[string]any
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`[]`))
	})

	var out []map[string]any
	if err := c.SearchRead(context.Background(), "maintenance.equipment", SearchReadRequest{}, &out); err != nil {
		t.Fatalf("SearchRead 失败: %v", err)
	}
	if gotBody["limit"] != float64(DefaultSearchLimit) {
		t.Fatalf("默认 limit 应为 %d，得到 %v", DefaultSearchLimit, gotBody["limit"])
	}
}

// TestClient_ErrorBodyTruncated 验证错误体被截断（避免把整页 HTML 灌进日志）。
func TestClient_ErrorBodyTruncated(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(strings.Repeat("x", 4096)))
	})

	err := c.Call(context.Background(), "res.partner", "search_read", nil, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("应为 *APIError，得到 %v", err)
	}
	if len(apiErr.Body) > 600 {
		t.Fatalf("错误体应被截断，实际 %d 字节", len(apiErr.Body))
	}
}

// TestNew_Validation 验证必填项与 BaseURL 形态校验。
func TestNew_Validation(t *testing.T) {
	cases := map[string]Config{
		"缺 BaseURL":   {Database: "d", APIKey: "k"},
		"缺 Database":  {BaseURL: "https://x", APIKey: "k"},
		"缺 APIKey":    {BaseURL: "https://x", Database: "d"},
		"BaseURL 无主机": {BaseURL: "not-a-url", Database: "d", APIKey: "k"},
		"BaseURL 缺协议": {BaseURL: "odoo.example.com", Database: "d", APIKey: "k"},
	}
	for name, cfg := range cases {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s：应报错但通过了", name)
		}
	}
}
