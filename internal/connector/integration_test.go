package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/odoo"
)

// TestIntegration_连接器套真实Odoo客户端 把**真实的 odoo.Client** 接进连接器，
// 用 httptest 模拟 Odoo 的 JSON-2 端点，验证两个包之间的接缝：
// 请求路径、Bearer 鉴权、**多库路由头**、命名参数形态，
// 以及「502 后重试一次成功」在真实 HTTP 上的行为。
//
// 这一层是单测覆盖不到的：fakeClient 绕过了 HTTP，而 httptest 能证明
// 连接器的编排与 odoo 客户端的传输确实能接上。
func TestIntegration_连接器套真实Odoo客户端(t *testing.T) {
	var attempts atomic.Int32
	var gotPath, gotAuth, gotDB atomic.Value

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath.Store(r.URL.Path)
		gotAuth.Store(r.Header.Get("Authorization"))
		gotDB.Store(r.Header.Get("X-Odoo-Database"))

		if attempts.Add(1) == 1 {
			// 第一次模拟 Odoo 抖动：5xx 属可重试的技术错误。
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"boom"}`))
			return
		}
		_, _ = w.Write([]byte(`[{"id":1}]`))
	}))
	defer srv.Close()

	policy := Policy{
		RatePerSecond: 100, RateBurst: 100,
		MaxInFlight: 4, MaxQueue: 16,
		RetryBackoff: []time.Duration{time.Millisecond},
	}.Normalize()

	client, err := odoo.New(odoo.Config{
		BaseURL:    srv.URL,
		Database:   "odoo20",
		APIKey:     "k",
		HTTPClient: policy.HTTPClient(),
	})
	if err != nil {
		t.Fatalf("构造 Odoo 客户端失败: %v", err)
	}

	c, err := New(client, Options{
		Policy: policy,
		Logger: testLogger(),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("构造连接器失败: %v", err)
	}

	var out []map[string]any
	err = c.Call(context.Background(), Request{
		Model: "res.partner", Method: "search_read",
		Params: map[string]any{"domain": []any{}, "fields": []string{"id"}, "limit": 1},
	}, &out)
	if err != nil {
		t.Fatalf("502 后应重试成功，得到 %v", err)
	}

	if got := attempts.Load(); got != 2 {
		t.Fatalf("应请求 2 次（首调 + 1 次重试），实际 %d", got)
	}
	if got := gotPath.Load(); got != "/json/2/res.partner/search_read" {
		t.Errorf("请求路径错误: %v", got)
	}
	if got := gotAuth.Load(); got != "Bearer k" {
		t.Errorf("鉴权头错误: %v", got)
	}
	// 多库部署下缺这个头会静默路由到错误库 —— 必须由客户端强制注入。
	if got := gotDB.Load(); got != "odoo20" {
		t.Errorf("X-Odoo-Database 错误: %v", got)
	}
	if len(out) != 1 {
		t.Fatalf("响应未解析: %v", out)
	}
	if c.Metrics().RetriesTotal.Load() != 1 {
		t.Fatalf("应记 1 次重试，得到 %d", c.Metrics().RetriesTotal.Load())
	}
}

// TestIntegration_真实HTTP下的4xx不重试 验证 §4.3.2 的分类在真实响应上成立。
func TestIntegration_真实HTTP下的4xx不重试(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":"validation"}`))
	}))
	defer srv.Close()

	policy := Policy{
		RatePerSecond: 100, RateBurst: 100, MaxInFlight: 4, MaxQueue: 16,
		RetryBackoff: []time.Duration{time.Millisecond},
	}.Normalize()

	client, err := odoo.New(odoo.Config{BaseURL: srv.URL, Database: "odoo20", APIKey: "k", HTTPClient: policy.HTTPClient()})
	if err != nil {
		t.Fatalf("构造 Odoo 客户端失败: %v", err)
	}
	c, err := New(client, Options{Policy: policy, Logger: testLogger(), Sleep: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatalf("构造连接器失败: %v", err)
	}

	err = c.Call(context.Background(), Request{Model: "m", Method: "x"}, nil)
	if code := codeOf(t, err); code != CodeBusinessRejected {
		t.Fatalf("422 应映射为 BUSINESS_REJECTED，得到 %s", code)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("4xx 不应重试，实际请求 %d 次", got)
	}
}
