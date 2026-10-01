package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const webhookBody = `{"model":"maintenance.equipment","id":7,` +
	`"write_date":"2026-10-01 06:30:00","company_id":1,"data":{"name":"冲床 A"}}`

func newTestWebhook(t *testing.T, pub Publisher, dedup Deduper) *Webhook {
	t.Helper()
	w, err := NewWebhook(WebhookOptions{
		Publisher: pub,
		Deduper:   dedup,
		Token:     "s3cret",
		Logger:    testLogger(),
	})
	if err != nil {
		t.Fatalf("构造 webhook 失败: %v", err)
	}
	return w
}

func doWebhook(h http.Handler, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, WebhookPath, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// failingDeduper 模拟去重存储故障。
type failingDeduper struct{}

func (failingDeduper) FirstSeen(context.Context, string) (bool, error) {
	return false, ErrGuardUnavailable
}

// TestWebhook_空令牌拒绝构造：一个匿名可写总线的端点比没有端点更危险。
func TestWebhook_空令牌拒绝构造(t *testing.T) {
	if _, err := NewWebhook(WebhookOptions{Publisher: new(fakePub)}); err == nil {
		t.Fatal("空 Token 应拒绝构造（应显式禁用而非放行）")
	}
}

func TestWebhook_鉴权(t *testing.T) {
	pub := new(fakePub)
	w := newTestWebhook(t, pub, NewMemDeduper(time.Minute))

	if rec := doWebhook(w, "", webhookBody); rec.Code != http.StatusUnauthorized {
		t.Fatalf("缺令牌应为 401，得到 %d", rec.Code)
	}
	if rec := doWebhook(w, "wrong", webhookBody); rec.Code != http.StatusUnauthorized {
		t.Fatalf("错误令牌应为 401，得到 %d", rec.Code)
	}
	if len(pub.published()) != 0 {
		t.Fatal("鉴权失败不应发布任何事件")
	}
	if w.metrics.WebhookRejected.Load() != 2 {
		t.Fatalf("应记 2 次拒绝，得到 %d", w.metrics.WebhookRejected.Load())
	}
}

func TestWebhook_只接受POST(t *testing.T) {
	w := newTestWebhook(t, new(fakePub), NewMemDeduper(time.Minute))
	req := httptest.NewRequest(http.MethodGet, WebhookPath, nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	rec := httptest.NewRecorder()
	w.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET 应为 405，得到 %d", rec.Code)
	}
}

// TestWebhook_正常发布 验证 C-2 → NATS 的主路径与事件字段。
func TestWebhook_正常发布(t *testing.T) {
	pub := new(fakePub)
	w := newTestWebhook(t, pub, NewMemDeduper(time.Minute))

	rec := doWebhook(w, "s3cret", webhookBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("应为 200，得到 %d：%s", rec.Code, rec.Body.String())
	}

	calls := pub.published()
	if len(calls) != 1 {
		t.Fatalf("应发布 1 条事件，得到 %d", len(calls))
	}
	if calls[0].subject != "iot.odoo.maintenance_equipment" {
		t.Fatalf("主题错误: %s", calls[0].subject)
	}

	var ev OdooEvent
	if err := json.Unmarshal(calls[0].data, &ev); err != nil {
		t.Fatalf("事件反序列化失败: %v", err)
	}
	// 去重口径即 (model, id, write_date)（§4.4）。
	if ev.EventID != "c2:maintenance.equipment:7:2026-10-01 06:30:00" {
		t.Fatalf("event_id 应为去重口径，得到 %s", ev.EventID)
	}
	if ev.AggregateID != 7 || ev.CompanyID != 1 {
		t.Fatalf("归属字段错误: %+v", ev)
	}
	if string(ev.Payload) != `{"name":"冲床 A"}` {
		t.Fatalf("载荷应取 data 字段，得到 %s", ev.Payload)
	}
	want := time.Date(2026, 10, 1, 6, 30, 0, 0, time.UTC)
	if !ev.OccurredAt.Equal(want) {
		t.Fatalf("occurred_at 应为 %s，得到 %s", want, ev.OccurredAt)
	}
}

// TestWebhook_重复投递被去重 是 §4.4 的口径：
// C-2 没有事务，重复投递若不拦就会产生重复事件。
func TestWebhook_重复投递被去重(t *testing.T) {
	pub := new(fakePub)
	w := newTestWebhook(t, pub, NewMemDeduper(time.Minute))

	if rec := doWebhook(w, "s3cret", webhookBody); rec.Code != http.StatusOK {
		t.Fatalf("首次应为 200，得到 %d", rec.Code)
	}
	rec := doWebhook(w, "s3cret", webhookBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("重复投递应仍为 200（幂等响应），得到 %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "duplicate") {
		t.Fatalf("响应应标明重复: %s", rec.Body.String())
	}
	if got := len(pub.published()); got != 1 {
		t.Fatalf("重复投递不应再发布，实际共发布 %d 条", got)
	}
}

// TestWebhook_去重器故障放行：重复投递比丢事件轻，下游还能按 event_id 兜底。
func TestWebhook_去重器故障放行(t *testing.T) {
	pub := new(fakePub)
	w := newTestWebhook(t, pub, failingDeduper{})

	if rec := doWebhook(w, "s3cret", webhookBody); rec.Code != http.StatusOK {
		t.Fatalf("去重器故障时应放行，得到 %d", rec.Code)
	}
	if got := len(pub.published()); got != 1 {
		t.Fatalf("应照常发布，实际 %d 条", got)
	}
}

func TestWebhook_缺字段拒绝(t *testing.T) {
	pub := new(fakePub)
	w := newTestWebhook(t, pub, NewMemDeduper(time.Minute))

	cases := map[string]string{
		"缺 model": `{"id":7, "write_date":"2026-10-01 06:30:00"}`,
		"缺 id":    `{"model":"maintenance.equipment"}`,
		"非 JSON":  `不是 JSON`,
	}
	for name, body := range cases {
		if rec := doWebhook(w, "s3cret", body); rec.Code != http.StatusUnprocessableEntity &&
			rec.Code != http.StatusBadRequest {
			t.Errorf("%s：应为 4xx，得到 %d", name, rec.Code)
		}
	}
	if len(pub.published()) != 0 {
		t.Fatal("非法请求不应发布")
	}
}

// TestWebhook_发布失败返回503 验证失败会被如实上报（而非假装成功）。
func TestWebhook_发布失败返回503(t *testing.T) {
	pub := &fakePub{errs: []error{context.DeadlineExceeded}}
	w := newTestWebhook(t, pub, NewMemDeduper(time.Minute))

	rec := doWebhook(w, "s3cret", webhookBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("发布失败应为 503，得到 %d", rec.Code)
	}
	if w.metrics.PublishErrors.Load() != 1 {
		t.Fatal("应记 1 次发布失败")
	}
}

// TestWebhook_请求体超限 验证匿名入口不会无限读入。
func TestWebhook_请求体超限(t *testing.T) {
	pub := new(fakePub)
	w := newTestWebhook(t, pub, NewMemDeduper(time.Minute))

	huge := `{"model":"m","id":1,"data":"` + strings.Repeat("x", MaxWebhookBody+16) + `"}`
	if rec := doWebhook(w, "s3cret", huge); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("超大请求体应为 413，得到 %d", rec.Code)
	}
	if len(pub.published()) != 0 {
		t.Fatal("超限请求不应发布")
	}
}
