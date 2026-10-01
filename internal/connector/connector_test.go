package connector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sony/gobreaker"

	"github.com/SNCIC/odoo20iot/internal/odoo"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeClient 按顺序返回预设错误，并记录调用次数。
type fakeClient struct {
	mu    sync.Mutex
	errs  []error
	calls int
}

func (f *fakeClient) Call(_ context.Context, _, _ string, _, _ any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	f.calls++
	if i < len(f.errs) {
		return f.errs[i]
	}
	return nil
}

func (f *fakeClient) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func apiErr(status int) error {
	return &odoo.APIError{StatusCode: status, Model: "res.partner", Method: "search_read", Body: "x"}
}

func codeOf(t *testing.T, err error) Code {
	t.Helper()
	if err == nil {
		t.Fatal("期望错误，得到 nil")
	}
	var ce *Error
	if !errors.As(err, &ce) {
		t.Fatalf("期望 *connector.Error，得到 %T: %v", err, err)
	}
	return ce.Code
}

// fastPolicy 不节流、不熔断，只用于验证重试语义。
func fastPolicy() Policy {
	return Policy{
		RatePerSecond: 10000, RateBurst: 10000,
		MaxInFlight: 64, MaxQueue: 1024,
		RetryBackoff:               []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
		BreakerConsecutiveFailures: 100000,
		BreakerMinSamples:          100000,
	}
}

// noRetryPolicy 关闭重试，便于对「熔断计数」做精确断言。
func noRetryPolicy() Policy {
	return Policy{
		RatePerSecond: 10000, RateBurst: 10000,
		MaxInFlight: 8, MaxQueue: 64,
		RetryBackoff: []time.Duration{},
	}
}

func newTestConnector(t *testing.T, client Client, p Policy) *Connector {
	t.Helper()
	c, err := New(client, Options{
		Policy: p,
		Logger: testLogger(),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("构造连接器失败: %v", err)
	}
	return c
}

// TestConnector_业务拒绝不重试 是 §4.3.2 的核心约定：
// 4xx 重放多少次结果都一样，重试只会放大问题。
func TestConnector_业务拒绝不重试(t *testing.T) {
	client := &fakeClient{errs: []error{apiErr(http.StatusUnprocessableEntity)}}
	c := newTestConnector(t, client, fastPolicy())

	err := c.Call(context.Background(), Request{Model: "res.partner", Method: "search_read"}, nil)
	if code := codeOf(t, err); code != CodeBusinessRejected {
		t.Fatalf("422 应映射为 BUSINESS_REJECTED，得到 %s", code)
	}
	if got := client.count(); got != 1 {
		t.Fatalf("4xx 不应重试，Odoo 被调用 %d 次", got)
	}
	if c.Metrics().RetriesTotal.Load() != 0 {
		t.Fatal("4xx 不应产生重试计数")
	}
}

// TestConnector_5xx重试到上限 验证「首调 1 次 + 重试 3 次」后放弃。
func TestConnector_5xx重试到上限(t *testing.T) {
	client := &fakeClient{errs: []error{apiErr(500), apiErr(500), apiErr(503), apiErr(502)}}
	c := newTestConnector(t, client, fastPolicy())

	err := c.Call(context.Background(), Request{Model: "m", Method: "x"}, nil)
	if code := codeOf(t, err); code != CodeUpstreamError {
		t.Fatalf("5xx 应映射为 UPSTREAM_ERROR，得到 %s", code)
	}
	if got := client.count(); got != 4 {
		t.Fatalf("应首调 1 次 + 重试 3 次 = 4，实际 %d", got)
	}
	if got := c.Metrics().RetriesTotal.Load(); got != 3 {
		t.Fatalf("重试计数应为 3，得到 %d", got)
	}
}

// TestConnector_重试后成功 验证退避重试能救回一次抖动。
func TestConnector_重试后成功(t *testing.T) {
	client := &fakeClient{errs: []error{apiErr(500), apiErr(503)}} // 第三次返回 nil
	c := newTestConnector(t, client, fastPolicy())

	if err := c.Call(context.Background(), Request{Model: "m", Method: "x"}, nil); err != nil {
		t.Fatalf("第三次应成功，得到 %v", err)
	}
	if got := client.count(); got != 3 {
		t.Fatalf("应调用 3 次，实际 %d", got)
	}
	if c.Metrics().SuccessTotal.Load() != 1 {
		t.Fatal("应记 1 次成功")
	}
}

// TestConnector_超时仅在携带幂等键时重试 是 §4.3.2 的精确要求：
// 没有幂等键的重试可能造成重复写入。
func TestConnector_超时仅在携带幂等键时重试(t *testing.T) {
	t.Run("无幂等键", func(t *testing.T) {
		client := &fakeClient{errs: []error{apiErr(http.StatusGatewayTimeout)}}
		c := newTestConnector(t, client, fastPolicy())

		err := c.Call(context.Background(), Request{Model: "m", Method: "x"}, nil)
		if code := codeOf(t, err); code != CodeUpstreamTimeout {
			t.Fatalf("504 应映射为 UPSTREAM_TIMEOUT，得到 %s", code)
		}
		if got := client.count(); got != 1 {
			t.Fatalf("无幂等键时超时不应重试，实际调用 %d 次", got)
		}
	})

	t.Run("有幂等键", func(t *testing.T) {
		client := &fakeClient{errs: []error{apiErr(http.StatusGatewayTimeout), apiErr(http.StatusGatewayTimeout)}}
		c := newTestConnector(t, client, fastPolicy())

		err := c.Call(context.Background(), Request{
			Model: "m", Method: "x", IdempotencyKey: "idem:1:op:key",
		}, nil)
		if err != nil {
			t.Fatalf("携带幂等键时应重试到成功，得到 %v", err)
		}
		if got := client.count(); got != 3 {
			t.Fatalf("应重试，实际调用 %d 次", got)
		}
	})
}

// TestConnector_429可重试 验证限流属技术错误。
func TestConnector_429可重试(t *testing.T) {
	client := &fakeClient{errs: []error{apiErr(http.StatusTooManyRequests), apiErr(http.StatusTooManyRequests)}}
	c := newTestConnector(t, client, fastPolicy())

	if err := c.Call(context.Background(), Request{Model: "m", Method: "x"}, nil); err != nil {
		t.Fatalf("429 应重试到成功，得到 %v", err)
	}
	if got := client.count(); got != 3 {
		t.Fatalf("应调用 3 次，实际 %d", got)
	}
}

// TestConnector_连续失败触发熔断 验证 §4.3 的阈值与「打开后快速失败」。
func TestConnector_连续失败触发熔断(t *testing.T) {
	client := &fakeClient{errs: []error{apiErr(500), apiErr(500), apiErr(500)}}
	p := noRetryPolicy()
	p.BreakerConsecutiveFailures = 3
	p.BreakerMinSamples = 100000
	c := newTestConnector(t, client, p)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_ = c.Call(ctx, Request{Model: "m", Method: "x"}, nil)
	}
	if c.BreakerState() != gobreaker.StateOpen {
		t.Fatalf("连续 3 次失败后熔断应打开，当前 %v", c.BreakerState())
	}

	before := client.count()
	err := c.Call(ctx, Request{Model: "m", Method: "x"}, nil)
	if code := codeOf(t, err); code != CodeCircuitOpen {
		t.Fatalf("熔断打开后应返回 CIRCUIT_OPEN，得到 %s", code)
	}
	if client.count() != before {
		t.Fatal("熔断打开后不应再触达 Odoo（快速失败）")
	}
}

// TestConnector_认证失败立即熔断 验证 §4.2：
// 一次 401 就该熔断，否则失效的凭据会先把 Odoo 账号推向被锁。
func TestConnector_认证失败立即熔断(t *testing.T) {
	var alert string
	client := &fakeClient{errs: []error{apiErr(http.StatusUnauthorized), apiErr(http.StatusUnauthorized)}}
	p := noRetryPolicy()
	p.BreakerConsecutiveFailures = 10 // 远未达到
	p.BreakerMinSamples = 10000       // 失败率条件也不可能满足
	p.BreakerOpenP1Alert = func(reason string) { alert = reason }
	c := newTestConnector(t, client, p)

	err := c.Call(context.Background(), Request{Model: "m", Method: "x"}, nil)
	if code := codeOf(t, err); code != CodeAuthRequired {
		t.Fatalf("401 应映射为 AUTH_REQUIRED，得到 %s", code)
	}
	if c.BreakerState() != gobreaker.StateOpen {
		t.Fatalf("401 后熔断应立即打开，当前 %v", c.BreakerState())
	}
	if alert == "" {
		t.Fatal("应触发 P1 告警回调")
	}
}

// TestConnector_业务拒绝不熔断：一个参数 bug 不该把整条链路熔断掉。
func TestConnector_业务拒绝不熔断(t *testing.T) {
	client := &fakeClient{errs: []error{apiErr(422), apiErr(422), apiErr(422), apiErr(422)}}
	p := noRetryPolicy()
	p.BreakerConsecutiveFailures = 3
	p.BreakerMinSamples = 3
	p.BreakerFailureRatio = 0.5
	c := newTestConnector(t, client, p)

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		_ = c.Call(ctx, Request{Model: "m", Method: "x"}, nil)
	}
	if c.BreakerState() != gobreaker.StateClosed {
		t.Fatal("业务拒绝（422）不应触发熔断")
	}
}

// TestConnector_排队满拒绝 验证 §4.3「队列上限，超出拒绝并告警」。
func TestConnector_排队满拒绝(t *testing.T) {
	p := Policy{MaxInFlight: 1, MaxQueue: 1, RatePerSecond: 10000, RateBurst: 10000}.Normalize()
	a := newAdmission(p)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := a.Acquire(ctx); err != nil {
		t.Fatalf("首个请求应通过: %v", err)
	}
	defer a.Release()

	go func() { _ = a.Acquire(ctx) }() // 占住唯一排队位后卡在在途名额上

	deadline := time.Now().Add(2 * time.Second)
	for a.Waiting() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if a.Waiting() == 0 {
		t.Fatal("第二个请求未进入排队")
	}

	if err := a.Acquire(ctx); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("排队已满应拒绝，得到 %v", err)
	}
}

// TestConnector_错误携带TraceID 验证 §4.3.2 的统一响应体要求。
func TestConnector_错误携带TraceID(t *testing.T) {
	client := &fakeClient{errs: []error{apiErr(500)}}
	c := newTestConnector(t, client, noRetryPolicy())

	err := c.Call(context.Background(), Request{Model: "m", Method: "x", TraceID: "t-1"}, nil)
	var ce *Error
	if !errors.As(err, &ce) {
		t.Fatalf("应为 *Error，得到 %v", err)
	}
	if ce.TraceID != "t-1" {
		t.Fatalf("trace_id 应透传，得到 %q", ce.TraceID)
	}
	// 对外文案不得包含底层原文以外的敏感信息（此处只断言可读性）。
	if ce.Code != CodeUpstreamError {
		t.Fatalf("code 应为 UPSTREAM_ERROR，得到 %s", ce.Code)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Code
	}{
		{"401", apiErr(401), CodeAuthRequired},
		{"403", apiErr(403), CodeForbidden},
		{"409", apiErr(409), CodeIdempotencyConflict},
		{"422", apiErr(422), CodeBusinessRejected},
		{"400 归业务拒绝", apiErr(400), CodeBusinessRejected},
		{"404 归业务拒绝", apiErr(404), CodeBusinessRejected},
		{"429", apiErr(429), CodeRateLimited},
		{"500", apiErr(500), CodeUpstreamError},
		{"502", apiErr(502), CodeUpstreamError},
		{"503 是上游错误，不是我们的熔断码", apiErr(503), CodeUpstreamError},
		{"504", apiErr(504), CodeUpstreamTimeout},
		{"context 超时", context.DeadlineExceeded, CodeUpstreamTimeout},
		{"其他错误归上游", errors.New("boom"), CodeUpstreamError},
	}
	for _, tc := range cases {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("%s: 期望 %s，得到 %s", tc.name, tc.want, got)
		}
	}
}

func TestRetryable(t *testing.T) {
	cases := []struct {
		code Code
		key  bool
		want bool
	}{
		{CodeUpstreamError, false, true},
		{CodeRateLimited, false, true},
		{CodeUpstreamTimeout, false, false},
		{CodeUpstreamTimeout, true, true},
		{CodeAuthRequired, true, false},
		{CodeForbidden, true, false},
		{CodeBusinessRejected, true, false},
		{CodeIdempotencyConflict, true, false},
		{CodeCircuitOpen, true, false},
	}
	for _, tc := range cases {
		if got := Retryable(tc.code, tc.key); got != tc.want {
			t.Errorf("Retryable(%s, key=%v) 期望 %v，得到 %v", tc.code, tc.key, tc.want, got)
		}
	}
}

func TestMetrics_按错误码计数(t *testing.T) {
	client := &fakeClient{errs: []error{apiErr(422), apiErr(500)}}
	c := newTestConnector(t, client, noRetryPolicy())

	ctx := context.Background()
	_ = c.Call(ctx, Request{Model: "m", Method: "x"}, nil)
	_ = c.Call(ctx, Request{Model: "m", Method: "x"}, nil)

	got := c.Metrics().ErrorsByCode()
	if got[CodeBusinessRejected] != 1 || got[CodeUpstreamError] != 1 {
		t.Fatalf("错误码计数不符: %v", got)
	}
	if c.Metrics().CallsTotal.Load() != 2 {
		t.Fatalf("调用计数应为 2，得到 %d", c.Metrics().CallsTotal.Load())
	}
}

func TestNew_拒绝空客户端(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("client 为 nil 时应报错")
	}
}

// TestPolicy_HTTPClient超时 验证 §4.3 的「连接 3s / 读 15s」落到传输层。
func TestPolicy_HTTPClient超时(t *testing.T) {
	p := Policy{DialTimeout: 3 * time.Second, ReadTimeout: 15 * time.Second}.Normalize()
	hc := p.HTTPClient()

	if hc.Timeout != 15*time.Second {
		t.Errorf("读超时应为 15s，得到 %s", hc.Timeout)
	}
	tr, ok := hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport 类型不符: %T", hc.Transport)
	}
	if tr.ResponseHeaderTimeout != 15*time.Second {
		t.Errorf("响应头超时应为 15s，得到 %s", tr.ResponseHeaderTimeout)
	}
	if tr.DialContext == nil {
		t.Error("应配置 DialContext（连接超时）")
	}
}

// TestPolicy_NormalizeDefaults 验证零值策略取文档默认。
func TestPolicy_NormalizeDefaults(t *testing.T) {
	p := Policy{}.Normalize()

	if p.RatePerSecond != DefaultRatePerSecond {
		t.Errorf("限流速率应为 %v，得到 %v", DefaultRatePerSecond, p.RatePerSecond)
	}
	if p.MaxInFlight != DefaultMaxInFlight || p.MaxQueue != DefaultMaxQueue {
		t.Errorf("并发/队列默认不符: %d / %d", p.MaxInFlight, p.MaxQueue)
	}
	if p.ReadTimeout != DefaultReadTimeout || p.DialTimeout != DefaultDialTimeout {
		t.Errorf("超时默认不符: %s / %s", p.DialTimeout, p.ReadTimeout)
	}
	if len(p.RetryBackoff) != 3 {
		t.Errorf("退避序列应为 3 档（1s/3s/9s），得到 %v", p.RetryBackoff)
	}
	if p.BreakerConsecutiveFailures != DefaultBreakerConsecutiveFailures {
		t.Errorf("熔断连续失败阈值应为 %d", DefaultBreakerConsecutiveFailures)
	}
}

// TestPolicy_显式空切片表示不重试 验证 nil 与空切片的语义区分。
func TestPolicy_显式空切片表示不重试(t *testing.T) {
	if got := (Policy{}).Normalize().RetryBackoff; got == nil {
		t.Fatal("nil 应取默认退避序列")
	}
	if got := (Policy{RetryBackoff: []time.Duration{}}).Normalize().RetryBackoff; len(got) != 0 {
		t.Fatalf("显式空切片应表示不重试，得到 %v", got)
	}
}
