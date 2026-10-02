package querysvc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/apiauth"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/latest"
	"github.com/SNCIC/odoo20iot/internal/notifyconfig"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// fakeReader 是 SeriesReader 的假实现：可返回固定结果/错误，也可阻塞以测试排队。
type fakeReader struct {
	mu    sync.Mutex
	res   tsdb.SeriesResult
	err   error
	block chan struct{} // 非 nil 时读到它才返回
	calls int
}

type identityVerifier struct{ identity apiauth.Identity }

func (v identityVerifier) Verify(context.Context, string) (apiauth.Identity, error) {
	return v.identity, nil
}

type fakeEndpointStore struct{}

type fakeAlarmAcknowledger struct {
	projectID int64
	alarmID   string
	actorID   string
}

func (f *fakeAlarmAcknowledger) Acknowledge(_ context.Context, projectID int64, alarmID, actorID string, _ time.Time) error {
	f.projectID, f.alarmID, f.actorID = projectID, alarmID, actorID
	return nil
}

func (fakeEndpointStore) List(context.Context, int64) ([]notifyconfig.Endpoint, error) {
	return nil, nil
}
func (fakeEndpointStore) Create(_ context.Context, projectID int64, name, channel, _ string) (notifyconfig.Endpoint, error) {
	return notifyconfig.Endpoint{ProjectID: projectID, Name: name, Channel: channel, Enabled: true}, nil
}
func (fakeEndpointStore) Delete(context.Context, int64, int64) error { return nil }

func (f *fakeReader) QuerySeries(ctx context.Context, _ tsdb.Plan, _ tsdb.SeriesQuery) (tsdb.SeriesResult, error) {
	f.mu.Lock()
	f.calls++
	block, res, err := f.block, f.res, f.err
	f.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return tsdb.SeriesResult{}, ctx.Err()
		}
	}
	if err != nil {
		return tsdb.SeriesResult{}, err
	}
	return res, nil
}

func (f *fakeReader) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func newTestService(t *testing.T, reader SeriesReader, cfg *Config) (*Service, *catalog.MemStore) {
	t.Helper()
	store := catalog.NewMemStore()
	store.Add(
		catalog.Device{ID: 1, ProjectID: 1, DeviceTypeID: 10, DeviceKey: "a-1", Name: "泵 A", Status: "active", CreatedAt: time.Unix(0, 0).UTC(), UpdatedAt: time.Unix(0, 0).UTC()},
		catalog.Device{ID: 2, ProjectID: 1, DeviceTypeID: 10, DeviceKey: "a-2", Name: "泵 B", Status: "inactive", CreatedAt: time.Unix(0, 0).UTC(), UpdatedAt: time.Unix(0, 0).UTC()},
		catalog.Device{ID: 9, ProjectID: 2, DeviceTypeID: 20, DeviceKey: "x-1", Name: "他租户", Status: "active", CreatedAt: time.Unix(0, 0).UTC(), UpdatedAt: time.Unix(0, 0).UTC()},
	)
	verifier, err := apiauth.NewStaticTokenVerifier("tok", 1)
	if err != nil {
		t.Fatalf("构造校验器失败: %v", err)
	}
	c := DefaultConfig()
	if cfg != nil {
		c = *cfg
	}
	svc, err := New(c, Deps{
		Reader:   reader,
		Catalog:  store,
		Verifier: verifier,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:      func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("构造服务失败: %v", err)
	}
	return svc, store
}

func get(t *testing.T, h http.Handler, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeErr(t *testing.T, rec *httptest.ResponseRecorder) apiError {
	t.Helper()
	var e apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("响应不是错误 JSON：%s（%v）", rec.Body.String(), err)
	}
	return e
}

func TestHandlers_Auth(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	h := svc.Handler()

	if rec := get(t, h, "/api/v1/devices", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("无令牌期望 401，得到 %d", rec.Code)
	}
	if rec := get(t, h, "/api/v1/devices", "wrong"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("错令牌期望 401，得到 %d", rec.Code)
	}
	if rec := get(t, h, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("/healthz 不该要令牌，得到 %d", rec.Code)
	}
}

func TestHandlers_Devices(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	h := svc.Handler()

	rec := get(t, h, "/api/v1/devices", "tok")
	if rec.Code != http.StatusOK {
		t.Fatalf("期望 200，得到 %d（%s）", rec.Code, rec.Body.String())
	}
	var resp devicesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if len(resp.Devices) != 2 {
		t.Fatalf("本租户应有 2 台，得到 %d", len(resp.Devices))
	}
	// 绝不能出现任何凭据字段。
	if strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("设备响应不得含凭据字段：%s", rec.Body.String())
	}

	// 分页：limit=1 → next_cursor=1；再取下一页得 id=2。
	rec = get(t, h, "/api/v1/devices?limit=1", "tok")
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Devices) != 1 || resp.NextCursor != "1" {
		t.Fatalf("首页期望 1 台/游标 1，得到 %d/%q", len(resp.Devices), resp.NextCursor)
	}
	rec = get(t, h, "/api/v1/devices?limit=1&cursor="+resp.NextCursor, "tok")
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Devices) != 1 || resp.Devices[0].ID != 2 {
		t.Fatalf("次页期望 id=2，得到 %+v", resp.Devices)
	}

	// 过滤器。
	rec = get(t, h, "/api/v1/devices?status=active", "tok")
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Devices) != 1 {
		t.Fatalf("状态过滤期望 1 台，得到 %d", len(resp.Devices))
	}
}

func TestHandlers_TenantParamRejected(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	h := svc.Handler()

	for _, path := range []string{
		"/api/v1/devices?project_id=999",
		"/api/v1/series?project_id=999&device_ids=1&metric=temperature",
	} {
		rec := get(t, h, path, "tok")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s 期望 400，得到 %d", path, rec.Code)
		}
		if e := decodeErr(t, rec); e.Code != CodeTenantNotAllowed {
			t.Fatalf("%s 期望 TENANT_NOT_ALLOWED，得到 %s", path, e.Code)
		}
	}
}

func TestNotificationEndpointScopes(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	svc.deps.Endpoints = fakeEndpointStore{}
	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"notification:read"}}}
	svc.mux = svc.routes()

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/notification-endpoints", nil)
	getReq.Header.Set("Authorization", "Bearer scoped")
	getRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("notification:read 应允许 GET，得到 %d: %s", getRec.Code, getRec.Body.String())
	}

	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/notification-endpoints", strings.NewReader(`{"name":"x","channel":"webhook","target":"https://example.test/hook"}`))
	postReq.Header.Set("Authorization", "Bearer scoped")
	postRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusForbidden {
		t.Fatalf("缺少 notification:write 应拒绝 POST，得到 %d", postRec.Code)
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"notification:write"}}}
	svc.mux = svc.routes()
	postReq = httptest.NewRequest(http.MethodPost, "/api/v1/notification-endpoints", strings.NewReader(`{"name":"x","channel":"webhook","target":"https://example.test/hook"}`))
	postReq.Header.Set("Authorization", "Bearer scoped")
	postRec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusCreated {
		t.Fatalf("notification:write 应允许 POST，得到 %d: %s", postRec.Code, postRec.Body.String())
	}
}

func TestAlarmAcknowledgeRequiresScope(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	ack := new(fakeAlarmAcknowledger)
	svc.deps.Alarms = ack
	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, ActorID: "user-1", Scopes: []string{"telemetry:read"}}}
	svc.mux = svc.routes()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alarms/alarm-1/ack", nil)
	req.Header.Set("Authorization", "Bearer scoped")
	rec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("缺少 alarm:write 应拒绝，得到 %d", rec.Code)
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 7, ActorID: "user-2", Scopes: []string{"alarm:write"}}}
	svc.mux = svc.routes()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/alarms/alarm-9/ack", nil)
	req.Header.Set("Authorization", "Bearer scoped")
	rec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("alarm:write 应允许确认，得到 %d: %s", rec.Code, rec.Body.String())
	}
	if ack.projectID != 7 || ack.alarmID != "alarm-9" || ack.actorID != "user-2" {
		t.Fatalf("确认参数不正确: %+v", ack)
	}
}

func TestHandlers_Latest(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	store := latest.NewMemStore()
	row := tsdb.Row{
		ProjectID: 1, DeviceID: 1, TS: time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC),
		Values: []tsdb.Value{tsdb.Number(25.5), tsdb.Null(), tsdb.Null(), tsdb.Null(), tsdb.Bool(true)},
	}
	if err := store.Put(context.Background(), row, tsdb.BenchMetrics); err != nil {
		t.Fatal(err)
	}
	svc.deps.Latest = store
	svc.mux = svc.routes()

	rec := get(t, svc.Handler(), "/api/v1/latest?device_ids=1,2", "tok")
	if rec.Code != http.StatusOK {
		t.Fatalf("期望 200，得到 %d（%s）", rec.Code, rec.Body.String())
	}
	var resp latestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Latest) != 2 || !resp.Latest[0].Available || resp.Latest[1].Available {
		t.Fatalf("最新值响应不符: %+v", resp)
	}
	if resp.Latest[0].Values["temperature"] != float64(25.5) || resp.Latest[0].Values["running"] != true {
		t.Fatalf("最新值指标不符: %+v", resp.Latest[0])
	}
	if rec := get(t, svc.Handler(), "/api/v1/latest?device_ids=9", "tok"); rec.Code != http.StatusForbidden {
		t.Fatalf("跨租户最新值应返回 403，得到 %d", rec.Code)
	}
}

func TestHandlers_SeriesRaw(t *testing.T) {
	ts := time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC)
	reader := &fakeReader{res: tsdb.SeriesResult{
		Granularity: tsdb.GranularityRaw,
		Source:      tsdb.SourceRawJSON,
		Points:      []tsdb.SeriesPoint{{TS: ts, DeviceID: 1, Value: 25.5}, {TS: ts.Add(time.Second), DeviceID: 1, Null: true}},
	}}
	svc, _ := newTestService(t, reader, nil)

	rec := get(t, svc.Handler(), "/api/v1/series?device_ids=1,2&metric=temperature&since=2026-10-01T11:00:00Z&until=2026-10-01T12:00:00Z", "tok")
	if rec.Code != http.StatusOK {
		t.Fatalf("期望 200，得到 %d（%s）", rec.Code, rec.Body.String())
	}
	var resp seriesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if resp.Granularity != "raw" || resp.Source != "raw_json" || resp.CapHit {
		t.Fatalf("元数据不符: %+v", resp)
	}
	if len(resp.Points) != 2 || resp.Points[1].Value != nil {
		t.Fatalf("点不符（第二点应为 null）: %+v", resp.Points)
	}
	if len(resp.Buckets) != 0 {
		t.Fatalf("raw 结果不该带 buckets")
	}
}

func TestHandlers_SeriesAggregatedBucket(t *testing.T) {
	var got tsdb.SeriesQuery
	reader := &captureReader{res: tsdb.SeriesResult{
		Granularity: tsdb.GranularityAggregated, Source: tsdb.SourceRawJSON, Bucket: 5 * time.Minute,
		Buckets: []tsdb.BucketRow{{Bucket: time.Unix(0, 0).UTC(), DeviceID: 1, Avg: 1.5, Max: 2, Count: 3}},
	}, got: &got}
	svc, _ := newTestService(t, reader, nil)

	rec := get(t, svc.Handler(), "/api/v1/series?device_ids=1&metric=temperature&bucket=5m&since=2026-10-01T11:00:00Z&until=2026-10-01T12:00:00Z", "tok")
	if rec.Code != http.StatusOK {
		t.Fatalf("期望 200，得到 %d（%s）", rec.Code, rec.Body.String())
	}
	var resp seriesResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Granularity != "aggregated" || resp.Bucket != "5m0s" {
		t.Fatalf("聚合元数据不符: %+v", resp)
	}
	if got.Bucket != 5*time.Minute {
		t.Fatalf("应把 bucket 传给 Reader，得到 %s", got.Bucket)
	}
}

// captureReader 记录最近一次查询参数。
type captureReader struct {
	res tsdb.SeriesResult
	got *tsdb.SeriesQuery
}

func (c *captureReader) QuerySeries(_ context.Context, _ tsdb.Plan, q tsdb.SeriesQuery) (tsdb.SeriesResult, error) {
	*c.got = q
	return c.res, nil
}

func TestHandlers_SeriesForbidden(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	rec := get(t, svc.Handler(), "/api/v1/series?device_ids=9&metric=temperature", "tok")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("跨租户设备期望 403，得到 %d（%s）", rec.Code, rec.Body.String())
	}
	if e := decodeErr(t, rec); e.Code != CodeForbidden {
		t.Fatalf("期望 FORBIDDEN，得到 %s", e.Code)
	}
}

func TestHandlers_SeriesUnprocessable(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	h := svc.Handler()

	t.Run("指标不在白名单", func(t *testing.T) {
		rec := get(t, h, "/api/v1/series?device_ids=1&metric=not_a_metric", "tok")
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("期望 422，得到 %d", rec.Code)
		}
		if e := decodeErr(t, rec); e.Rule != "metric" {
			t.Fatalf("期望 rule=metric，得到 %q", e.Rule)
		}
	})
	t.Run("设备数超上限", func(t *testing.T) {
		ids := make([]string, 0, tsdb.MaxDetailDevices+1)
		for i := 1; i <= tsdb.MaxDetailDevices+1; i++ {
			ids = append(ids, strconv.Itoa(i))
		}
		rec := get(t, h, "/api/v1/series?device_ids="+strings.Join(ids, ",")+"&metric=temperature", "tok")
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("期望 422，得到 %d", rec.Code)
		}
	})
	t.Run("回溯超 90 天提示导出", func(t *testing.T) {
		rec := get(t, h, "/api/v1/series?device_ids=1&metric=temperature&since=2020-01-01T00:00:00Z&until=2026-10-01T12:00:00Z", "tok")
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("期望 422，得到 %d", rec.Code)
		}
		e := decodeErr(t, rec)
		if e.Rule != "lookback" || !e.ExportRequired {
			t.Fatalf("期望 lookback + export_required，得到 %+v", e)
		}
	})
}

func TestHandlers_SeriesBadParams(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	h := svc.Handler()

	cases := []string{
		"/api/v1/series?metric=temperature",                              // 缺 device_ids
		"/api/v1/series?device_ids=abc&metric=temperature",               // 非整数
		"/api/v1/series?device_ids=0&metric=temperature",                 // 非正
		"/api/v1/series?device_ids=1",                                    // 缺 metric
		"/api/v1/series?device_ids=1&metric=temperature&since=yesterday", // 时间格式
		"/api/v1/series?device_ids=1&metric=temperature&bucket=0s",       // 桶宽非正
		"/api/v1/series?device_ids=1&metric=temperature&limit=-1",        // 行数非正
	}
	for _, path := range cases {
		rec := get(t, h, path, "tok")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s 期望 400，得到 %d（%s）", path, rec.Code, rec.Body.String())
		}
	}
}

func TestHandlers_SeriesReaderErrors(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	h := svc.Handler()

	// Reader 返回 PolicyError（理论上已被本地 Normalize 拦下，这里模拟竞态后的兜底）。
	svc.deps.Reader.(*fakeReader).setErr(&tsdb.PolicyError{Rule: "devices", Msg: "设备太多"})
	rec := get(t, h, "/api/v1/series?device_ids=1&metric=temperature", "tok")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("PolicyError 期望 422，得到 %d", rec.Code)
	}

	svc.deps.Reader.(*fakeReader).setErr(context.DeadlineExceeded)
	rec = get(t, h, "/api/v1/series?device_ids=1&metric=temperature", "tok")
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("超时期望 504，得到 %d", rec.Code)
	}
	if e := decodeErr(t, rec); e.Code != CodeQueryTimeout {
		t.Fatalf("期望 QUERY_TIMEOUT，得到 %s", e.Code)
	}

	svc.deps.Reader.(*fakeReader).setErr(errors.New("boom"))
	rec = get(t, h, "/api/v1/series?device_ids=1&metric=temperature", "tok")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("内部错误期望 500，得到 %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "boom") {
		t.Fatalf("错误响应不得泄露内部细节: %s", rec.Body.String())
	}
}

func TestHandlers_SeriesBusy(t *testing.T) {
	block := make(chan struct{})
	reader := &fakeReader{block: block, res: tsdb.SeriesResult{Granularity: tsdb.GranularityRaw, Source: tsdb.SourceRawJSON}}
	cfg := DefaultConfig()
	cfg.Limiter = LimiterConfig{MaxConcurrency: 1, MaxQueue: 0, QueueTimeout: time.Second, MaxTenants: 10, IdleTTL: time.Minute}
	svc, _ := newTestService(t, reader, &cfg)
	h := svc.Handler()

	// 占住唯一的名额。
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = get(t, h, "/api/v1/series?device_ids=1&metric=temperature", "tok")
	}()
	// 等到第一个请求确实进了 Reader。
	waitCalls(t, reader, 1)

	rec := get(t, h, "/api/v1/series?device_ids=1&metric=temperature", "tok")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("排队已满期望 503，得到 %d（%s）", rec.Code, rec.Body.String())
	}
	if e := decodeErr(t, rec); e.Code != CodeQueryBusy {
		t.Fatalf("期望 QUERY_BUSY，得到 %s", e.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("503 应带 Retry-After")
	}

	close(block)
	<-done
}

func waitCalls(t *testing.T, f *fakeReader, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		c := f.calls
		f.mu.Unlock()
		if c >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等待 Reader 被调用 %d 次超时", n)
}

func TestHandlers_ReadyzAndMetrics(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	h := svc.Handler()

	if rec := get(t, h, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("/healthz 期望 200，得到 %d", rec.Code)
	}
	if rec := get(t, h, "/readyz", ""); rec.Code != http.StatusOK {
		t.Fatalf("/readyz 期望 200，得到 %d", rec.Code)
	}

	// 注入失败的探针 → 503。
	svc.deps.Health.PingPG = func(context.Context) error { return errors.New("pg down") }
	if rec := get(t, h, "/readyz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PG 不可达期望 503，得到 %d", rec.Code)
	}
	svc.deps.Health.PingPG = nil
	svc.deps.Health.PendingMigrations = func(context.Context) ([]string, error) { return []string{"0007_x"}, nil }
	if rec := get(t, h, "/readyz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("有待应用迁移期望 503，得到 %d", rec.Code)
	}

	rec := get(t, h, "/metrics", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "querysvc_requests_total") {
		t.Fatalf("/metrics 内容不符: %d %s", rec.Code, rec.Body.String())
	}
}
