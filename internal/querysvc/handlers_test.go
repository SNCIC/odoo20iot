package querysvc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/apiauth"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/latest"
	"github.com/SNCIC/odoo20iot/internal/metering"
	"github.com/SNCIC/odoo20iot/internal/modbusgw"
	"github.com/SNCIC/odoo20iot/internal/notifyconfig"
	"github.com/SNCIC/odoo20iot/internal/ota"
	"github.com/SNCIC/odoo20iot/internal/quota"
	"github.com/SNCIC/odoo20iot/internal/ruleconfig"
	"github.com/SNCIC/odoo20iot/internal/shadow"
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

type fakeModbusStore struct {
	configs []modbusgw.Config
	deleted string
}

type fakeAlarmAcknowledger struct {
	projectID int64
	alarmID   string
	actorID   string
}

type fakeAlarmLister struct {
	alarms []*alarm.Alarm
}

type fakeQuotaPolicyStore struct {
	saved   quota.Policy
	deleted string
}

type fakeRuleStore struct {
	rules   []ruleconfig.Rule
	project string
	actor   string
}

func (f *fakeRuleStore) List(_ context.Context, projectID string) ([]ruleconfig.Rule, error) {
	if f.project != "" && f.project != projectID {
		return nil, nil
	}
	return append([]ruleconfig.Rule(nil), f.rules...), nil
}

func (f *fakeRuleStore) SetEnabled(_ context.Context, projectID, ruleID string, enabled bool, actor string) (ruleconfig.Rule, error) {
	for index := range f.rules {
		if f.rules[index].RuleID == ruleID {
			f.rules[index].ProjectID = projectID
			f.rules[index].Enabled = enabled
			f.rules[index].Version++
			f.actor = actor
			return f.rules[index], nil
		}
	}
	return ruleconfig.Rule{}, pgx.ErrNoRows
}

func (f *fakeRuleStore) Upsert(_ context.Context, projectID string, draft ruleconfig.Draft, actor string) (ruleconfig.Rule, error) {
	for index := range f.rules {
		if f.rules[index].RuleID == draft.RuleID {
			f.rules[index].ProjectID = projectID
			f.rules[index].Name = draft.Name
			f.rules[index].Level = draft.Level
			f.rules[index].Enabled = draft.Enabled
			f.rules[index].Priority = draft.Priority
			f.rules[index].Version++
			f.actor = actor
			return f.rules[index], nil
		}
	}
	item := ruleconfig.Rule{ProjectID: projectID, RuleID: draft.RuleID, Name: draft.Name, Level: draft.Level, Enabled: draft.Enabled, Priority: draft.Priority, Version: 1}
	f.rules = append(f.rules, item)
	f.actor = actor
	return item, nil
}

func (f *fakeRuleStore) Delete(_ context.Context, projectID, ruleID, actor string) error {
	for index := range f.rules {
		if f.rules[index].ProjectID == projectID && f.rules[index].RuleID == ruleID {
			f.rules = append(f.rules[:index], f.rules[index+1:]...)
			f.actor = actor
			return nil
		}
	}
	return pgx.ErrNoRows
}

type meterRecorder struct {
	counts map[string]int64
}

type fakeShadowService struct {
	snapshot shadow.Snapshot
	err      error
}

type fakeOTAStore struct {
	firmwares []ota.Firmware
	task      ota.Task
	devices   []ota.TaskDevice
}

type fakeOTAArtifactStore struct {
	artifact ota.Artifact
}

type fakeOTASigner struct{}

func (fakeOTASigner) SignManifest(manifest ota.Manifest, now time.Time, ttl time.Duration) (ota.Manifest, error) {
	manifest.SigningKeyID = "test-key"
	manifest.ExpiresAt = now.Add(ttl).UTC().Format(time.RFC3339)
	manifest.Signature = "test-signature"
	return manifest, nil
}

func (f *fakeOTAArtifactStore) PutContext(_ context.Context, projectID int64, filename string, _ io.Reader) (ota.Artifact, error) {
	f.artifact = ota.Artifact{Key: "1/hash", Filename: filename, SizeBytes: 4, SHA256: ota.Digest([]byte("test"))}
	f.artifact.Key = fmt.Sprintf("%d/hash", projectID)
	return f.artifact, nil
}

func (f *fakeOTAArtifactStore) Open(int64, string) (*os.File, error) {
	return nil, os.ErrNotExist
}

func (f *fakeOTAStore) ListFirmwares(context.Context, int64) ([]ota.Firmware, error) {
	return f.firmwares, nil
}

func (f *fakeOTAStore) ListTasks(context.Context, int64) ([]ota.Task, error) {
	if f.task.ID == "" {
		return nil, nil
	}
	return []ota.Task{f.task}, nil
}

func (f *fakeOTAStore) GetFirmware(context.Context, int64, int64) (ota.Firmware, error) {
	if len(f.firmwares) == 0 {
		return ota.Firmware{}, ota.ErrFirmwareNotFound
	}
	return f.firmwares[0], nil
}

func (f *fakeOTAStore) RegisterFirmware(_ context.Context, firmware ota.Firmware, createdBy string) (ota.Firmware, error) {
	firmware.ID = int64(len(f.firmwares) + 1)
	_ = createdBy
	f.firmwares = append(f.firmwares, firmware)
	return firmware, nil
}

func (f *fakeOTAStore) CreateTask(_ context.Context, projectID, firmwareID int64, deviceKeys []string, rollout ota.Rollout, offlineTTL time.Duration, createdBy string) (ota.Task, error) {
	f.task = ota.Task{ID: "550e8400-e29b-41d4-a716-446655440000", ProjectID: projectID, FirmwareID: firmwareID, Status: ota.TaskDraft, Rollout: rollout, OfflineTTL: offlineTTL, CreatedBy: createdBy}
	f.devices = make([]ota.TaskDevice, 0, len(deviceKeys))
	for _, deviceKey := range deviceKeys {
		f.devices = append(f.devices, ota.TaskDevice{TaskID: f.task.ID, ProjectID: projectID, DeviceKey: deviceKey, Status: ota.DevicePending})
	}
	return f.task, nil
}

func (f *fakeOTAStore) CreateRollbackTask(_ context.Context, projectID int64, sourceTaskID string, firmwareID int64, reason, createdBy string) (ota.Task, error) {
	f.task = ota.Task{ID: "550e8400-e29b-41d4-a716-446655440001", ProjectID: projectID, FirmwareID: firmwareID, Status: ota.TaskDraft, Rollout: ota.DefaultRollout(), OfflineTTL: ota.DefaultOfflineTTL, RollbackOf: &sourceTaskID, RollbackReason: reason, IsRollback: true, CreatedBy: createdBy}
	return f.task, nil
}

func (f *fakeOTAStore) GetTask(context.Context, int64, string) (ota.Task, error) {
	return f.task, nil
}

func (f *fakeOTAStore) StartTask(_ context.Context, _ int64, _ string) (ota.Task, error) {
	f.task.Status = ota.TaskRunning
	return f.task, nil
}

func (f *fakeOTAStore) MarkNotified(_ context.Context, _ int64, _ string, deviceKey string) error {
	for index := range f.devices {
		if f.devices[index].DeviceKey == deviceKey {
			f.devices[index].Status = ota.DeviceNotified
			return nil
		}
	}
	return ota.ErrDeviceNotFound
}

func (f *fakeOTAStore) ReconcileTask(context.Context, int64, string) error { return nil }
func (f *fakeOTAStore) ReconcileStale(context.Context, int64) error        { return nil }
func (f *fakeOTAStore) ClaimRunningTasks(context.Context, int64, int, time.Duration) ([]ota.Task, error) {
	return nil, nil
}
func (f *fakeOTAStore) SetBatchState(context.Context, int64, string, int, int, ota.TaskStatus) error {
	return nil
}
func (f *fakeOTAStore) ReleaseDispatchLease(context.Context, int64, string) error  { return nil }
func (f *fakeOTAStore) BeginDispatch(context.Context, int64, string, string) error { return nil }
func (f *fakeOTAStore) ResetDispatch(context.Context, int64, string, string) error { return nil }

func (f *fakeOTAStore) ListTaskDevices(context.Context, int64, string) ([]ota.TaskDevice, error) {
	return f.devices, nil
}

func (f *fakeShadowService) Get(context.Context, int64, string) (shadow.Snapshot, error) {
	if f.err != nil {
		return shadow.Snapshot{}, f.err
	}
	return f.snapshot, nil
}

func (f *fakeShadowService) UpdateDesired(_ context.Context, _ int64, _ string, patch map[string]any, expectedVersion *int64) (shadow.Snapshot, error) {
	if f.err != nil {
		return shadow.Snapshot{}, f.err
	}
	if expectedVersion != nil && *expectedVersion != f.snapshot.Version {
		return shadow.Snapshot{}, shadow.ErrVersionConflict
	}
	f.snapshot.Desired = shadow.Merge(f.snapshot.Desired, patch)
	f.snapshot.Delta = shadow.Delta(f.snapshot.Desired, f.snapshot.Reported)
	f.snapshot.Version++
	return f.snapshot, nil
}

func (m *meterRecorder) Add(_ int64, metric string, delta int64) {
	if m.counts == nil {
		m.counts = make(map[string]int64)
	}
	m.counts[metric] += delta
}

func (f *fakeQuotaPolicyStore) Policies(context.Context, int64) ([]quota.Policy, error) {
	return []quota.Policy{f.saved}, nil
}
func (f *fakeQuotaPolicyStore) SetPolicy(_ context.Context, p quota.Policy, _ string) error {
	f.saved = p
	return nil
}
func (f *fakeQuotaPolicyStore) DeletePolicy(_ context.Context, _ int64, metric, _ string) error {
	f.deleted = metric
	return nil
}

func (f *fakeAlarmAcknowledger) Acknowledge(_ context.Context, projectID int64, alarmID, actorID string, _ time.Time) error {
	f.projectID, f.alarmID, f.actorID = projectID, alarmID, actorID
	return nil
}

func (f *fakeAlarmLister) ActiveForProject(_ context.Context, _ string, states ...alarm.State) ([]*alarm.Alarm, error) {
	if len(states) == 0 {
		return f.alarms, nil
	}
	allowed := make(map[alarm.State]struct{}, len(states))
	for _, state := range states {
		allowed[state] = struct{}{}
	}
	var out []*alarm.Alarm
	for _, item := range f.alarms {
		if _, ok := allowed[item.State]; ok {
			out = append(out, item)
		}
	}
	return out, nil
}

func (fakeEndpointStore) List(context.Context, int64) ([]notifyconfig.Endpoint, error) {
	return nil, nil
}
func (fakeEndpointStore) Create(_ context.Context, projectID int64, name, channel, _ string) (notifyconfig.Endpoint, error) {
	return notifyconfig.Endpoint{ProjectID: projectID, Name: name, Channel: channel, Enabled: true}, nil
}
func (fakeEndpointStore) Delete(context.Context, int64, int64) error { return nil }

func (f *fakeModbusStore) ListAll(context.Context, int64) ([]modbusgw.Config, error) {
	return f.configs, nil
}
func (f *fakeModbusStore) Upsert(_ context.Context, cfg modbusgw.Config) (modbusgw.Config, error) {
	cfg.ID = 1
	f.configs = []modbusgw.Config{cfg}
	return cfg, nil
}
func (f *fakeModbusStore) Delete(_ context.Context, _ int64, deviceKey string) error {
	f.deleted = deviceKey
	return nil
}

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

func TestReadEndpointsRequireScopes(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"device:read"}}}
	svc.mux = svc.routes()

	for _, tc := range []struct {
		path string
		want int
	}{
		{path: "/api/v1/devices", want: http.StatusOK},
		{path: "/api/v1/series?metric=temp", want: http.StatusForbidden},
	} {
		rec := get(t, svc.Handler(), tc.path, "scoped")
		if rec.Code != tc.want {
			t.Fatalf("%s 期望 %d，得到 %d: %s", tc.path, tc.want, rec.Code, rec.Body.String())
		}
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"telemetry:read"}}}
	svc.mux = svc.routes()
	if rec := get(t, svc.Handler(), "/api/v1/series?metric=temp", "scoped"); rec.Code == http.StatusForbidden {
		t.Fatalf("telemetry:read 不应被拒绝: %s", rec.Body.String())
	}
}

func TestShadowEndpointsRequireScopesAndHandleConflict(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	shadowSvc := &fakeShadowService{snapshot: shadow.Snapshot{
		ProjectID: 1, DeviceKey: "a-1", Desired: map[string]any{}, Reported: map[string]any{}, Delta: map[string]any{}, Version: 1,
	}}
	svc.deps.Shadows = shadowSvc

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"shadow:read"}}}
	svc.mux = svc.routes()
	rec := get(t, svc.Handler(), "/api/v1/shadows/a-1", "scoped")
	if rec.Code != http.StatusOK {
		t.Fatalf("shadow:read 期望 200，得到 %d: %s", rec.Code, rec.Body.String())
	}
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/shadows/a-1/desired", strings.NewReader(`{"patch":{"mode":"auto"}}`))
	req.Header.Set("Authorization", "Bearer scoped")
	patchRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(patchRec, req)
	if patchRec.Code != http.StatusForbidden {
		t.Fatalf("缺 shadow:write 期望 403，得到 %d: %s", patchRec.Code, patchRec.Body.String())
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"shadow:write"}}}
	svc.mux = svc.routes()
	req = httptest.NewRequest(http.MethodPatch, "/api/v1/shadows/a-1/desired", strings.NewReader(`{"patch":{"mode":"auto"},"expected_version":0}`))
	req.Header.Set("Authorization", "Bearer scoped")
	patchRec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(patchRec, req)
	if patchRec.Code != http.StatusConflict {
		t.Fatalf("版本冲突期望 409，得到 %d: %s", patchRec.Code, patchRec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPatch, "/api/v1/shadows/a-1/desired", strings.NewReader(`{"patch":{"mode":"auto"},"expected_version":1}`))
	req.Header.Set("Authorization", "Bearer scoped")
	patchRec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(patchRec, req)
	if patchRec.Code != http.StatusAccepted {
		t.Fatalf("正常 patch 期望 202，得到 %d: %s", patchRec.Code, patchRec.Body.String())
	}
}

func TestOTAEndpointsScopesAndLifecycle(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	otaStore := &fakeOTAStore{}
	svc.deps.OTA = otaStore

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, ActorID: "operator", Scopes: []string{"telemetry:read"}}}
	svc.mux = svc.routes()
	rec := get(t, svc.Handler(), "/api/v1/ota/firmwares", "scoped")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("缺 ota:read 期望 403，得到 %d: %s", rec.Code, rec.Body.String())
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, ActorID: "operator", Scopes: []string{"ota:write"}}}
	svc.mux = svc.routes()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ota/firmwares", strings.NewReader(`{"version":"v1.0.0","filename":"edge.bin","object_key":"1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":10,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","signature":"sig","signing_key_id":"key-1"}`))
	req.Header.Set("Authorization", "Bearer scoped")
	rec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated || len(otaStore.firmwares) != 1 {
		t.Fatalf("登记固件期望 201，得到 %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/ota/tasks", strings.NewReader(`{"firmware_id":1,"device_keys":["dev-1","dev-2"]}`))
	req.Header.Set("Authorization", "Bearer scoped")
	rec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || len(otaStore.devices) != 2 {
		t.Fatalf("创建任务期望 202/2 台设备，得到 %d/%d: %s", rec.Code, len(otaStore.devices), rec.Body.String())
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"ota:read"}}}
	svc.mux = svc.routes()
	rec = get(t, svc.Handler(), "/api/v1/ota/tasks", "scoped")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), otaStore.task.ID) {
		t.Fatalf("查询 OTA 任务期望 200，得到 %d: %s", rec.Code, rec.Body.String())
	}
	rec = get(t, svc.Handler(), "/api/v1/ota/tasks/550e8400-e29b-41d4-a716-446655440000/devices", "scoped")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "dev-1") {
		t.Fatalf("查询 OTA 设备任务期望 200，得到 %d: %s", rec.Code, rec.Body.String())
	}
}

func TestOTAArtifactUpload(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	artifactStore := &fakeOTAArtifactStore{}
	svc.deps.OTA = &fakeOTAStore{}
	svc.deps.OTAArtifact = artifactStore
	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"ota:write"}}}
	svc.mux = svc.routes()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("firmware", "edge.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte("test")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ota/artifacts", &body)
	req.Header.Set("Authorization", "Bearer scoped")
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated || artifactStore.artifact.Filename != "edge.bin" {
		t.Fatalf("上传固件期望 201，得到 %d: %s", rec.Code, rec.Body.String())
	}
}

func TestOTAManifestAndRangeDownload(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	artifactStore, err := ota.NewArtifactStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := artifactStore.Put(1, "edge.bin", strings.NewReader("firmware-image"))
	if err != nil {
		t.Fatal(err)
	}
	otaStore := &fakeOTAStore{firmwares: []ota.Firmware{{ID: 1, ProjectID: 1, Version: "v1", Filename: artifact.Filename, ObjectKey: artifact.Key, SizeBytes: artifact.SizeBytes, SHA256: artifact.SHA256}}}
	svc.deps.OTA = otaStore
	svc.deps.OTAArtifact = artifactStore
	svc.deps.OTASigner = fakeOTASigner{}
	svc.deps.OTADownloadSecret = "download-secret"
	svc.deps.OTADownloadBaseURL = "https://iot.invalid/api/v1/ota/download"
	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"ota:read"}}}
	svc.mux = svc.routes()
	rec := get(t, svc.Handler(), "/api/v1/ota/firmwares/1/manifest", "scoped")
	if rec.Code != http.StatusOK {
		t.Fatalf("清单接口期望 200，得到 %d: %s", rec.Code, rec.Body.String())
	}
	var envelope struct {
		Manifest ota.Manifest `json:"manifest"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	downloadURL, err := url.Parse(envelope.Manifest.URL)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, downloadURL.Path+"?"+downloadURL.RawQuery, nil)
	req.Header.Set("Range", "bytes=0-7")
	down := httptest.NewRecorder()
	svc.Handler().ServeHTTP(down, req)
	if down.Code != http.StatusPartialContent || down.Body.String() != "firmware" {
		t.Fatalf("Range 下载期望 206/firmware，得到 %d/%q", down.Code, down.Body.String())
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

func TestControlEndpointsCountOnlyAuthorizedCalls(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	svc.deps.Endpoints = fakeEndpointStore{}
	meter := &meterRecorder{}
	svc.deps.Meter = meter
	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"notification:read"}}}
	svc.mux = svc.routes()

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/notification-endpoints", nil)
	getReq.Header.Set("Authorization", "Bearer scoped")
	getRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK || meter.counts[metering.MetricAPICalls] != 1 {
		t.Fatalf("授权 GET 应成功并计数 1，得到 status=%d count=%d", getRec.Code, meter.counts[metering.MetricAPICalls])
	}

	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/notification-endpoints", strings.NewReader(`{"name":"x","channel":"webhook","target":"https://example.test/hook"}`))
	postReq.Header.Set("Authorization", "Bearer scoped")
	postRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusForbidden || meter.counts[metering.MetricAPICalls] != 1 {
		t.Fatalf("未授权 POST 不应计数，得到 status=%d count=%d", postRec.Code, meter.counts[metering.MetricAPICalls])
	}
}

func TestQuotaPolicyScopesAndValidation(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	store := &fakeQuotaPolicyStore{}
	svc.deps.Quota = store
	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, ActorID: "user-1", Scopes: []string{"quota:read"}}}
	svc.mux = svc.routes()

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/quota/policies", nil)
	getReq.Header.Set("Authorization", "Bearer scoped")
	getRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("quota:read 应允许 GET，得到 %d", getRec.Code)
	}

	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/quota/policies", strings.NewReader(`{"metric":"api_calls","soft_limit":10,"hard_limit":20,"window":"day"}`))
	postReq.Header.Set("Authorization", "Bearer scoped")
	postRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusForbidden {
		t.Fatalf("缺少 quota:write 应拒绝，得到 %d", postRec.Code)
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, ActorID: "user-1", Scopes: []string{"quota:write"}}}
	svc.mux = svc.routes()
	postReq = httptest.NewRequest(http.MethodPost, "/api/v1/quota/policies", strings.NewReader(`{"metric":"api_calls","soft_limit":10,"hard_limit":20,"window":"day"}`))
	postReq.Header.Set("Authorization", "Bearer scoped")
	postRec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusOK || store.saved.Metric != "api_calls" || store.saved.SoftLimit != 10 || store.saved.WarningLimit != 18 {
		t.Fatalf("quota:write 应保存策略，得到 %d: %s", postRec.Code, postRec.Body.String())
	}
}

func TestModbusConfigScopesAndCRUD(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	store := &fakeModbusStore{configs: []modbusgw.Config{{
		ID: 1, Enabled: true, ProjectID: 1, DeviceKey: "plc-1", Transport: "tcp",
		Endpoint: "127.0.0.1:502", UnitID: 1, Interval: 10 * time.Second, Timeout: 5 * time.Second,
		BaudRate: 9600, DataBits: 8, StopBits: 1, Parity: "none",
		Points: []modbusgw.Point{{Name: "temperature", Address: 0, Type: "int16"}},
	}}}
	svc.deps.Modbus = store

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"telemetry:read"}}}
	svc.mux = svc.routes()
	if rec := get(t, svc.Handler(), "/api/v1/modbus/configs", "scoped"); rec.Code != http.StatusForbidden {
		t.Fatalf("缺少 modbus:read 应拒绝，得到 %d", rec.Code)
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"modbus:read"}}}
	svc.mux = svc.routes()
	rec := get(t, svc.Handler(), "/api/v1/modbus/configs", "scoped")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "plc-1") || !strings.Contains(rec.Body.String(), "interval_ms") {
		t.Fatalf("modbus:read 应返回配置，得到 %d: %s", rec.Code, rec.Body.String())
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"modbus:write"}}}
	svc.mux = svc.routes()
	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/modbus/configs", strings.NewReader(`{"device_key":"plc-2","transport":"tcp","endpoint":"127.0.0.1:502","unit_id":1,"interval_ms":1000,"timeout_ms":500,"points":[{"name":"temperature","address":0,"type":"int16"}]}`))
	postReq.Header.Set("Authorization", "Bearer scoped")
	postRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusOK || len(store.configs) != 1 || store.configs[0].DeviceKey != "plc-2" {
		t.Fatalf("modbus:write 应保存配置，得到 %d: %s", postRec.Code, postRec.Body.String())
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/modbus/configs/plc-2", nil)
	deleteReq.Header.Set("Authorization", "Bearer scoped")
	deleteRec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK || store.deleted != "plc-2" {
		t.Fatalf("modbus:write 应删除配置，得到 %d/%q", deleteRec.Code, store.deleted)
	}
}

func TestRuleListAndToggleScopes(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	store := &fakeRuleStore{rules: []ruleconfig.Rule{{
		ProjectID: "1", RuleID: "temperature-high", Name: "温度超限", Level: "P1",
		Enabled: true, Priority: 10, Version: 2,
	}}}
	svc.deps.Rules = store

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"telemetry:read"}}}
	svc.mux = svc.routes()
	if rec := get(t, svc.Handler(), "/api/v1/rules", "scoped"); rec.Code != http.StatusForbidden {
		t.Fatalf("缺少 rule:read 应拒绝，得到 %d", rec.Code)
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"rule:read"}}}
	svc.mux = svc.routes()
	rec := get(t, svc.Handler(), "/api/v1/rules", "scoped")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "temperature-high") || !strings.Contains(rec.Body.String(), "rule_name") {
		t.Fatalf("rule:read 应返回规则列表，得到 %d: %s", rec.Code, rec.Body.String())
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, ActorID: "user-1", Scopes: []string{"rule:write"}}}
	svc.mux = svc.routes()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/rules/temperature-high", strings.NewReader(`{"enabled":false}`))
	req.Header.Set("Authorization", "Bearer scoped")
	rec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || store.rules[0].Enabled || store.rules[0].Version != 3 || store.actor != "user-1" {
		t.Fatalf("rule:write 应更新启用状态，得到 %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodDelete, "/api/v1/rules/temperature-high", nil)
	req.Header.Set("Authorization", "Bearer scoped")
	rec = httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || len(store.rules) != 0 || store.actor != "user-1" {
		t.Fatalf("rule:write 应删除规则，得到 %d: %s", rec.Code, rec.Body.String())
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

func TestAlarmListRequiresScopeAndFiltersStatus(t *testing.T) {
	svc, _ := newTestService(t, &fakeReader{}, nil)
	svc.deps.Alarms = new(fakeAlarmAcknowledger)
	svc.deps.AlarmLister = &fakeAlarmLister{alarms: []*alarm.Alarm{
		{ID: "a-1", DeviceID: "device-1", RuleID: "r-1", RuleName: "温度超限", Level: "P1", State: alarm.StateActive, FirstTS: time.Now().UTC()},
		{ID: "a-2", DeviceID: "device-2", RuleID: "r-2", RuleName: "已确认告警", Level: "P2", State: alarm.StateActive, AcknowledgedAt: time.Now().UTC(), FirstTS: time.Now().UTC()},
	}}
	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"telemetry:read"}}}
	svc.mux = svc.routes()
	if rec := get(t, svc.Handler(), "/api/v1/alarms", "scoped"); rec.Code != http.StatusForbidden {
		t.Fatalf("缺 alarm:read 应拒绝，得到 %d", rec.Code)
	}

	svc.deps.Verifier = identityVerifier{identity: apiauth.Identity{ProjectID: 1, Scopes: []string{"alarm:read"}}}
	svc.mux = svc.routes()
	if rec := get(t, svc.Handler(), "/api/v1/alarms?status=acknowledged", "scoped"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "a-2") || strings.Contains(rec.Body.String(), "a-1") {
		t.Fatalf("acknowledged 过滤结果不符，得到 %d: %s", rec.Code, rec.Body.String())
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
