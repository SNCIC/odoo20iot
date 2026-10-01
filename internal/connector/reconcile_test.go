package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// testNow 是测试用的固定时钟，让 domain 里的 cutoff 可精确断言。
var testNow = time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

// fakeCaller 按模型排队返回预设 JSON（每次调用消费队首）。
type fakeCaller struct {
	mu      sync.Mutex
	replies map[string][][]byte
	calls   []Request
	err     error
}

func newFakeCaller() *fakeCaller {
	return &fakeCaller{replies: make(map[string][][]byte)}
}

func (f *fakeCaller) push(model string, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replies[model] = append(f.replies[model], raw)
}

func (f *fakeCaller) Call(_ context.Context, req Request, out any) error {
	f.mu.Lock()
	q := f.replies[req.Model]
	var raw []byte
	if len(q) > 0 {
		raw = q[0]
		f.replies[req.Model] = q[1:]
	} else {
		raw = []byte("[]")
	}
	f.calls = append(f.calls, req)
	err := f.err
	f.mu.Unlock()

	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (f *fakeCaller) callsFor(model string) []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Request
	for _, c := range f.calls {
		if c.Model == model {
			out = append(out, c)
		}
	}
	return out
}

func newTestReconciler(t *testing.T, c caller, pub Publisher, wm Watermarks, models ...string) *Reconciler {
	t.Helper()
	r, err := NewReconciler(ReconcileOptions{
		Caller:     c,
		Publisher:  pub,
		Watermarks: wm,
		Models:     models,
		Logger:     testLogger(),
		Now:        func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("构造对账器失败: %v", err)
	}
	return r
}

func stuckOutboxRow(eventID string) map[string]any {
	return map[string]any{
		"event_id":        eventID,
		"company_id":      []any{1, "SNCIC"},
		"aggregate_model": "mrp.workorder",
		"aggregate_id":    4242,
		"version":         3,
		"occurred_at":     "2026-10-01 06:30:00",
		"payload":         map[string]any{"state": "done"},
		"state":           "pending",
		"attempts":        2,
	}
}

// ---------------------------------------------------------------------------
// 水位
// ---------------------------------------------------------------------------

func TestWatermark_排序语义(t *testing.T) {
	base := Watermark{WriteDate: time.Date(2026, 10, 1, 6, 0, 0, 0, time.UTC), ID: 10}

	cases := []struct {
		name string
		w    Watermark
		want bool
	}{
		{"时间更晚", Watermark{WriteDate: base.WriteDate.Add(time.Second), ID: 1}, true},
		{"同秒但 id 更大", Watermark{WriteDate: base.WriteDate, ID: 11}, true},
		{"同秒同 id", Watermark{WriteDate: base.WriteDate, ID: 10}, false},
		{"同秒但 id 更小", Watermark{WriteDate: base.WriteDate, ID: 9}, false},
		{"时间更早", Watermark{WriteDate: base.WriteDate.Add(-time.Second), ID: 999}, false},
	}
	for _, tc := range cases {
		if got := tc.w.After(base); got != tc.want {
			t.Errorf("%s: After 期望 %v，得到 %v", tc.name, tc.want, got)
		}
	}
}

// TestMemWatermarks_只进不退 验证水位不会被更小的值拉回去。
func TestMemWatermarks_只进不退(t *testing.T) {
	wm := NewMemWatermarks()
	ctx := context.Background()
	later := Watermark{WriteDate: testNow, ID: 10}
	earlier := Watermark{WriteDate: testNow.Add(-time.Hour), ID: 5}

	if _, ok, _ := wm.Get(ctx, "m"); ok {
		t.Fatal("初始不应有水位")
	}
	if err := wm.Advance(ctx, "m", later); err != nil {
		t.Fatalf("推进水位失败: %v", err)
	}
	if err := wm.Advance(ctx, "m", earlier); err != nil {
		t.Fatalf("推进水位失败: %v", err)
	}

	got, ok, err := wm.Get(ctx, "m")
	if err != nil || !ok {
		t.Fatalf("读水位失败: ok=%v err=%v", ok, err)
	}
	if got.ID != 10 {
		t.Fatalf("水位不应被更小的值覆盖，得到 %+v", got)
	}
}

// ---------------------------------------------------------------------------
// ① Outbox 非终态行
// ---------------------------------------------------------------------------

// TestReconciler_Outbox补投 验证遗漏的 Outbox 行会被补投，且沿用原 event_id。
func TestReconciler_Outbox补投(t *testing.T) {
	caller := newFakeCaller()
	caller.push("edge.outbox", []map[string]any{stuckOutboxRow("ev-stuck")})
	pub := new(fakePub)
	r := newTestReconciler(t, caller, pub, nil)

	res, err := r.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("对账失败: %v", err)
	}
	if res.OutboxStale != 1 || res.OutboxDelivered != 1 {
		t.Fatalf("统计不符: %+v", res)
	}

	calls := pub.published()
	if len(calls) != 1 {
		t.Fatalf("应补投 1 条，得到 %d", len(calls))
	}
	if calls[0].subject != "iot.odoo.mrp_workorder" {
		t.Fatalf("主题错误: %s", calls[0].subject)
	}

	var ev OdooEvent
	if err := json.Unmarshal(calls[0].data, &ev); err != nil {
		t.Fatalf("事件反序列化失败: %v", err)
	}
	// 沿用原 event_id 是补投的前提：原事件若其实到达过，下游靠它去重。
	if ev.EventID != "ev-stuck" {
		t.Fatalf("必须沿用原 event_id，得到 %s", ev.EventID)
	}
	if ev.AggregateID != 4242 || ev.Version != 3 || ev.CompanyID != 1 {
		t.Fatalf("字段解析错误: %+v", ev)
	}
	if string(ev.Payload) != `{"state":"done"}` {
		t.Fatalf("业务载荷必须原样透传（对账不该改写它），得到 %s", ev.Payload)
	}
	if r.metrics.ReconcileRepublished.Load() != 1 {
		t.Fatal("应记 1 次对账补投")
	}
}

// TestReconciler_退避中的行不当遗漏 验证 domain 带了宽限窗口，
// 否则正在退避重试的行会被误判成遗漏并立刻补投（绕过退避）。
func TestReconciler_退避中的行不当遗漏(t *testing.T) {
	caller := newFakeCaller()
	r := newTestReconciler(t, caller, new(fakePub), nil)

	if _, err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("对账失败: %v", err)
	}

	reqs := caller.callsFor("edge.outbox")
	if len(reqs) != 1 {
		t.Fatalf("应查询一次 edge.outbox，得到 %d", len(reqs))
	}
	params, ok := reqs[0].Params.(map[string]any)
	if !ok {
		t.Fatalf("Params 类型不符: %T", reqs[0].Params)
	}
	domain, _ := json.Marshal(params["domain"])

	wantCutoff := testNow.Add(-DefaultReconcileStaleAfter).Format(odooDatetimeLayout)
	if !strings.Contains(string(domain), wantCutoff) {
		t.Fatalf("domain 应带「%s 之前」的宽限窗口，得到 %s", wantCutoff, domain)
	}
	if !strings.Contains(string(domain), `"pending","dead"`) {
		t.Fatalf("domain 应限定非终态（pending/dead），得到 %s", domain)
	}
}

// TestReconciler_连续两轮遗漏才告警 是 §4.4「对连续两次遗漏告警」的直接断言。
func TestReconciler_连续两轮遗漏才告警(t *testing.T) {
	caller := newFakeCaller()
	caller.push("edge.outbox", []map[string]any{stuckOutboxRow("ev-1")})
	caller.push("edge.outbox", []map[string]any{stuckOutboxRow("ev-2")})
	pub := new(fakePub)
	r := newTestReconciler(t, caller, pub, nil)
	ctx := context.Background()

	res1, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("首轮失败: %v", err)
	}
	// 单轮遗漏很可能只是抖动（webhook 正在重投、cron 这一分钟没跑）。
	if res1.Alerts != 0 {
		t.Fatalf("首轮不应告警，得到 %d", res1.Alerts)
	}

	res2, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("次轮失败: %v", err)
	}
	if res2.Alerts != 1 {
		t.Fatalf("连续第二轮仍遗漏应告警，得到 %d", res2.Alerts)
	}
}

// TestReconciler_无遗漏清零连续计数 验证偶发抖动不会被累积成误报。
func TestReconciler_无遗漏清零连续计数(t *testing.T) {
	caller := newFakeCaller()
	caller.push("edge.outbox", []map[string]any{stuckOutboxRow("ev-1")}) // 第 1 轮：有遗漏
	caller.push("edge.outbox", []map[string]any{})                       // 第 2 轮：没有
	caller.push("edge.outbox", []map[string]any{stuckOutboxRow("ev-3")}) // 第 3 轮：又有
	r := newTestReconciler(t, caller, new(fakePub), nil)
	ctx := context.Background()

	if res, _ := r.ReconcileOnce(ctx); res.Alerts != 0 {
		t.Fatal("第 1 轮不该告警")
	}
	if res, _ := r.ReconcileOnce(ctx); res.Alerts != 0 {
		t.Fatal("第 2 轮没有遗漏")
	}
	res3, _ := r.ReconcileOnce(ctx)
	if res3.Alerts != 0 {
		t.Fatalf("中间清零后第 3 轮不该告警（连续计数应重新从 1 开始），得到 %d", res3.Alerts)
	}
}

// TestReconciler_死信行补投并告警 验证死信既被救急补投、又被标记为需人工排查。
func TestReconciler_死信行补投并告警(t *testing.T) {
	row := stuckOutboxRow("ev-dead")
	row["state"] = "dead"
	row["attempts"] = 10

	caller := newFakeCaller()
	caller.push("edge.outbox", []map[string]any{row})
	pub := new(fakePub)
	r := newTestReconciler(t, caller, pub, nil)

	res, err := r.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("对账失败: %v", err)
	}
	if res.OutboxDead != 1 || res.OutboxDelivered != 1 {
		t.Fatalf("死信应被补投并计数: %+v", res)
	}
	if len(pub.published()) != 1 {
		t.Fatal("死信也应补投（救急）")
	}
}

// ---------------------------------------------------------------------------
// ② C-2 水位差
// ---------------------------------------------------------------------------

// TestReconciler_水位基线不做全量补投 是避免「首次对账把历史全表打出去」的关键断言。
func TestReconciler_水位基线不做全量补投(t *testing.T) {
	caller := newFakeCaller()
	caller.push("maintenance.equipment", []map[string]any{
		{"id": 99, "write_date": "2026-10-01 06:00:00"},
	})
	pub := new(fakePub)
	wm := NewMemWatermarks()
	r := newTestReconciler(t, caller, pub, wm, "maintenance.equipment")

	res, err := r.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("对账失败: %v", err)
	}
	if res.CursorRecords != 0 || res.CursorModels != 0 {
		t.Fatalf("首轮应只建立基线，不补投: %+v", res)
	}
	if len(pub.published()) != 0 {
		t.Fatal("基线段不应发布任何事件")
	}

	got, ok, err := wm.Get(context.Background(), "maintenance.equipment")
	if err != nil || !ok {
		t.Fatalf("应写入基线水位: ok=%v err=%v", ok, err)
	}
	if got.ID != 99 {
		t.Fatalf("基线应是当前最新记录，得到 %+v", got)
	}

	// 基线查询必须是「倒序取 1 条」，否则就不是基线而是全量。
	params := caller.callsFor("maintenance.equipment")[0].Params.(map[string]any)
	if params["limit"] != 1 {
		t.Fatalf("基线查询 limit 应为 1，得到 %v", params["limit"])
	}
}

// TestReconciler_水位差补投并推进 验证 (write_date, id) 二元边界与水位推进。
func TestReconciler_水位差补投并推进(t *testing.T) {
	caller := newFakeCaller()
	caller.push("maintenance.equipment", []map[string]any{
		{"id": 10, "write_date": "2026-10-01 06:00:00"},
	})
	// 两条同秒记录：只靠 write_date 比较会漏掉第二条。
	caller.push("maintenance.equipment", []map[string]any{
		{"id": 11, "write_date": "2026-10-01 06:30:00"},
		{"id": 12, "write_date": "2026-10-01 06:30:00"},
	})

	pub := new(fakePub)
	wm := NewMemWatermarks()
	r := newTestReconciler(t, caller, pub, wm, "maintenance.equipment")
	ctx := context.Background()

	if _, err := r.ReconcileOnce(ctx); err != nil {
		t.Fatalf("基线轮失败: %v", err)
	}
	res, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("补投轮失败: %v", err)
	}
	if res.CursorModels != 1 || res.CursorRecords != 2 {
		t.Fatalf("应补投 2 条: %+v", res)
	}

	calls := pub.published()
	if len(calls) != 2 {
		t.Fatalf("应发布 2 条，得到 %d", len(calls))
	}
	if calls[0].subject != "iot.odoo.maintenance_equipment" {
		t.Fatalf("主题错误: %s", calls[0].subject)
	}

	var ev OdooEvent
	if err := json.Unmarshal(calls[1].data, &ev); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	// 与 webhook 路径同一口径，便于下游去重。
	if ev.EventID != "c2:maintenance.equipment:12:2026-10-01 06:30:00" {
		t.Fatalf("event_id 口径错误: %s", ev.EventID)
	}
	// 桩载荷必须显式标注，避免下游误当记录快照。
	if !strings.Contains(string(ev.Payload), `"reconciled":true`) {
		t.Fatalf("补投载荷应标注 reconciled: %s", ev.Payload)
	}

	got, _, err := wm.Get(ctx, "maintenance.equipment")
	if err != nil {
		t.Fatalf("读水位失败: %v", err)
	}
	if got.ID != 12 {
		t.Fatalf("水位应推进到最后一条（id=12），得到 %+v", got)
	}
}

// TestReconciler_补投失败不推进水位 验证失败的那条下轮还会被扫到 —— 这才是「不漏」。
func TestReconciler_补投失败不推进水位(t *testing.T) {
	caller := newFakeCaller()
	caller.push("maintenance.equipment", []map[string]any{
		{"id": 10, "write_date": "2026-10-01 06:00:00"},
	})
	caller.push("maintenance.equipment", []map[string]any{
		{"id": 11, "write_date": "2026-10-01 06:30:00"},
	})

	pub := &fakePub{errs: []error{fmt.Errorf("nats 不可达")}}
	wm := NewMemWatermarks()
	r := newTestReconciler(t, caller, pub, wm, "maintenance.equipment")
	ctx := context.Background()

	if _, err := r.ReconcileOnce(ctx); err != nil {
		t.Fatalf("基线轮失败: %v", err)
	}
	res, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("补投轮失败: %v", err)
	}
	if res.CursorRecords != 0 {
		t.Fatalf("补投失败不该计入成功: %+v", res)
	}

	got, _, err := wm.Get(ctx, "maintenance.equipment")
	if err != nil {
		t.Fatalf("读水位失败: %v", err)
	}
	if got.ID != 10 {
		t.Fatalf("补投失败时水位必须停在原地（否则那条就永久漏了），得到 %+v", got)
	}
	if r.metrics.PublishErrors.Load() != 1 {
		t.Fatal("应记 1 次发布失败")
	}
}

// TestReconciler_对账查询失败即中止 验证 Odoo 不可达时整轮失败、不误判为「没有遗漏」。
func TestReconciler_对账查询失败即中止(t *testing.T) {
	caller := newFakeCaller()
	caller.err = fmt.Errorf("odoo 不可达")
	r := newTestReconciler(t, caller, new(fakePub), nil)

	if _, err := r.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("查询失败应让整轮报错（否则会静默地认为「没有遗漏」）")
	}
}

// TestReconciler_追平时滞后归零 是 gauge 语义的关键断言。
//
// 「最新记录很旧」是**数据陈旧**，不是**我们落后**。实测踩过：基线落在 8 天前的
// 记录上，若按「now - 水位」上报，指标会直接报 693941 秒并一直挂着 ——
// 一个永远为真的告警等于没有告警。
func TestReconciler_追平时滞后归零(t *testing.T) {
	caller := newFakeCaller()
	caller.push("res.partner", []map[string]any{{"id": 3, "write_date": "2026-09-23 06:32:41"}})
	caller.push("res.partner", []map[string]any{}) // 第二轮：已追平

	wm := NewMemWatermarks()
	r := newTestReconciler(t, caller, new(fakePub), wm, "res.partner")
	ctx := context.Background()

	if _, err := r.ReconcileOnce(ctx); err != nil {
		t.Fatalf("基线轮失败: %v", err)
	}
	if got := r.metrics.CursorLagSeconds.Load(); got != 0 {
		t.Fatalf("基线轮不该产生滞后，得到 %d", got)
	}

	if _, err := r.ReconcileOnce(ctx); err != nil {
		t.Fatalf("追平轮失败: %v", err)
	}
	if got := r.metrics.CursorLagSeconds.Load(); got != 0 {
		t.Fatalf("追平时滞后必须归零（数据陈旧不等于我们落后），得到 %d", got)
	}
}

// TestReconciler_有水位差时记录滞后 验证真落后时指标会亮。
func TestReconciler_有水位差时记录滞后(t *testing.T) {
	caller := newFakeCaller()
	caller.push("res.partner", []map[string]any{{"id": 3, "write_date": "2026-09-23 06:32:41"}})
	caller.push("res.partner", []map[string]any{{"id": 4, "write_date": "2026-10-01 07:00:00"}})

	wm := NewMemWatermarks()
	r := newTestReconciler(t, caller, new(fakePub), wm, "res.partner")
	ctx := context.Background()

	if _, err := r.ReconcileOnce(ctx); err != nil {
		t.Fatalf("基线轮失败: %v", err)
	}
	if _, err := r.ReconcileOnce(ctx); err != nil {
		t.Fatalf("补投轮失败: %v", err)
	}

	// 滞后 = testNow - 水位（2026-09-23 06:32:41）≈ 8 天。
	wantMin := int64((8 * 24 * time.Hour).Seconds())
	if got := r.metrics.CursorLagSeconds.Load(); got < wantMin {
		t.Fatalf("有水位差时应记录滞后（≥ %d 秒），得到 %d", wantMin, got)
	}
}

// ---------------------------------------------------------------------------
// 接缝：对账必须走编排层
// ---------------------------------------------------------------------------

// TestReconciler_对账走编排层 证明对账的 Odoo 查询经过限流/熔断，
// 而不是绕开编排层裸打 Odoo —— 否则「一对账就把 Odoo 打满」会是最讽刺的故障。
func TestReconciler_对账走编排层(t *testing.T) {
	client := new(fakeClient)
	c, err := New(client, Options{
		Policy: noRetryPolicy(),
		Logger: testLogger(),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("构造连接器失败: %v", err)
	}

	r, err := NewReconciler(ReconcileOptions{
		Caller: c, Publisher: new(fakePub), Logger: testLogger(),
		Now: func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("构造对账器失败: %v", err)
	}

	if _, err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("对账失败: %v", err)
	}
	if c.Metrics().CallsTotal.Load() == 0 {
		t.Fatal("对账的 Odoo 查询应计入编排层的调用计数（说明它走了限流/熔断）")
	}
}

func TestNewReconciler_拒绝缺依赖(t *testing.T) {
	if _, err := NewReconciler(ReconcileOptions{Publisher: new(fakePub)}); err == nil {
		t.Fatal("缺 Caller 应报错")
	}
	if _, err := NewReconciler(ReconcileOptions{Caller: newFakeCaller()}); err == nil {
		t.Fatal("缺 Publisher 应报错")
	}
}
