package alarm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// harness 用注入时钟把「60s 观察期 / 10min 抑制期 / 5min 自动关闭」压成瞬时。
type harness struct {
	t     *testing.T
	eng   *Engine
	store *MemStore
	now   time.Time
}

func newHarness(t *testing.T, tweak func(*Options)) *harness {
	t.Helper()
	h := &harness{t: t, store: NewMemStore(), now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	opts := Options{
		Store:  h.store,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:    func() time.Time { return h.now },
	}
	if tweak != nil {
		tweak(&opts)
	}
	eng, err := New(opts)
	if err != nil {
		t.Fatalf("构造引擎: %v", err)
	}
	h.eng = eng
	return h
}

func (h *harness) advance(d time.Duration) { h.now = h.now.Add(d) }

// trigger 造一次默认触发（p1/d1，规则 ar_001，device_type 55）。
func (h *harness) trigger() Trigger {
	return Trigger{
		ProjectID:    "p1",
		DeviceID:     "d1",
		DeviceTypeID: 55,
		RuleID:       "ar_001",
		RuleName:     "温度过高",
		Level:        "critical",
		At:           h.now,
	}
}

func (h *harness) observe() Decision {
	h.t.Helper()
	d, err := h.eng.Observe(context.Background(), h.trigger())
	if err != nil {
		h.t.Fatalf("Observe: %v", err)
	}
	return d
}

func (h *harness) tick() []Decision {
	h.t.Helper()
	ds, err := h.eng.Tick(context.Background())
	if err != nil {
		h.t.Fatalf("Tick: %v", err)
	}
	return ds
}

func (h *harness) recover() Decision {
	h.t.Helper()
	d, err := h.eng.Recover(context.Background(), "p1", "d1", "ar_001", h.now)
	if err != nil {
		h.t.Fatalf("Recover: %v", err)
	}
	return d
}

func (h *harness) getKey(project, device, rule string) *Alarm {
	h.t.Helper()
	a, err := h.store.Get(context.Background(), DedupKey(project, device, rule))
	if err != nil {
		h.t.Fatalf("Get: %v", err)
	}
	return a
}

func (h *harness) get() *Alarm { return h.getKey("p1", "d1", "ar_001") }

// activate 把默认告警推到 active（走完观察期 + 一次扫描通知）。
func (h *harness) activate() *Alarm {
	h.t.Helper()
	h.observe()
	h.advance(DefaultDetectWindow + time.Second)
	h.observe()
	ds := h.tick()
	if len(ds) != 1 || ds[0].Action != ActionNotified {
		h.t.Fatalf("期望扫描后通知，得 %+v", ds)
	}
	return h.get()
}

func TestFSMForwardPath(t *testing.T) {
	h := newHarness(t, nil)

	d := h.observe()
	if d.Action != ActionCreated {
		t.Fatalf("首次触发应新建，得 %s", d.Action)
	}
	if d.Notify {
		t.Fatal("观察期内不应通知（04 §2.1：detected 是观察期）")
	}

	// 观察期内再触发：只更新 last_ts，状态不动。
	h.advance(30 * time.Second)
	if d := h.observe(); d.Action != ActionObserved {
		t.Fatalf("观察期内应只观察，得 %s", d.Action)
	}
	if a := h.get(); a.State != StateDetected {
		t.Fatalf("观察期内状态应仍为 detected，得 %s", a.State)
	}

	// 观察期满（累计 61s）：再触发即确认。
	h.advance(31 * time.Second)
	if d := h.observe(); d.Action != ActionConfirmed {
		t.Fatalf("观察期满应确认，得 %s", d.Action)
	}
	if a := h.get(); a.State != StateConfirmed || a.ConfirmedTS.IsZero() {
		t.Fatalf("应进入 confirmed 且记 confirmed_ts，得 %s/%v", a.State, a.ConfirmedTS)
	}

	// 扫描推进：confirmed → active，并发出通知。
	ds := h.tick()
	if len(ds) != 1 || ds[0].Action != ActionNotified || !ds[0].Notify {
		t.Fatalf("扫描应发通知，得 %+v", ds)
	}
	a := h.get()
	if a.State != StateActive {
		t.Fatalf("应进入 active，得 %s", a.State)
	}
	if a.NotifyCount != 1 {
		t.Fatalf("通知次数应为 1，得 %d", a.NotifyCount)
	}
	if a.NotifiedTS.IsZero() {
		t.Fatal("应记录 notified_ts（04 §2.2.1 的原始数据）")
	}
}

func TestActiveSuppressesRepeatNotify(t *testing.T) {
	h := newHarness(t, nil)
	h.activate()

	// 04 §2.2 抑制：active 状态下重复触发只更新 last_ts，不重复通知。
	for i := 0; i < 5; i++ {
		h.advance(time.Second)
		d := h.observe()
		if d.Notify {
			t.Fatalf("第 %d 次重复触发不该通知，得 %+v", i+1, d)
		}
		if d.Action != ActionObserved {
			t.Fatalf("应只观察，得 %s", d.Action)
		}
	}
	a := h.get()
	if a.NotifyCount != 1 {
		t.Fatalf("抑制期内通知次数应恒为 1，得 %d", a.NotifyCount)
	}
	if a.LastTS.Sub(a.FirstTS) != 66*time.Second {
		t.Fatalf("last_ts 应被持续更新，得 %v", a.LastTS.Sub(a.FirstTS))
	}
}

func TestRecoverSemantics(t *testing.T) {
	t.Run("detected 恢复直接回 idle（从未通知过）", func(t *testing.T) {
		h := newHarness(t, nil)
		h.observe()
		d := h.recover()
		if d.Action != ActionRecoveredQuiet {
			t.Fatalf("应静默恢复，得 %s", d.Action)
		}
		if a := h.get(); a != nil {
			t.Fatalf("应释放去重键，得 %+v", a)
		}
	})

	t.Run("confirmed 恢复直接回 idle", func(t *testing.T) {
		h := newHarness(t, nil)
		h.observe()
		h.advance(DefaultDetectWindow + time.Second)
		h.observe()
		if d := h.recover(); d.Action != ActionRecoveredQuiet {
			t.Fatalf("应静默恢复，得 %s", d.Action)
		}
		if a := h.get(); a != nil {
			t.Fatalf("应释放去重键，得 %+v", a)
		}
	})

	t.Run("active 恢复进 resolved 并等自动关闭", func(t *testing.T) {
		h := newHarness(t, nil)
		h.activate()
		d := h.recover()
		if d.Action != ActionResolved {
			t.Fatalf("应进入 resolved，得 %s", d.Action)
		}
		a := h.get()
		if a.State != StateResolved || a.ResolvedTS.IsZero() {
			t.Fatalf("应记 resolved_ts，得 %s/%v", a.State, a.ResolvedTS)
		}

		// 不到自动关闭延迟：扫描不应关闭。
		h.advance(4 * time.Minute)
		if ds := h.tick(); len(ds) != 0 {
			t.Fatalf("未到自动关闭延迟不该关闭，得 %+v", ds)
		}

		// 到延迟：关闭并释放去重键。
		h.advance(61 * time.Second)
		ds := h.tick()
		if len(ds) != 1 || ds[0].Action != ActionClosed {
			t.Fatalf("应自动关闭，得 %+v", ds)
		}
		if a := h.get(); a != nil {
			t.Fatalf("关闭后应释放去重键，得 %+v", a)
		}
	})

	t.Run("无告警时恢复是空操作", func(t *testing.T) {
		h := newHarness(t, nil)
		if d := h.recover(); d.Action != ActionObserved {
			t.Fatalf("应忽略，得 %s", d.Action)
		}
	})
}

func TestFlapDuringResolvedReopensObservation(t *testing.T) {
	h := newHarness(t, nil)
	h.activate()
	h.recover() // active → resolved

	h.advance(time.Second)
	d := h.observe()
	if d.Action != ActionReopened {
		t.Fatalf("恢复期内再触发应重新观察，得 %s", d.Action)
	}
	if d.Notify {
		t.Fatal("抖动不该立即再通知一次（否则抖动会变成通知风暴）")
	}
	a := h.get()
	if a.State != StateDetected {
		t.Fatalf("应回到 detected，得 %s", a.State)
	}
	if a.FlapCount != 1 {
		t.Fatalf("抖动次数应为 1，得 %d", a.FlapCount)
	}
	if !a.ResolvedTS.IsZero() {
		t.Fatal("重新观察后应清空 resolved_ts")
	}
	if a.NotifyCount != 1 {
		t.Fatalf("通知次数不该因抖动增加，得 %d", a.NotifyCount)
	}
}

func TestDedupAllowsOnlyOneActiveAlarm(t *testing.T) {
	h := newHarness(t, nil)
	// 04 §2.2：同一设备同一规则只允许一个活跃告警。
	for i := 0; i < 10; i++ {
		h.advance(time.Second)
		h.observe()
	}
	alarms, err := h.store.Active(context.Background())
	if err != nil {
		t.Fatalf("列举活跃告警: %v", err)
	}
	if len(alarms) != 1 {
		t.Fatalf("应只有一条活跃告警，得 %d 条", len(alarms))
	}
}

func TestDedupKeyHasNoConcatAmbiguity(t *testing.T) {
	// 裸拼接（文档字面写法）会让这两组算出同一个键 —— 两台不同设备的告警
	// 会互相压制。本实现用 0x00 分隔，故必须不同。
	if DedupKey("a", "bc", "d") == DedupKey("ab", "c", "d") {
		t.Fatal("拼接歧义未消除：不同设备可能共用去重键")
	}
}

// conflictStore 在前 N 次写入上注入乐观锁冲突（04 §2.5）。
type conflictStore struct {
	Store
	fails int
}

func (s *conflictStore) Update(ctx context.Context, a *Alarm, expected State) error {
	if s.fails > 0 {
		s.fails--
		return fmt.Errorf("%w: 测试注入", ErrStateConflict)
	}
	return s.Store.Update(ctx, a, expected)
}

func TestObserveRetriesOnCASConflict(t *testing.T) {
	h := newHarness(t, func(o *Options) {
		o.Store = &conflictStore{Store: NewMemStore(), fails: 2}
	})
	d, err := h.eng.Observe(context.Background(), h.trigger())
	if err != nil {
		t.Fatalf("应在重试后成功（04 §2.5 最多 3 次），得 %v", err)
	}
	if d.Action != ActionCreated {
		t.Fatalf("得 %s", d.Action)
	}
}

func TestObserveSurfacesErrorAfterRetriesExhausted(t *testing.T) {
	h := newHarness(t, func(o *Options) {
		o.Store = &conflictStore{Store: NewMemStore(), fails: 99}
		o.Config.MaxCASRetry = 2
	})
	_, err := h.eng.Observe(context.Background(), h.trigger())
	if !errors.Is(err, ErrStateConflict) {
		t.Fatalf("重试耗尽后应上抛冲突（不能静默吞掉），得 %v", err)
	}
}

// findDecision 按去重键取决策 —— 扫描返回的是**全体**活跃告警的决策，
// 且顺序不定（Store 用 map），断言必须按键定位而不是按下标。
func findDecision(t *testing.T, ds []Decision, key string) Decision {
	t.Helper()
	for _, d := range ds {
		if d.Alarm != nil && d.Alarm.DedupKey == key {
			return d
		}
	}
	t.Fatalf("扫描结果中没有 %s：%+v", key, ds)
	return Decision{}
}

// hasAction 判断某告警在（可能合并了多轮扫描的）决策集合里是否出现过指定动作。
func hasAction(ds []Decision, key string, act Action) bool {
	for _, d := range ds {
		if d.Alarm != nil && d.Alarm.DedupKey == key && d.Action == act {
			return true
		}
	}
	return false
}

func TestSilenceWindowSuppressesThenReleases(t *testing.T) {
	h := newHarness(t, func(o *Options) {
		o.Silences = NewSilences()
		o.Silences.Add(Silence{
			ID: "m1", DeviceID: "d1", Reason: "计划检修",
			Start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
		})
	})

	h.observe()
	h.advance(DefaultDetectWindow + time.Second) // 00:01:01，仍在窗口内
	h.observe()

	ds := h.tick()
	if len(ds) != 1 || ds[0].Action != ActionSuppressed || ds[0].Notify {
		t.Fatalf("维护窗口内应只记录不通知，得 %+v", ds)
	}
	a := h.get()
	if a.State != StateConfirmed || !a.Suppressed {
		t.Fatalf("应停在 confirmed 并标记抑制，得 %s/%v", a.State, a.Suppressed)
	}
	if !strings.Contains(a.SuppressReason, "计划检修") {
		t.Fatalf("抑制原因应可解释（排障要靠它），得 %q", a.SuppressReason)
	}

	h.advance(5 * time.Minute) // 00:06:01，窗口已过
	ds = h.tick()
	if len(ds) != 1 || ds[0].Action != ActionNotified || !ds[0].Notify {
		t.Fatalf("窗口过后应恢复通知，得 %+v", ds)
	}
	if a := h.get(); a.Suppressed {
		t.Fatal("放行后应清除抑制标记")
	}
}

func TestRootCauseSuppression(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	// 先造父告警（设备离线），拿到它的 ID。
	parent := h.trigger()
	parent.RuleID = "ar_offline"
	parent.RuleName = "设备离线"
	if _, err := h.eng.Observe(ctx, parent); err != nil {
		t.Fatalf("Observe 父告警: %v", err)
	}
	p := h.getKey("p1", "d1", "ar_offline")
	if p == nil || p.ID == "" {
		t.Fatal("父告警应已创建并分配 ID")
	}

	// 再造挂着 ParentID 的子告警，推到 confirmed。
	child := h.trigger()
	child.ParentID = p.ID
	if _, err := h.eng.Observe(ctx, child); err != nil {
		t.Fatalf("Observe 子告警: %v", err)
	}
	h.advance(DefaultDetectWindow + time.Second)
	child.At = h.now
	if _, err := h.eng.Observe(ctx, child); err != nil {
		t.Fatalf("Observe 子告警: %v", err)
	}

	childKey := DedupKey("p1", "d1", "ar_001")
	dec := findDecision(t, h.tick(), childKey)
	if dec.Action != ActionSuppressed {
		t.Fatalf("父告警活跃时子告警应被抑制，得 %s", dec.Action)
	}
	if !strings.Contains(dec.Reason, "根因抑制") {
		t.Fatalf("原因应说明根因抑制，得 %q", dec.Reason)
	}

	// 父告警恢复 + 自动关闭后，子告警应能放行。
	if _, err := h.eng.Recover(ctx, "p1", "d1", "ar_offline", h.now); err != nil {
		t.Fatalf("Recover 父告警: %v", err)
	}
	h.advance(DefaultAutoClose + time.Second)
	// Store 用 map 遍历，父告警与子告警的求值顺序不定：父告警可能要到本轮才被
	// 关闭，此时子告警仍应被判为「被抑制」。故扫两轮，再从合并结果里找放行那条。
	ds := append(h.tick(), h.tick()...)
	if p := h.getKey("p1", "d1", "ar_offline"); p != nil {
		t.Fatalf("父告警应已自动关闭，得 %s", p.State)
	}
	if !hasAction(ds, childKey, ActionNotified) {
		t.Fatalf("根因解除后应放行通知，得 %+v", ds)
	}
	if a := h.getKey("p1", "d1", "ar_001"); a == nil || a.State != StateActive {
		t.Fatalf("子告警应进入 active，得 %+v", a)
	}
}

func TestAggregationEndToEnd(t *testing.T) {
	h := newHarness(t, nil) // 默认阈值：同 device_type 下 5 台设备 / 60s
	ctx := context.Background()
	const n = 5

	for i := 0; i < n; i++ {
		tr := h.trigger()
		tr.DeviceID = fmt.Sprintf("d%d", i)
		if _, err := h.eng.Observe(ctx, tr); err != nil {
			t.Fatalf("Observe: %v", err)
		}
	}
	h.advance(DefaultDetectWindow + time.Second)
	for i := 0; i < n; i++ {
		tr := h.trigger()
		tr.DeviceID = fmt.Sprintf("d%d", i)
		if _, err := h.eng.Observe(ctx, tr); err != nil {
			t.Fatalf("Observe: %v", err)
		}
	}

	ds := h.tick() // confirmed → active，同时做聚合判定
	if len(ds) != n {
		t.Fatalf("应有 %d 条决策，得 %d", n, len(ds))
	}
	batched := 0
	for _, d := range ds {
		if d.Action == ActionBatched {
			batched++
			if d.Alarm.BatchID == "" {
				t.Fatal("批量告警应带 BatchID")
			}
		}
		if !d.Notify {
			t.Fatalf("每条都该通知（合并后由其中一条承载），得 %+v", d)
		}
	}
	if batched != 1 {
		t.Fatalf("恰有一条应被合并为批量告警，得 %d", batched)
	}
}
