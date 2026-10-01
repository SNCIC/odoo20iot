package alarm_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/pg/pgtest"
)

// 这些用例打的是**真 PostgreSQL**（表来自 internal/pg/migrations）：
// CAS、唯一约束、JSONB 往返这些恰恰是内存替身测不出来的部分。

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type pgHarness struct {
	t     *testing.T
	eng   *alarm.Engine
	store *alarm.PGStore
	pool  *pgxpool.Pool
	now   time.Time
}

func newPGHarness(t *testing.T) *pgHarness {
	t.Helper()
	pool := pgtest.DB(t)
	store, err := alarm.NewPGStore(pool)
	if err != nil {
		t.Fatalf("NewPGStore: %v", err)
	}
	h := &pgHarness{t: t, store: store, pool: pool, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	eng, err := alarm.New(alarm.Options{
		Store:  store,
		Logger: quietLogger(),
		Now:    func() time.Time { return h.now },
	})
	if err != nil {
		t.Fatalf("alarm.New: %v", err)
	}
	h.eng = eng
	return h
}

func (h *pgHarness) advance(d time.Duration) { h.now = h.now.Add(d) }

func (h *pgHarness) trigger(device string) alarm.Trigger {
	return alarm.Trigger{
		ProjectID:    "p1",
		DeviceID:     device,
		DeviceTypeID: 55,
		RuleID:       "ar_001",
		RuleName:     "温度过高",
		Level:        "critical",
		Timing: alarm.Timing{
			DetectWindow: 60 * time.Second,
			Suppress:     10 * time.Minute,
			AutoClose:    5 * time.Minute,
		},
		At: h.now,
	}
}

func (h *pgHarness) observe(device string) alarm.Decision {
	h.t.Helper()
	d, err := h.eng.Observe(context.Background(), h.trigger(device))
	if err != nil {
		h.t.Fatalf("Observe(%s): %v", device, err)
	}
	return d
}

func (h *pgHarness) tick() []alarm.Decision {
	h.t.Helper()
	ds, err := h.eng.Tick(context.Background())
	if err != nil {
		h.t.Fatalf("Tick: %v", err)
	}
	return ds
}

func (h *pgHarness) key(device string) string {
	return alarm.DedupKey("p1", device, "ar_001")
}

func (h *pgHarness) get(device string) *alarm.Alarm {
	h.t.Helper()
	a, err := h.store.Get(context.Background(), h.key(device))
	if err != nil {
		h.t.Fatalf("Get(%s): %v", device, err)
	}
	return a
}

func TestPGStoreEndToEndFSM(t *testing.T) {
	h := newPGHarness(t)
	ctx := context.Background()

	if d := h.observe("d1"); d.Action != alarm.ActionCreated || d.Notify {
		t.Fatalf("首次触发应新建且不通知，得 %+v", d)
	}
	// 落库了：直接读回来（证明状态在 PG，不是内存）。
	a := h.get("d1")
	if a == nil || a.State != alarm.StateDetected {
		t.Fatalf("得 %+v", a)
	}
	if !strings.HasPrefix(a.ID, "alarm-") {
		t.Fatalf("id 应由数据库生成并回填，得 %q", a.ID)
	}

	h.advance(61 * time.Second)
	if d := h.observe("d1"); d.Action != alarm.ActionConfirmed {
		t.Fatalf("观察期满应确认，得 %s", d.Action)
	}

	ds := h.tick()
	if len(ds) != 1 || ds[0].Action != alarm.ActionNotified || !ds[0].Notify {
		t.Fatalf("扫描应发通知，得 %+v", ds)
	}
	a = h.get("d1")
	if a.State != alarm.StateActive || a.NotifyCount != 1 || a.NotifiedTS.IsZero() {
		t.Fatalf("得 %+v", a)
	}

	// 抑制期内重复触发只更新 last_ts（04 §2.2）。
	h.advance(time.Second)
	h.observe("d1")
	if a = h.get("d1"); a.NotifyCount != 1 {
		t.Fatalf("抑制期内通知次数应恒为 1，得 %d", a.NotifyCount)
	}

	// 恢复 → resolved → 自动关闭（行被删除，去重键释放）。
	if d, err := h.eng.Recover(ctx, "p1", "d1", "ar_001", h.now); err != nil || d.Action != alarm.ActionResolved {
		t.Fatalf("恢复应进 resolved，得 %+v / %v", d, err)
	}
	if a = h.get("d1"); a.State != alarm.StateResolved || a.ResolvedTS.IsZero() {
		t.Fatalf("得 %+v", a)
	}
	h.advance(5*time.Minute + time.Second)
	ds = h.tick()
	if len(ds) != 1 || ds[0].Action != alarm.ActionClosed {
		t.Fatalf("应自动关闭，得 %+v", ds)
	}
	if a = h.get("d1"); a != nil {
		t.Fatalf("关闭后应释放去重键，得 %+v", a)
	}
}

func TestPGStoreSurvivesEngineRestart(t *testing.T) {
	pool := pgtest.DB(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	store, err := alarm.NewPGStore(pool)
	if err != nil {
		t.Fatalf("NewPGStore: %v", err)
	}
	eng1, err := alarm.New(alarm.Options{
		Store: store, Logger: quietLogger(),
		Now: func() time.Time { return base },
	})
	if err != nil {
		t.Fatalf("alarm.New: %v", err)
	}
	if _, err := eng1.Observe(ctx, alarm.Trigger{
		ProjectID: "p1", DeviceID: "d1", DeviceTypeID: 55,
		RuleID: "ar_001", Level: "warn", At: base,
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	// 引擎 2 的内存是空的、时钟推过了观察期。它必须能接着推进 ——
	// 这正是 04 §2.5 说的「启动时从 PG 重建活跃索引」。
	later := base.Add(2 * time.Minute)
	eng2, err := alarm.New(alarm.Options{
		Store: store, Logger: quietLogger(),
		Now: func() time.Time { return later },
	})
	if err != nil {
		t.Fatalf("alarm.New: %v", err)
	}
	ds, err := eng2.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(ds) != 1 || ds[0].Action != alarm.ActionConfirmed {
		t.Fatalf("新引擎应能从 PG 接着推进，得 %+v", ds)
	}
}

func TestPGStoreCASRejectsStaleWrite(t *testing.T) {
	h := newPGHarness(t)
	ctx := context.Background()
	h.observe("d1")

	// 同一行的两份副本，各自推进 —— 第二次的 expected 已经过期。
	first := h.get("d1")
	stale := first.Clone()

	na := first.Clone()
	na.State = alarm.StateConfirmed
	na.StateTS = h.now
	na.ConfirmedTS = h.now
	if err := h.store.Update(ctx, na, first.State); err != nil {
		t.Fatalf("首次更新应成功: %v", err)
	}

	nb := stale.Clone()
	nb.State = alarm.StateConfirmed
	nb.ConfirmedTS = h.now
	err := h.store.Update(ctx, nb, stale.State)
	if !errors.Is(err, alarm.ErrStateConflict) {
		t.Fatalf("陈旧的写入应被乐观锁拒掉（04 §2.5），得 %v", err)
	}
}

func TestPGStoreConcurrentObserveKeepsOneRow(t *testing.T) {
	h := newPGHarness(t)
	ctx := context.Background()

	// 模拟多副本同时收到同一个触发：都应成功（靠 CAS 重试收敛），
	// 且库里恰好一行 —— 04 §2.2「同一设备同一规则只允许一个活跃告警」。
	const n = 8
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.eng.Observe(ctx, h.trigger("d1"))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发 Observe 不该失败（应经 CAS 重试收敛）: %v", err)
		}
	}

	rows, err := h.store.Active(ctx)
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("应恰好一行，得 %d", len(rows))
	}
}

func TestPGStoreTimingRoundTrips(t *testing.T) {
	h := newPGHarness(t)
	want := alarm.Timing{DetectWindow: 45 * time.Second, Suppress: 7 * time.Minute, AutoClose: 90 * time.Second}
	tr := h.trigger("d1")
	tr.Timing = want
	if _, err := h.eng.Observe(context.Background(), tr); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	// timing 存在 JSONB 里，必须往返一致 —— 不一致会让重启后的窗口悄悄变默认值，
	// 表现为「扫描要么提前通知、要么永远不通知」。
	got := h.get("d1")
	if got.Timing != want {
		t.Fatalf("timing 往返不一致：得 %+v，期望 %+v", got.Timing, want)
	}
	// 确认真的是按 04 §2.4 的可读格式存的 —— 运维会直接 psql 看这张表，
	// 存成 base64/二进制对排障毫无帮助。
	ctx := context.Background()
	var raw string
	if err := h.pool.QueryRow(ctx,
		`SELECT timing::text FROM t_alarm_active WHERE dedup_key = $1`,
		h.key("d1")).Scan(&raw); err != nil {
		t.Fatalf("读 timing 原文: %v", err)
	}
	for _, k := range []string{"detect_window_s", "suppress_s", "auto_close_s"} {
		if !strings.Contains(raw, k) {
			t.Fatalf("timing 应存成含 %s 的可读 JSON，得 %s", k, raw)
		}
	}
}

func TestPGStoreActiveOrderIsStable(t *testing.T) {
	h := newPGHarness(t)
	ctx := context.Background()

	// 三条不同设备，state_ts 依次递增。
	for _, d := range []string{"d1", "d2", "d3"} {
		h.advance(time.Second)
		h.observe(d)
	}

	rows, err := h.store.Active(ctx)
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("应三条，得 %d", len(rows))
	}
	// 顺序必须稳定（按 state_ts）：Store 无序时，同一轮里父告警与子告警的
	// 求值顺序会变得不确定，抑制判定的结果也就不可复现。
	for i := 1; i < len(rows); i++ {
		if rows[i].StateTS.Before(rows[i-1].StateTS) {
			t.Fatalf("应按 state_ts 升序，得 %v", rows)
		}
	}
	if rows[0].DeviceID != "d1" || rows[2].DeviceID != "d3" {
		t.Fatalf("最先触发的应排最前，得 %s…%s", rows[0].DeviceID, rows[2].DeviceID)
	}
}

func TestPGStoreRootCauseSuppressionAcrossRestart(t *testing.T) {
	h := newPGHarness(t)
	ctx := context.Background()

	// 父告警（设备离线）落库，取它的 id。
	parent := h.trigger("d1")
	parent.RuleID = "ar_offline"
	parent.RuleName = "设备离线"
	if _, err := h.eng.Observe(ctx, parent); err != nil {
		t.Fatalf("Observe 父告警: %v", err)
	}
	p, err := h.store.Get(ctx, alarm.DedupKey("p1", "d1", "ar_offline"))
	if err != nil || p == nil {
		t.Fatalf("父告警应已落库，得 %+v / %v", p, err)
	}
	// GetByID 走的是 id 列索引 —— 根因抑制每次判定都要走这条路径。
	if byID, err := h.store.GetByID(ctx, p.ID); err != nil || byID == nil || byID.DedupKey != p.DedupKey {
		t.Fatalf("GetByID 应能取回父告警，得 %+v / %v", byID, err)
	}

	// 子告警挂到父告警上，推到 confirmed。
	child := h.trigger("d1")
	child.ParentID = p.ID
	if _, err := h.eng.Observe(ctx, child); err != nil {
		t.Fatalf("Observe 子告警: %v", err)
	}
	h.advance(61 * time.Second)
	child.At = h.now
	if _, err := h.eng.Observe(ctx, child); err != nil {
		t.Fatalf("Observe 子告警: %v", err)
	}

	childKey := h.key("d1")
	found := false
	for _, d := range h.tick() {
		if d.Alarm.DedupKey == childKey {
			found = true
			if d.Action != alarm.ActionSuppressed {
				t.Fatalf("父告警活跃时子告警应被抑制，得 %s", d.Action)
			}
			if !strings.Contains(d.Reason, "根因抑制") {
				t.Fatalf("原因应说明根因抑制，得 %q", d.Reason)
			}
		}
	}
	if !found {
		t.Fatal("扫描结果里没有子告警")
	}
	if a := h.get("d1"); a.Suppressed && a.State != alarm.StateConfirmed {
		t.Fatalf("被抑制的告警应停在 confirmed，得 %s", a.State)
	}
}
