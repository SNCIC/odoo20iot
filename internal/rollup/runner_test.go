package rollup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// memStore 是水位账本的内存实现，可注入 Advance 失败次数。
type memStore struct {
	found         bool
	start         time.Time
	lastErr       string
	advanceFailOn int // 第 N 次 Advance 失败（1-based，0=从不）
	advanceCalls  int
}

func (m *memStore) Get(_ context.Context, g string) (Watermark, bool, error) {
	if !m.found {
		return Watermark{Granularity: g}, false, nil
	}
	return Watermark{Granularity: g, WindowStart: m.start}, true, nil
}

func (m *memStore) Advance(_ context.Context, _ string, ws time.Time) error {
	m.advanceCalls++
	if m.advanceFailOn > 0 && m.advanceCalls == m.advanceFailOn {
		return errors.New("advance boom")
	}
	m.start, m.found = ws, true
	return nil
}

func (m *memStore) RecordError(_ context.Context, _, errText string) error {
	m.lastErr = errText
	return nil
}

// memMat 是物化执行体的内存实现，可注入第 N 个窗口失败。
type memMat struct {
	windows   []Window
	failOn    int // 第 N 次 RollupWindow 失败（1-based，0=从不）
	calls     int
	ensureErr error
}

func (m *memMat) EnsureRollupTable(_ context.Context, _ tsdb.Rollup) error { return m.ensureErr }

func (m *memMat) RollupWindow(_ context.Context, _ tsdb.Rollup, start, end time.Time) (int, error) {
	m.calls++
	if m.failOn > 0 && m.calls == m.failOn {
		return 0, errors.New("rollup boom")
	}
	m.windows = append(m.windows, Window{Start: start, End: end})
	return 5, nil
}

func newTestRunner(t *testing.T, store *memStore, mat *memMat) *Runner {
	t.Helper()
	r, err := NewRunner(store, mat, Config{
		Lag:             2 * time.Minute,
		InitialLookback: 5 * time.Minute,
		MaxWindows:      100,
		Now:             func() time.Time { return at(12, 0) },
	}, nil)
	if err != nil {
		t.Fatalf("构造 Runner 失败: %v", err)
	}
	return r
}

// TestRunner_AdvancesOnSuccess 校验首轮从有界水位起、逐窗口物化并推进到 closedEnd。
func TestRunner_AdvancesOnSuccess(t *testing.T) {
	store := &memStore{}
	mat := &memMat{}
	r := newTestRunner(t, store, mat)

	res, err := r.RunOnce(context.Background(), tsdb.Rollup1m)
	if err != nil {
		t.Fatalf("RunOnce 失败: %v", err)
	}
	// start = 12:00 - 2m - 5m = 11:53；closedEnd = 12:00 - 2m = 11:58 → 5 个窗口。
	if res.Windows != 5 {
		t.Fatalf("期望 5 个窗口，得到 %d", res.Windows)
	}
	if !res.Watermark.Equal(at(11, 58)) {
		t.Errorf("水位期望 11:58，得到 %s", res.Watermark)
	}
	if !store.found || !store.start.Equal(at(11, 58)) {
		t.Errorf("账本水位应为 11:58，得到 found=%v start=%s", store.found, store.start)
	}
	if len(mat.windows) != 5 || !mat.windows[0].Start.Equal(at(11, 53)) {
		t.Errorf("物化窗口不符: %+v", mat.windows)
	}
	if store.lastErr != "" {
		t.Errorf("成功时不应留下 last_error，得到 %q", store.lastErr)
	}
}

// TestRunner_DoesNotAdvanceOnWriteFailure 校验单窗口写失败时**不半推进**：
// 水位停在失败窗口之前，下一轮从同一位置重试。
func TestRunner_DoesNotAdvanceOnWriteFailure(t *testing.T) {
	store := &memStore{}
	mat := &memMat{failOn: 3}
	r := newTestRunner(t, store, mat)

	res, err := r.RunOnce(context.Background(), tsdb.Rollup1m)
	if err == nil {
		t.Fatal("第 3 个窗口失败时 RunOnce 必须返回错误")
	}
	if res.Windows != 2 {
		t.Fatalf("失败前应完成 2 个窗口，得到 %d", res.Windows)
	}
	if !store.start.Equal(at(11, 55)) {
		t.Errorf("水位应停在 11:55（第 3 个窗口之前），得到 %s", store.start)
	}
	if store.lastErr == "" {
		t.Error("失败原因应写入账本")
	}
}

// TestRunner_ReplaysWindowWhenAdvanceFails 校验「写成功但推进失败」的下轮重放。
//
// 这正是 rollup 幂等性的用武之地：预聚合表是覆盖写，重放同一窗口结果不变。
func TestRunner_ReplaysWindowWhenAdvanceFails(t *testing.T) {
	store := &memStore{advanceFailOn: 1}
	mat := &memMat{}
	r := newTestRunner(t, store, mat)

	if _, err := r.RunOnce(context.Background(), tsdb.Rollup1m); err == nil {
		t.Fatal("Advance 失败时 RunOnce 必须返回错误")
	}
	if store.found {
		t.Fatal("Advance 失败后水位不应被认为已推进")
	}
	firstWindow := mat.windows[0]

	// 第二轮：Advance 恢复正常，应重放同一批窗口。
	store.advanceFailOn = 0
	if _, err := r.RunOnce(context.Background(), tsdb.Rollup1m); err != nil {
		t.Fatalf("第二轮失败: %v", err)
	}
	if len(mat.windows) < 2 {
		t.Fatalf("第二轮应重放窗口，物化次数 %d 不足", len(mat.windows))
	}
	if !mat.windows[1].Start.Equal(firstWindow.Start) || !mat.windows[1].End.Equal(firstWindow.End) {
		t.Fatalf("第二轮首个窗口应为 %+v，得到 %+v", firstWindow, mat.windows[1])
	}
}

// TestRunner_AlreadyCaughtUp 校验追平后不再物化（空转不报错）。
func TestRunner_AlreadyCaughtUp(t *testing.T) {
	store := &memStore{found: true, start: at(11, 58)} // == closedEnd
	mat := &memMat{}
	r := newTestRunner(t, store, mat)

	res, err := r.RunOnce(context.Background(), tsdb.Rollup1m)
	if err != nil {
		t.Fatalf("RunOnce 失败: %v", err)
	}
	if res.Windows != 0 || mat.calls != 0 {
		t.Fatalf("已追平不应物化，得到 windows=%d calls=%d", res.Windows, mat.calls)
	}
}

// TestRunner_EnsureFailureIsRecorded 校验建表失败也会留痕且不推进。
func TestRunner_EnsureFailureIsRecorded(t *testing.T) {
	store := &memStore{}
	mat := &memMat{ensureErr: errors.New("ddl boom")}
	r := newTestRunner(t, store, mat)

	if _, err := r.RunOnce(context.Background(), tsdb.Rollup1m); err == nil {
		t.Fatal("建表失败必须返回错误")
	}
	if store.lastErr == "" {
		t.Error("建表失败应写入 last_error")
	}
	if store.found {
		t.Error("建表失败不应推进水位")
	}
}
