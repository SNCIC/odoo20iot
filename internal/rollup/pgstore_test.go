package rollup_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/pg/pgtest"
	"github.com/SNCIC/odoo20iot/internal/rollup"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// TestPGStoreWatermark 校验水位账本的核心语义：
// 首次无记录、只进不退、失败留痕不推进、成功清空错误。
func TestPGStoreWatermark(t *testing.T) {
	pool := pgtest.DB(t)
	store, err := rollup.NewPGStore(pool)
	if err != nil {
		t.Fatalf("构造 PGStore 失败: %v", err)
	}
	ctx := context.Background()

	if _, found, err := store.Get(ctx, "1m"); err != nil || found {
		t.Fatalf("首次 Get 应为 found=false，得到 found=%v err=%v", found, err)
	}

	if err := store.Advance(ctx, "1m", base); err != nil {
		t.Fatalf("推进水位失败: %v", err)
	}
	wm, found, err := store.Get(ctx, "1m")
	if err != nil || !found {
		t.Fatalf("推进后应读到记录，得到 found=%v err=%v", found, err)
	}
	if !wm.WindowStart.Equal(base) {
		t.Fatalf("水位期望 %s，得到 %s", base, wm.WindowStart)
	}

	// 回退不改：只进不退，否则会重复物化已完成的窗口。
	if err := store.Advance(ctx, "1m", base.Add(-time.Hour)); err != nil {
		t.Fatalf("回退推进不应报错: %v", err)
	}
	if wm, _, _ = store.Get(ctx, "1m"); !wm.WindowStart.Equal(base) {
		t.Fatalf("回退不应改变水位，得到 %s", wm.WindowStart)
	}

	// RecordError 记录原因、不动水位。
	if err := store.RecordError(ctx, "1m", "boom"); err != nil {
		t.Fatalf("记录错误失败: %v", err)
	}
	wm, _, _ = store.Get(ctx, "1m")
	if !wm.WindowStart.Equal(base) {
		t.Fatalf("RecordError 不应推进水位，得到 %s", wm.WindowStart)
	}
	if wm.LastError != "boom" {
		t.Fatalf("last_error 期望 boom，得到 %q", wm.LastError)
	}

	// 成功的 Advance 清空 last_error。
	if err := store.Advance(ctx, "1m", base.Add(time.Minute)); err != nil {
		t.Fatalf("推进失败: %v", err)
	}
	wm, _, _ = store.Get(ctx, "1m")
	if !wm.WindowStart.Equal(base.Add(time.Minute)) || wm.LastError != "" {
		t.Fatalf("成功推进后期望水位 %s 且 last_error 为空，得到 %s / %q",
			base.Add(time.Minute), wm.WindowStart, wm.LastError)
	}
}

// TestPGStoreRecordErrorWithoutRow 校验无记录时 RecordError **不创建**水位行。
//
// 若它建了一行（哪怕 window_start 是 epoch），下一轮就会读到「已物化到 epoch」
// 从而从 1970 年重扫全量 —— 这是账本被假水位污染的经典方式。
func TestPGStoreRecordErrorWithoutRow(t *testing.T) {
	pool := pgtest.DB(t)
	store, err := rollup.NewPGStore(pool)
	if err != nil {
		t.Fatalf("构造 PGStore 失败: %v", err)
	}
	ctx := context.Background()

	if err := store.RecordError(ctx, "1h", "before-any-advance"); err != nil {
		t.Fatalf("记录错误失败: %v", err)
	}
	if _, found, err := store.Get(ctx, "1h"); err != nil || found {
		t.Fatalf("RecordError 不得创建水位行，得到 found=%v err=%v", found, err)
	}
}

// TestPGStoreConcurrentAdvance 校验并发推进收敛到最大值（GREATEST）。
func TestPGStoreConcurrentAdvance(t *testing.T) {
	pool := pgtest.DB(t)
	store, err := rollup.NewPGStore(pool)
	if err != nil {
		t.Fatalf("构造 PGStore 失败: %v", err)
	}
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = store.Advance(ctx, "1m", base.Add(time.Duration(i)*time.Minute))
		}(i)
	}
	wg.Wait()

	wm, found, err := store.Get(ctx, "1m")
	if err != nil || !found {
		t.Fatalf("应读到记录，得到 found=%v err=%v", found, err)
	}
	if want := base.Add(7 * time.Minute); !wm.WindowStart.Equal(want) {
		t.Fatalf("并发推进应收敛到最大值 %s，得到 %s", want, wm.WindowStart)
	}
}
