package pipeline

import (
	"context"
	"testing"
	"time"
)

// TestMemIdempotency_Lifecycle 覆盖两阶段标记的完整生命周期。
func TestMemIdempotency_Lifecycle(t *testing.T) {
	m := NewMemIdempotency()
	ctx := context.Background()
	const key = "idemp:1:telemetry:99-1-1"

	// 初始：未完成，可抢占。
	if done, err := m.IsDone(ctx, key); err != nil || done {
		t.Fatalf("初始应为未完成: done=%v err=%v", done, err)
	}
	if ok, err := m.AcquireProcessing(ctx, key, time.Minute); err != nil || !ok {
		t.Fatalf("首次抢占应成功: ok=%v err=%v", ok, err)
	}

	// 二次抢占必须失败（并发互斥）。
	if ok, err := m.AcquireProcessing(ctx, key, time.Minute); err != nil || ok {
		t.Fatalf("二次抢占应失败: ok=%v err=%v", ok, err)
	}

	// 写 done 后应命中。
	if err := m.MarkDone(ctx, key, time.Minute); err != nil {
		t.Fatalf("MarkDone 失败: %v", err)
	}
	if done, err := m.IsDone(ctx, key); err != nil || !done {
		t.Fatalf("MarkDone 后应命中: done=%v err=%v", done, err)
	}

	// ReleaseProcessing **不得**删掉 done —— 否则重投会重复入库。
	if err := m.ReleaseProcessing(ctx, key); err != nil {
		t.Fatalf("ReleaseProcessing 失败: %v", err)
	}
	if done, _ := m.IsDone(ctx, key); !done {
		t.Fatal("ReleaseProcessing 不应删除 done 标记")
	}
}

// TestMemIdempotency_ProcessingExpires 验证 processing 的 TTL 到期后自动释放：
// 这是「崩溃后的重投可重新进入处理」的依据（03 §4.3 规则 2）。
func TestMemIdempotency_ProcessingExpires(t *testing.T) {
	m := NewMemIdempotency()
	ctx := context.Background()
	const key = "k"

	if ok, _ := m.AcquireProcessing(ctx, key, 10*time.Millisecond); !ok {
		t.Fatal("首次抢占应成功")
	}
	time.Sleep(25 * time.Millisecond)

	if ok, err := m.AcquireProcessing(ctx, key, time.Minute); err != nil || !ok {
		t.Fatalf("TTL 到期后应可重新抢占: ok=%v err=%v", ok, err)
	}
}
