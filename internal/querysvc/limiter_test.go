package querysvc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimiter_ConcurrencyCap(t *testing.T) {
	l := NewLimiter(LimiterConfig{MaxConcurrency: 3, MaxQueue: 100, QueueTimeout: time.Second}, nil, time.Now)
	ctx := context.Background()

	var inFlight, maxInFlight atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := l.Acquire(ctx, 1)
			if err != nil {
				return
			}
			cur := inFlight.Add(1)
			for {
				m := maxInFlight.Load()
				if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(3 * time.Millisecond)
			inFlight.Add(-1)
			release()
		}()
	}
	wg.Wait()

	if got := maxInFlight.Load(); got > 3 {
		t.Fatalf("在途数不得超过 3，实测峰值 %d", got)
	}
	if got := maxInFlight.Load(); got < 2 {
		t.Fatalf("并发度未被用满（峰值 %d），用例可能没真正并发", got)
	}
}

func TestLimiter_QueueBoundRejects(t *testing.T) {
	// 并发 1 + 队列 2：同时来 10 个，必然有 503。
	l := NewLimiter(LimiterConfig{MaxConcurrency: 1, MaxQueue: 2, QueueTimeout: 5 * time.Second}, nil, time.Now)
	ctx := context.Background()

	release, err := l.Acquire(ctx, 1)
	if err != nil {
		t.Fatalf("第一个应拿到名额: %v", err)
	}
	defer release()

	var full, queued atomic.Int64
	var wg sync.WaitGroup
	block := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := l.Acquire(ctx, 1)
			switch {
			case errors.Is(err, ErrQueueFull):
				full.Add(1)
			case err == nil:
				queued.Add(1)
				rel()
			}
		}()
	}
	// 给排队的 goroutine 一点时间进入等待。
	time.Sleep(50 * time.Millisecond)
	close(block)
	wg.Wait()

	if full.Load() == 0 {
		t.Fatal("队列有界时必然有请求被拒（503）")
	}
}

func TestLimiter_QueueTimeout(t *testing.T) {
	l := NewLimiter(LimiterConfig{MaxConcurrency: 1, MaxQueue: 10, QueueTimeout: 30 * time.Millisecond}, nil, time.Now)
	ctx := context.Background()

	release, err := l.Acquire(ctx, 1)
	if err != nil {
		t.Fatalf("第一个应拿到名额: %v", err)
	}
	defer release()

	start := time.Now()
	if _, err := l.Acquire(ctx, 1); !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("期望 ErrQueueTimeout，得到 %v", err)
	}
	if d := time.Since(start); d < 20*time.Millisecond {
		t.Fatalf("应真的等过 QueueTimeout，实际只等了 %s", d)
	}
}

func TestLimiter_ContextCancelNotTimeout(t *testing.T) {
	l := NewLimiter(LimiterConfig{MaxConcurrency: 1, MaxQueue: 10, QueueTimeout: time.Minute}, nil, time.Now)
	release, err := l.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("第一个应拿到名额: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	// 取消应与「排队超时」区分开（映射到不同的错误）。
	if _, err := l.Acquire(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("期望 context.Canceled，得到 %v", err)
	}
}

func TestLimiter_IdleEviction(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	l := NewLimiter(LimiterConfig{MaxConcurrency: 1, MaxQueue: 0, MaxTenants: 2, IdleTTL: time.Minute}, nil, clock)
	ctx := context.Background()

	for id := int64(1); id <= 2; id++ {
		rel, err := l.Acquire(ctx, id)
		if err != nil {
			t.Fatalf("租户 %d 应能拿到名额: %v", id, err)
		}
		rel()
	}
	if got := l.Tenants(); got != 2 {
		t.Fatalf("应有 2 个在册租户，得到 %d", got)
	}

	// 时间推进到超过 IdleTTL：新租户到来时应能驱逐空闲租户而不是被拒。
	// 注意驱逐会清掉**所有**空闲租户（它们会按需重建），所以之后只剩新租户一个。
	now = now.Add(5 * time.Minute)
	rel, err := l.Acquire(ctx, 3)
	if err != nil {
		t.Fatalf("空闲租户应可驱逐，不该拒绝新租户: %v", err)
	}
	rel()
	if got := l.Tenants(); got != 1 {
		t.Fatalf("空闲租户应被驱逐，只留新租户，得到 %d", got)
	}
}

func TestLimiter_MaxTenantsRejects(t *testing.T) {
	l := NewLimiter(LimiterConfig{MaxConcurrency: 1, MaxQueue: 0, MaxTenants: 1, IdleTTL: time.Hour}, nil, time.Now)
	ctx := context.Background()

	rel, err := l.Acquire(ctx, 1)
	if err != nil {
		t.Fatalf("租户 1 应能拿到名额: %v", err)
	}
	rel() // 释放但保持「刚用过」，不可驱逐

	if _, err := l.Acquire(ctx, 2); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("在册租户打满且无可驱逐时应拒绝，得到 %v", err)
	}
}

func TestLimiter_ReleaseIsIdempotent(t *testing.T) {
	l := NewLimiter(LimiterConfig{MaxConcurrency: 1, MaxQueue: 0}, nil, time.Now)
	rel, err := l.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("应拿到名额: %v", err)
	}
	rel()
	rel() // 重复释放不得 panic 或超额释放

	rel2, err := l.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("释放后应能再拿到名额: %v", err)
	}
	rel2()
}

func TestLimiter_IndependentTenants(t *testing.T) {
	l := NewLimiter(LimiterConfig{MaxConcurrency: 1, MaxQueue: 0, MaxTenants: 10}, nil, time.Now)
	ctx := context.Background()

	rel1, err := l.Acquire(ctx, 1)
	if err != nil {
		t.Fatalf("租户 1 应拿到名额: %v", err)
	}
	defer rel1()

	// 租户 2 的名额与租户 1 无关，不该被租户 1 占满而拒绝。
	rel2, err := l.Acquire(ctx, 2)
	if err != nil {
		t.Fatalf("租户 2 不应受租户 1 影响: %v", err)
	}
	rel2()
}
