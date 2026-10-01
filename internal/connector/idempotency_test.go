package connector

import (
	"context"
	"testing"
	"time"
)

func TestMemGuard_同键同摘要放行(t *testing.T) {
	g := NewMemGuard(time.Minute)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		res, err := g.Reserve(ctx, "idem:1:op:k", "hash-a")
		if err != nil {
			t.Fatalf("Reserve 失败: %v", err)
		}
		if res != ReservationProceed {
			t.Fatalf("同键同摘要应放行（并发重复交由 Odoo 账本裁决），得到 %v", res)
		}
	}
}

func TestMemGuard_同键异摘要冲突(t *testing.T) {
	g := NewMemGuard(time.Minute)
	ctx := context.Background()

	if _, err := g.Reserve(ctx, "k", "hash-a"); err != nil {
		t.Fatalf("Reserve 失败: %v", err)
	}
	res, err := g.Reserve(ctx, "k", "hash-b")
	if err != nil {
		t.Fatalf("Reserve 失败: %v", err)
	}
	if res != ReservationConflict {
		t.Fatalf("同键异摘要应判为冲突，得到 %v", res)
	}
}

func TestMemGuard_TTL过期后可重来(t *testing.T) {
	g := NewMemGuard(10 * time.Millisecond)
	ctx := context.Background()

	if _, err := g.Reserve(ctx, "k", "hash-a"); err != nil {
		t.Fatalf("Reserve 失败: %v", err)
	}
	time.Sleep(25 * time.Millisecond)

	res, err := g.Reserve(ctx, "k", "hash-b")
	if err != nil {
		t.Fatalf("Reserve 失败: %v", err)
	}
	if res == ReservationConflict {
		t.Fatal("占位过期后不应再判为冲突")
	}
}

// failingGuard 模拟占位存储故障。
type failingGuard struct{}

func (failingGuard) Reserve(context.Context, string, string) (Reservation, error) {
	return ReservationProceed, ErrGuardUnavailable
}

// TestConnector_幂等占位冲突不触达Odoo 验证 §4.3.1 第 2 步「快速拦截」：
// 同键异摘要必须在**调用 Odoo 之前**被挡下。
func TestConnector_幂等占位冲突不触达Odoo(t *testing.T) {
	client := new(fakeClient)
	c, err := New(client, Options{
		Policy: noRetryPolicy(),
		Logger: testLogger(),
		Guard:  NewMemGuard(time.Minute),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("构造连接器失败: %v", err)
	}

	ctx := context.Background()
	req := Request{Model: "m", Method: "create", IdempotencyKey: "idem:1:op:k", RequestHash: "h1"}

	if err := c.Call(ctx, req, nil); err != nil {
		t.Fatalf("首次应成功: %v", err)
	}

	bad := req
	bad.RequestHash = "h2"
	err = c.Call(ctx, bad, nil)
	if code := codeOf(t, err); code != CodeIdempotencyConflict {
		t.Fatalf("应返回 IDEMPOTENCY_CONFLICT，得到 %s", code)
	}
	if got := client.count(); got != 1 {
		t.Fatalf("冲突请求不应触达 Odoo，实际调用 %d 次", got)
	}
}

// TestConnector_占位不可用降级放行 是关键的降级取舍：
// Redis 只是加速层，它挂了不该让所有写回停摆（§5.2.1）。
func TestConnector_占位不可用降级放行(t *testing.T) {
	client := new(fakeClient)
	c, err := New(client, Options{
		Policy: noRetryPolicy(),
		Logger: testLogger(),
		Guard:  failingGuard{},
		Sleep:  func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("构造连接器失败: %v", err)
	}

	err = c.Call(context.Background(), Request{
		Model: "m", Method: "create", IdempotencyKey: "k", RequestHash: "h",
	}, nil)
	if err != nil {
		t.Fatalf("占位不可用时应降级放行，得到 %v", err)
	}
	if client.count() != 1 {
		t.Fatal("降级后应正常调用 Odoo")
	}
}

// TestConnector_缺摘要不走占位 验证 Guard 的触发条件（键与摘要都要有）。
func TestConnector_缺摘要不走占位(t *testing.T) {
	client := new(fakeClient)
	c, err := New(client, Options{
		Policy: noRetryPolicy(),
		Logger: testLogger(),
		Guard:  NewMemGuard(time.Minute),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("构造连接器失败: %v", err)
	}

	ctx := context.Background()
	// 只有 key、没有 hash：两次都不应被判冲突。
	for i := 0; i < 2; i++ {
		if err := c.Call(ctx, Request{
			Model: "m", Method: "create", IdempotencyKey: "k",
		}, nil); err != nil {
			t.Fatalf("第 %d 次调用失败: %v", i+1, err)
		}
	}
	if client.count() != 2 {
		t.Fatalf("缺摘要时不应拦截，实际调用 %d 次", client.count())
	}
}
