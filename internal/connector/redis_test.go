package connector

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// newTestRedis 连接真实 Redis；未设置 IOT_REDIS_URL 或不可达时跳过。
func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()

	raw := os.Getenv("IOT_REDIS_URL")
	if raw == "" {
		t.Skip("未设置 IOT_REDIS_URL，跳过真实 Redis 用例")
	}
	opts, err := redis.ParseURL(raw)
	if err != nil {
		t.Fatalf("解析 IOT_REDIS_URL: %v", err)
	}

	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis 不可达（%v），跳过", err)
	}
	return rdb
}

// testStreamName 生成一个隔离的 Stream 名并登记清理。
//
// 复用调用方已建好的连接：在 t.Cleanup 里再调 newTestRedis 会在测试结束后
// 触发 t.Skip，那是不允许的（Skip 只能在测试运行中调用）。
func testStreamName(t *testing.T, rdb *redis.Client, prefix string) string {
	t.Helper()
	name := fmt.Sprintf("test:%s:%d", prefix, time.Now().UnixNano()%1_000_000)
	t.Cleanup(func() { _ = rdb.Del(context.Background(), name).Err() })
	return name
}

// TestRedisStreams_建组读确认往返 用真实 Redis 验证三个方法的契约。
func TestRedisStreams_建组读确认往返(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()
	stream := testStreamName(t, rdb, "outbox")

	s := NewRedisStreams(rdb)
	if err := s.EnsureGroup(ctx, stream, "g"); err != nil {
		t.Fatalf("建组失败: %v", err)
	}
	// 幂等：重复建组必须吞掉 BUSYGROUP，否则多副本同时启动会有一个起不来。
	if err := s.EnsureGroup(ctx, stream, "g"); err != nil {
		t.Fatalf("重复建组应幂等: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{
			"event_id":        fmt.Sprintf("ev-%d", i),
			"aggregate_model": "mrp.workorder",
		}}).Result(); err != nil {
			t.Fatalf("XADD 失败: %v", err)
		}
	}

	entries, err := s.ReadGroup(ctx, stream, "g", "c1", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("应读到 3 条，得到 %d", len(entries))
	}
	ids := make([]string, 0, 3)
	for _, e := range entries {
		if e.Fields["aggregate_model"] != "mrp.workorder" {
			t.Fatalf("字段丢失: %v", e.Fields)
		}
		ids = append(ids, e.ID)
	}

	// 未 ACK 前它沉在 PEL 里，`>` 读不到 —— 这正是必须做 XAUTOCLAIM 的原因。
	again, err := s.ReadGroup(ctx, stream, "g", "c1", 10, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("二次读取失败: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("未 ACK 的消息不应被 `>` 重读，得到 %d 条", len(again))
	}

	if err := s.Ack(ctx, stream, "g", ids...); err != nil {
		t.Fatalf("ACK 失败: %v", err)
	}
	pending, err := rdb.XPending(ctx, stream, "g").Result()
	if err != nil {
		t.Fatalf("XPENDING 失败: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("确认后待确认应为 0，得到 %d", pending.Count)
	}
}

// TestRedisStreams_接管PEL 验证 XAUTOCLAIM 能把未确认消息捞回来
// —— 这是「至少一次」在 Redis 侧的唯一兜底。
func TestRedisStreams_接管PEL(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()
	stream := testStreamName(t, rdb, "claim")

	s := NewRedisStreams(rdb)
	if err := s.EnsureGroup(ctx, stream, "g"); err != nil {
		t.Fatalf("建组失败: %v", err)
	}
	if _, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{
		"event_id": "ev-claim", "aggregate_model": "mrp.workorder",
	}}).Result(); err != nil {
		t.Fatalf("XADD 失败: %v", err)
	}

	// c1 读了但**故意不 ACK**，模拟「发布失败」。
	first, err := s.ReadGroup(ctx, stream, "g", "c1", 10, 100*time.Millisecond)
	if err != nil || len(first) != 1 {
		t.Fatalf("首读应得 1 条（err=%v, n=%d）", err, len(first))
	}

	// idle 未到阈值：不该被接管（否则会把正在处理的活抢走）。
	none, err := s.ClaimStale(ctx, stream, "g", "c2", time.Hour, 10)
	if err != nil {
		t.Fatalf("接管失败: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("未到 idle 阈值不应接管，得到 %d 条", len(none))
	}

	// idle=0：应立刻接管。
	claimed, err := s.ClaimStale(ctx, stream, "g", "c2", 0, 10)
	if err != nil {
		t.Fatalf("接管失败: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != first[0].ID {
		t.Fatalf("应接管刚才那条，得到 %+v", claimed)
	}
	if claimed[0].Fields["event_id"] != "ev-claim" {
		t.Fatalf("接管的消息应保留字段: %v", claimed[0].Fields)
	}

	if err := s.Ack(ctx, stream, "g", claimed[0].ID); err != nil {
		t.Fatalf("ACK 失败: %v", err)
	}
	pending, err := rdb.XPending(ctx, stream, "g").Result()
	if err != nil {
		t.Fatalf("XPENDING 失败: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("接管并确认后待确认应为 0，得到 %d", pending.Count)
	}
}

// TestRedisWatermarks_只进不退 用真实 Redis 验证水位推进的原子比较。
//
// 水位被拉回去的后果是**重复处理一整段记录**，所以「只进不退」必须是原子的
// （多副本并发推进时，非原子的读—比较—写在 Lua 里才靠得住）。
func TestRedisWatermarks_只进不退(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()
	model := fmt.Sprintf("test.wm.%d", time.Now().UnixNano()%1_000_000)
	key := watermarkKey(model)
	t.Cleanup(func() { _ = rdb.Del(context.Background(), key).Err() })

	wm := NewRedisWatermarks(rdb)
	if _, ok, err := wm.Get(ctx, model); err != nil || ok {
		t.Fatalf("初始不应有水位: ok=%v err=%v", ok, err)
	}

	base := Watermark{WriteDate: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC), ID: 10}
	if err := wm.Advance(ctx, model, base); err != nil {
		t.Fatalf("推进失败: %v", err)
	}
	// 时间戳更早：不该覆盖。
	if err := wm.Advance(ctx, model, Watermark{WriteDate: base.WriteDate.Add(-time.Hour), ID: 99}); err != nil {
		t.Fatalf("推进失败: %v", err)
	}
	// 同秒但 id 更小：也不该覆盖。
	if err := wm.Advance(ctx, model, Watermark{WriteDate: base.WriteDate, ID: 9}); err != nil {
		t.Fatalf("推进失败: %v", err)
	}

	got, ok, err := wm.Get(ctx, model)
	if err != nil || !ok {
		t.Fatalf("读水位失败: ok=%v err=%v", ok, err)
	}
	if !got.WriteDate.Equal(base.WriteDate) || got.ID != 10 {
		t.Fatalf("水位不应被更小的值拉回，期望 %+v，得到 %+v", base, got)
	}

	// 同秒但 id 更大：应推进（这正是 (write_date, id) 二元边界的意义）。
	if err := wm.Advance(ctx, model, Watermark{WriteDate: base.WriteDate, ID: 11}); err != nil {
		t.Fatalf("推进失败: %v", err)
	}
	if got, _, _ = wm.Get(ctx, model); got.ID != 11 {
		t.Fatalf("同秒更大 id 应推进水位，得到 %+v", got)
	}
}

// TestRedisStreams_Block零不永久阻塞 是一条回归用例。
//
// go-redis 在 `Block: 0` 时会照发 `BLOCK 0`，而 **Redis 的 `BLOCK 0` 是
// 「永久阻塞」而不是「不阻塞」**。ReadGroup 必须把 block<=0 翻译成非阻塞，
// 否则消费循环一旦进入「手里已有重投消息」的分支就会挂死在这里。
func TestRedisStreams_Block零不永久阻塞(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()
	stream := testStreamName(t, rdb, "block")

	s := NewRedisStreams(rdb)
	if err := s.EnsureGroup(ctx, stream, "g"); err != nil {
		t.Fatalf("建组失败: %v", err)
	}

	type result struct {
		entries []StreamEntry
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		entries, err := s.ReadGroup(ctx, stream, "g", "c1", 10, 0)
		ch <- result{entries, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("读取失败: %v", r.err)
		}
		if len(r.entries) != 0 {
			t.Fatalf("空流应返回 0 条，得到 %d", len(r.entries))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("block=0 时 ReadGroup 永久阻塞（go-redis 会把 0 下发给 BLOCK 0）")
	}
}

// TestOutboxConsumer_真实Redis搬运 端到端跑一次「真实 Redis + fake 发布器」。
func TestOutboxConsumer_真实Redis搬运(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()
	stream := testStreamName(t, rdb, "outbox-run")

	for i := 0; i < 2; i++ {
		if _, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{
			"event_id":        fmt.Sprintf("ev-%d", i),
			"aggregate_model": "mrp.workorder",
			"aggregate_id":    "1",
			"company_id":      "1",
			"payload":         `{}`,
		}}).Result(); err != nil {
			t.Fatalf("XADD 失败: %v", err)
		}
	}

	pub := new(fakePub)
	c, err := NewOutboxConsumer(OutboxOptions{
		Store:       NewRedisStreams(rdb),
		Publisher:   pub,
		Stream:      stream,
		Group:       "g",
		Consumer:    "c1",
		Logger:      testLogger(),
		Block:       100 * time.Millisecond,
		ReclaimIdle: time.Hour, // 避免把本轮的活当陈旧消息抢回
	})
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	if err := c.store.EnsureGroup(ctx, stream, "g"); err != nil {
		t.Fatalf("建组失败: %v", err)
	}

	res, err := c.ConsumeOnce(ctx)
	if err != nil {
		t.Fatalf("消费失败: %v", err)
	}
	if res.Fetched != 2 || res.Published != 2 || res.Failed != 0 {
		t.Fatalf("统计不符: %+v", res)
	}
	if got := len(pub.published()); got != 2 {
		t.Fatalf("应发布 2 条，得到 %d", got)
	}

	pending, err := rdb.XPending(ctx, stream, "g").Result()
	if err != nil {
		t.Fatalf("XPENDING 失败: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("搬运后待确认应为 0，得到 %d", pending.Count)
	}
}

// TestOutboxConsumer_真实Redis发布失败留PEL 验证「失败不 ACK」在真实 Redis 上成立，
// 并且下一轮能被 ClaimStale 捞回来重投 —— 这两步合起来才是「至少一次」。
func TestOutboxConsumer_真实Redis发布失败留PEL(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()
	stream := testStreamName(t, rdb, "outbox-fail")

	if _, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{
		"event_id": "ev-fail", "aggregate_model": "mrp.workorder", "payload": `{}`,
	}}).Result(); err != nil {
		t.Fatalf("XADD 失败: %v", err)
	}

	// 第一轮：发布必失败。
	failingPub := &fakePub{errs: []error{fmt.Errorf("nats 不可达")}}
	c, err := NewOutboxConsumer(OutboxOptions{
		Store:     NewRedisStreams(rdb),
		Publisher: failingPub,
		Stream:    stream,
		Group:     "g",
		Consumer:  "c1",
		Logger:    testLogger(),
		Block:     100 * time.Millisecond,
		// 取 10ms 而非 0：0 会被 Normalize 成默认的 30s（刻意的 —— 在
		// 生产里 minIdle=0 会把别人正在处理的活抢走）。另外 go-redis 会把
		// min-idle 截断到毫秒（传 1ns 会警告并降为 1ms），而本地 Redis
		// 两次调用间隔往往不足 1ms，故用毫秒级阈值 + 下面的显式等待。
		ReclaimIdle: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	if err := c.store.EnsureGroup(ctx, stream, "g"); err != nil {
		t.Fatalf("建组失败: %v", err)
	}

	res, err := c.ConsumeOnce(ctx)
	if err != nil {
		t.Fatalf("消费失败: %v", err)
	}
	if res.Failed != 1 || res.Published != 0 {
		t.Fatalf("首轮应发布失败: %+v", res)
	}
	pending, err := rdb.XPending(ctx, stream, "g").Result()
	if err != nil {
		t.Fatalf("XPENDING 失败: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("发布失败的消息必须留在 PEL，待确认应为 1，得到 %d", pending.Count)
	}

	// 第二轮：换成能成功的发布器，同一消费者应把 PEL 里的捞回来重投。
	// 先等过 reclaim idle 阈值（XAUTOCLAIM 只接管空闲够了的那条）。
	time.Sleep(25 * time.Millisecond)
	c.pub = new(fakePub)
	res, err = c.ConsumeOnce(ctx)
	if err != nil {
		t.Fatalf("重投轮失败: %v", err)
	}
	if res.Reclaimed != 1 || res.Published != 1 {
		t.Fatalf("第二轮应接管并发布 1 条: %+v", res)
	}
	pending, err = rdb.XPending(ctx, stream, "g").Result()
	if err != nil {
		t.Fatalf("XPENDING 失败: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("重投成功后待确认应为 0，得到 %d", pending.Count)
	}
}
