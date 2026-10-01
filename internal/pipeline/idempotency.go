package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// 03 §4.3 的两阶段幂等标记 TTL。
const (
	// DefaultProcessingTTL 是并发互斥标记的存活时间（≥ AckWait × 2）。
	DefaultProcessingTTL = 60 * time.Second
	// DefaultDoneTTL 覆盖 NATS 最大重投窗口：TTL 内的重投可直接跳过。
	DefaultDoneTTL = 5 * time.Minute
)

// 标记取值（03 §4.3）。
const (
	markProcessing = "processing"
	markDone       = "done"
)

// IdempotencyStore 是 03 §4.3 的两阶段幂等标记存储。
//
// 两阶段的意义在于修正两条**永久丢数据**路径：
//   - 若「标记」与「处理完成」不分阶段，SETNX 成功后崩溃、消息重投会被
//     误判为重复并直接 ACK —— 数据永久丢失；
//   - 因此 `processing` **只作并发互斥**（命中必须 NAK 重试，绝不放行 ACK），
//     只有 `done`（落库后才写）命中才允许提前 ACK。
type IdempotencyStore interface {
	// IsDone 查询该消息是否已落库。
	IsDone(ctx context.Context, key string) (bool, error)
	// AcquireProcessing 以 SETNX 抢占 processing 标记。
	// 返回 true 表示抢占成功；false 表示已有其他实例在处理（或上一次尚未完成）。
	AcquireProcessing(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// MarkDone 写入 done 标记（**必须**在持久化确认之后调用）。
	MarkDone(ctx context.Context, key string, ttl time.Duration) error
	// ReleaseProcessing 释放 processing 标记（失败路径），让重投可以重新处理。
	// 实现必须保证**不会误删 done 标记**。
	ReleaseProcessing(ctx context.Context, key string) error
}

// IdempotencyKey 按 03 §4.3 构造幂等键：`idemp:{pid}:telemetry:{did}-{ts}-{seq}`。
//
// ts 用 UnixNano：与端侧毫秒精度相比更细，且对同一份报文恒定（端侧补发保留原始 ts）。
func IdempotencyKey(projectID, deviceID int64, ts time.Time, seq int64) string {
	return fmt.Sprintf("idemp:%d:telemetry:%d-%d-%d", projectID, deviceID, ts.UnixNano(), seq)
}

// MemIdempotency 是 IdempotencyStore 的内存实现（单实例与测试用）。
//
// ⚠️ 多实例部署必须用 Redis 实现：名额互斥与 done 判定都要跨实例可见。
type MemIdempotency struct {
	mu    sync.Mutex
	items map[string]memMark
}

type memMark struct {
	value     string
	expiresAt time.Time
}

// NewMemIdempotency 构造内存幂等存储。
func NewMemIdempotency() *MemIdempotency {
	return &MemIdempotency{items: make(map[string]memMark, 1024)}
}

func (m *MemIdempotency) get(key string) (string, bool) {
	item, ok := m.items[key]
	if !ok {
		return "", false
	}
	if time.Now().After(item.expiresAt) {
		delete(m.items, key)
		return "", false
	}
	return item.value, true
}

// IsDone 实现 IdempotencyStore。
func (m *MemIdempotency) IsDone(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.get(key)
	return ok && v == markDone, nil
}

// AcquireProcessing 实现 IdempotencyStore。
func (m *MemIdempotency) AcquireProcessing(_ context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.get(key); ok {
		return false, nil
	}
	m.items[key] = memMark{value: markProcessing, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

// MarkDone 实现 IdempotencyStore。
func (m *MemIdempotency) MarkDone(_ context.Context, key string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = memMark{value: markDone, expiresAt: time.Now().Add(ttl)}
	return nil
}

// ReleaseProcessing 实现 IdempotencyStore（只删 processing，不动 done）。
func (m *MemIdempotency) ReleaseProcessing(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.get(key); ok && v == markProcessing {
		delete(m.items, key)
	}
	return nil
}
