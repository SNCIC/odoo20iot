package connector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultDedupTTL 是 C-2 webhook 的去重窗口。
//
// 取 1 小时：07 §4.4 未给窗口，但要求「靠 15 min 定时对账兜底」——
// 窗口只要显著大于「重复投递的实际间隔」（Odoo 重试 + 网络重放，秒级到分钟级）
// 即可；取太长只会在 Redis 里白占内存。
const DefaultDedupTTL = time.Hour

// Deduper 判定「这条事件是否已经见过」（07 §4.4 的 C-2 去重：
// 按 `(ext_model, ext_id, write_date)`）。
//
// 与 Guard 的区别值得说清：Guard 的语义是「同键同摘要 → **放行**」
// （因为权威账本在 Odoo，连接器不该替它裁决）；而 Deduper 的语义是
// 「见过 → **丢弃**」—— webhook 路径没有事务，重复投递若不拦就会产生重复事件。
type Deduper interface {
	// FirstSeen 原子登记 key，返回 true 表示本次是首次见到。
	FirstSeen(ctx context.Context, key string) (bool, error)
}

// MemDeduper 是 Deduper 的内存实现（单实例与测试）。
type MemDeduper struct {
	mu   sync.Mutex
	ttl  time.Duration
	seen map[string]time.Time
}

var _ Deduper = (*MemDeduper)(nil)

// NewMemDeduper 构造内存去重器。
func NewMemDeduper(ttl time.Duration) *MemDeduper {
	if ttl <= 0 {
		ttl = DefaultDedupTTL
	}
	return &MemDeduper{ttl: ttl, seen: make(map[string]time.Time, 64)}
}

// FirstSeen 实现 Deduper。
func (d *MemDeduper) FirstSeen(_ context.Context, key string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if exp, ok := d.seen[key]; ok && now.Before(exp) {
		return false, nil
	}
	d.seen[key] = now.Add(d.ttl)
	return true, nil
}

// RedisDeduper 是 Deduper 的 Redis 实现（多实例部署必须用它）。
type RedisDeduper struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

var _ Deduper = (*RedisDeduper)(nil)

// NewRedisDeduper 构造 Redis 去重器。
func NewRedisDeduper(rdb redis.UniversalClient, ttl time.Duration) *RedisDeduper {
	if ttl <= 0 {
		ttl = DefaultDedupTTL
	}
	return &RedisDeduper{rdb: rdb, ttl: ttl}
}

// FirstSeen 用 SETNX 原子的「不存在才写入」实现去重。
func (d *RedisDeduper) FirstSeen(ctx context.Context, key string) (bool, error) {
	ok, err := d.rdb.SetNX(ctx, key, "1", d.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrGuardUnavailable, err)
	}
	return ok, nil
}
