package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisIdempotency 是 IdempotencyStore 的 Redis 实现。
//
// 多实例部署**必须**用它：并发互斥与 done 判定都要跨实例可见，
// 内存实现（MemIdempotency）只在单实例下成立。
type RedisIdempotency struct {
	rdb redis.UniversalClient
}

var _ IdempotencyStore = (*RedisIdempotency)(nil)

// NewRedisIdempotency 构造 Redis 幂等存储。
func NewRedisIdempotency(rdb redis.UniversalClient) *RedisIdempotency {
	return &RedisIdempotency{rdb: rdb}
}

// IsDone 实现 IdempotencyStore。
func (r *RedisIdempotency) IsDone(ctx context.Context, key string) (bool, error) {
	v, err := r.rdb.Get(ctx, key).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return false, nil // 无标记：新消息
	case err != nil:
		return false, fmt.Errorf("查询幂等标记 %s: %w", key, err)
	}
	return v == markDone, nil
}

// AcquireProcessing 实现 IdempotencyStore。
func (r *RedisIdempotency) AcquireProcessing(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := r.rdb.SetNX(ctx, key, markProcessing, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("抢占处理标记 %s: %w", key, err)
	}
	return ok, nil
}

// MarkDone 实现 IdempotencyStore。
func (r *RedisIdempotency) MarkDone(ctx context.Context, key string, ttl time.Duration) error {
	if err := r.rdb.Set(ctx, key, markDone, ttl).Err(); err != nil {
		return fmt.Errorf("写入完成标记 %s: %w", key, err)
	}
	return nil
}

// ReleaseProcessing 实现 IdempotencyStore。
//
// 用 Lua 保证**只删 processing**：若标记已升为 done，删除它会让重投重复入库。
// 与 cluster.RedisLocator.Unbind 同源的写法（见 locator.go）。
func (r *RedisIdempotency) ReleaseProcessing(ctx context.Context, key string) error {
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`

	if err := r.rdb.Eval(ctx, script, []string{key}, markProcessing).Err(); err != nil {
		return fmt.Errorf("释放处理标记 %s: %w", key, err)
	}
	return nil
}
