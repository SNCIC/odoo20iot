package connector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Reservation 是一次幂等占位的结论。
type Reservation int

const (
	// ReservationProceed 表示可以继续调用（无占位，或摘要一致）。
	ReservationProceed Reservation = iota
	// ReservationConflict 表示同键但请求摘要不同（§4.3.1：返回 409）。
	ReservationConflict
)

// ErrGuardUnavailable 表示占位存储不可用。
//
// ⚠️ 这**不是**业务失败：按 §5.2.1，写回路径的权威账本是 Odoo 的
// `edge_idempotency`，Redis 只做加速。占位不可用时连接器应**降级放行**，
// 让 Odoo 的唯一约束去裁决，而不是把业务挡在门外。
var ErrGuardUnavailable = errors.New("connector: 幂等占位存储不可用")

// Guard 是 §4.3.1 第 2 步的幂等占位：在调用 Odoo 之前**快速拦截并发重复**。
//
// 它刻意只回答「能不能继续」，不回答「结果是什么」—— 后者只有 Odoo 的账本
// 知道（§5.2.1「Odoo 侧必须是权威」）。
type Guard interface {
	// Reserve 为 (key, requestHash) 占位并返回结论。
	Reserve(ctx context.Context, key, requestHash string) (Reservation, error)
}

// DefaultGuardTTL 是占位窗口。取得较短：它只需覆盖「同类请求的并发窗口」，
// 真正的长期幂等由 Odoo 的 `expires_at`（7/30/90 天分档）承担（§4.3.1）。
const DefaultGuardTTL = 5 * time.Minute

// MemGuard 是 Guard 的内存实现（单实例与测试）。
type MemGuard struct {
	mu   sync.Mutex
	ttl  time.Duration
	seen map[string]memEntry
}

type memEntry struct {
	hash      string
	expiresAt time.Time
}

// NewMemGuard 构造内存幂等占位。
func NewMemGuard(ttl time.Duration) *MemGuard {
	if ttl <= 0 {
		ttl = DefaultGuardTTL
	}
	return &MemGuard{ttl: ttl, seen: make(map[string]memEntry, 64)}
}

// Reserve 实现 Guard。
func (g *MemGuard) Reserve(_ context.Context, key, requestHash string) (Reservation, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if e, ok := g.seen[key]; ok && time.Now().Before(e.expiresAt) {
		if e.hash != requestHash {
			return ReservationConflict, nil
		}
		return ReservationProceed, nil
	}
	g.seen[key] = memEntry{hash: requestHash, expiresAt: time.Now().Add(g.ttl)}
	return ReservationProceed, nil
}

// RedisGuard 是 Guard 的 Redis 实现（多实例部署必须用它）。
type RedisGuard struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

var _ Guard = (*RedisGuard)(nil)

// NewRedisGuard 构造 Redis 幂等占位。
func NewRedisGuard(rdb redis.UniversalClient, ttl time.Duration) *RedisGuard {
	if ttl <= 0 {
		ttl = DefaultGuardTTL
	}
	return &RedisGuard{rdb: rdb, ttl: ttl}
}

// reserveScript 在**一条** Lua 里完成「查—比—写」。
//
// 必须原子：若拆成先 GET 再 SETNX，两个并发同键请求可能同时看到「不存在」
// 而双双通过 —— 那正是占位要拦的情形。
const reserveScript = `
local cur = redis.call("GET", KEYS[1])
if cur then
  if cur == ARGV[1] then
    return 0
  end
  return 1
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
return 0`

// Reserve 实现 Guard。返回 1 表示摘要冲突。
func (g *RedisGuard) Reserve(ctx context.Context, key, requestHash string) (Reservation, error) {
	res, err := g.rdb.Eval(ctx, reserveScript, []string{key}, requestHash, int(g.ttl.Seconds())).Int()
	if err != nil {
		return ReservationProceed, fmt.Errorf("%w: %v", ErrGuardUnavailable, err)
	}
	if res == 1 {
		return ReservationConflict, nil
	}
	return ReservationProceed, nil
}
