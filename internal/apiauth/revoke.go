package apiauth

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Revoker 查询 jti 是否已被吊销。
//
// 实现**必须**在自身依赖不可用时返回 error（而不是 false）——
// 调用方据此走 fail-closed（05 §3.3：无吊销信息即视为可能已吊销）。
type Revoker interface {
	Revoked(ctx context.Context, jti string) (bool, error)
}

// RevokedKeyPrefix 是吊销记录的 Redis 键前缀（平台内部键，无 company 维度）。
const RevokedKeyPrefix = "revoked:jti:"

// RevokedKey 返回某个 jti 的吊销键。
func RevokedKey(jti string) string { return RevokedKeyPrefix + jti }

// RedisRevoker 是吊销表的 Redis 实现。
type RedisRevoker struct{ rdb redis.UniversalClient }

var _ Revoker = (*RedisRevoker)(nil)

// NewRedisRevoker 构造吊销表客户端。
func NewRedisRevoker(rdb redis.UniversalClient) (*RedisRevoker, error) {
	if rdb == nil {
		return nil, fmt.Errorf("apiauth: RedisRevoker 需要非空客户端")
	}
	return &RedisRevoker{rdb: rdb}, nil
}

// Revoked 查询 jti 是否在吊销表里。Redis 出错时返回 error（fail-closed 由调用方决定）。
func (r *RedisRevoker) Revoked(ctx context.Context, jti string) (bool, error) {
	n, err := r.rdb.Exists(ctx, RevokedKey(jti)).Result()
	if err != nil {
		return false, fmt.Errorf("查询吊销表: %w", err)
	}
	return n > 0, nil
}

// Revoke 写入一条吊销记录（供未来的管理接口使用）。ttl ≤ 0 时按 24h。
func (r *RedisRevoker) Revoke(ctx context.Context, jti string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if err := r.rdb.Set(ctx, RevokedKey(jti), "1", ttl).Err(); err != nil {
		return fmt.Errorf("写入吊销表: %w", err)
	}
	return nil
}

// MemRevoker 是 Revoker 的内存实现（测试用），可注入故障以验证 fail-closed。
type MemRevoker struct {
	mu      sync.Mutex
	revoked map[string]bool
	fail    error
}

var _ Revoker = (*MemRevoker)(nil)

// NewMemRevoker 构造空的内存吊销表。
func NewMemRevoker() *MemRevoker { return &MemRevoker{revoked: map[string]bool{}} }

// Revoke 标记某 jti 已吊销。
func (m *MemRevoker) Revoke(jti string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revoked[jti] = true
}

// Fail 让后续查询一律返回该错误（用于验证 fail-closed）。
func (m *MemRevoker) Fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = err
}

// Revoked 实现 Revoker。
func (m *MemRevoker) Revoked(_ context.Context, jti string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return false, m.fail
	}
	return m.revoked[jti], nil
}
