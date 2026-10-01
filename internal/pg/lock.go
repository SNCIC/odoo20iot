package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Lock 是一个**会话级** PG 咨询锁。
//
// 为什么用 PG 而不是 Redis（04 §2.5 写的是「Redis 分布式锁」）：业务库本来就是
// 告警扫描的强依赖，再引一个外部依赖只是多一个会挂的东西；而
// `pg_try_advisory_lock` 的语义正好是「拿不到立刻返回」，天然适合
// 每 5s 一次的扫描选举 —— 不需要等待、不需要租约续期。
//
// ⚠️ 咨询锁是**会话级**的：必须在**同一条连接**上取和放。用 `pool.Exec`
// 会让「取锁」和「解锁」落到不同连接上，表现为锁取到了却永远释放不掉，
// 其他实例会一直扫不上 —— 而且没有任何报错。故这里持有专用连接。
type Lock struct {
	conn *pgxpool.Conn
	key  int64
	held bool
}

// TryLock 尝试取锁。拿不到时返回 (nil, false, nil) ——
// 「别人正在扫」是正常情况，不是错误，用 error 表达会逼调用方做错误串比对。
func TryLock(ctx context.Context, pool *pgxpool.Pool, key int64) (*Lock, bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("pg: 取连接以加锁: %w", err)
	}

	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&got); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("pg: 尝试加锁: %w", err)
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	return &Lock{conn: conn, key: key, held: true}, true, nil
}

// Unlock 释放锁并归还连接。重复调用是安全的。
//
// 用 `context.WithoutCancel` 兜底：如果调用方的 ctx 已经取消（比如进程正在退出），
// 用它会直接放弃解锁，连接被归还但锁还挂在会话上 —— 得等连接池回收这条连接
// 才会释放，期间所有实例都扫不上。
func (l *Lock) Unlock(ctx context.Context) error {
	if l == nil || !l.held {
		return nil
	}
	l.held = false
	defer l.conn.Release()

	if _, err := l.conn.Exec(context.WithoutCancel(ctx),
		"SELECT pg_advisory_unlock($1)", l.key); err != nil {
		return fmt.Errorf("pg: 解锁: %w", err)
	}
	return nil
}

// Held 报告锁是否仍被持有（便于断言与观测）。
func (l *Lock) Held() bool { return l != nil && l.held }

// AdvisoryKey 把一段有意义的字符串稳定地映射成咨询锁的键。
//
// 直接用常量也行，但用字符串派生能让「这把锁是干什么的」写在调用点上，
// 而不是靠注释。用 FNV-1a —— 只需要稳定与分散，不需要抗碰撞。
func AdvisoryKey(name string) int64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= prime64
	}
	return int64(h)
}
