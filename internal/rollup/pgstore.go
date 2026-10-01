package rollup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore 是水位账本的 PostgreSQL 实现。
type PGStore struct{ pool *pgxpool.Pool }

var _ Store = (*PGStore)(nil)

// NewPGStore 构造水位账本存储。
func NewPGStore(pool *pgxpool.Pool) (*PGStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("rollup: PGStore 需要非空连接池")
	}
	return &PGStore{pool: pool}, nil
}

// advanceSQL 用 GREATEST 保证**只进不退**：并发实例（多副本）或乱序调用
// 都不会把水位推回去 —— 推回去意味着重复物化已完成的窗口。
const advanceSQL = `
INSERT INTO t_rollup_watermark (granularity, window_start, updated_at, last_error)
VALUES ($1, $2, now(), '')
ON CONFLICT (granularity) DO UPDATE
SET window_start = GREATEST(t_rollup_watermark.window_start, EXCLUDED.window_start),
    updated_at   = now(),
    last_error   = ''`

// Advance 推进水位。
func (s *PGStore) Advance(ctx context.Context, granularity string, windowStart time.Time) error {
	if _, err := s.pool.Exec(ctx, advanceSQL, granularity, windowStart.UTC()); err != nil {
		return fmt.Errorf("rollup: 推进水位 %s: %w", granularity, err)
	}
	return nil
}

const getSQL = `SELECT window_start, updated_at, last_error FROM t_rollup_watermark WHERE granularity = $1`

// Get 读取水位；不存在时 found=false（首次运行）。
func (s *PGStore) Get(ctx context.Context, granularity string) (Watermark, bool, error) {
	var (
		wm        Watermark
		start     time.Time
		updatedAt time.Time
		lastErr   string
	)
	err := s.pool.QueryRow(ctx, getSQL, granularity).Scan(&start, &updatedAt, &lastErr)
	if errors.Is(err, pgx.ErrNoRows) {
		return Watermark{Granularity: granularity}, false, nil
	}
	if err != nil {
		return Watermark{}, false, fmt.Errorf("rollup: 读水位 %s: %w", granularity, err)
	}
	wm = Watermark{Granularity: granularity, WindowStart: start, UpdatedAt: updatedAt, LastError: lastErr}
	return wm, true, nil
}

// recordErrorSQL 刻意只 UPDATE、**不 INSERT**：
//
// 若为记账而插入一行，就得编一个 window_start（如 epoch），而下一轮 Get 会读到它、
// 认为「已物化到 epoch」→ 从 1970 年重扫全量。宁可首次失败时不留 last_error
// （服务日志仍有），也不能让假水位污染账本。
const recordErrorSQL = `
UPDATE t_rollup_watermark SET updated_at = now(), last_error = $2 WHERE granularity = $1`

// RecordError 记录失败原因，不动 window_start。
func (s *PGStore) RecordError(ctx context.Context, granularity, errText string) error {
	if _, err := s.pool.Exec(ctx, recordErrorSQL, granularity, errText); err != nil {
		return fmt.Errorf("rollup: 记录错误 %s: %w", granularity, err)
	}
	return nil
}
