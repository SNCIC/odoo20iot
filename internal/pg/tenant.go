package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WithProjectTx starts a transaction with the tenant context attached to the
// transaction. RLS policies must never depend on a pool-wide setting: a pooled
// connection can be reused by another tenant immediately after the callback.
func WithProjectTx(ctx context.Context, pool *pgxpool.Pool, projectID int64, fn func(context.Context, pgx.Tx) error) error {
	return WithProjectValueTx(ctx, pool, fmt.Sprint(projectID), fn)
}

// WithProjectValueTx is the same transaction boundary for legacy tables whose
// project_id is TEXT (notably the alarm state table).
func WithProjectValueTx(ctx context.Context, pool *pgxpool.Pool, projectID string, fn func(context.Context, pgx.Tx) error) error {
	if pool == nil {
		return fmt.Errorf("pg: 连接池不能为空")
	}
	if projectID == "" {
		return fmt.Errorf("pg: project_id 必须为正数")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg: 开启租户事务: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('app.project_id', $1, true)`, projectID); err != nil {
		return fmt.Errorf("pg: 设置租户上下文: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg: 提交租户事务: %w", err)
	}
	return nil
}
