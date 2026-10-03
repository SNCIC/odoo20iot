package quota

import (
	"context"
	"fmt"
	"strings"

	"github.com/SNCIC/odoo20iot/internal/metering"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGStore struct{ pool *pgxpool.Pool }

func NewPGStore(pool *pgxpool.Pool) (*PGStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("quota: PGStore 需要连接池")
	}
	return &PGStore{pool: pool}, nil
}

func (s *PGStore) Record(ctx context.Context, report metering.UsageReport, metric string, delta int64) error {
	if report.ProjectID <= 0 || metric == "" || delta <= 0 {
		return fmt.Errorf("quota: project_id、metric 和正增量必填")
	}
	reportID := strings.TrimSpace(report.ReportID)
	if reportID == "" {
		return fmt.Errorf("quota: report_id 缺失")
	}
	return pg.WithProjectTx(ctx, s.pool, report.ProjectID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO t_quota_usage(project_id, metric, window_start, report_id, delta, node_id)
VALUES ($1, $2, date_trunc('minute', $3::timestamptz), $4, $5, $6)
ON CONFLICT (project_id, metric, report_id)
DO UPDATE SET delta=EXCLUDED.delta, window_start=EXCLUDED.window_start, node_id=EXCLUDED.node_id, updated_at=now()`,
			report.ProjectID, metric, report.Window, reportID, delta, report.NodeID)
		return err
	})
}

func (s *PGStore) Total(ctx context.Context, projectID int64, metric string) (int64, error) {
	if projectID <= 0 || metric == "" {
		return 0, fmt.Errorf("quota: project_id 和 metric 必填")
	}
	var total int64
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT COALESCE(sum(delta),0) FROM t_quota_usage WHERE project_id=$1 AND metric=$2`, projectID, metric).Scan(&total)
	})
	return total, err
}

var _ DurableStore = (*PGStore)(nil)
