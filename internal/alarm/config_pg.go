package alarm

import (
	"context"
	"fmt"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadSilences reads the persistent maintenance-window configuration.
func LoadSilences(ctx context.Context, pool *pgxpool.Pool) (*Silences, error) {
	if pool == nil {
		return nil, fmt.Errorf("alarm: PG 连接池不能为空")
	}
	windows := NewSilences()
	projects, err := pool.Query(ctx, `SELECT id::text FROM t_project WHERE status='active' AND deleted_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("alarm: 枚举静默窗口租户: %w", err)
	}
	defer projects.Close()
	for projects.Next() {
		var projectID string
		if err := projects.Scan(&projectID); err != nil {
			return nil, fmt.Errorf("alarm: 扫描静默窗口租户: %w", err)
		}
		err := pg.WithProjectValueTx(ctx, pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT id, project_id, device_type_id, device_id, rule_id, start_at, end_at, reason FROM t_alarm_silence WHERE project_id=$1 AND enabled AND end_at > now() ORDER BY start_at, id`, projectID)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var w Silence
				if err := rows.Scan(&w.ID, &w.ProjectID, &w.DeviceTypeID, &w.DeviceID, &w.RuleID, &w.Start, &w.End, &w.Reason); err != nil {
					return err
				}
				windows.Add(w)
			}
			return rows.Err()
		})
		if err != nil {
			return nil, fmt.Errorf("alarm: 读取租户 %s 静默窗口: %w", projectID, err)
		}
	}
	if err := projects.Err(); err != nil {
		return nil, fmt.Errorf("alarm: 遍历静默窗口租户: %w", err)
	}
	return windows, nil
}
