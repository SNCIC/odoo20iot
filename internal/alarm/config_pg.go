package alarm

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadSilences reads the persistent maintenance-window configuration.
func LoadSilences(ctx context.Context, pool *pgxpool.Pool) (*Silences, error) {
	if pool == nil {
		return nil, fmt.Errorf("alarm: PG 连接池不能为空")
	}
	windows := NewSilences()
	rows, err := pool.Query(ctx, `SELECT id, project_id, device_type_id, device_id, rule_id, start_at, end_at, reason FROM t_alarm_silence WHERE enabled AND end_at > now() ORDER BY start_at, id`)
	if err != nil {
		return nil, fmt.Errorf("alarm: 读取静默窗口: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var w Silence
		if err := rows.Scan(&w.ID, &w.ProjectID, &w.DeviceTypeID, &w.DeviceID, &w.RuleID, &w.Start, &w.End, &w.Reason); err != nil {
			return nil, fmt.Errorf("alarm: 扫描静默窗口: %w", err)
		}
		windows.Add(w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("alarm: 遍历静默窗口: %w", err)
	}
	return windows, nil
}
