package quota

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/metering"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGStore struct{ pool *pgxpool.Pool }

type Policy struct {
	ProjectID    int64
	Metric       string
	SoftLimit    int64
	WarningLimit int64
	HardLimit    int64
	Window       string
}

var AllowedMetrics = map[string]struct{}{
	"msg_count": {}, "conn_peak": {}, "device_count": {}, "storage_bytes": {}, "api_calls": {},
}

func (s *PGStore) Policies(ctx context.Context, projectID int64) ([]Policy, error) {
	var out []Policy
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT project_id,metric,soft_limit,warning_limit,hard_limit,window_kind FROM t_quota_policy WHERE project_id=$1 AND enabled ORDER BY metric`, projectID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var p Policy
			if err := rows.Scan(&p.ProjectID, &p.Metric, &p.SoftLimit, &p.WarningLimit, &p.HardLimit, &p.Window); err != nil {
				return err
			}
			out = append(out, p)
		}
		return rows.Err()
	})
	return out, err
}

func (s *PGStore) SetPolicy(ctx context.Context, p Policy, actor string) error {
	p = NormalizePolicy(p)
	if p.ProjectID <= 0 || p.Metric == "" || p.SoftLimit < 0 || p.HardLimit < p.SoftLimit || (p.Window != "day" && p.Window != "month") {
		return fmt.Errorf("quota: 策略字段非法")
	}
	if p.WarningLimit < p.SoftLimit || p.WarningLimit > p.HardLimit {
		return fmt.Errorf("quota: warning_limit 必须在 soft_limit 与 hard_limit 之间")
	}
	if _, ok := AllowedMetrics[p.Metric]; !ok {
		return fmt.Errorf("quota: 不支持的计量项 %q", p.Metric)
	}
	return s.writePolicy(ctx, p, actor)
}

func NormalizePolicy(p Policy) Policy {
	if p.HardLimit > 0 {
		if p.SoftLimit == 0 {
			p.SoftLimit = percentThreshold(p.HardLimit, 80)
		}
		if p.WarningLimit == 0 {
			p.WarningLimit = percentThreshold(p.HardLimit, 90)
		}
	}
	return p
}

func (s *PGStore) writePolicy(ctx context.Context, p Policy, actor string) error {
	return pg.WithProjectTx(ctx, s.pool, p.ProjectID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO t_quota_policy(project_id,metric,soft_limit,warning_limit,hard_limit,window_kind,enabled,updated_at) VALUES($1,$2,$3,$4,$5,$6,true,now()) ON CONFLICT(project_id,metric) DO UPDATE SET soft_limit=EXCLUDED.soft_limit,warning_limit=EXCLUDED.warning_limit,hard_limit=EXCLUDED.hard_limit,window_kind=EXCLUDED.window_kind,enabled=true,updated_at=now()`, p.ProjectID, p.Metric, p.SoftLimit, p.WarningLimit, p.HardLimit, p.Window); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO t_audit_log(project_id,action,actor_id,resource_type,resource_id,details) VALUES($1,'quota.policy.upsert',$2,'quota_policy',$3,jsonb_build_object('soft_limit',$4,'warning_limit',$5,'hard_limit',$6,'window',$7))`, p.ProjectID, actor, p.Metric, p.SoftLimit, p.WarningLimit, p.HardLimit, p.Window)
		return err
	})
}

func (s *PGStore) DeletePolicy(ctx context.Context, projectID int64, metric, actor string) error {
	if _, ok := AllowedMetrics[metric]; !ok {
		return fmt.Errorf("quota: 不支持的计量项 %q", metric)
	}
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE t_quota_policy SET enabled=false,updated_at=now() WHERE project_id=$1 AND metric=$2`, projectID, metric); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO t_audit_log(project_id,action,actor_id,resource_type,resource_id) VALUES($1,'quota.policy.delete',$2,'quota_policy',$3)`, projectID, actor, metric)
		return err
	})
}

type Alert struct {
	ProjectID   int64
	Metric      string
	Level       string
	Usage       int64
	Limit       int64
	WindowStart time.Time
}

func (s *PGStore) MarkAlertPublished(ctx context.Context, alert Alert) error {
	return pg.WithProjectTx(ctx, s.pool, alert.ProjectID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE t_quota_alert SET published_at=now() WHERE project_id=$1 AND metric=$2 AND level=$3 AND window_start=$4 AND published_at IS NULL`, alert.ProjectID, alert.Metric, alert.Level, alert.WindowStart)
		return err
	})
}

func (s *PGStore) PendingAlerts(ctx context.Context) ([]Alert, error) {
	projects, err := s.Projects(ctx)
	if err != nil {
		return nil, err
	}
	var out []Alert
	for _, projectID := range projects {
		err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT project_id,metric,level,usage,limit_value,window_start FROM t_quota_alert WHERE project_id=$1 AND published_at IS NULL ORDER BY created_at LIMIT 100`, projectID)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var a Alert
				if err := rows.Scan(&a.ProjectID, &a.Metric, &a.Level, &a.Usage, &a.Limit, &a.WindowStart); err != nil {
					return err
				}
				out = append(out, a)
			}
			return rows.Err()
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type ReconcileResult struct {
	ProjectID  int64
	Metric     string
	Day        time.Time
	PGTotal    int64
	RedisTotal int64
	Delta      int64
}

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
ON CONFLICT (project_id, metric, report_id) DO NOTHING`,
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

func (s *PGStore) Policy(ctx context.Context, projectID int64, metric string) (Policy, error) {
	var p Policy
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT project_id, metric, soft_limit, warning_limit, hard_limit, window_kind FROM t_quota_policy WHERE project_id=$1 AND metric=$2 AND enabled`, projectID, metric).Scan(&p.ProjectID, &p.Metric, &p.SoftLimit, &p.WarningLimit, &p.HardLimit, &p.Window)
	})
	return p, err
}

func (s *PGStore) RecordAlert(ctx context.Context, alert Alert) (bool, error) {
	created := false
	err := pg.WithProjectTx(ctx, s.pool, alert.ProjectID, func(ctx context.Context, tx pgx.Tx) error {
		result, err := tx.Exec(ctx, `INSERT INTO t_quota_alert(project_id, metric, level, window_start, usage, limit_value) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, alert.ProjectID, alert.Metric, alert.Level, alert.WindowStart, alert.Usage, alert.Limit)
		created = result.RowsAffected() == 1
		return err
	})
	return created, err
}

func (s *PGStore) Evaluate(ctx context.Context, report metering.UsageReport, metric string) (*Alert, error) {
	p, err := s.Policy(ctx, report.ProjectID, metric)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	windowStart, windowEnd := quotaWindow(report.Window, p.Window)
	if windowStart.IsZero() {
		return nil, nil
	}
	total, err := s.dailyTotal(ctx, report.ProjectID, metric, windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	level, limit := quotaAlertLevel(total, p)
	if level == "" {
		return nil, nil
	}
	return &Alert{ProjectID: report.ProjectID, Metric: metric, Level: level, Usage: total, Limit: limit, WindowStart: windowStart}, nil
}

func quotaAlertLevel(total int64, p Policy) (string, int64) {
	if p.HardLimit <= 0 {
		return "", 0
	}
	if total >= p.HardLimit {
		return "critical", p.HardLimit
	}
	if total >= p.WarningLimit {
		return "warning", p.WarningLimit
	}
	if total >= p.SoftLimit {
		return "info", p.SoftLimit
	}
	return "", 0
}

func percentThreshold(limit, percent int64) int64 {
	return (limit/100)*percent + ((limit%100)*percent+99)/100
}

func quotaWindow(at time.Time, window string) (time.Time, time.Time) {
	at = at.UTC()
	switch window {
	case "day":
		start := at.Truncate(24 * time.Hour)
		return start, start.Add(24 * time.Hour)
	case "month":
		start := time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, 0)
	default:
		return time.Time{}, time.Time{}
	}
}

func (s *PGStore) ReconcileDay(ctx context.Context, redisStore *RedisCounter, projectID int64, metric string, day time.Time) (ReconcileResult, error) {
	dayStart := day.UTC().Truncate(24 * time.Hour)
	pgTotal, err := s.dailyTotal(ctx, projectID, metric, dayStart, dayStart.Add(24*time.Hour))
	if err != nil {
		return ReconcileResult{}, err
	}
	redisTotal, err := redisStore.Daily(ctx, projectID, metric, day)
	if err != nil {
		return ReconcileResult{}, err
	}
	return ReconcileResult{ProjectID: projectID, Metric: metric, Day: dayStart, PGTotal: pgTotal, RedisTotal: redisTotal, Delta: pgTotal - redisTotal}, nil
}

func (s *PGStore) Projects(ctx context.Context) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM t_project WHERE status='active' AND deleted_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *PGStore) DailyTotal(ctx context.Context, projectID int64, metric string, since time.Time) (int64, error) {
	return s.dailyTotal(ctx, projectID, metric, since, time.Time{})
}

func (s *PGStore) dailyTotal(ctx context.Context, projectID int64, metric string, since, until time.Time) (int64, error) {
	var total int64
	aggregate := "sum(delta)"
	if metric == metering.MetricConnPeak {
		aggregate = "max(delta)"
	}
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		query := `SELECT COALESCE(` + aggregate + `,0) FROM t_quota_usage WHERE project_id=$1 AND metric=$2 AND window_start >= $3 AND ($4::timestamptz IS NULL OR window_start < $4)`
		return tx.QueryRow(ctx, query, projectID, metric, since, nullableTime(until)).Scan(&total)
	})
	return total, err
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

var _ DurableStore = (*PGStore)(nil)
