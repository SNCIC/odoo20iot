package ruleconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/dag"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/rules"
)

type Match struct {
	DeviceTypeID int64  `json:"device_type_id"`
	Expr         string `json:"expr"`
}

type Rule struct {
	ProjectID     string
	RuleID        string
	Name          string
	Level         string
	Priority      int
	Version       int64
	Match         Match
	Window        map[string]any
	DAG           dag.Definition
	Capabilities  []string
	ErrorPolicy   dag.ErrorPolicy
	EffectiveFrom *time.Time
	Program       *rules.Program
	Graph         *dag.Graph
}

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("ruleconfig: 连接池不能为空")
	}
	return &Store{pool: pool}, nil
}

func (s *Store) LoadEnabled(ctx context.Context, projectID string, schema *rules.DeviceSchema, _ *dag.Registry) ([]Rule, error) {
	var out []Rule
	err := pg.WithProjectValueTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT rule_id, rule_name, level, priority, "match", "window", dag, capabilities, error_policy, version, effective_from
FROM t_alarm_rule
WHERE project_id=$1 AND enabled AND (effective_from IS NULL OR effective_from <= now())
  AND COALESCE("match"->>'expr', '') <> ''
ORDER BY priority, rule_id, version DESC`, projectID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r Rule
			var matchRaw, windowRaw, dagRaw, capsRaw []byte
			var policy string
			if err := rows.Scan(&r.RuleID, &r.Name, &r.Level, &r.Priority, &matchRaw, &windowRaw, &dagRaw, &capsRaw, &policy, &r.Version, &r.EffectiveFrom); err != nil {
				return err
			}
			r.ProjectID, r.ErrorPolicy = projectID, dag.ErrorPolicy(policy)
			if err := json.Unmarshal(matchRaw, &r.Match); err != nil {
				return fmt.Errorf("规则 %s match: %w", r.RuleID, err)
			}
			if err := json.Unmarshal(windowRaw, &r.Window); err != nil {
				return fmt.Errorf("规则 %s window: %w", r.RuleID, err)
			}
			if err := json.Unmarshal(dagRaw, &r.DAG); err != nil {
				return fmt.Errorf("规则 %s dag: %w", r.RuleID, err)
			}
			if err := json.Unmarshal(capsRaw, &r.Capabilities); err != nil {
				return fmt.Errorf("规则 %s capabilities: %w", r.RuleID, err)
			}
			r.Program, err = rules.NewCompiler(10000).Compile(fmt.Sprintf("%s:%d", r.RuleID, r.Version), r.Match.Expr, schema)
			if err != nil {
				return fmt.Errorf("规则 %s 表达式: %w", r.RuleID, err)
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}
