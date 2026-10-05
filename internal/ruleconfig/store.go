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
	Enabled       bool
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

func (s *Store) DeviceSchema(ctx context.Context, projectID string, deviceTypeID int64) (*rules.DeviceSchema, error) {
	sch := &rules.DeviceSchema{Metrics: map[string]rules.Kind{}}
	err := pg.WithProjectValueTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT thing_model FROM t_device_type WHERE project_id=$1 AND id=$2 AND deleted_at IS NULL`, projectID, deviceTypeID).Scan(&raw); err != nil {
			return err
		}
		return decodeThingModel(raw, sch)
	})
	if err != nil {
		sch.AllowUnknownMetrics = true
	}
	return sch, nil
}

func decodeThingModel(raw []byte, sch *rules.DeviceSchema) error {
	var model struct {
		Metrics    any `json:"metrics"`
		Properties map[string]struct {
			Type string `json:"type"`
			Kind string `json:"kind"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &model); err != nil {
		return err
	}
	switch metrics := model.Metrics.(type) {
	case []any:
		for _, item := range metrics {
			if key, ok := item.(string); ok && key != "" {
				sch.Metrics[key] = rules.KindAny
			}
		}
	case map[string]any:
		for key, value := range metrics {
			kind := rules.KindAny
			if spec, ok := value.(map[string]any); ok {
				kind = thingModelKind(fmt.Sprint(spec["type"]), fmt.Sprint(spec["kind"]))
			}
			sch.Metrics[key] = kind
		}
	}
	for key, spec := range model.Properties {
		sch.Metrics[key] = thingModelKind(spec.Type, spec.Kind)
	}
	if len(sch.Metrics) == 0 {
		sch.AllowUnknownMetrics = true
	}
	return nil
}

func thingModelKind(typ, kind string) rules.Kind {
	switch typ {
	case "bool", "boolean":
		return rules.KindBool
	case "string", "text":
		return rules.KindString
	}
	switch kind {
	case "bool", "boolean":
		return rules.KindBool
	case "string", "text":
		return rules.KindString
	}
	return rules.KindNumber
}

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

// List returns the control-plane summary for all rules in one tenant.
// It deliberately does not compile expressions: the rule worker owns runtime
// validation, while the control plane must remain usable when one rule is bad.
func (s *Store) List(ctx context.Context, projectID string) ([]Rule, error) {
	var out []Rule
	err := pg.WithProjectValueTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
SELECT rule_id, rule_name, level, enabled, priority, version
FROM t_alarm_rule
WHERE project_id=$1
ORDER BY priority, rule_id, version DESC`, projectID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var rule Rule
			if err := rows.Scan(&rule.RuleID, &rule.Name, &rule.Level, &rule.Enabled, &rule.Priority, &rule.Version); err != nil {
				return err
			}
			rule.ProjectID = projectID
			out = append(out, rule)
		}
		return rows.Err()
	})
	return out, err
}

// SetEnabled changes only the publication switch and advances the version.
// The update and audit row share one tenant-scoped transaction.
func (s *Store) SetEnabled(ctx context.Context, projectID, ruleID string, enabled bool, actor string) (Rule, error) {
	var rule Rule
	err := pg.WithProjectValueTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
UPDATE t_alarm_rule
SET enabled=$3, version=version+1, updated_at=now()
WHERE project_id=$1 AND rule_id=$2
RETURNING rule_id, rule_name, level, enabled, priority, version`, projectID, ruleID, enabled).
			Scan(&rule.RuleID, &rule.Name, &rule.Level, &rule.Enabled, &rule.Priority, &rule.Version)
		if err != nil {
			return err
		}
		rule.ProjectID = projectID
		_, err = tx.Exec(ctx, `
INSERT INTO t_audit_log(project_id, action, actor_id, resource_type, resource_id, details)
VALUES($1::BIGINT, 'rule.enabled.update', $2, 'alarm_rule', $3, jsonb_build_object('enabled', $4, 'version', $5))`,
			projectID, actor, ruleID, enabled, rule.Version)
		return err
	})
	return rule, err
}
