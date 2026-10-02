// Package extref 管理 IoT 与 Odoo 的外部引用映射。
package extref

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/pg"
)

type Ref struct {
	ID             int64
	ProjectID      int64
	OdooCompanyID  *int64
	ExtSystem      string
	ExtModel       string
	ExtID          int64
	ExtVersion     string
	LocalEntity    string
	LocalID        int64
	BindSource     string
	BindConfidence int16
	Status         string
	SyncedAt       *time.Time
	UpdatedAt      time.Time
}

type IntegrationIssue struct {
	ProjectID  int64
	System     string
	Model      string
	ExternalID int64
	Type       string
	Details    map[string]any
}

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Upsert(ctx context.Context, ref Ref) (Ref, error) {
	if ref.ProjectID <= 0 || ref.ExtSystem == "" || ref.ExtModel == "" || ref.ExtID <= 0 || ref.LocalEntity == "" || ref.LocalID <= 0 {
		return Ref{}, fmt.Errorf("extref: 必填字段非法")
	}
	if ref.BindConfidence < 0 || ref.BindConfidence > 100 {
		return Ref{}, fmt.Errorf("extref: bind_confidence 必须在 0..100")
	}
	if ref.Status == "" {
		ref.Status = "active"
	}
	var out Ref
	err := pg.WithProjectTx(ctx, s.pool, ref.ProjectID, func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
INSERT INTO t_external_ref (project_id, odoo_company_id, ext_system, ext_model, ext_id, ext_version,
  local_entity, local_id, bind_source, bind_confidence, status, synced_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now())
ON CONFLICT (project_id, ext_system, ext_model, ext_id) DO UPDATE SET
  odoo_company_id=EXCLUDED.odoo_company_id, ext_version=EXCLUDED.ext_version,
  local_entity=EXCLUDED.local_entity, local_id=EXCLUDED.local_id,
  bind_source=EXCLUDED.bind_source, bind_confidence=EXCLUDED.bind_confidence,
  status=EXCLUDED.status, synced_at=EXCLUDED.synced_at, updated_at=now()
RETURNING id, project_id, odoo_company_id, ext_system, ext_model, ext_id, ext_version,
  local_entity, local_id, bind_source, bind_confidence, status, synced_at, updated_at`,
			ref.ProjectID, ref.OdooCompanyID, ref.ExtSystem, ref.ExtModel, ref.ExtID, ref.ExtVersion,
			ref.LocalEntity, ref.LocalID, ref.BindSource, ref.BindConfidence, ref.Status, ref.SyncedAt)
		return row.Scan(&out.ID, &out.ProjectID, &out.OdooCompanyID, &out.ExtSystem, &out.ExtModel, &out.ExtID,
			&out.ExtVersion, &out.LocalEntity, &out.LocalID, &out.BindSource, &out.BindConfidence, &out.Status, &out.SyncedAt, &out.UpdatedAt)
	})
	if err == nil {
		_ = s.ResolveIssue(ctx, ref.ExtSystem, ref.ExtModel, ref.ExtID, "unbound")
	}
	return out, err
}

func (s *Store) RecordIssue(ctx context.Context, issue IntegrationIssue) error {
	if issue.System == "" || issue.Model == "" || issue.ExternalID <= 0 || issue.Type == "" {
		return fmt.Errorf("extref: 集成问题字段非法")
	}
	details, err := json.Marshal(issue.Details)
	if err != nil {
		return fmt.Errorf("extref: 序列化问题详情: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO t_integration_issue (project_id, ext_system, ext_model, ext_id, issue_type, details)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (ext_system, ext_model, ext_id, issue_type) DO UPDATE SET
 project_id=EXCLUDED.project_id, details=EXCLUDED.details, status='open', last_seen_at=now(), resolved_at=NULL`,
		issue.ProjectID, issue.System, issue.Model, issue.ExternalID, issue.Type, details)
	if err != nil {
		return fmt.Errorf("extref: 记录集成问题: %w", err)
	}
	return nil
}

func (s *Store) ResolveIssue(ctx context.Context, system, model string, externalID int64, issueType string) error {
	_, err := s.pool.Exec(ctx, `UPDATE t_integration_issue SET status='resolved', resolved_at=now(), last_seen_at=now() WHERE ext_system=$1 AND ext_model=$2 AND ext_id=$3 AND issue_type=$4`, system, model, externalID, issueType)
	return err
}

func (s *Store) FindByLocal(ctx context.Context, projectID int64, entity string, localID int64) (Ref, error) {
	var out Ref
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT id, project_id, odoo_company_id, ext_system, ext_model, ext_id, ext_version,
local_entity, local_id, bind_source, bind_confidence, status, synced_at, updated_at
FROM t_external_ref WHERE project_id=$1 AND local_entity=$2 AND local_id=$3 ORDER BY updated_at DESC LIMIT 1`, projectID, entity, localID).
			Scan(&out.ID, &out.ProjectID, &out.OdooCompanyID, &out.ExtSystem, &out.ExtModel, &out.ExtID, &out.ExtVersion,
				&out.LocalEntity, &out.LocalID, &out.BindSource, &out.BindConfidence, &out.Status, &out.SyncedAt, &out.UpdatedAt)
	})
	return out, err
}

func (s *Store) FindByRemote(ctx context.Context, projectID int64, system, model string, extID int64) (Ref, error) {
	var out Ref
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT id, project_id, odoo_company_id, ext_system, ext_model, ext_id, ext_version,
local_entity, local_id, bind_source, bind_confidence, status, synced_at, updated_at
FROM t_external_ref WHERE project_id=$1 AND ext_system=$2 AND ext_model=$3 AND ext_id=$4`, projectID, system, model, extID).
			Scan(&out.ID, &out.ProjectID, &out.OdooCompanyID, &out.ExtSystem, &out.ExtModel, &out.ExtID, &out.ExtVersion,
				&out.LocalEntity, &out.LocalID, &out.BindSource, &out.BindConfidence, &out.Status, &out.SyncedAt, &out.UpdatedAt)
	})
	return out, err
}

func (s *Store) SetStatus(ctx context.Context, projectID int64, system, model string, extID int64, status string) error {
	if status != "active" && status != "conflict" && status != "orphan" {
		return fmt.Errorf("extref: 状态非法: %q", status)
	}
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE t_external_ref SET status=$1, updated_at=now() WHERE project_id=$2 AND ext_system=$3 AND ext_model=$4 AND ext_id=$5`, status, projectID, system, model, extID)
		return err
	})
}
