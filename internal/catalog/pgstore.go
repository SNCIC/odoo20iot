package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore 是控制面主数据的 PostgreSQL 实现。
//
// 读侧实现 catalog.Store；写侧方法（Upsert*）供 cmd/iot-seed 与未来的 Odoo 同步使用，
// 读服务（svc-query）只依赖 Store 接口，拿不到写侧。
type PGStore struct{ pool *pgxpool.Pool }

var _ Store = (*PGStore)(nil)

// NewPGStore 构造存储。
func NewPGStore(pool *pgxpool.Pool) (*PGStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("catalog: PGStore 需要非空连接池")
	}
	return &PGStore{pool: pool}, nil
}

// deviceColumns 是设备读取列（顺序与 scanDevice 严格一致）。
const deviceColumns = `id, project_id, device_type_id, device_key, name, auth_mode, status, online,
       last_seen_at, thing_model_version, config_template_id, tags, version, created_at, updated_at, deleted_at`

// rowScanner 让 scanDevice 同时接受 pgx.Row 与 pgx.Rows。
type rowScanner interface{ Scan(dest ...any) error }

// ListDevices 按过滤条件列设备（keyset 分页，按 id 升序）。
func (s *PGStore) ListDevices(ctx context.Context, f DeviceFilter) (DevicePage, error) {
	nf, err := f.Normalize()
	if err != nil {
		return DevicePage{}, err
	}

	args := []any{nf.ProjectID}
	next := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	where := "project_id = $1 AND deleted_at IS NULL"
	if nf.DeviceTypeID > 0 {
		where += " AND device_type_id = " + next(nf.DeviceTypeID)
	}
	if nf.Status != "" {
		where += " AND status = " + next(nf.Status)
	}
	if nf.Query != "" {
		like := "%" + nf.Query + "%"
		where += fmt.Sprintf(" AND (name ILIKE %s OR device_key ILIKE %s)", next(like), next(like))
	}
	if nf.AfterID > 0 {
		where += " AND id > " + next(nf.AfterID)
	}
	// 多取一行用于判断是否还有下一页。
	limit := next(nf.Limit + 1)

	sql := fmt.Sprintf("SELECT %s FROM t_device WHERE %s ORDER BY id LIMIT %s", deviceColumns, where, limit)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return DevicePage{}, fmt.Errorf("catalog: 列设备: %w", err)
	}
	defer rows.Close()

	var out []Device
	for rows.Next() {
		d, err := scanDevice(rows)
		if err != nil {
			return DevicePage{}, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return DevicePage{}, fmt.Errorf("catalog: 迭代设备行: %w", err)
	}

	page := DevicePage{}
	if len(out) > nf.Limit {
		page.NextAfterID = out[nf.Limit-1].ID
		out = out[:nf.Limit]
	}
	page.Devices = out
	return page, nil
}

// DeviceIDsOwned 返回 ids 中属于 projectID 且未软删的子集。
func (s *PGStore) DeviceIDsOwned(ctx context.Context, projectID int64, ids []int64) (map[int64]bool, error) {
	out := map[int64]bool{}
	if projectID <= 0 {
		return out, fmt.Errorf("catalog: 必须提供正的 project_id")
	}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM t_device WHERE project_id = $1 AND deleted_at IS NULL AND id = ANY($2)`,
		projectID, ids)
	if err != nil {
		return nil, fmt.Errorf("catalog: 校验设备归属: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("catalog: 扫描设备 id: %w", err)
		}
		out[id] = true
	}
	return out, rows.Err()
}

// DeviceKeysExisting 返回 keys 中**已存在**（含软删）的 device_key 子集。
//
// 供 cmd/iot-seed 判断是否需要为新设备生成凭据：Argon2 单次上百毫秒，
// 幂等重跑时不该对已存在的设备白算一遍。
func (s *PGStore) DeviceKeysExisting(ctx context.Context, projectID int64, keys []string) (map[string]bool, error) {
	out := map[string]bool{}
	if projectID <= 0 {
		return out, fmt.Errorf("catalog: 必须提供正的 project_id")
	}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT device_key FROM t_device WHERE project_id = $1 AND device_key = ANY($2)`, projectID, keys)
	if err != nil {
		return nil, fmt.Errorf("catalog: 查询已存在设备键: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("catalog: 扫描 device_key: %w", err)
		}
		out[k] = true
	}
	return out, rows.Err()
}

const upsertProjectSQL = `
INSERT INTO t_project (id, project_key, name, status, odoo_company_id, timezone)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE SET
    project_key     = EXCLUDED.project_key,
    name            = EXCLUDED.name,
    status          = EXCLUDED.status,
    odoo_company_id = EXCLUDED.odoo_company_id,
    timezone        = EXCLUDED.timezone,
    updated_at      = now(),
    version         = t_project.version + 1
RETURNING id`

// UpsertProject 幂等写入租户（要求 p.ID > 0，便于种子与测试引用确定 id）。
func (s *PGStore) UpsertProject(ctx context.Context, p Project) (int64, error) {
	if p.ID <= 0 {
		return 0, fmt.Errorf("catalog: UpsertProject 需要正的 id")
	}
	if p.ProjectKey == "" || p.Name == "" {
		return 0, fmt.Errorf("catalog: project_key 与 name 必填")
	}
	if p.Status == "" {
		p.Status = "active"
	}
	if p.Timezone == "" {
		p.Timezone = "UTC"
	}
	var id int64
	if err := s.pool.QueryRow(ctx, upsertProjectSQL,
		p.ID, p.ProjectKey, p.Name, p.Status, p.OdooCompanyID, p.Timezone).Scan(&id); err != nil {
		return 0, fmt.Errorf("catalog: upsert 租户: %w", err)
	}
	return id, nil
}

const upsertDeviceTypeSQL = `
INSERT INTO t_device_type (id, project_id, type_key, name, category, thing_model, thing_model_version)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
ON CONFLICT (id) DO UPDATE SET
    type_key            = EXCLUDED.type_key,
    name                = EXCLUDED.name,
    category            = EXCLUDED.category,
    thing_model         = EXCLUDED.thing_model,
    thing_model_version = EXCLUDED.thing_model_version,
    updated_at          = now(),
    version             = t_device_type.version + 1
RETURNING id`

// UpsertDeviceType 幂等写入设备类型。
func (s *PGStore) UpsertDeviceType(ctx context.Context, t DeviceType) (int64, error) {
	if t.ID <= 0 {
		return 0, fmt.Errorf("catalog: UpsertDeviceType 需要正的 id")
	}
	if t.ProjectID <= 0 || t.TypeKey == "" || t.Name == "" {
		return 0, fmt.Errorf("catalog: project_id / type_key / name 必填")
	}
	model := t.ThingModel
	if len(model) == 0 {
		model = json.RawMessage(`{}`)
	}
	if t.ThingModelVersion <= 0 {
		t.ThingModelVersion = 1
	}
	var id int64
	if err := s.pool.QueryRow(ctx, upsertDeviceTypeSQL,
		t.ID, t.ProjectID, t.TypeKey, t.Name, t.Category, string(model), t.ThingModelVersion).Scan(&id); err != nil {
		return 0, fmt.Errorf("catalog: upsert 设备类型: %w", err)
	}
	return id, nil
}

const upsertDeviceSQL = `
INSERT INTO t_device (id, project_id, device_type_id, device_key, name,
                      secret_hash, secret_version, auth_mode, status, thing_model_version, tags)
VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, $9, $10::jsonb)
ON CONFLICT (project_id, device_key) DO UPDATE SET
    device_type_id      = EXCLUDED.device_type_id,
    name                = EXCLUDED.name,
    auth_mode           = EXCLUDED.auth_mode,
    status              = EXCLUDED.status,
    thing_model_version = EXCLUDED.thing_model_version,
    tags                = EXCLUDED.tags,
    -- 默认**保留**已有凭据；只有显式 RotateSecret 才覆盖并递增版本。
    secret_hash    = CASE WHEN $11 THEN EXCLUDED.secret_hash ELSE t_device.secret_hash END,
    secret_version = CASE WHEN $11 THEN t_device.secret_version + 1 ELSE t_device.secret_version END,
    updated_at     = now(),
    version        = t_device.version + 1
RETURNING id`

// UpsertDevice 幂等写入设备。冲突键是 (project_id, device_key)。
func (s *PGStore) UpsertDevice(ctx context.Context, d DeviceUpsert) (int64, error) {
	if d.ProjectID <= 0 || d.DeviceTypeID <= 0 || d.DeviceKey == "" || d.Name == "" {
		return 0, fmt.Errorf("catalog: project_id / device_type_id / device_key / name 必填")
	}
	if d.AuthMode == "" {
		d.AuthMode = "per_device"
	}
	if d.Status == "" {
		d.Status = "inactive"
	}
	if d.ThingModelVersion <= 0 {
		d.ThingModelVersion = 1
	}
	tags := d.Tags
	if tags == nil {
		tags = map[string]any{}
	}
	tagJSON, err := json.Marshal(tags)
	if err != nil {
		return 0, fmt.Errorf("catalog: 序列化 tags: %w", err)
	}

	var id int64
	if err := s.pool.QueryRow(ctx, upsertDeviceSQL,
		d.ID, d.ProjectID, d.DeviceTypeID, d.DeviceKey, d.Name,
		d.SecretHash, d.AuthMode, d.Status, d.ThingModelVersion, string(tagJSON), d.RotateSecret).Scan(&id); err != nil {
		return 0, fmt.Errorf("catalog: upsert 设备: %w", err)
	}
	return id, nil
}

func scanDevice(sc rowScanner) (Device, error) {
	var (
		d        Device
		lastSeen *time.Time
		cfgTmpl  *int64
		tags     []byte
		deleted  *time.Time
	)
	if err := sc.Scan(
		&d.ID, &d.ProjectID, &d.DeviceTypeID, &d.DeviceKey, &d.Name, &d.AuthMode, &d.Status,
		&d.Online, &lastSeen, &d.ThingModelVersion, &cfgTmpl, &tags, &d.Version,
		&d.CreatedAt, &d.UpdatedAt, &deleted,
	); err != nil {
		return Device{}, fmt.Errorf("catalog: 扫描设备行: %w", err)
	}
	d.LastSeenAt = lastSeen
	d.ConfigTemplateID = cfgTmpl
	d.DeletedAt = deleted
	if len(tags) > 0 {
		if err := json.Unmarshal(tags, &d.Tags); err != nil {
			return Device{}, fmt.Errorf("catalog: 解析设备 tags: %w", err)
		}
	}
	return d, nil
}
