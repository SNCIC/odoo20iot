package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/extref"
	"github.com/jackc/pgx/v5"
)

const (
	DefaultMasterDataModel = "maintenance.equipment"
	DefaultMasterDataBatch = 200
)

type MasterDataStore interface {
	UpsertDeviceType(ctx context.Context, t catalog.DeviceType) (int64, error)
	UpsertDevice(ctx context.Context, d catalog.DeviceUpsert) (int64, error)
}

type MasterDataOptions struct {
	Caller   caller
	Catalog  MasterDataStore
	Projects catalog.ProjectByOdooCompany
	Refs     *extref.Store
	Batch    int
	Logger   *slog.Logger
	Now      func() time.Time
}

type MasterDataSync struct {
	caller   caller
	catalog  MasterDataStore
	projects catalog.ProjectByOdooCompany
	refs     *extref.Store
	batch    int
	logger   *slog.Logger
	now      func() time.Time
}

type MasterDataResult struct {
	Read     int
	Upserted int
	Skipped  int
	Unbound  int
}

type equipmentRow struct {
	ID         int64          `json:"id"`
	Name       string         `json:"name"`
	SerialNo   string         `json:"serial_no"`
	Model      optionalString `json:"model"`
	WriteDate  string         `json:"write_date"`
	Active     bool           `json:"active"`
	CompanyID  any            `json:"company_id"`
	CategoryID any            `json:"category_id"`
}

type optionalString string

func (s *optionalString) UnmarshalJSON(data []byte) error {
	if string(data) == "null" || string(data) == "false" {
		*s = ""
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("可选字符串: %w", err)
	}
	*s = optionalString(value)
	return nil
}

func NewMasterDataSync(opts MasterDataOptions) (*MasterDataSync, error) {
	if opts.Caller == nil || opts.Catalog == nil || opts.Projects == nil || opts.Refs == nil {
		return nil, fmt.Errorf("connector: 主数据同步缺少 Caller/Catalog/Projects/Refs")
	}
	if opts.Batch <= 0 {
		opts.Batch = DefaultMasterDataBatch
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &MasterDataSync{caller: opts.Caller, catalog: opts.Catalog, projects: opts.Projects, refs: opts.Refs, batch: opts.Batch, logger: opts.Logger, now: opts.Now}, nil
}

// SyncOnce 同步一个 Odoo 模型的一个增量窗口。
// cursor 是上一条已成功写入的 (write_date,id)；只有整条记录完成后才推进返回值。
func (s *MasterDataSync) SyncOnce(ctx context.Context, model string, cursor Watermark) (Watermark, MasterDataResult, error) {
	return s.syncOnce(ctx, model, cursor, 0, 0)
}

func (s *MasterDataSync) SyncOnceForCompany(ctx context.Context, model string, companyID int64, cursor Watermark) (Watermark, MasterDataResult, error) {
	return s.SyncOnceForProjectCompany(ctx, model, 0, companyID, cursor)
}

func (s *MasterDataSync) SyncOnceForProjectCompany(ctx context.Context, model string, projectID, companyID int64, cursor Watermark) (Watermark, MasterDataResult, error) {
	if companyID <= 0 {
		return cursor, MasterDataResult{}, fmt.Errorf("connector: Odoo company_id 必须为正数")
	}
	return s.syncOnce(ctx, model, cursor, projectID, companyID)
}

func (s *MasterDataSync) syncOnce(ctx context.Context, model string, cursor Watermark, projectID, companyID int64) (Watermark, MasterDataResult, error) {
	if strings.TrimSpace(model) == "" {
		model = DefaultMasterDataModel
	}
	var rows []equipmentRow
	wm := cursor.WriteDate.UTC().Format(odooDatetimeLayout)
	domain := []any{}
	if companyID > 0 {
		domain = append(domain, []any{"company_id", "=", companyID})
	}
	if !cursor.WriteDate.IsZero() {
		cursorDomain := []any{"|", "&", []any{"write_date", "=", wm}, []any{"id", ">", cursor.ID}, []any{"write_date", ">", wm}}
		if companyID > 0 {
			domain = []any{"&", []any{"company_id", "=", companyID}}
			domain = append(domain, cursorDomain...)
		} else {
			domain = cursorDomain
		}
	}
	err := s.caller.Call(ctx, Request{ProjectID: projectID, Model: model, Method: "search_read", Params: map[string]any{
		"domain": domain,
		"fields": []string{"id", "name", "serial_no", "model", "write_date", "active", "company_id", "category_id"},
		"order":  "write_date asc, id asc", "limit": s.batch,
	}}, &rows)
	if err != nil {
		return cursor, MasterDataResult{}, fmt.Errorf("主数据查询 %s: %w", model, err)
	}
	res := MasterDataResult{Read: len(rows)}
	next := cursor
	for _, row := range rows {
		if row.ID <= 0 {
			res.Skipped++
			continue
		}
		companyID, err := relationID(row.CompanyID)
		if err != nil {
			return next, res, fmt.Errorf("设备 %d company_id: %w", row.ID, err)
		}
		categoryID, err := relationID(row.CategoryID)
		if err != nil {
			return next, res, fmt.Errorf("设备 %d category_id: %w", row.ID, err)
		}
		if categoryID <= 0 {
			res.Skipped++
			res.Unbound++
			if err := s.refs.RecordIssue(ctx, extref.IntegrationIssue{System: "odoo20tbb", Model: model, ExternalID: row.ID, Type: "unbound", Details: map[string]any{"reason": "missing_category", "company_id": companyID}}); err != nil {
				return next, res, err
			}
			s.logger.Warn("Odoo 设备缺少设备类型映射，跳过", "odoo_id", row.ID, "company_id", companyID)
			continue
		}
		project, err := s.projects.FindProjectByOdooCompany(ctx, companyID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				res.Skipped++
				res.Unbound++
				if issueErr := s.refs.RecordIssue(ctx, extref.IntegrationIssue{System: "odoo20tbb", Model: model, ExternalID: row.ID, Type: "unbound", Details: map[string]any{"reason": "missing_project", "company_id": companyID}}); issueErr != nil {
					return next, res, issueErr
				}
				s.logger.Warn("Odoo 设备无 IoT 租户映射", "company_id", companyID, "odoo_id", row.ID)
			} else {
				return next, res, err
			}
			continue
		}
		if _, err := s.catalog.UpsertDeviceType(ctx, catalog.DeviceType{ID: categoryID, ProjectID: project.ID, TypeKey: "odoo-category:" + strconv.FormatInt(categoryID, 10), Name: "Odoo category " + strconv.FormatInt(categoryID, 10), Category: "odoo"}); err != nil {
			return next, res, fmt.Errorf("写入设备类型 %d: %w", categoryID, err)
		}
		ref, err := s.refs.FindByRemote(ctx, project.ID, "odoo20tbb", "maintenance.equipment", row.ID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return next, res, fmt.Errorf("读取设备外部引用 %d: %w", row.ID, err)
		}
		key := strings.TrimSpace(row.SerialNo)
		if key == "" {
			key = "odoo:" + strconv.FormatInt(row.ID, 10)
		}
		name := strings.TrimSpace(row.Name)
		if name == "" {
			name = key
		}
		status := "inactive"
		if row.Active {
			status = "active"
		}
		localID, err := s.catalog.UpsertDevice(ctx, catalog.DeviceUpsert{ID: ref.LocalID, ProjectID: project.ID, DeviceTypeID: categoryID, DeviceKey: key, Name: name, Status: status, AuthMode: "per_device", Tags: map[string]any{"odoo_model": model, "odoo_id": row.ID, "odoo_model_name": string(row.Model)}})
		if err != nil {
			return next, res, fmt.Errorf("写入 IoT 设备 %d: %w", row.ID, err)
		}
		if _, err := s.refs.Upsert(ctx, extref.Ref{ProjectID: project.ID, OdooCompanyID: &project.OdooCompanyID, ExtSystem: "odoo20tbb", ExtModel: model, ExtID: row.ID, LocalEntity: "device", LocalID: localID, BindSource: "masterdata", BindConfidence: 100, ExtVersion: row.WriteDate, SyncedAt: ptr(s.now())}); err != nil {
			return next, res, fmt.Errorf("写入设备外部引用 %d: %w", row.ID, err)
		}
		res.Upserted++
		if ts, err := parseOdooTime(row.WriteDate); err == nil {
			next = Watermark{WriteDate: ts, ID: row.ID}
		}
	}
	return next, res, nil
}

func relationID(v any) (int64, error) {
	switch x := v.(type) {
	case nil:
		return 0, fmt.Errorf("为空")
	case bool:
		if !x {
			return 0, nil
		}
		return 0, fmt.Errorf("布尔值 true 不支持")
	case float64:
		return int64(x), nil
	case int64:
		return x, nil
	case []any:
		if len(x) == 0 {
			return 0, fmt.Errorf("为空")
		}
		return relationID(x[0])
	case json.Number:
		return x.Int64()
	case string:
		return strconv.ParseInt(x, 10, 64)
	default:
		return 0, fmt.Errorf("格式 %T 不支持", v)
	}
}

func ptr[T any](v T) *T { return &v }
