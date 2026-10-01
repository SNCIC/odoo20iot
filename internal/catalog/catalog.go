// Package catalog 是控制面主数据的读写契约与实现（租户 / 设备类型 / 设备台账）。
//
// 租户隔离由本包强制：**所有查询都接受 projectID 作为首参**，且不提供
// 「按资源 id 查、不带租户」的入口（05 §3.3：统一鉴权中间件要求先校验 tenant，
// 禁止只按资源 ID 查询）。
//
// ⚠️ 本批**未启用 PG RLS**（理由见 `internal/pg/migrations/0006_*.sql` 的注释），
// 因此隔离完全落在应用层 —— 这是遗留项，不是「已完成」。
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// DefaultListLimit 是未指定 limit 时的页大小。
	DefaultListLimit = 100
	// MaxListLimit 是单页上限。
	MaxListLimit = 1000
)

// ErrNotFound 表示实体不存在。**不存在与不属于本租户刻意不区分** ——
// 区分会泄露「这个 id 存在但不是你的」，属于跨租户信息泄露。
var ErrNotFound = errors.New("catalog: 未找到")

// Project 是租户（project_id 即 tenant 维度）。
type Project struct {
	ID            int64
	ProjectKey    string
	Name          string
	Status        string
	OdooCompanyID int64
	Timezone      string
	Version       int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	DeletedAt     *time.Time
}

// DeviceType 是设备类型；物模型内联在 JSONB 上（不建 t_thing_model 表，见 0006 注释）。
type DeviceType struct {
	ID                int64
	ProjectID         int64
	TypeKey           string
	Name              string
	Category          string
	ThingModel        json.RawMessage
	ThingModelVersion int64
	Version           int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
	DeletedAt         *time.Time
}

// Device 是设备台账（**不含** secret_hash —— 读侧永不外泄凭据）。
type Device struct {
	ID                int64
	ProjectID         int64
	DeviceTypeID      int64
	DeviceKey         string
	Name              string
	AuthMode          string
	Status            string
	Online            bool
	LastSeenAt        *time.Time
	ThingModelVersion int64
	ConfigTemplateID  *int64
	Tags              map[string]any
	Version           int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
	DeletedAt         *time.Time
}

// DeviceUpsert 是写入（种子 / 未来 Odoo 同步）用的设备载体，含凭据摘要。
type DeviceUpsert struct {
	ID                int64 // 0 = 用序列
	ProjectID         int64
	DeviceTypeID      int64
	DeviceKey         string
	Name              string
	AuthMode          string
	Status            string
	SecretHash        string // catalog.EncodeSecretHash(...)
	ThingModelVersion int64
	Tags              map[string]any
	// RotateSecret 为真时才覆盖已存在设备的 secret_hash 并递增 secret_version；
	// 为假时保留原凭据（种子的默认幂等行为）。
	RotateSecret bool
}

// DeviceFilter 是设备列表过滤条件。ProjectID 必须 > 0。
type DeviceFilter struct {
	ProjectID    int64
	DeviceTypeID int64  // 0 = 不限
	Status       string // "" = 不限
	Query        string // name / device_key 子串，可选
	AfterID      int64  // keyset 游标：上一页最后一行的 id；0 = 首页
	Limit        int    // 0 = DefaultListLimit；超过 MaxListLimit 会被夹到上限
}

// Normalize 夹取分页参数并校验租户。
func (f DeviceFilter) Normalize() (DeviceFilter, error) {
	if f.ProjectID <= 0 {
		return f, fmt.Errorf("catalog: 必须提供正的 project_id")
	}
	if f.Limit <= 0 {
		f.Limit = DefaultListLimit
	}
	if f.Limit > MaxListLimit {
		f.Limit = MaxListLimit
	}
	f.Query = strings.TrimSpace(f.Query)
	f.Status = strings.TrimSpace(f.Status)
	return f, nil
}

// DevicePage 是一页设备；NextAfterID > 0 表示还有下一页。
type DevicePage struct {
	Devices     []Device
	NextAfterID int64
}

// Store 是设备台账的**读**契约。写侧只在 PGStore 上暴露（读服务不依赖）。
type Store interface {
	ListDevices(ctx context.Context, f DeviceFilter) (DevicePage, error)
	// DeviceIDsOwned 返回 ids 中属于 projectID 且未软删的子集。
	// 不存在与跨租户都表现为「不在返回值里」。
	DeviceIDsOwned(ctx context.Context, projectID int64, ids []int64) (map[int64]bool, error)
}
