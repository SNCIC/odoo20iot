package querysvc

import (
	"encoding/json"
	"net/http"
	"time"
)

// Code 是稳定的机器可读错误码（前端/客户端据此分支，不解析 message）。
type Code string

const (
	CodeInvalidArgument  Code = "INVALID_ARGUMENT"   // 400 参数非法
	CodeUnauthenticated  Code = "UNAUTHENTICATED"    // 401
	CodeForbidden        Code = "FORBIDDEN"          // 403 设备不属于本租户
	CodeNotFound         Code = "NOT_FOUND"          // 404（预留）
	CodeUnprocessable    Code = "UNPROCESSABLE"      // 422 违反查询保护规则
	CodeTenantNotAllowed Code = "TENANT_NOT_ALLOWED" // 400 请求里出现了 project_id
	CodeQueryBusy        Code = "QUERY_BUSY"         // 503 每租户排队已满/超时
	CodeRateLimited      Code = "RATE_LIMITED"       // 429 租户速率超限
	CodeQueryTimeout     Code = "QUERY_TIMEOUT"      // 504 查询超时
	CodeUpstream         Code = "UPSTREAM_UNAVAILABLE"
	CodeInternal         Code = "INTERNAL"
)

// apiError 是所有错误响应的统一形态。
//
// 刻意**不含** SQL / DSN / 堆栈 —— 那些进日志，不进响应体。
type apiError struct {
	OK      bool   `json:"ok"`
	Code    Code   `json:"code"`
	Message string `json:"message"`
	// Rule 是 tsdb.PolicyError 的规则名，便于客户端区分「怎么改参数」。
	Rule string `json:"rule,omitempty"`
	// ExportRequired 提示「该走异步导出了」（目前只有回溯超 90d 会置位）。
	ExportRequired bool `json:"export_required,omitempty"`
}

// pointDTO 是逐点明细的一行。
type pointDTO struct {
	TS       time.Time `json:"ts"`
	DeviceID int64     `json:"device_id"`
	// Value 为 null 表示设备本次未上报该指标（不是 0）。
	Value *float64 `json:"value"`
}

// bucketDTO 是分桶聚合的一行。
type bucketDTO struct {
	Bucket   time.Time `json:"bucket"`
	DeviceID int64     `json:"device_id"`
	Avg      *float64  `json:"avg"`
	Max      *float64  `json:"max"`
	Count    int64     `json:"count"`
}

// seriesResponse 是 /api/v1/series 的响应体。
//
// Granularity/Source/Bucket/CapHit 是**必须回传的元数据**：调用方要知道
// 「我拿到的是原始点还是聚合值、来自哪张表、桶多宽、是不是被行数上限降采样了」。
type seriesResponse struct {
	OK          bool        `json:"ok"`
	Granularity string      `json:"granularity"`
	Source      string      `json:"source"`
	Bucket      string      `json:"bucket,omitempty"`
	CapHit      bool        `json:"cap_hit"`
	Points      []pointDTO  `json:"points,omitempty"`
	Buckets     []bucketDTO `json:"buckets,omitempty"`
}

type latestDTO struct {
	DeviceID  int64          `json:"device_id"`
	Available bool           `json:"available"`
	TS        *time.Time     `json:"ts,omitempty"`
	Values    map[string]any `json:"values,omitempty"`
}

type latestResponse struct {
	OK     bool        `json:"ok"`
	Latest []latestDTO `json:"latest"`
}

// deviceDTO 是设备列表的一行。**永不包含 secret_hash**。
type deviceDTO struct {
	ID           int64          `json:"id"`
	DeviceKey    string         `json:"device_key"`
	Name         string         `json:"name"`
	DeviceTypeID int64          `json:"device_type_id"`
	Status       string         `json:"status"`
	AuthMode     string         `json:"auth_mode"`
	Online       bool           `json:"online"`
	LastSeenAt   *time.Time     `json:"last_seen_at,omitempty"`
	Tags         map[string]any `json:"tags,omitempty"`
	Version      int64          `json:"version"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type devicesResponse struct {
	OK         bool        `json:"ok"`
	Devices    []deviceDTO `json:"devices"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code Code, msg string) {
	writeJSON(w, status, apiError{OK: false, Code: code, Message: msg})
}
