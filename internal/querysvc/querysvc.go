package querysvc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/SNCIC/odoo20iot/internal/apiauth"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/command"
	"github.com/SNCIC/odoo20iot/internal/latest"
	"github.com/SNCIC/odoo20iot/internal/notifyconfig"
	"github.com/SNCIC/odoo20iot/internal/ota"
	"github.com/SNCIC/odoo20iot/internal/quota"
	"github.com/SNCIC/odoo20iot/internal/shadow"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// SeriesReader 是查询服务依赖的**时序读接口**。
//
// 生产实现是 `*greptimedb.Store`（编译期断言写在 cmd/svc-query，避免本包依赖方言包）。
// 抽成接口的直接收益：handler 单测可以用假实现覆盖全部状态码，不必连真实 GreptimeDB。
type SeriesReader interface {
	QuerySeries(ctx context.Context, p tsdb.Plan, q tsdb.SeriesQuery) (tsdb.SeriesResult, error)
}

type LatestReader interface {
	Get(ctx context.Context, projectID, deviceID int64) (latest.Snapshot, error)
}

type NotificationEndpointStore interface {
	List(context.Context, int64) ([]notifyconfig.Endpoint, error)
	Create(context.Context, int64, string, string, string) (notifyconfig.Endpoint, error)
	Delete(context.Context, int64, int64) error
}

type AlarmAcknowledger interface {
	Acknowledge(context.Context, int64, string, string, time.Time) error
}

type QuotaPolicyStore interface {
	Policies(context.Context, int64) ([]quota.Policy, error)
	SetPolicy(context.Context, quota.Policy, string) error
	DeletePolicy(context.Context, int64, string, string) error
}

type Meter interface {
	Add(projectID int64, metric string, delta int64)
}

type CommandIssuer interface {
	Issue(context.Context, command.Record) (command.Record, error)
	Get(context.Context, int64, string) (command.Record, error)
}

type ShadowService interface {
	Get(context.Context, int64, string) (shadow.Snapshot, error)
	UpdateDesired(context.Context, int64, string, map[string]any, *int64) (shadow.Snapshot, error)
}

type OTAStore interface {
	ListFirmwares(context.Context, int64) ([]ota.Firmware, error)
	RegisterFirmware(context.Context, ota.Firmware, string) (ota.Firmware, error)
	GetFirmware(context.Context, int64, int64) (ota.Firmware, error)
	CreateTask(context.Context, int64, int64, []string, ota.Rollout, time.Duration, string) (ota.Task, error)
	GetTask(context.Context, int64, string) (ota.Task, error)
	ListTaskDevices(context.Context, int64, string) ([]ota.TaskDevice, error)
}

type OTAArtifactStore interface {
	PutContext(context.Context, int64, string, io.Reader) (ota.Artifact, error)
	Open(int64, string) (*os.File, error)
}

type OTASigner interface {
	SignManifest(ota.Manifest, time.Time, time.Duration) (ota.Manifest, error)
}

// Health 提供就绪探测所需的三类探针。任一为 nil 时该项跳过（测试便利）。
type Health struct {
	PingPG            func(context.Context) error
	PingTSDB          func(context.Context) error
	PendingMigrations func(context.Context) ([]string, error)
}

// Config 是查询服务的可调参数。
type Config struct {
	// Plan 是要读的原始表模型（默认 tsdb.PlanJSON）。
	Plan tsdb.Plan
	// DefaultDeviceLimit / MaxDeviceLimit 是设备列表的页大小。
	DefaultDeviceLimit int
	MaxDeviceLimit     int
	// QueryTimeout 是单次时序查询的上限。
	QueryTimeout time.Duration
	// SlowQueryThreshold 超过即记指标 + WARN（02 §4.3 默认 3s）。
	SlowQueryThreshold time.Duration
	// Limiter 是每租户并发/排队参数。
	Limiter LimiterConfig
}

// DefaultConfig 给出与服务文档一致的默认值。
func DefaultConfig() Config {
	return Config{
		Plan:               tsdb.PlanJSON,
		DefaultDeviceLimit: catalog.DefaultListLimit,
		MaxDeviceLimit:     catalog.MaxListLimit,
		QueryTimeout:       10 * time.Second,
		SlowQueryThreshold: 3 * time.Second,
		Limiter:            DefaultLimiterConfig(),
	}
}

// Deps 是服务依赖。
type Deps struct {
	Reader             SeriesReader
	Latest             LatestReader
	Endpoints          NotificationEndpointStore
	Alarms             AlarmAcknowledger
	Quota              QuotaPolicyStore
	Commands           CommandIssuer
	Shadows            ShadowService
	OTA                OTAStore
	OTAArtifact        OTAArtifactStore
	OTASigner          OTASigner
	OTADownloadSecret  string
	OTADownloadBaseURL string
	Meter              Meter
	Catalog            catalog.Store
	Verifier           apiauth.Verifier
	Health             Health
	Metrics            *Metrics
	AuthMetrics        *apiauth.Metrics
	Logger             *slog.Logger
	Now                func() time.Time
}

// Service 是查询服务的 HTTP 处理器集合。
type Service struct {
	cfg     Config
	deps    Deps
	limiter *Limiter
	mux     http.Handler
}

// New 构造服务（补齐默认值并做必填校验）。
func New(cfg Config, deps Deps) (*Service, error) {
	if deps.Reader == nil {
		return nil, fmt.Errorf("querysvc: 需要 SeriesReader")
	}
	if deps.Catalog == nil {
		return nil, fmt.Errorf("querysvc: 需要 catalog.Store")
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Metrics == nil {
		deps.Metrics = new(Metrics)
	}

	def := DefaultConfig()
	if cfg.Plan == "" {
		cfg.Plan = def.Plan
	}
	if cfg.DefaultDeviceLimit <= 0 {
		cfg.DefaultDeviceLimit = def.DefaultDeviceLimit
	}
	if cfg.MaxDeviceLimit <= 0 {
		cfg.MaxDeviceLimit = def.MaxDeviceLimit
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = def.QueryTimeout
	}
	if cfg.SlowQueryThreshold <= 0 {
		cfg.SlowQueryThreshold = def.SlowQueryThreshold
	}

	s := &Service{cfg: cfg, deps: deps}
	s.limiter = NewLimiter(cfg.Limiter, deps.Metrics, deps.Now)
	s.mux = s.routes()
	return s, nil
}

// Handler 返回装配好的路由。
func (s *Service) Handler() http.Handler { return s.mux }
