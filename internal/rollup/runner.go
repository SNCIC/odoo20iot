package rollup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// Materializer 是预聚合物化的执行体。生产实现是 internal/tsdb/greptimedb.Store；
// 抽成接口是为了让调度逻辑（窗口推进、失败不推进）能脱离真实库单测。
type Materializer interface {
	EnsureRollupTable(ctx context.Context, r tsdb.Rollup) error
	// RollupWindow 物化半开窗口 [start, end)，返回执行的 INSERT 条数。
	RollupWindow(ctx context.Context, r tsdb.Rollup, start, end time.Time) (int, error)
}

// Config 是 Runner 的可调参数。
type Config struct {
	// Lag 是抗迟到边界；0 表示取 2×width。
	Lag time.Duration
	// InitialLookback 是首次运行的回看范围；0 表示 24h。
	InitialLookback time.Duration
	// BackfillSince 非零时覆盖首次水位（显式回填）。
	BackfillSince time.Time
	// MaxWindows 是每轮每粒度处理的窗口数上限；0 表示 60。
	MaxWindows int
	// Now 注入时钟（测试用）；nil 表示 time.Now。
	Now func() time.Time
}

// Result 是一轮物化的结果（供日志与 /metrics）。
type Result struct {
	Granularity string
	Windows     int
	Inserts     int
	Watermark   time.Time
}

// Runner 把「读水位 → 找窗口 → 物化 → 推进水位」串起来。
type Runner struct {
	store  Store
	mat    Materializer
	cfg    Config
	logger *slog.Logger
}

// NewRunner 构造 Runner。
func NewRunner(store Store, mat Materializer, cfg Config, logger *slog.Logger) (*Runner, error) {
	if store == nil {
		return nil, fmt.Errorf("rollup: Runner 需要非空 Store")
	}
	if mat == nil {
		return nil, fmt.Errorf("rollup: Runner 需要非空 Materializer")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxWindows <= 0 {
		cfg.MaxWindows = 60
	}
	if cfg.InitialLookback <= 0 {
		cfg.InitialLookback = 24 * time.Hour
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{store: store, mat: mat, cfg: cfg, logger: logger}, nil
}

// RunOnce 处理一个粒度上所有已闭合的窗口。
//
// 失败即返回（**不半推进**）：某个窗口写失败时水位停在它之前，下一轮从同一位置重试。
// 「写成功但 Advance 失败」会让该窗口下轮重放 —— 因为预聚合表是覆盖写，重放安全（见包注释）。
func (r *Runner) RunOnce(ctx context.Context, g tsdb.Rollup) (Result, error) {
	width, err := tsdb.RollupWidth(g)
	if err != nil {
		return Result{}, err
	}
	lag := r.cfg.Lag
	if lag <= 0 {
		lag = 2 * width
	}

	if err := r.mat.EnsureRollupTable(ctx, g); err != nil {
		r.recordError(ctx, g, err)
		return Result{}, err
	}

	now := r.cfg.Now()
	wm, found, err := r.store.Get(ctx, string(g))
	if err != nil {
		return Result{}, err
	}

	var start time.Time
	switch {
	case found:
		start = wm.WindowStart
	case !r.cfg.BackfillSince.IsZero():
		start = r.cfg.BackfillSince.UTC()
	default:
		start = InitialWatermark(now, width, lag, r.cfg.InitialLookback)
	}
	start = AlignFloor(start, width)

	closedEnd := ClosedEnd(now, width, lag)
	windows := WindowsToProcess(start, closedEnd, width, r.cfg.MaxWindows)

	res := Result{Granularity: string(g), Watermark: start}
	for _, w := range windows {
		inserts, err := r.mat.RollupWindow(ctx, g, w.Start, w.End)
		if err != nil {
			r.recordError(ctx, g, err)
			return res, err
		}
		if err := r.store.Advance(ctx, string(g), w.End); err != nil {
			// 写入已成功但水位没推进：下轮会重放同一窗口，覆盖写保证不重复。
			return res, err
		}
		res.Windows++
		res.Inserts += inserts
		res.Watermark = w.End
	}
	return res, nil
}

func (r *Runner) recordError(ctx context.Context, g tsdb.Rollup, err error) {
	r.logger.Error("预聚合物化失败", "granularity", string(g), "error", err)
	// 失败常发生在 ctx 已取消时（进程退出），仍要尝试留痕，故不继承取消。
	_ = r.store.RecordError(context.WithoutCancel(ctx), string(g), err.Error())
}
