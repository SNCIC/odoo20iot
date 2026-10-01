// Command svc-rollup 把原始时序**增量物化**成预聚合表（B1 补充项（2）· 02 §7）。
//
// 职责边界：
//   - 只做「按窗口把 telemetry 聚合进 telemetry_1m / telemetry_1h」这一件事；
//   - 水位（已物化到哪）落在 PG 的 `t_rollup_watermark`，不在内存、也不在 Redis
//     （Redis 是加速层不是事实层，02 §5.3）；
//   - 查询侧的跨度路由在 internal/tsdb/greptimedb（见 QuerySeries），本服务不参与。
//
// 幂等性：预聚合表刻意**不设 append_mode**，同 (主键, ts) 重写即覆盖
// （探针实测 greptimedb/capability_test.go · P-b）；配合「写入成功后才推进水位」，
// 「写成功但推进失败」的下轮重放是安全的。
//
// 未实现（如实留白）：
//   - 迟到数据回修/重算：窗口在 `lag` 之后不再修正（见 02 §7 边界）；
//   - 更早历史的回填需显式 `-backfill-since`（首次水位只回看 `-initial-lookback`）。
//
// 用法（devbox 内）：
//
//	go run ./cmd/svc-rollup -pg-dsn "$IOT_PG_DSN" -dsn "postgres://greptime:greptime@100.64.0.3:28403/public"
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/rollup"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
	"github.com/SNCIC/odoo20iot/internal/tsdb/greptimedb"
)

// greptimeDSN 是 GreptimeDB 的 PG wire 地址（与 tsdb-bench / svc-pipeline 一致）。
const greptimeDSN = "postgres://greptime:greptime@100.64.0.3:28403/public"

type config struct {
	dsn             string
	pgDSN           string
	httpAddr        string
	interval        time.Duration
	runTimeout      time.Duration
	lag             time.Duration
	granularities   string
	initialLookback time.Duration
	backfillSince   time.Time
	maxWindows      int
	readyProbe      time.Duration
	logFormat       string
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nsvc-rollup 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.dsn, "dsn", greptimeDSN, "GreptimeDB PG wire DSN")
	flag.StringVar(&cfg.pgDSN, "pg-dsn", pg.DefaultDSN, "业务库 DSN（水位账本 t_rollup_watermark）")
	flag.StringVar(&cfg.httpAddr, "http-addr", "127.0.0.1:18093", "健康检查与指标监听地址")
	flag.DurationVar(&cfg.interval, "interval", 30*time.Second, "调度周期")
	flag.DurationVar(&cfg.runTimeout, "run-timeout", 2*time.Minute, "单轮物化超时")
	flag.DurationVar(&cfg.lag, "lag", 0, "抗迟到边界；0 表示按粒度取 2×窗口宽度")
	flag.StringVar(&cfg.granularities, "granularities", "1m,1h", "要物化的粒度，逗号分隔（1m / 1h）")
	flag.DurationVar(&cfg.initialLookback, "initial-lookback", 24*time.Hour, "首次运行的回看范围（更早历史需 -backfill-since）")
	var backfill string
	flag.StringVar(&backfill, "backfill-since", "", "覆盖首次水位的 RFC3339 时刻（显式回填）")
	flag.IntVar(&cfg.maxWindows, "max-windows", 60, "每轮每粒度处理的窗口数上限")
	flag.DurationVar(&cfg.readyProbe, "ready-probe", 3*time.Second, "就绪探测超时")
	// ⚠️ 用字符串开关而不是 `-log-json` 布尔开关：布尔开关被写成 `-log-json false` 时
	// Go 的 flag 会把 false 当位置参数并停止解析，其后参数被静默丢弃（09 §6 踩过）。
	flag.StringVar(&cfg.logFormat, "log-format", "json", "日志格式：json 或 text")
	flag.Parse()

	if strings.TrimSpace(backfill) != "" {
		ts, err := time.Parse(time.RFC3339, backfill)
		if err != nil {
			fmt.Fprintf(os.Stderr, "非法 -backfill-since %q: %v\n", backfill, err)
			os.Exit(2)
		}
		cfg.backfillSince = ts
	}
	return cfg
}

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if strings.EqualFold(format, "text") {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

// counters 是服务级计数（/metrics 用）。
type counters struct {
	Ticks     atomic.Int64
	NotLeader atomic.Int64
	Windows   atomic.Int64
	Inserts   atomic.Int64
	Errors    atomic.Int64
}

// app 把调度所需依赖收在一起。
type app struct {
	pool    *pgxpool.Pool
	gres    *greptimedb.Store
	store   rollup.Store
	runner  *rollup.Runner
	grans   []tsdb.Rollup
	lockKey int64
	cfg     config
	counts  *counters
	logger  *slog.Logger
}

func run(cfg config) error {
	logger := newLogger(cfg.logFormat)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	grans, err := parseGranularities(cfg.granularities)
	if err != nil {
		return err
	}

	pool, err := pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
	if err != nil {
		return err
	}
	defer pool.Close()

	// 迁移是部署动作（cmd/iot-migrate）；但启动必须检查 ——
	// 否则水位表不存在，每一轮物化都会失败。
	if pending, err := pendingMigrations(ctx, pool); err != nil {
		return fmt.Errorf("检查迁移状态: %w", err)
	} else if len(pending) > 0 {
		return fmt.Errorf("数据库有 %d 个未应用的迁移 %v：请先执行 cmd/iot-migrate", len(pending), pending)
	}

	gres, err := greptimedb.Open(ctx, cfg.dsn, 4)
	if err != nil {
		return fmt.Errorf("连接 GreptimeDB: %w", err)
	}
	defer gres.Close()

	store, err := rollup.NewPGStore(pool)
	if err != nil {
		return err
	}
	runner, err := rollup.NewRunner(store, gres, rollup.Config{
		Lag:             cfg.lag,
		InitialLookback: cfg.initialLookback,
		BackfillSince:   cfg.backfillSince,
		MaxWindows:      cfg.maxWindows,
		Now:             time.Now,
	}, logger)
	if err != nil {
		return err
	}

	a := &app{
		pool:    pool,
		gres:    gres,
		store:   store,
		runner:  runner,
		grans:   grans,
		lockKey: pg.AdvisoryKey("svc-rollup:materialize"),
		cfg:     cfg,
		counts:  new(counters),
		logger:  logger,
	}

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           routes(a),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("svc-rollup 已就绪",
		"granularities", cfg.granularities,
		"interval", cfg.interval.String(),
		"http_addr", cfg.httpAddr,
		"version", buildinfo.Version, "commit", buildinfo.Commit)

	errCh := make(chan error, 2)
	go func() { errCh <- serveHTTP(srv, logger) }()
	go func() { errCh <- rollupLoop(ctx, a) }()

	select {
	case <-ctx.Done():
		logger.Info("收到退出信号")
	case err := <-errCh:
		if err != nil {
			logger.Error("主循环异常退出", "error", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	logger.Info("已退出",
		"ticks", a.counts.Ticks.Load(),
		"not_leader", a.counts.NotLeader.Load(),
		"windows", a.counts.Windows.Load(),
		"inserts", a.counts.Inserts.Load(),
		"errors", a.counts.Errors.Load())
	return nil
}

func serveHTTP(srv *http.Server, logger *slog.Logger) error {
	logger.Info("HTTP 已启动", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP 服务: %w", err)
	}
	return nil
}

// rollupLoop 是定时调度：立即跑一轮（不等首个 tick），之后按 interval 重复。
func rollupLoop(ctx context.Context, a *app) error {
	t := time.NewTicker(a.cfg.interval)
	defer t.Stop()

	runOnce := func() {
		runCtx, cancel := context.WithTimeout(ctx, a.cfg.runTimeout)
		defer cancel()
		a.tick(runCtx)
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			runOnce()
		}
	}
}

// tick 是一轮调度：单飞取锁 → 逐粒度物化。
//
// 任何失败都**不能退出**：物化失败下一轮原地重试即可，让定时任务把服务停掉毫无收益。
func (a *app) tick(ctx context.Context) {
	a.counts.Ticks.Add(1)

	lock, ok, err := pg.TryLock(ctx, a.pool, a.lockKey)
	if err != nil {
		a.counts.Errors.Add(1)
		a.logger.Error("取调度锁失败", "error", err)
		return
	}
	if !ok {
		// 别的副本正在物化，正常。
		a.counts.NotLeader.Add(1)
		return
	}
	defer func() {
		if err := lock.Unlock(ctx); err != nil {
			a.logger.Warn("释放调度锁失败", "error", err)
		}
	}()

	for _, g := range a.grans {
		res, err := a.runner.RunOnce(ctx, g)
		if err != nil {
			a.counts.Errors.Add(1)
			a.logger.Error("粒度物化失败，下轮原地重试",
				"granularity", string(g), "watermark", res.Watermark.UTC().Format(time.RFC3339), "error", err)
			continue
		}
		a.counts.Windows.Add(int64(res.Windows))
		a.counts.Inserts.Add(int64(res.Inserts))
		if res.Windows > 0 {
			a.logger.Info("完成物化",
				"granularity", string(g), "windows", res.Windows, "inserts", res.Inserts,
				"watermark", res.Watermark.UTC().Format(time.RFC3339))
		}
	}
}

// parseGranularities 解析逗号分隔的粒度列表。
func parseGranularities(s string) ([]tsdb.Rollup, error) {
	var out []tsdb.Rollup
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		g, err := tsdb.ParseRollup(part)
		if err != nil {
			return nil, fmt.Errorf("非法粒度 %q：可选 1m / 1h", part)
		}
		out = append(out, g)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-granularities 不能为空")
	}
	return out, nil
}

// pendingMigrations 返回尚未应用的迁移版本。
func pendingMigrations(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	migs, err := pg.LoadMigrations()
	if err != nil {
		return nil, err
	}
	applied, err := pg.AppliedVersions(ctx, pool)
	if err != nil {
		return nil, err
	}
	var pending []string
	for _, m := range migs {
		if _, ok := applied[m.Version]; !ok {
			pending = append(pending, m.Version)
		}
	}
	return pending, nil
}
