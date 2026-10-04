// Command svc-query 是 IoT 平台的查询与控制面 API（02 §4.3）。
//
// 职责边界：
//   - 查询：时序走 `greptimedb.Store.QuerySeries`（受保护入口：行数上限 5000、自适应降采样、
//     跨度路由到预聚合表），控制面走 `internal/catalog`；
//   - 鉴权：JWT（05 §3.3，`internal/apiauth`）+ 可选的开发静态令牌；
//   - 保护：每租户并发上限（默认 20）+ 有界排队、慢查询（>3s）指标与日志。
//
// 未实现（如实留白，见 09 §5.1）：
//   - 历史导出（Parquet + 对象存储）没有；
//   - 慢查询**自动降级**到预聚合表只有指标+日志，没有自动切换；
//   - 每租户**速率**限制（只有并发上限）、读写连接池分离未做；
//   - 多指标投影（`MaxProjectedMetrics=4` 只是契约常量，API 是单指标）。
//
// 用法（devbox 内）：
//
//	go run ./cmd/svc-query -pg-dsn "$IOT_PG_DSN" -auth-mode dev -dev-token devtoken -dev-project-id 1
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/apiauth"
	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/cluster"
	"github.com/SNCIC/odoo20iot/internal/command"
	"github.com/SNCIC/odoo20iot/internal/gateway"
	"github.com/SNCIC/odoo20iot/internal/latest"
	"github.com/SNCIC/odoo20iot/internal/metering"
	"github.com/SNCIC/odoo20iot/internal/notifyconfig"
	"github.com/SNCIC/odoo20iot/internal/ota"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/querysvc"
	"github.com/SNCIC/odoo20iot/internal/quota"
	"github.com/SNCIC/odoo20iot/internal/secureconfig"
	"github.com/SNCIC/odoo20iot/internal/shadow"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
	"github.com/SNCIC/odoo20iot/internal/tsdb/greptimedb"
)

// greptimeDSN 与 svc-pipeline / svc-rollup / tsdb-bench 一致。
const greptimeDSN = "postgres://greptime:greptime@100.64.0.3:28403/public"

// 编译期断言：Store 必须满足查询服务的读接口。
var _ querysvc.SeriesReader = (*greptimedb.Store)(nil)

type config struct {
	dsn      string
	pgDSN    string
	httpAddr string
	plan     string

	authMode       string
	jwtIssuers     string
	jwtAudience    string
	jwtJWKSURL     string
	jwtJWKSFile    string
	jwtJWKSTTL     time.Duration
	jwtLeeway      time.Duration
	redisURL       string
	latestRedisURL string
	natsURL        string
	jwtRevocation  string
	devToken       string
	devProjectID   int64
	allowDevLAN    bool

	limitDefaultDevices int
	limitMaxDevices     int
	queryTimeout        time.Duration
	slowQueryThreshold  time.Duration
	maxConcurrency      int
	queueDepth          int
	queueTimeout        time.Duration
	ratePerSecond       float64
	rateBurst           int
	readyProbe          time.Duration
	logFormat           string
	ensureTSDBSchema    bool
	commandOriginID     string
	shadowOriginID      string
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nsvc-query 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.dsn, "dsn", greptimeDSN, "GreptimeDB PG wire DSN")
	flag.StringVar(&cfg.pgDSN, "pg-dsn", pg.DefaultDSN, "业务库 DSN（设备台账 / 租户）")
	flag.StringVar(&cfg.httpAddr, "http-addr", "127.0.0.1:18094", "监听地址（默认仅本机）")
	flag.StringVar(&cfg.plan, "plan", string(tsdb.PlanJSON), "读取的表模型：json 或 wide")

	flag.StringVar(&cfg.authMode, "auth-mode", "jwt", "认证模式：jwt | dev | both")
	flag.StringVar(&cfg.jwtIssuers, "jwt-issuer", "", "允许的签发方（逗号分隔的允许列表）")
	flag.StringVar(&cfg.jwtAudience, "jwt-audience", "iot-api", "必须命中的 aud")
	flag.StringVar(&cfg.jwtJWKSURL, "jwt-jwks-url", "", "JWKS 地址（url 与 file 至少一个）")
	flag.StringVar(&cfg.jwtJWKSFile, "jwt-jwks-file", "", "JWKS 本地文件（离线/测试）")
	flag.DurationVar(&cfg.jwtJWKSTTL, "jwt-jwks-ttl", 10*time.Minute, "JWKS 缓存有效期")
	flag.DurationVar(&cfg.jwtLeeway, "jwt-leeway", 30*time.Second, "时间声明容差")
	flag.StringVar(&cfg.redisURL, "redis-url", "", "Redis 地址（jti 吊销表）")
	flag.StringVar(&cfg.latestRedisURL, "latest-redis-url", "redis://100.64.0.3:28637/0", "最新值 Redis 地址")
	flag.StringVar(&cfg.natsURL, "nats-url", "nats://100.64.0.3:28222", "计量上报 NATS 地址")
	flag.StringVar(&cfg.jwtRevocation, "jwt-revocation", "on", "jti 吊销检查：on | off（off 仅开发，启动打 WARN）")
	flag.StringVar(&cfg.devToken, "dev-token", "", "开发静态令牌（非空即启用；仅开发/PoC）")
	flag.Int64Var(&cfg.devProjectID, "dev-project-id", 0, "开发令牌绑定的 project_id")
	flag.BoolVar(&cfg.allowDevLAN, "allow-dev-lan", false, "允许开发令牌监听非回环地址（仅限隔离开发环境）")

	flag.IntVar(&cfg.limitDefaultDevices, "limit-default-devices", catalog.DefaultListLimit, "设备列表默认页大小")
	flag.IntVar(&cfg.limitMaxDevices, "limit-max-devices", catalog.MaxListLimit, "设备列表页大小上限")
	flag.DurationVar(&cfg.queryTimeout, "query-timeout", 10*time.Second, "单次时序查询超时")
	flag.DurationVar(&cfg.slowQueryThreshold, "slow-query-threshold", 3*time.Second, "慢查询阈值（记指标 + WARN）")
	flag.IntVar(&cfg.maxConcurrency, "max-concurrency", 20, "每租户并发查询上限")
	flag.IntVar(&cfg.queueDepth, "queue-depth", 40, "每租户排队位上限")
	flag.DurationVar(&cfg.queueTimeout, "queue-timeout", 2*time.Second, "排队等待上限")
	flag.Float64Var(&cfg.ratePerSecond, "rate-per-second", 20, "每租户查询速率；0 表示关闭")
	flag.IntVar(&cfg.rateBurst, "rate-burst", 40, "每租户查询突发容量")
	flag.DurationVar(&cfg.readyProbe, "ready-probe", 3*time.Second, "就绪探测超时")
	// ⚠️ 字符串开关而非 -log-json 布尔：布尔被写成 `-log-json false` 时会吞掉后续参数（09 §6）。
	flag.StringVar(&cfg.logFormat, "log-format", "json", "日志格式：json 或 text")
	flag.BoolVar(&cfg.ensureTSDBSchema, "ensure-tsdb-schema", true, "启动时幂等创建遥测与预聚合表")
	flag.StringVar(&cfg.commandOriginID, "command-origin-id", "", "启用命令 API 的路由源 ID")
	flag.StringVar(&cfg.shadowOriginID, "shadow-origin-id", "svc-query-shadow", "设备影子下行路由源 ID")
	flag.Parse()
	return cfg
}

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if strings.EqualFold(format, "text") {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

func run(cfg config) error {
	logger := newLogger(cfg.logFormat)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	plan, err := parsePlan(cfg.plan)
	if err != nil {
		return err
	}
	verifier, err := buildVerifier(ctx, cfg, logger)
	if err != nil {
		return err
	}

	pool, err := pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
	if err != nil {
		return err
	}
	defer pool.Close()

	// 迁移是部署动作；启动时只检查。schema 落后会让设备列表每查必失败。
	if pending, err := pendingMigrations(ctx, pool); err != nil {
		return fmt.Errorf("检查迁移状态: %w", err)
	} else if len(pending) > 0 {
		return fmt.Errorf("数据库有 %d 个未应用的迁移 %v：请先执行 cmd/iot-migrate", len(pending), pending)
	}

	gres, err := greptimedb.Open(ctx, cfg.dsn, 8)
	if err != nil {
		return fmt.Errorf("连接 GreptimeDB: %w", err)
	}
	defer gres.Close()
	if cfg.ensureTSDBSchema {
		if err := gres.CreateTable(ctx, plan); err != nil {
			return fmt.Errorf("初始化 GreptimeDB 遥测表: %w", err)
		}
		if err := gres.EnsureRollupTable(ctx, tsdb.Rollup1m); err != nil {
			return fmt.Errorf("初始化 GreptimeDB 1m 预聚合表: %w", err)
		}
		if err := gres.EnsureRollupTable(ctx, tsdb.Rollup1h); err != nil {
			return fmt.Errorf("初始化 GreptimeDB 1h 预聚合表: %w", err)
		}
	}

	store, err := catalog.NewPGStore(pool)
	if err != nil {
		return err
	}
	alarmStore, err := alarm.NewPGStore(pool)
	if err != nil {
		return err
	}
	quotaStore, err := quota.NewPGStore(pool)
	if err != nil {
		return err
	}
	otaStore, err := ota.NewPGStore(pool)
	if err != nil {
		return err
	}
	var otaArtifactStore *ota.ArtifactStore
	var otaSigner *ota.Signer
	otaDownloadSecret := strings.TrimSpace(os.Getenv("IOT_OTA_DOWNLOAD_SECRET"))
	otaDownloadBaseURL := strings.TrimSpace(os.Getenv("IOT_OTA_PUBLIC_BASE_URL"))
	if artifactDir := strings.TrimSpace(os.Getenv("IOT_OTA_ARTIFACT_DIR")); artifactDir != "" {
		otaArtifactStore, err = ota.NewArtifactStore(artifactDir, ota.DefaultMaxArtifactBytes)
		if err != nil {
			return fmt.Errorf("初始化 OTA 固件存储: %w", err)
		}
		logger.Info("OTA 固件上传已启用", "directory", artifactDir, "max_bytes", ota.DefaultMaxArtifactBytes)
	} else {
		logger.Warn("IOT_OTA_ARTIFACT_DIR 未设置，OTA 固件上传 API 未启用")
	}
	if keyFile := strings.TrimSpace(os.Getenv("IOT_OTA_SIGNING_KEY_FILE")); keyFile != "" {
		keyID := strings.TrimSpace(os.Getenv("IOT_OTA_SIGNING_KEY_ID"))
		otaSigner, err = ota.LoadSigner(keyFile, keyID)
		if err != nil {
			return fmt.Errorf("初始化 OTA 签名器: %w", err)
		}
		if otaDownloadSecret == "" || otaDownloadBaseURL == "" {
			return fmt.Errorf("OTA 签名器启用时必须配置 IOT_OTA_DOWNLOAD_SECRET 和 IOT_OTA_PUBLIC_BASE_URL")
		}
		logger.Info("OTA 清单签名已启用", "key_id", keyID, "public_base_url", otaDownloadBaseURL)
	} else {
		logger.Warn("IOT_OTA_SIGNING_KEY_FILE 未设置，OTA 清单签名/下载 API 未启用")
	}
	var endpointStore *notifyconfig.Store
	if rawKey := os.Getenv("IOT_CONFIG_KEY"); rawKey != "" {
		key, keyErr := secureconfig.NewKey(rawKey)
		if keyErr != nil {
			return keyErr
		}
		endpointStore, err = notifyconfig.New(pool, key)
		if err != nil {
			return err
		}
	} else {
		logger.Warn("IOT_CONFIG_KEY 未设置，通知端点配置 API 未启用")
	}
	latestOpts, err := redis.ParseURL(cfg.latestRedisURL)
	if err != nil {
		return fmt.Errorf("解析最新值 Redis 地址: %w", err)
	}
	latestRedis := redis.NewClient(latestOpts)
	defer latestRedis.Close()
	if err := latestRedis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("连接最新值 Redis: %w", err)
	}
	var commandService *command.Service
	var otaRouter *cluster.Node
	var shadowService *shadow.Service
	shadowStore, err := shadow.NewPGStore(pool)
	if err != nil {
		return err
	}
	shadowNode, err := cluster.New(ctx, cluster.Options{ID: cfg.shadowOriginID, NATSURL: cfg.natsURL, Cursor: cluster.NewRedisCursor(latestRedis, "gw:offline:cursor:shadow"), Logger: logger}, cluster.NewRedisLocator(latestRedis, "gw:client", cluster.DefaultLocatorTTL))
	if err != nil {
		return fmt.Errorf("初始化影子下行路由: %w", err)
	}
	defer shadowNode.Close()
	shadowService = &shadow.Service{Store: shadowStore, Router: shadowNode}
	shadowNC, err := nats.Connect(cfg.natsURL, nats.Name("svc-query-shadow-reported"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("连接影子上报 NATS: %w", err)
	}
	defer shadowNC.Close()
	shadowJS, err := shadowNC.JetStream()
	if err != nil {
		return err
	}
	if err := shadow.EnsureReportedStream(shadowJS); err != nil {
		return err
	}
	if err := ota.EnsureProgressStream(shadowJS); err != nil {
		return fmt.Errorf("确保 OTA 进度 Stream 存在失败: %w", err)
	}
	go func() {
		if err := shadow.ConsumeReported(ctx, shadowJS, shadowStore, "svc-shadow-reported", logger); err != nil && ctx.Err() == nil {
			logger.Error("影子 reported 消费者退出", "error", err)
		}
	}()
	go func() {
		if err := ota.ConsumeProgress(ctx, shadowJS, otaStore, "svc-ota-progress", logger); err != nil && ctx.Err() == nil {
			logger.Error("OTA 进度消费者退出", "error", err)
		}
	}()

	if cfg.commandOriginID != "" {
		node, nodeErr := cluster.New(ctx, cluster.Options{ID: cfg.commandOriginID, NATSURL: cfg.natsURL, Cursor: cluster.NewRedisCursor(latestRedis, "gw:offline:cursor"), Logger: logger}, cluster.NewRedisLocator(latestRedis, "gw:client", cluster.DefaultLocatorTTL))
		if nodeErr != nil {
			return fmt.Errorf("初始化命令下行路由: %w", nodeErr)
		}
		defer node.Close()
		otaRouter = node
		cmdStore, storeErr := command.NewStore(pool)
		if storeErr != nil {
			return storeErr
		}
		commandService = &command.Service{Store: cmdStore, Sender: command.Sender{Router: node}}
		shadowStore, shadowErr := shadow.NewPGStore(pool)
		if shadowErr != nil {
			return shadowErr
		}
		shadowService = &shadow.Service{Store: shadowStore, Router: node}
	}
	quotaPub, err := gateway.NewNATSPublisher(cfg.natsURL, "IOT_QUOTA")
	if err != nil {
		return fmt.Errorf("连接计量上报 NATS: %w", err)
	}
	defer quotaPub.Close()
	meterAcc := metering.NewAccumulator()
	meterReporter, err := metering.NewReporter(metering.ReporterOptions{Accumulator: meterAcc, Publisher: quotaPub, NodeID: "svc-query", Logger: logger})
	if err != nil {
		return err
	}
	go meterReporter.Run(ctx)

	authMetrics := new(apiauth.Metrics)
	svc, err := querysvc.New(querysvc.Config{
		Plan:               plan,
		DefaultDeviceLimit: cfg.limitDefaultDevices,
		MaxDeviceLimit:     cfg.limitMaxDevices,
		QueryTimeout:       cfg.queryTimeout,
		SlowQueryThreshold: cfg.slowQueryThreshold,
		Limiter: querysvc.LimiterConfig{
			MaxConcurrency: cfg.maxConcurrency,
			MaxQueue:       cfg.queueDepth,
			QueueTimeout:   cfg.queueTimeout,
			RatePerSecond:  cfg.ratePerSecond,
			Burst:          cfg.rateBurst,
		},
	}, querysvc.Deps{
		Reader:             gres,
		Latest:             latest.NewRedisStore(latestRedis),
		Endpoints:          endpointStore,
		Alarms:             alarmStore,
		Quota:              quotaStore,
		Commands:           commandService,
		Shadows:            shadowService,
		OTA:                otaStore,
		OTARouter:          otaRouter,
		OTAArtifact:        otaArtifactStore,
		OTASigner:          otaSigner,
		OTADownloadSecret:  otaDownloadSecret,
		OTADownloadBaseURL: otaDownloadBaseURL,
		Meter:              meterAcc,
		Catalog:            store,
		Verifier:           verifier,
		Health: querysvc.Health{
			PingPG:            pool.Ping,
			PingTSDB:          gres.Ping,
			PendingMigrations: func(c context.Context) ([]string, error) { return pendingMigrations(c, pool) },
		},
		AuthMetrics: authMetrics,
		Logger:      logger,
	})
	if err != nil {
		return err
	}

	var rolloutWorker *ota.RolloutWorker
	if otaRouter != nil && otaSigner != nil && otaDownloadSecret != "" && otaDownloadBaseURL != "" {
		rolloutWorker, err = ota.NewRolloutWorker(otaStore, otaRouter, otaSigner, otaDownloadSecret, otaDownloadBaseURL)
		if err != nil {
			return fmt.Errorf("初始化 OTA 灰度 worker: %w", err)
		}
		logger.Info("OTA 灰度自动推进已启用")
	} else {
		logger.Warn("OTA 灰度自动推进未启用：命令路由、签名器或下载配置缺失")
	}
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				projects, err := store.ListProjects(ctx)
				if err != nil {
					logger.Warn("读取 OTA 租户列表失败", "error", err)
					continue
				}
				for _, project := range projects {
					if err := otaStore.ReconcileStale(ctx, project.ID); err != nil {
						logger.Warn("OTA 任务超时收敛失败", "project_id", project.ID, "error", err)
					}
					if rolloutWorker != nil {
						if err := rolloutWorker.RunProject(ctx, project.ID); err != nil {
							logger.Warn("OTA 灰度推进失败", "project_id", project.ID, "error", err)
						}
					}
				}
			}
		}
	}()

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           svc.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("svc-query 已就绪",
		"auth_mode", cfg.authMode,
		"plan", string(plan),
		"http_addr", cfg.httpAddr,
		"max_concurrency", cfg.maxConcurrency,
		"version", buildinfo.Version, "commit", buildinfo.Commit)

	errCh := make(chan error, 1)
	go func() { errCh <- serveHTTP(srv, logger) }()

	select {
	case <-ctx.Done():
		logger.Info("收到退出信号")
	case err := <-errCh:
		if err != nil {
			logger.Error("HTTP 服务异常退出", "error", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	logger.Info("已退出")
	return nil
}

func serveHTTP(srv *http.Server, logger *slog.Logger) error {
	logger.Info("HTTP 已启动", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP 服务: %w", err)
	}
	return nil
}

// buildVerifier 按认证模式装配校验器，并把配置错误挡在启动阶段。
func buildVerifier(ctx context.Context, cfg config, logger *slog.Logger) (apiauth.Verifier, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.authMode))
	switch mode {
	case "jwt", "dev", "both":
	default:
		return nil, fmt.Errorf("非法 -auth-mode %q（可选 jwt / dev / both）", cfg.authMode)
	}

	var verifiers apiauth.MultiVerifier

	if mode == "dev" || mode == "both" {
		if strings.TrimSpace(cfg.devToken) == "" {
			return nil, fmt.Errorf("-auth-mode=%s 需要 -dev-token", mode)
		}
		if cfg.devProjectID <= 0 {
			return nil, fmt.Errorf("-auth-mode=%s 需要 -dev-project-id > 0", mode)
		}
		if err := requireDevListen(cfg.httpAddr, cfg.allowDevLAN); err != nil {
			return nil, err
		}
		dev, err := apiauth.NewStaticTokenVerifier(cfg.devToken, cfg.devProjectID)
		if err != nil {
			return nil, err
		}
		verifiers = append(verifiers, dev)
		logger.Warn("开发静态令牌已启用：仅限开发/PoC，禁止用于生产",
			"project_id", cfg.devProjectID, "http_addr", cfg.httpAddr,
			"allow_dev_lan", cfg.allowDevLAN)
	}

	if mode == "jwt" || mode == "both" {
		issuers := splitNonEmpty(cfg.jwtIssuers)
		if len(issuers) == 0 {
			return nil, fmt.Errorf("-auth-mode=%s 需要 -jwt-issuer（允许的签发方列表）", mode)
		}
		if cfg.jwtJWKSURL == "" && cfg.jwtJWKSFile == "" {
			return nil, fmt.Errorf("-auth-mode=%s 需要 -jwt-jwks-url 或 -jwt-jwks-file", mode)
		}
		keys, err := apiauth.NewJWKS(apiauth.JWKSConfig{
			URL: cfg.jwtJWKSURL, File: cfg.jwtJWKSFile, TTL: cfg.jwtJWKSTTL,
		})
		if err != nil {
			// 拿不到 JWKS 就拒绝启动：让配置错误在启动时暴露，而不是在第一个请求上。
			return nil, fmt.Errorf("初始化 JWKS: %w", err)
		}

		var revoker apiauth.Revoker
		switch strings.ToLower(strings.TrimSpace(cfg.jwtRevocation)) {
		case "on":
			if cfg.redisURL == "" {
				return nil, fmt.Errorf("-jwt-revocation=on 需要 -redis-url（jti 吊销表）")
			}
			opt, err := redis.ParseURL(cfg.redisURL)
			if err != nil {
				return nil, fmt.Errorf("解析 -redis-url: %w", err)
			}
			rdb := redis.NewClient(opt)
			if err := rdb.Ping(ctx).Err(); err != nil {
				return nil, fmt.Errorf("连接 Redis（吊销表）: %w", err)
			}
			r, err := apiauth.NewRedisRevoker(rdb)
			if err != nil {
				return nil, err
			}
			revoker = r
		case "off":
			logger.Warn("jti 吊销检查已关闭：仅限开发，生产必须开启（fail-closed）")
		default:
			return nil, fmt.Errorf("非法 -jwt-revocation %q（可选 on / off）", cfg.jwtRevocation)
		}

		jv, err := apiauth.NewJWTVerifier(apiauth.JWTConfig{
			Issuers: issuers, Audience: cfg.jwtAudience, Leeway: cfg.jwtLeeway,
			Keys: keys, Revoker: revoker,
		})
		if err != nil {
			return nil, err
		}
		verifiers = append(verifiers, jv)
	}

	if len(verifiers) == 0 {
		return nil, fmt.Errorf("没有可用的认证校验器")
	}
	return verifiers, nil
}

// requireDevListen 默认拒绝在非回环地址上启用开发令牌。
//
// 这不是洁癖：开发令牌是共享常量、无过期、无吊销，一旦绑到 0.0.0.0 就等于把整个
// 租户的数据敞给同网段。allowDevLAN 只应在外层已有网络隔离和访问控制时启用。
func requireDevListen(addr string, allowDevLAN bool) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("解析 -http-addr %q: %w", addr, err)
	}
	if allowDevLAN {
		if host == "" || host == "0.0.0.0" || host == "::" {
			return fmt.Errorf("-allow-dev-lan 禁止监听全接口地址 %q；请绑定容器网卡地址", addr)
		}
		if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf("-allow-dev-lan 需要明确的非回环 IP 地址，得到 %q", addr)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return fmt.Errorf("开发令牌模式禁止监听 %q（会暴露到全网段）；请绑 127.0.0.1", addr)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("开发令牌模式只允许回环地址，得到 %q", addr)
}

func parsePlan(s string) (tsdb.Plan, error) {
	switch tsdb.Plan(strings.ToLower(strings.TrimSpace(s))) {
	case tsdb.PlanJSON:
		return tsdb.PlanJSON, nil
	case tsdb.PlanWide:
		return tsdb.PlanWide, nil
	default:
		return "", fmt.Errorf("非法 -plan %q（可选 json / wide）", s)
	}
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
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
