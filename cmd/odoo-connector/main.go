// Command odoo-connector 是 07 §4.1 的独立 Go 服务：**不嵌入**网关或管道。
//
// 已实现：
//   - 编排层（§4.3）：令牌桶限流、有界排队准入、熔断（含 401/403 立即跳闸）、
//     退避重试、超时、错误码映射、幂等占位；
//   - 事件入口 C-1（§4.4）：消费 Odoo Outbox 投递到 Redis Streams 的事件，
//     翻译为 `iot.odoo.*` 发布到 NATS —— **发布成功才 XACK**（至少一次），
//     未确认的由 `XAUTOCLAIM` 接管重投；
//   - 事件入口 C-2（§4.4）：Odoo postcommit webhook → NATS，
//     按 `(model, id, write_date)` 去重，并推进 C-2 水位；
//   - 定时对账（§4.4，每 15 min）：① 补投 Odoo `edge.outbox` 的非终态行；
//     ② 比对 C-2 水位差并补投遗漏记录；连续两轮仍有遗漏则告警。
//
// 已实现：
//   - 可选主数据增量拉取（按 `write_date` 水位 + id 断点，Redis 游标，写入 IoT PG）；
//
// 尚未实现（如实留白，不做假实现）：
//   - 死信对象存储原文（当前先落 PostgreSQL），以及告警去重/升级策略；
//   - 对账的第 ③ 项（未绑定告警待办）：依赖 `t_external_ref`，该表尚未建立；
//   - 幂等**权威**账本仍在 Odoo 侧 `edge.idempotency`（§5.2.1）：
//     本服务只做占位拦截，不承担裁决。
//
// 用法（devbox 内）：
//
//	ODOO_API_KEY=xxx ODOO_WEBHOOK_TOKEN=yyy go run ./cmd/odoo-connector \
//	  -odoo-url http://127.0.0.1:8070 -odoo-db odoo20 \
//	  -reconcile-models maintenance.equipment
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/connector"
	"github.com/SNCIC/odoo20iot/internal/dlq"
	"github.com/SNCIC/odoo20iot/internal/extref"
	"github.com/SNCIC/odoo20iot/internal/notify"
	"github.com/SNCIC/odoo20iot/internal/odoo"
	"github.com/SNCIC/odoo20iot/internal/pg"
)

type config struct {
	odooURL  string
	odooDB   string
	apiKey   string
	redisURL string
	httpAddr string
	// logFormat 取 `json` 或 `text`。
	//
	// 刻意不用布尔 flag：Go 的 `flag` 包**遇到第一个非 flag 参数就停止解析**，
	// 而布尔 flag 只接受 `-x` 或 `-x=false`，写成 `-log-json false` 会让 `false`
	// 之后的**所有参数被静默丢弃** —— 一个不报错的配置错误，
	// 比一个报错的配置错误危险得多。
	logFormat string

	readyProbe time.Duration

	natsURL       string
	natsStream    string
	outboxStream  string
	consumerGroup string
	outboxBatch   int

	webhookToken string

	reconcileModels       string
	reconcileInterval     time.Duration
	reconcileStaleAfter   time.Duration
	reconcileBatch        int
	pgDSN                 string
	masterDataModel       string
	masterDataInterval    time.Duration
	masterDataBatch       int
	alarmToOdoo           bool
	alarmStream           string
	alarmConsumer         string
	dlqEnabled            bool
	dlqReplayInterval     time.Duration
	dlqReplayBatch        int
	dlqAlertInterval      time.Duration
	dlqAlertWindow        time.Duration
	dlqAlertThreshold     int64
	dlqAlertWebhookURL    string
	dlqAlertEgressAllow   string
	dlqAlertAllowLoopback bool
	dlqObjectDir          string
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nodoo-connector 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.odooURL, "odoo-url", "http://127.0.0.1:8070", "Odoo 基地址（不含路径）")
	flag.StringVar(&cfg.odooDB, "odoo-db", "odoo20", "Odoo 库名（每次请求都会携带 X-Odoo-Database）")
	flag.StringVar(&cfg.apiKey, "odoo-api-key", os.Getenv("ODOO_API_KEY"),
		"Odoo API Key（默认取环境变量 ODOO_API_KEY；生产由 Vault 注入，禁止明文写配置文件）")
	flag.StringVar(&cfg.redisURL, "redis-url", envOrDefault("IOT_REDIS_URL", "redis://100.64.0.3:28637/0"), "Redis 地址（默认取 IOT_REDIS_URL）")
	flag.StringVar(&cfg.httpAddr, "http-addr", "127.0.0.1:18091", "健康检查/指标/webhook 监听地址")
	flag.StringVar(&cfg.logFormat, "log-format", "json", "日志格式：json（默认）或 text")
	flag.DurationVar(&cfg.readyProbe, "ready-probe-timeout", 5*time.Second, "就绪探针单次探测超时")

	flag.StringVar(&cfg.natsURL, "nats-url", envOrDefault("IOT_NATS_URL", "nats://100.64.0.3:28222"), "NATS JetStream 地址（默认取 IOT_NATS_URL）")
	flag.StringVar(&cfg.natsStream, "nats-stream", connector.OdooStream, "承载 iot.odoo.> 的 Stream 名")
	flag.StringVar(&cfg.outboxStream, "outbox-stream", connector.OutboxStream,
		"Odoo Outbox 投递的 Redis Stream 键（须与 sn_edge_integration 一致）")
	flag.StringVar(&cfg.consumerGroup, "consumer-group", connector.DefaultConsumerGroup, "Redis Streams 消费组名")
	flag.IntVar(&cfg.outboxBatch, "outbox-batch", connector.DefaultOutboxBatch, "单轮拉取的事件数")

	flag.StringVar(&cfg.webhookToken, "webhook-token", os.Getenv("ODOO_WEBHOOK_TOKEN"),
		"C-2 webhook 的 Bearer 令牌；**留空即禁用该入口**")

	flag.StringVar(&cfg.reconcileModels, "reconcile-models", "",
		"要对账 C-2 水位差的 Odoo 模型（逗号分隔）；留空则只对账 Outbox 非终态行")
	flag.DurationVar(&cfg.reconcileInterval, "reconcile-interval", connector.DefaultReconcileInterval,
		"对账周期（07 §4.4：15 min）")
	flag.DurationVar(&cfg.reconcileStaleAfter, "reconcile-stale-after", connector.DefaultReconcileStaleAfter,
		"判定 Outbox 行「卡住」的宽限窗口")
	flag.IntVar(&cfg.reconcileBatch, "reconcile-batch", connector.DefaultReconcileBatch,
		"对账单轮单来源的处理上限")
	flag.StringVar(&cfg.pgDSN, "pg-dsn", envOrDefault("IOT_PG_DSN", pg.DefaultDSN), "IoT 业务库 DSN（默认取 IOT_PG_DSN）")
	flag.StringVar(&cfg.masterDataModel, "masterdata-model", "", "启用 Odoo 主数据增量同步的模型；留空关闭")
	flag.DurationVar(&cfg.masterDataInterval, "masterdata-interval", time.Minute, "主数据增量同步周期")
	flag.IntVar(&cfg.masterDataBatch, "masterdata-batch", connector.DefaultMasterDataBatch, "主数据单轮拉取上限")
	flag.BoolVar(&cfg.alarmToOdoo, "alarm-to-odoo", false, "启用 IoT 告警到 Odoo maintenance.request 建单")
	flag.StringVar(&cfg.alarmStream, "alarm-stream", connector.AlarmStream, "IoT 告警 JetStream 流名")
	flag.StringVar(&cfg.alarmConsumer, "alarm-consumer", connector.AlarmConsumerName, "IoT 告警持久消费者名")
	flag.BoolVar(&cfg.dlqEnabled, "dlq-enabled", true, "启用 Odoo 调用死信落库；默认开启")
	flag.DurationVar(&cfg.dlqReplayInterval, "dlq-replay-interval", 0, "Odoo DLQ 自动重放周期；0 表示关闭")
	flag.IntVar(&cfg.dlqReplayBatch, "dlq-replay-batch", 50, "Odoo DLQ 单轮重放上限")
	flag.DurationVar(&cfg.dlqAlertInterval, "dlq-alert-interval", 5*time.Minute, "DLQ 聚合告警周期；0 表示关闭")
	flag.DurationVar(&cfg.dlqAlertWindow, "dlq-alert-window", 15*time.Minute, "DLQ 聚合告警统计窗口")
	flag.Int64Var(&cfg.dlqAlertThreshold, "dlq-alert-threshold", 1, "触发 DLQ 聚合告警的最小条数")
	flag.StringVar(&cfg.dlqAlertWebhookURL, "dlq-alert-webhook", os.Getenv("IOT_DLQ_ALERT_WEBHOOK"), "DLQ 告警 Webhook；默认取 IOT_DLQ_ALERT_WEBHOOK")
	flag.StringVar(&cfg.dlqAlertEgressAllow, "dlq-alert-egress-allow", os.Getenv("IOT_DLQ_ALERT_EGRESS_ALLOW"), "DLQ 告警出站白名单；默认取 IOT_DLQ_ALERT_EGRESS_ALLOW")
	flag.BoolVar(&cfg.dlqAlertAllowLoopback, "dlq-alert-allow-loopback", false, "仅本地联调放行 DLQ 告警回环地址")
	flag.StringVar(&cfg.dlqObjectDir, "dlq-object-dir", os.Getenv("IOT_DLQ_OBJECT_DIR"), "DLQ 原文对象目录；为空则仅保留 PG 原文")
	flag.Parse()
	return cfg
}

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if strings.EqualFold(strings.TrimSpace(format), "text") {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

func run(cfg config) error {
	logger := newLogger(cfg.logFormat)

	if cfg.apiKey == "" {
		return errors.New("缺少 Odoo API Key：请设置 ODOO_API_KEY 或 -odoo-api-key")
	}

	// 07 §4.3 的编排策略；零值取文档默认（20 req/s、在途 8、队列 1000、3s/15s、1s/3s/9s、10 次或 50%）。
	policy := connector.Policy{
		BreakerOpenP1Alert: func(reason string) {
			// 文档要求 401/403「立即熔断 + P1 告警」；此处先落日志，
			// 接入告警通道后改为推送 P1。
			logger.Error("P1：Odoo 凭据失效，连接器已熔断，请立即更换 API Key", "reason", reason)
		},
	}.Normalize()

	client, err := odoo.New(odoo.Config{
		BaseURL:    cfg.odooURL,
		Database:   cfg.odooDB,
		APIKey:     cfg.apiKey,
		HTTPClient: policy.HTTPClient(), // 连接 3s / 读 15s（§4.3）
	})
	if err != nil {
		return fmt.Errorf("构造 Odoo 客户端: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(mustParseRedis(cfg.redisURL))
	defer func() { _ = rdb.Close() }()

	var businessPool *pgxpool.Pool
	if cfg.dlqEnabled || strings.TrimSpace(cfg.masterDataModel) != "" || cfg.dlqReplayInterval > 0 || cfg.dlqAlertInterval > 0 || cfg.alarmToOdoo {
		businessPool, err = pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
		if err != nil {
			return fmt.Errorf("连接 IoT 业务库: %w", err)
		}
		defer businessPool.Close()
	}
	var dlqStoreInstance *dlq.Store
	if businessPool != nil {
		var objectStore dlq.ObjectStore
		if cfg.dlqObjectDir != "" {
			objectStore, err = dlq.NewFileObjectStore(cfg.dlqObjectDir)
			if err != nil {
				return fmt.Errorf("初始化 DLQ 原文对象存储: %w", err)
			}
		}
		dlqStoreInstance = dlq.NewWithObjectStore(businessPool, objectStore)
	}

	// 指标由四处共用：编排层、Outbox 消费、webhook、对账。
	metrics := new(connector.Metrics)

	conn, err := connector.New(client, connector.Options{
		Policy:  policy,
		Metrics: metrics,
		Logger:  logger,
		Guard:   connector.NewRedisGuard(rdb, connector.DefaultGuardTTL),
		DLQ:     dlqStoreInstance,
	})
	if err != nil {
		return fmt.Errorf("构造连接器: %w", err)
	}

	// 事件出口：NATS。MaxReconnects(-1) 表示永不放弃重连 ——
	// 连接器是搬运工，NATS 抖动时应该等它回来，而不是退出重启。
	nc, err := nats.Connect(cfg.natsURL, nats.Name("odoo-connector"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("连接 NATS: %w", err)
	}
	defer nc.Close()

	publisher, err := connector.NewNATSPublisher(nc, cfg.natsStream)
	if err != nil {
		return fmt.Errorf("初始化 NATS 发布器: %w", err)
	}

	// C-2 水位：webhook 推进、对账比对，两侧必须共用同一份存储。
	watermarks := connector.NewRedisWatermarksWithFallback(rdb, businessPool)
	var refs *extref.Store
	if businessPool != nil {
		refs = extref.NewStore(businessPool)
	}

	// 事件入口 C-1：Odoo Outbox（Redis Streams）→ NATS。
	consumer, err := connector.NewOutboxConsumer(connector.OutboxOptions{
		Store:     connector.NewRedisStreams(rdb),
		Publisher: publisher,
		Stream:    cfg.outboxStream,
		Group:     cfg.consumerGroup,
		Batch:     cfg.outboxBatch,
		Metrics:   metrics,
		Logger:    logger,
	})
	if err != nil {
		return fmt.Errorf("构造 Outbox 消费者: %w", err)
	}

	// 事件入口 C-2：webhook（无令牌即禁用）。
	var webhook *connector.Webhook
	if cfg.webhookToken != "" {
		webhook, err = connector.NewWebhook(connector.WebhookOptions{
			Publisher:  publisher,
			Deduper:    connector.NewRedisDeduper(rdb, connector.DefaultDedupTTL),
			Watermarks: watermarks,
			Token:      cfg.webhookToken,
			Metrics:    metrics,
			Logger:     logger,
		})
		if err != nil {
			return fmt.Errorf("构造 C-2 webhook: %w", err)
		}
	}

	// 定时对账（§4.4）。Caller 传的是编排层本身，
	// 这样对账的 Odoo 查询同样受限流与熔断保护。
	reconcileOpts := connector.ReconcileOptions{
		Caller:     conn,
		Publisher:  publisher,
		Watermarks: watermarks,
		Models:     splitNonEmpty(cfg.reconcileModels),
		Interval:   cfg.reconcileInterval,
		StaleAfter: cfg.reconcileStaleAfter,
		Batch:      cfg.reconcileBatch,
		Metrics:    metrics,
		Logger:     logger,
	}
	if refs != nil {
		reconcileOpts.ExternalRefs = refs
	}
	reconciler, err := connector.NewReconciler(reconcileOpts)
	if err != nil {
		return fmt.Errorf("构造对账器: %w", err)
	}

	var masterSync *connector.MasterDataSync
	var masterCatalog *catalog.PGStore
	if strings.TrimSpace(cfg.masterDataModel) != "" {
		cat, err := catalog.NewPGStore(businessPool)
		if err != nil {
			return err
		}
		masterCatalog = cat
		masterSync, err = connector.NewMasterDataSync(connector.MasterDataOptions{
			Caller: conn, Catalog: cat, Projects: cat, Refs: extref.NewStore(businessPool),
			Batch: cfg.masterDataBatch, Logger: logger,
		})
		if err != nil {
			return fmt.Errorf("构造主数据同步器: %w", err)
		}
	}
	var alarmConsumer *connector.AlarmConsumer
	if cfg.alarmToOdoo {
		js, err := nc.JetStream()
		if err != nil {
			return fmt.Errorf("获取告警 JetStream 上下文: %w", err)
		}
		alarmConsumer, err = connector.NewAlarmConsumer(connector.AlarmConsumerOptions{
			JS: js, Odoo: client, Refs: extref.NewStore(businessPool),
			Stream: cfg.alarmStream, Durable: cfg.alarmConsumer, Logger: logger,
		})
		if err != nil {
			return fmt.Errorf("构造 Odoo 告警消费者: %w", err)
		}
	}

	var replayer *dlq.Replayer
	if cfg.dlqReplayInterval > 0 {
		replayer, err = dlq.NewReplayer(dlq.New(businessPool), 3)
		if err != nil {
			return fmt.Errorf("构造 Odoo DLQ 重放器: %w", err)
		}
		if err := replayer.Register("odoo_call", func(ctx context.Context, row dlq.Record) error {
			var req connector.Request
			if err := json.Unmarshal([]byte(row.Payload), &req); err != nil {
				return fmt.Errorf("解析 Odoo DLQ 请求: %w", err)
			}
			if req.Model == "" || req.Method == "" {
				return fmt.Errorf("Odoo DLQ 请求缺少 model 或 method")
			}
			return conn.Call(connector.SuppressDLQ(ctx), req, nil)
		}); err != nil {
			return fmt.Errorf("注册 Odoo DLQ 重放器: %w", err)
		}
	}
	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           routes(conn, webhook, logger, cfg),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("odoo-connector 已就绪",
		"odoo", cfg.odooURL, "database", cfg.odooDB, "http", cfg.httpAddr,
		"nats", cfg.natsURL, "nats_stream", cfg.natsStream,
		"outbox_stream", cfg.outboxStream, "consumer_group", cfg.consumerGroup,
		"webhook_enabled", webhook != nil,
		"reconcile_interval", cfg.reconcileInterval, "reconcile_models", reconciler.Models(),
		"rate_per_sec", policy.RatePerSecond, "max_in_flight", policy.MaxInFlight,
		"version", buildinfo.Version, "commit", buildinfo.Commit)

	errCh := make(chan error, 4)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("HTTP 服务异常退出: %w", err)
		}
	}()
	go func() {
		if err := consumer.Run(ctx); err != nil {
			errCh <- fmt.Errorf("Outbox 消费异常退出: %w", err)
		}
	}()
	go func() {
		if err := reconciler.Run(ctx); err != nil {
			errCh <- fmt.Errorf("对账异常退出: %w", err)
		}
	}()
	if masterSync != nil {
		go func() {
			if err := runMasterDataLoop(ctx, masterSync, watermarks, cfg.masterDataModel, cfg.masterDataInterval, logger, masterCatalog); err != nil {
				errCh <- fmt.Errorf("主数据同步异常退出: %w", err)
			}
		}()
	}
	if alarmConsumer != nil {
		go func() {
			if err := alarmConsumer.Run(ctx); err != nil {
				errCh <- fmt.Errorf("Odoo 告警消费者异常退出: %w", err)
			}
		}()
	}
	if replayer != nil {
		go func() {
			if err := runOdooDLQReplay(ctx, replayer, cfg.dlqReplayInterval, cfg.dlqReplayBatch, logger); err != nil {
				errCh <- fmt.Errorf("Odoo DLQ 重放异常退出: %w", err)
			}
		}()
	}
	if dlqStoreInstance != nil && cfg.dlqAlertInterval > 0 {
		var dlqAlertSender *notify.WebhookChannel
		if strings.TrimSpace(cfg.dlqAlertWebhookURL) != "" {
			allow := splitNonEmpty(cfg.dlqAlertEgressAllow)
			if len(allow) == 0 {
				return errors.New("配置 DLQ 告警 Webhook 时必须提供 -dlq-alert-egress-allow 或 IOT_DLQ_ALERT_EGRESS_ALLOW")
			}
			guard := &notify.Guard{AllowedHosts: allow, AllowLoopback: cfg.dlqAlertAllowLoopback}
			dlqAlertSender = notify.NewWebhookChannel(guard, 5*time.Second)
		}
		go func() {
			if err := runDLQAlertLoop(ctx, dlqStoreInstance, cfg.dlqAlertInterval, cfg.dlqAlertWindow, cfg.dlqAlertThreshold, dlqAlertSender, cfg.dlqAlertWebhookURL, logger); err != nil {
				errCh <- fmt.Errorf("DLQ 聚合告警异常退出: %w", err)
			}
		}()
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("收到退出信号，开始优雅关闭")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP 优雅关闭失败", "error", err)
	}

	logger.Info("已退出",
		"calls", metrics.CallsTotal.Load(), "success", metrics.SuccessTotal.Load(),
		"retries", metrics.RetriesTotal.Load(), "rate_limited", metrics.RateLimitedTotal.Load(),
		"published", metrics.PublishedTotal.Load(), "publish_errors", metrics.PublishErrors.Load(),
		"reconcile_republished", metrics.ReconcileRepublished.Load())
	return nil
}

func runMasterDataLoop(ctx context.Context, syncer *connector.MasterDataSync, watermarks connector.Watermarks, model string, interval time.Duration, logger *slog.Logger, projects catalog.ProjectLister) error {
	if interval <= 0 {
		interval = time.Minute
	}
	run := func() error {
		if scoped, ok := watermarks.(connector.TenantWatermarks); ok && projects != nil {
			items, err := projects.ListProjects(ctx)
			if err != nil {
				return err
			}
			for _, project := range items {
				tenant := strconv.FormatInt(project.ID, 10)
				cursor, found, err := scoped.GetForTenant(ctx, tenant, model)
				if err != nil {
					return err
				}
				if !found {
					cursor = connector.Watermark{}
				}
				next, result, err := syncer.SyncOnceForCompany(ctx, model, project.OdooCompanyID, cursor)
				if err != nil {
					return err
				}
				if next.After(cursor) {
					if err := scoped.AdvanceForTenant(ctx, tenant, model, next); err != nil {
						return err
					}
				}
				logger.Info("主数据租户同步完成", "project_id", project.ID, "company_id", project.OdooCompanyID, "model", model, "read", result.Read, "upserted", result.Upserted, "skipped", result.Skipped, "unbound", result.Unbound, "cursor", next)
			}
			return nil
		}
		key := "masterdata:" + model
		cursor, ok, err := watermarks.Get(ctx, key)
		if err != nil {
			return err
		}
		if !ok {
			cursor = connector.Watermark{}
		}
		next, result, err := syncer.SyncOnce(ctx, model, cursor)
		if err != nil {
			return err
		}
		if next.After(cursor) {
			if err := watermarks.Advance(ctx, key, next); err != nil {
				return err
			}
		}
		logger.Info("主数据同步完成", "model", model, "read", result.Read, "upserted", result.Upserted, "skipped", result.Skipped, "unbound", result.Unbound, "cursor", next)
		return nil
	}
	if err := run(); err != nil {
		logger.Error("主数据首次同步失败", "model", model, "error", err)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := run(); err != nil {
				logger.Error("主数据同步失败", "model", model, "error", err)
			}
		}
	}
}

func runOdooDLQReplay(ctx context.Context, replayer *dlq.Replayer, interval time.Duration, batch int, logger *slog.Logger) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	replay := func() {
		result, err := replayer.Replay(ctx, "odoo-connector", "", batch)
		if err != nil {
			logger.Error("Odoo DLQ 扫描失败", "error", err)
			return
		}
		if result.Scanned > 0 {
			logger.Info("Odoo DLQ 扫描完成", "scanned", result.Scanned, "succeeded", result.Succeeded, "failed", result.Failed, "skipped", result.Skipped)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			replay()
		}
	}
}

func runDLQAlertLoop(ctx context.Context, store *dlq.Store, interval, window time.Duration, threshold int64, sender *notify.WebhookChannel, webhookURL string, logger *slog.Logger) error {
	if interval <= 0 {
		return nil
	}
	if window <= 0 {
		window = 15 * time.Minute
	}
	if threshold <= 0 {
		threshold = 1
	}
	alert := func() {
		since := time.Now().Add(-window)
		aggregates, err := store.AggregateSince(ctx, since)
		if err != nil {
			logger.Error("DLQ 聚合查询失败", "error", err)
			return
		}
		for _, aggregate := range aggregates {
			if aggregate.Count < threshold {
				continue
			}
			logger.Warn("P2：DLQ 聚合告警",
				"service", aggregate.Service,
				"subject", aggregate.Subject,
				"entity_type", aggregate.EntityType,
				"count", aggregate.Count,
				"window", window,
			)
			if sender != nil {
				msg := notify.Message{
					TraceID: "dlq-alert", AlarmID: fmt.Sprintf("dlq:%s:%s:%s", aggregate.Service, aggregate.Subject, aggregate.EntityType),
					Level: "critical", Subject: "IoT DLQ 聚合告警",
					Body: fmt.Sprintf("服务=%s\n主题=%s\n实体=%s\n数量=%d\n窗口=%s", aggregate.Service, aggregate.Subject, aggregate.EntityType, aggregate.Count, window),
				}
				if err := sender.Send(ctx, msg, []string{webhookURL}); err != nil {
					logger.Error("DLQ 聚合告警 Webhook 投递失败", "error", err, "service", aggregate.Service, "subject", aggregate.Subject)
				}
			}
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			alert()
		}
	}
}

func mustParseRedis(raw string) *redis.Options {
	opts, err := redis.ParseURL(raw)
	if err != nil {
		// 走到这里说明默认值或参数写错了，属于启动期配置错误，直接退出更清楚。
		fmt.Fprintf(os.Stderr, "解析 Redis 地址失败: %v\n", err)
		os.Exit(1)
	}
	return opts
}

func splitNonEmpty(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func routes(c *connector.Connector, webhook *connector.Webhook, logger *slog.Logger, cfg config) http.Handler {
	mux := http.NewServeMux()

	// 存活：进程还在就返回 200，不依赖 Odoo（否则 Odoo 抖一下会把容器重启）。
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "alive"})
	})

	// 就绪：真打一次 Odoo，走完整编排链路。
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), cfg.readyProbe)
		defer cancel()

		var out []map[string]any
		err := c.Call(ctx, connector.Request{
			Model:  "res.partner",
			Method: "search_read",
			Params: map[string]any{"domain": []any{}, "fields": []string{"id"}, "limit": 1},
		}, &out)
		if err != nil {
			code := errorCode(err)
			logger.Warn("就绪探测失败", "code", code, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": code, "odoo": map[string]any{"reachable": false},
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "odoo": map[string]any{"reachable": true}, "database": cfg.odooDB,
		})
	})

	// C-2 入口。未配置令牌时**不注册路由**：连探测面都不该存在。
	if webhook != nil {
		mux.Handle(connector.WebhookPath, webhook)
	}

	// 指标：对齐 07 §4.5 的命名，先以文本形式暴露。
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		m := c.Metrics()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "odoo_connector_calls_total %d\n", m.CallsTotal.Load())
		fmt.Fprintf(w, "odoo_connector_success_total %d\n", m.SuccessTotal.Load())
		fmt.Fprintf(w, "odoo_connector_retries_total %d\n", m.RetriesTotal.Load())
		fmt.Fprintf(w, "odoo_connector_rate_limited_total %d\n", m.RateLimitedTotal.Load())
		fmt.Fprintf(w, "odoo_connector_breaker_open_total %d\n", m.BreakerOpenTotal.Load())
		fmt.Fprintf(w, "odoo_connector_waiting %d\n", c.Waiting())
		fmt.Fprintf(w, "odoo_connector_events_published_total %d\n", m.PublishedTotal.Load())
		fmt.Fprintf(w, "odoo_connector_publish_errors_total %d\n", m.PublishErrors.Load())
		fmt.Fprintf(w, "odoo_connector_ingest_errors_total %d\n", m.IngestErrors.Load())
		fmt.Fprintf(w, "odoo_connector_poisoned_total %d\n", m.Poisoned.Load())
		fmt.Fprintf(w, "odoo_connector_reclaimed_total %d\n", m.Reclaimed.Load())
		fmt.Fprintf(w, "odoo_connector_reconcile_republished_total %d\n", m.ReconcileRepublished.Load())
		fmt.Fprintf(w, "odoo_connector_cursor_lag_seconds %d\n", m.CursorLagSeconds.Load())
		fmt.Fprintf(w, "odoo_connector_webhook_received_total %d\n", m.WebhookReceived.Load())
		fmt.Fprintf(w, "odoo_connector_webhook_rejected_total %d\n", m.WebhookRejected.Load())
		for code, n := range m.ErrorsByCode() {
			fmt.Fprintf(w, "odoo_connector_errors_total{code=%q} %d\n", code, n)
		}
	})

	return mux
}

// errorCode 从编排层错误里取对外错误码。
func errorCode(err error) connector.Code {
	var ce *connector.Error
	if errors.As(err, &ce) {
		return ce.Code
	}
	return connector.Classify(err)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
