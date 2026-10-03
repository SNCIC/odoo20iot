// Command svc-quota 是用量计量服务（04 §6）：消费网关/管道上报的计量增量，
// 累加到 Redis 计数器 `quota:{pid}:{metric}:{yyyymmdd}`。
//
// 用法（devbox 内）：
//
//	go run ./cmd/svc-quota \
//	  -nats-url nats://100.64.0.3:28222 -redis-url redis://100.64.0.3:28637/0
//
// 语义：计量是**最终一致（分钟级）** 的加速层（02 §5.3），因此
//   - 非法上报 → 计数后 ACK 释放（毒消息不重投）；
//   - Redis 不可用 → NAK 重投（恢复后补上计数，不丢用量）。
//
// PG 是计量事实账本，Redis 是 7 天快速计数层；服务启动和每小时执行一次两侧对账。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/metering"
	"github.com/SNCIC/odoo20iot/internal/natsjs"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/quota"
)

type alertPublisher struct {
	js    nats.JetStreamContext
	store *quota.PGStore
}

func (p alertPublisher) publishPending(ctx context.Context, logger *slog.Logger) {
	alerts, err := p.store.PendingAlerts(ctx)
	if err != nil {
		logger.Error("读取待投递配额告警失败", "error", err)
		return
	}
	for _, alert := range alerts {
		data, err := alert.Event().Data()
		if err != nil {
			logger.Error("编码配额告警失败", "error", err)
			continue
		}
		msg := nats.NewMsg(alert.Event().Subject())
		msg.Data = data
		msg.Header.Set(nats.MsgIdHdr, fmt.Sprintf("quota:%d:%s:%s:%s", alert.ProjectID, alert.Metric, alert.Level, alert.WindowStart.UTC().Format(time.RFC3339Nano)))
		if _, err := p.js.PublishMsg(msg, nats.Context(ctx)); err != nil {
			logger.Error("发布配额告警失败", "project_id", alert.ProjectID, "metric", alert.Metric, "error", err)
			continue
		}
		if err := p.store.MarkAlertPublished(ctx, alert); err != nil {
			logger.Error("标记配额告警已投递失败", "project_id", alert.ProjectID, "metric", alert.Metric, "error", err)
			continue
		}
		logger.Info("配额告警已发布", "project_id", alert.ProjectID, "metric", alert.Metric, "level", alert.Level)
	}
}

type config struct {
	natsURL           string
	redisURL          string
	pgDSN             string
	stream            string
	subject           string
	durable           string
	counterTTL        time.Duration
	fetchBatch        int
	ackWait           time.Duration
	consumerInactive  time.Duration
	reconcileInterval time.Duration
	logJSON           bool
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nsvc-quota 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.natsURL, "nats-url", "nats://100.64.0.3:28222", "NATS JetStream 地址")
	flag.StringVar(&cfg.redisURL, "redis-url", "redis://100.64.0.3:28637/0", "Redis 地址（用量计数器）")
	flag.StringVar(&cfg.pgDSN, "pg-dsn", envOrDefault("IOT_PG_DSN", pg.DefaultDSN), "PG 用量事实账本 DSN（默认开发库，可由 IOT_PG_DSN 覆盖）")
	flag.StringVar(&cfg.stream, "stream", "IOT_QUOTA", "计量 Stream 名称（须与网关上一致）")
	flag.StringVar(&cfg.subject, "subject", "iot.quota.usage", "消费的 subject")
	flag.StringVar(&cfg.durable, "durable", "svc-quota", "durable consumer 名")
	flag.DurationVar(&cfg.counterTTL, "counter-ttl", quota.DefaultCounterTTL, "计数器保留时长（02 §5.2：7d）")
	flag.IntVar(&cfg.fetchBatch, "fetch-batch", 256, "单次拉取的消息数")
	flag.DurationVar(&cfg.ackWait, "ack-wait", 30*time.Second, "总线等待 ACK 的上限")
	flag.DurationVar(&cfg.consumerInactive, "consumer-inactive", 24*time.Hour,
		"消费者空闲回收阈值（须 ≥ 流保留时长，见 internal/natsjs）")
	flag.DurationVar(&cfg.reconcileInterval, "reconcile-interval", time.Hour, "PG/Redis 用量对账周期；0 表示关闭")
	flag.BoolVar(&cfg.logJSON, "log-json", true, "日志输出为 JSON")
	flag.Parse()
	return cfg
}

func newLogger(json bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if json {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func run(cfg config) error {
	logger := newLogger(cfg.logJSON)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1) Redis：用量计数器（可丢的加速层，见包注释）。
	redisOpts, err := redis.ParseURL(cfg.redisURL)
	if err != nil {
		return fmt.Errorf("解析 Redis 地址: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("连接 Redis: %w", err)
	}

	// 2) NATS。
	nc, err := nats.Connect(cfg.natsURL, nats.Name("svc-quota"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("连接 NATS: %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("初始化 JetStream: %w", err)
	}
	// 3) 聚合器。
	metrics := new(quota.Metrics)
	agg := quota.NewAggregator(quota.NewRedisCounter(rdb, cfg.counterTTL), metrics, logger)
	if cfg.pgDSN != "" {
		pgPool, err := pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
		if err != nil {
			return fmt.Errorf("连接 PG 用量事实库: %w", err)
		}
		defer pgPool.Close()
		pending, err := pendingMigrations(ctx, pgPool)
		if err != nil {
			return fmt.Errorf("检查 PG 用量事实库迁移: %w", err)
		}
		if len(pending) > 0 {
			return fmt.Errorf("PG 用量事实库有未应用迁移: %v", pending)
		}
		pgStore, err := quota.NewPGStore(pgPool)
		if err != nil {
			return err
		}
		agg.WithDurableStore(pgStore)
		publisher := alertPublisher{js: js, store: pgStore}
		go func() {
			publisher.publishPending(ctx, logger)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					publisher.publishPending(ctx, logger)
				}
			}
		}()
		logger.Info("PG 用量事实账本已启用")
		if cfg.reconcileInterval > 0 {
			go reconcileLoop(ctx, pgStore, quota.NewRedisCounter(rdb, cfg.counterTTL), cfg.reconcileInterval, logger)
		}
	}

	// 4) 消费。
	//
	// ⚠️ 必须走 natsjs.Subscribe，且退出时**不要** Unsubscribe：对 JetStream 订阅
	// 调用它会**删除消费者**，重启后从头投递 —— 流里全是历史计量上报，等于把用量
	// 重放一遍再累加，计数器被多算（见 internal/natsjs 的说明与 §6 坑 42/43）。
	sub, err := natsjs.Subscribe(js, natsjs.Options{
		Subject:  cfg.subject,
		Durable:  cfg.durable,
		Stream:   cfg.stream,
		AckWait:  cfg.ackWait,
		Inactive: cfg.consumerInactive,
	})
	if err != nil {
		return err
	}

	logger.Info("svc-quota 已就绪",
		"subject", cfg.subject, "stream", cfg.stream, "durable", cfg.durable,
		"counter_ttl", cfg.counterTTL, "consumer_inactive", cfg.consumerInactive,
		"version", buildinfo.Version, "commit", buildinfo.Commit)

	consumeLoop(ctx, sub, agg, cfg, logger)

	logger.Info("已退出",
		"reports", metrics.ReportsTotal.Load(),
		"counters", metrics.CountersTotal.Load(),
		"poison", metrics.PermanentTotal.Load(),
		"errors", metrics.Errors.Load())
	return nil
}

func reconcileLoop(ctx context.Context, store *quota.PGStore, redisStore *quota.RedisCounter, interval time.Duration, logger *slog.Logger) {
	check := func() {
		var projects []int64
		rows, err := storeProjects(ctx, store)
		if err != nil {
			logger.Error("枚举计量租户失败", "error", err)
			return
		}
		projects = rows
		metrics := []string{metering.MetricMsgCount, metering.MetricConnPeak, metering.MetricDeviceCount, metering.MetricStorageBytes, metering.MetricAPICalls}
		// 只修正已关闭的昨天窗口，当前窗口继续只观测，避免覆盖并发写入。
		closedDay := time.Now().UTC().AddDate(0, 0, -1)
		for _, projectID := range projects {
			for _, metric := range metrics {
				result, err := store.ReconcileDay(ctx, redisStore, projectID, metric, closedDay)
				if err != nil {
					logger.Error("计量对账失败", "project_id", projectID, "metric", metric, "error", err)
					continue
				}
				if result.Delta != 0 {
					logger.Warn("PG/Redis 计量不一致", "project_id", projectID, "metric", metric, "pg", result.PGTotal, "redis", result.RedisTotal, "delta", result.Delta)
					if err := redisStore.ReplaceDaily(ctx, projectID, metric, closedDay, result.PGTotal, quota.DefaultCounterTTL); err != nil {
						logger.Error("自动修正 Redis 计量失败", "project_id", projectID, "metric", metric, "error", err)
					} else {
						logger.Info("已按 PG 事实值修正 Redis 计量", "project_id", projectID, "metric", metric, "day", result.Day, "value", result.PGTotal)
					}
				}
			}
		}
	}
	check()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

func storeProjects(ctx context.Context, store *quota.PGStore) ([]int64, error) {
	return store.Projects(ctx)
}

func pendingMigrations(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	migrations, err := pg.LoadMigrations()
	if err != nil {
		return nil, err
	}
	applied, err := pg.AppliedVersions(ctx, pool)
	if err != nil {
		return nil, err
	}
	var pending []string
	for _, migration := range migrations {
		if _, ok := applied[migration.Version]; !ok {
			pending = append(pending, migration.Version)
		}
	}
	return pending, nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// nakDelay 是可重试错误的退避重投间隔。
const nakDelay = 500 * time.Millisecond

func consumeLoop(ctx context.Context, sub *natsjs.Subscription, agg *quota.Aggregator, cfg config, logger *slog.Logger) {
	for {
		if ctx.Err() != nil {
			return
		}

		msgs, err := sub.Fetch(cfg.fetchBatch, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.Canceled) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			logger.Warn("拉取消息失败", "error", err)
			continue
		}

		for _, m := range msgs {
			if err := agg.Apply(ctx, m.Data); err != nil {
				if errors.Is(err, quota.ErrPermanent) {
					// 毒消息：计数后释放，不重投。
					logger.Warn("释放毒消息（不重投）", "error", err)
					_ = m.Ack()
					continue
				}
				// Redis 不可用等：重投以补上计数。
				_ = m.NakWithDelay(nakDelay)
				continue
			}
			_ = m.Ack()
		}
	}
}
