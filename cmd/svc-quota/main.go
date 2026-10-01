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
// ⚠️ 本轮范围（Phase 0 计量埋点原型）：只做 Redis 计数器累加。
// 文档 §6 的「每分钟落 PG（按 (metric, ts_minute) 幂等）+ 每小时对账」是后续工作。
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

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/quota"
)

type config struct {
	natsURL    string
	redisURL   string
	stream     string
	subject    string
	durable    string
	counterTTL time.Duration
	fetchBatch int
	ackWait    time.Duration
	logJSON    bool
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
	flag.StringVar(&cfg.stream, "stream", "IOT_QUOTA", "计量 Stream 名称（须与网关上一致）")
	flag.StringVar(&cfg.subject, "subject", "iot.quota.usage", "消费的 subject")
	flag.StringVar(&cfg.durable, "durable", "svc-quota", "durable consumer 名")
	flag.DurationVar(&cfg.counterTTL, "counter-ttl", quota.DefaultCounterTTL, "计数器保留时长（02 §5.2：7d）")
	flag.IntVar(&cfg.fetchBatch, "fetch-batch", 256, "单次拉取的消息数")
	flag.DurationVar(&cfg.ackWait, "ack-wait", 30*time.Second, "总线等待 ACK 的上限")
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

	// 4) 消费。
	sub, err := js.PullSubscribe(cfg.subject, cfg.durable,
		nats.BindStream(cfg.stream),
		nats.ManualAck(),
		nats.AckWait(cfg.ackWait),
		nats.MaxDeliver(-1),
	)
	if err != nil {
		return fmt.Errorf("订阅 %s（stream=%s）: %w", cfg.subject, cfg.stream, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	logger.Info("svc-quota 已就绪",
		"subject", cfg.subject, "stream", cfg.stream, "durable", cfg.durable,
		"counter_ttl", cfg.counterTTL,
		"version", buildinfo.Version, "commit", buildinfo.Commit)

	consumeLoop(ctx, sub, agg, cfg, logger)

	logger.Info("已退出",
		"reports", metrics.ReportsTotal.Load(),
		"counters", metrics.CountersTotal.Load(),
		"poison", metrics.PermanentTotal.Load(),
		"errors", metrics.Errors.Load())
	return nil
}

// nakDelay 是可重试错误的退避重投间隔。
const nakDelay = 500 * time.Millisecond

func consumeLoop(ctx context.Context, sub *nats.Subscription, agg *quota.Aggregator, cfg config, logger *slog.Logger) {
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
