// Command svc-pipeline 是消息管道服务（03 §4）：消费网关发布的设备上报，
// 经物模型解析后攒批写入 GreptimeDB。
//
// 用法（devbox 内）：
//
//	go run ./cmd/svc-pipeline \
//	  -nats-url nats://100.64.0.3:28222 \
//	  -dsn 'postgres://greptime:greptime@100.64.0.3:28403/public'
//
// 语义（03 §4.3，**落库后 ACK**）：
//   - 消息只有在**所属批次持久化成功**后才 ACK（ACK 延迟 = 攒批窗口 ≤ 200ms）；
//   - 批次落库失败 → NAK 重投（至少一次，宁可重复也不丢）；
//   - 毒消息（解析失败）→ 计数后 ACK 释放，不重投（重复投递坏报文只会堵住队列）。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/pipeline"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
	"github.com/SNCIC/odoo20iot/internal/tsdb/greptimedb"
)

type config struct {
	natsURL       string
	redisURL      string
	stream        string
	subject       string
	durable       string
	dsn           string
	batchSize     int
	batchWait     time.Duration
	fetchBatch    int
	concurrency   int
	maxAckPending int
	ackWait       time.Duration
	shutdownWait  time.Duration
	logJSON       bool
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nsvc-pipeline 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.natsURL, "nats-url", "nats://100.64.0.3:28222", "NATS JetStream 地址")
	flag.StringVar(&cfg.redisURL, "redis-url", "redis://100.64.0.3:28637/0", "Redis 地址（两阶段幂等标记）")
	flag.StringVar(&cfg.stream, "stream", "IOT_TELEMETRY", "遥测 Stream 名称（须与网关一致）")
	flag.StringVar(&cfg.subject, "subject", "iot.telemetry.>", "消费的 subject（覆盖全部分片）")
	flag.StringVar(&cfg.durable, "durable", "svc-pipeline", "durable consumer 名（分片消费时为每分片独立名）")
	flag.StringVar(&cfg.dsn, "dsn", "postgres://greptime:greptime@100.64.0.3:28403/public", "GreptimeDB PG wire DSN")
	flag.IntVar(&cfg.batchSize, "batch-size", pipeline.DefaultBatchSize, "攒批行数上限（03 §4.2：1000）")
	flag.DurationVar(&cfg.batchWait, "batch-wait", pipeline.DefaultBatchWait, "攒批等待上限（03 §4.2：200ms）")
	flag.IntVar(&cfg.fetchBatch, "fetch-batch", 512, "单次从总线拉取的消息数")
	flag.IntVar(&cfg.concurrency, "concurrency", 512, "单批并发处理的消息数（吞吐 ≈ concurrency / batch-wait）")
	flag.IntVar(&cfg.maxAckPending, "max-ack-pending", 2000, "在途未确认上限（03 §4.3：≥ 2 × batch-size）")
	flag.DurationVar(&cfg.ackWait, "ack-wait", 30*time.Second, "总线等待 ACK 的上限（03 §4.3：30s）")
	flag.DurationVar(&cfg.shutdownWait, "shutdown-wait", 10*time.Second, "优雅退出时 flush 剩余数据的上限")
	flag.BoolVar(&cfg.logJSON, "log-json", true, "日志输出为 JSON")
	flag.Parse()

	// 03 §4.3：MaxAckPending 必须 ≥ 2 × batchSize，否则吞吐被自身 ACK 窗口卡死。
	if min := 2 * cfg.batchSize; cfg.maxAckPending < min {
		fmt.Fprintf(os.Stderr, "警告：max-ack-pending(%d) < 2×batch-size(%d)，吞吐会被 ACK 窗口卡死，已自动提升\n",
			cfg.maxAckPending, min)
		cfg.maxAckPending = min
	}
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

	// 1) 存储：GreptimeDB（幂等建表，03 §4.2）。
	store, err := greptimedb.Open(ctx, cfg.dsn, 8)
	if err != nil {
		return fmt.Errorf("连接 GreptimeDB: %w", err)
	}
	defer store.Close()
	if err := store.CreateTable(ctx, tsdb.PlanJSON); err != nil {
		return fmt.Errorf("幂等建表失败: %w", err)
	}

	// 2) 幂等：Redis。两阶段标记必须跨实例共享（03 §4.3），
	//    否则「并发互斥」与「done 跳过」在多实例下都会失效。
	redisOpts, err := redis.ParseURL(cfg.redisURL)
	if err != nil {
		return fmt.Errorf("解析 Redis 地址: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("连接 Redis: %w", err)
	}

	// 3) 总线：NATS JetStream。
	nc, err := nats.Connect(cfg.natsURL, nats.Name("svc-pipeline"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("连接 NATS: %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("初始化 JetStream: %w", err)
	}

	// 4) 管道：解析器 + 攒批器（flush 即批量写库）。
	parser, err := pipeline.NewParser(tsdb.BenchMetrics)
	if err != nil {
		return fmt.Errorf("构造解析器: %w", err)
	}

	batcher := pipeline.NewBatcher[tsdb.Row](cfg.batchSize, cfg.batchWait,
		func(flushCtx context.Context, rows []tsdb.Row) error {
			return store.InsertRows(flushCtx, tsdb.PlanJSON, rows)
		})
	batcher.Start(ctx)

	metrics := new(pipeline.Metrics)
	handler := pipeline.NewHandler(parser, batcher, pipeline.NewRedisIdempotency(rdb), metrics, logger)

	// 5) 消费：durable pull consumer + 手动 ACK。
	sub, err := js.PullSubscribe(cfg.subject, cfg.durable,
		nats.BindStream(cfg.stream),
		nats.ManualAck(),
		nats.AckWait(cfg.ackWait),
		nats.MaxAckPending(cfg.maxAckPending),
		nats.MaxDeliver(-1), // 永不放弃：宁可重投也不能丢
	)
	if err != nil {
		return fmt.Errorf("订阅 %s（stream=%s）: %w", cfg.subject, cfg.stream, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	logger.Info("svc-pipeline 已就绪",
		"subject", cfg.subject, "stream", cfg.stream, "durable", cfg.durable,
		"batch_size", cfg.batchSize, "batch_wait", cfg.batchWait,
		"max_ack_pending", cfg.maxAckPending,
		"version", buildinfo.Version, "commit", buildinfo.Commit)

	consumeLoop(ctx, sub, handler, cfg, logger)

	// 5) 优雅退出：停止消费后 flush 剩余（03 §4.3）。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownWait)
	defer cancel()
	if err := batcher.Close(shutdownCtx); err != nil {
		logger.Error("关闭攒批器失败", "error", err)
	}
	logger.Info("已退出",
		"consumed", metrics.ConsumedTotal.Load(),
		"parsed", metrics.ParsedTotal.Load(),
		"poison", metrics.PermanentTotal.Load(),
		"duplicate", metrics.DuplicateTotal.Load(),
		"contended", metrics.ContendedTotal.Load(),
		"rows_written", metrics.RowsWritten.Load(),
		"flush_errors", metrics.FlushErrorTotal.Load(),
		"idem_errors", metrics.IdemErrorTotal.Load())
	return nil
}

// consumeLoop 是消费主循环：拉取 → 入批 → 等落库 → 确认。
func consumeLoop(ctx context.Context, sub *nats.Subscription, h *pipeline.Handler, cfg config, logger *slog.Logger) {
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
		processBatch(ctx, h, msgs, cfg.concurrency, logger)
	}
}

// nakDelay 是可重试错误的退避重投间隔（03 §4.3「NAK 延迟重试」）。
const nakDelay = 500 * time.Millisecond

// processBatch 并发处理一批消息，并按处理结果确认。
//
// 为什么必须并发：Handle 会阻塞到**所属批次落库**。若顺序处理，每条都要等
// 一个批次窗口（≤ batch-wait），吞吐被压到 1/batch-wait（≈5 msg/s，与 03 §4.2
// 的「单分片 ≥ 5000 msg/s」冲突）；并发入批让多条消息共享同一批次，
// 吞吐 ≈ concurrency / batch-wait。
func processBatch(ctx context.Context, h *pipeline.Handler, msgs []*nats.Msg, concurrency int, logger *slog.Logger) {
	if len(msgs) == 0 {
		return
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	results := make([]pipeline.Result, len(msgs))
	errs := make([]error, len(msgs))

	// 按下标写入不同槽位，无数据竞争。
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, m := range msgs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, m *nats.Msg) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i], errs[i] = h.Handle(ctx, m.Data)
		}(i, m)
	}
	wg.Wait()

	for i, m := range msgs {
		switch results[i] {
		case pipeline.ResultPersisted, pipeline.ResultDuplicate:
			// 已落库 / 重复交付：ACK。
			_ = m.Ack()
		case pipeline.ResultPoison:
			// 毒消息：计数已由 Handler 记入，这里只需释放（不重投）。
			logger.Warn("释放毒消息（不重投）", "error", errs[i])
			_ = m.Ack()
		case pipeline.ResultRetry:
			// 可重试：processing 占用或落库失败 → 延迟重投。
			_ = m.NakWithDelay(nakDelay)
		}
	}
}
