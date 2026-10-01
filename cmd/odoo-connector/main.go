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
// 尚未实现（如实留白，不做假实现）：
//   - 主数据增量拉取（每 60s 按 `write_date` 水位 + id 断点，§4.4）；
//   - 死信：`t_dlq` + 对象存储原文 + 按 entity_type 聚合告警（§4.3）；
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
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/connector"
	"github.com/SNCIC/odoo20iot/internal/odoo"
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

	reconcileModels     string
	reconcileInterval   time.Duration
	reconcileStaleAfter time.Duration
	reconcileBatch      int
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
	flag.StringVar(&cfg.redisURL, "redis-url", "redis://100.64.0.3:28637/0", "Redis 地址（幂等占位 / Outbox 流 / C-2 去重与水位）")
	flag.StringVar(&cfg.httpAddr, "http-addr", "127.0.0.1:18091", "健康检查/指标/webhook 监听地址")
	flag.StringVar(&cfg.logFormat, "log-format", "json", "日志格式：json（默认）或 text")
	flag.DurationVar(&cfg.readyProbe, "ready-probe-timeout", 5*time.Second, "就绪探针单次探测超时")

	flag.StringVar(&cfg.natsURL, "nats-url", "nats://100.64.0.3:28222", "NATS JetStream 地址（事件出口）")
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

	rdb := redis.NewClient(mustParseRedis(cfg.redisURL))
	defer func() { _ = rdb.Close() }()

	// 指标由四处共用：编排层、Outbox 消费、webhook、对账。
	metrics := new(connector.Metrics)

	conn, err := connector.New(client, connector.Options{
		Policy:  policy,
		Metrics: metrics,
		Logger:  logger,
		Guard:   connector.NewRedisGuard(rdb, connector.DefaultGuardTTL),
	})
	if err != nil {
		return fmt.Errorf("构造连接器: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	watermarks := connector.NewRedisWatermarks(rdb)

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
	reconciler, err := connector.NewReconciler(connector.ReconcileOptions{
		Caller:     conn,
		Publisher:  publisher,
		Watermarks: watermarks,
		Models:     splitNonEmpty(cfg.reconcileModels),
		Interval:   cfg.reconcileInterval,
		StaleAfter: cfg.reconcileStaleAfter,
		Batch:      cfg.reconcileBatch,
		Metrics:    metrics,
		Logger:     logger,
	})
	if err != nil {
		return fmt.Errorf("构造对账器: %w", err)
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

	errCh := make(chan error, 3)
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
