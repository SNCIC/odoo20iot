// Command iot-gateway 是 MQTT 接入网关。
//
// Phase 0 的目标是验证 A2（QoS1 PUBACK 时机，ADR-001 的关键定制点）：
// 网关必须在 NATS JetStream 确认持久化之后，才向设备回 PUBACK。
// 实现与判据见 internal/gateway。
//
// 尚未实现（刻意留白，不做假实现）：
//   - A1 三档设备认证：当前放行全部连接，见 gateway.New 中的说明；
//   - A3 集群路由：单节点，无跨节点投递；
//   - 会话持久化（CleanSession=false 的 inflight 窗口落 Redis）。
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

	"go.uber.org/zap"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/gateway"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP 监听地址（健康检查与指标端点）")
	mqttAddr := flag.String("mqtt-addr", ":1883", "MQTT 监听地址")
	natsURL := flag.String("nats-url", "nats://100.64.0.3:28222", "NATS JetStream 地址")
	natsStream := flag.String("nats-stream", "IOT_TELEMETRY", "遥测 Stream 名称（Phase 0 占位，待与 03 文档对齐）")
	natsSubjects := flag.String("nats-subjects", "iot.telemetry.>", "遥测 Stream 捕获的 subject，逗号分隔")
	pubackTimeout := flag.Duration("puback-timeout", gateway.DefaultPubackTimeout, "等待 PublishAck 的上限（超时则不回 PUBACK）")
	project := flag.String("project", "spike", "**占位**租户标识：A1 设备凭据注册表未实现，无法真正解析租户")
	shards := flag.Int("shards", gateway.DefaultShards, "总线分片数（编译期常量，见 P0-3）")
	logJSON := flag.Bool("log-json", true, "日志输出为 JSON（生产）；false 为开发可读格式")
	flag.Parse()

	logger, err := newLogger(*logJSON)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化日志失败: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	// 网关层（含 mochi-mqtt）使用 log/slog：mochi-mqtt 的 Hook 接口就是 slog 签名，
	// 用它可以让 broker 内部日志与业务日志共用同一条流水线。
	gwLog := newSlogLogger(*logJSON)

	// 总线不可达即拒绝启动：一个无法确认持久化的网关只会静默丢数据，
	// 而「不回 PUBACK」在设备侧表现为重传风暴 —— 两者都不该被掩盖。
	metrics := new(gateway.Metrics)
	pub, err := gateway.NewNATSPublisher(*natsURL, *natsStream)
	if err != nil {
		logger.Fatal("连接 NATS 失败", zap.Error(err))
	}
	defer func() { _ = pub.Close() }()

	if err := pub.EnsureStream(strings.Split(*natsSubjects, ","), 1); err != nil {
		logger.Fatal("确保遥测 Stream 存在失败", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker, err := gateway.New(ctx, gateway.Options{
		MQTTAddr:      *mqttAddr,
		Publisher:     pub,
		Router:        gateway.ContractRouter{Project: *project, Shards: *shards},
		PubackTimeout: *pubackTimeout,
		Metrics:       metrics,
		Log:           gwLog,
	})
	if err != nil {
		logger.Fatal("启动 MQTT 接入失败", zap.Error(err))
	}
	broker.Serve()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/metrics", metricsHandler(metrics))

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("iot-gateway 启动",
			zap.String("http_addr", *httpAddr),
			zap.String("mqtt_addr", broker.Addr()),
			zap.String("nats_url", *natsURL),
			zap.Duration("puback_timeout", *pubackTimeout),
			zap.Int("shards", *shards),
			zap.String("version", buildinfo.Version),
			zap.String("commit", buildinfo.Commit),
			zap.String("build_time", buildinfo.BuildTime),
			zap.String("runtime", buildinfo.Runtime()),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("HTTP 服务异常退出", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("收到退出信号，开始优雅关闭")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP 优雅关闭超时或失败", zap.Error(err))
	}
	// 关闭 broker 会取消在途的 PUBACK 等待：未确认的消息不回 PUBACK，
	// 由设备重传兜底 —— 这正是 A2 在关闭路径上的正确行为。
	if err := broker.Close(); err != nil {
		logger.Error("关闭 MQTT 接入失败", zap.Error(err))
	}
	logger.Info("已退出")
}

// newLogger 按配置构造 zap 实例。
//
// 构造失败时返回错误，由调用方终止启动 —— 一个没有日志输出的服务不应被允许运行，
// 因此这里不做静默降级。
func newLogger(jsonFormat bool) (*zap.Logger, error) {
	if jsonFormat {
		return zap.NewProduction()
	}
	cfg := zap.NewDevelopmentConfig()
	cfg.Encoding = "console"
	return cfg.Build()
}

// newSlogLogger 构造网关层使用的 slog 实例。
//
// 与 zap 并行存在属于 Phase 0 的临时状态：一旦团队确定统一口径，
// 二者应合并为一条流水线；此处不做「假装统一」的适配层。
func newSlogLogger(jsonFormat bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if jsonFormat {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func metricsHandler(m *gateway.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.WriteProm(w)
	}
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":     "ok",
		"version":    buildinfo.Version,
		"commit":     buildinfo.Commit,
		"build_time": buildinfo.BuildTime,
		"runtime":    buildinfo.Runtime(),
	})
}
