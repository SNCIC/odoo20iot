// Command iot-gateway 是 MQTT 接入网关。
//
// 当前覆盖的能力（Phase 0 验证 + Phase 1 首批）：
//   - A2：QoS1 确认时序 —— 等 NATS 确认持久化后再回 PUBACK（internal/gateway）；
//   - A1：三档设备认证 + 物模型驱动 ACL（internal/auth）；
//   - A3：显式配置节点 ID 后启用跨节点路由与 Redis 在线位置/离线游标。
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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/SNCIC/odoo20iot/internal/auth"
	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/cluster"
	"github.com/SNCIC/odoo20iot/internal/gateway"
	"github.com/SNCIC/odoo20iot/internal/metering"
	"github.com/SNCIC/odoo20iot/internal/pg"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP 监听地址（健康检查与指标端点）")
	mqttAddr := flag.String("mqtt-addr", ":1883", "MQTT 监听地址")
	natsURL := flag.String("nats-url", "nats://100.64.0.3:28222", "NATS JetStream 地址")
	clusterNodeID := flag.String("cluster-node-id", "", "A3 集群节点 ID；为空则关闭集群路由")
	clusterPeers := flag.String("cluster-peers", "", "A3 对端节点 ID，逗号分隔")
	redisURL := flag.String("redis-url", "redis://100.64.0.3:28637/0", "A3 集群 Redis 地址")
	natsStream := flag.String("nats-stream", "IOT_TELEMETRY", "遥测 Stream 名称（Phase 0 占位，待与 03 对齐）")
	deviceEventsStream := flag.String("device-events-stream", "IOT_DEVICE_EVENTS", "设备生命周期事件 Stream 名称")
	natsSubjects := flag.String("nats-subjects", "iot.telemetry.>", "遥测 Stream 捕获的 subject，逗号分隔")
	pubackTimeout := flag.Duration("puback-timeout", gateway.DefaultPubackTimeout, "等待 PublishAck 的上限（超时则不回 PUBACK）")
	project := flag.String("project", "spike", "**占位**租户标识：真实租户投影依赖 A1 的凭据注册表")
	shards := flag.Int("shards", gateway.DefaultShards, "总线分片数（编译期常量，见 P0-3）")
	meterWindow := flag.Duration("metering-window", metering.DefaultWindow, "计量上报窗口（04 §6：每 10s 批量上报）")

	authFile := flag.String("auth-file", "tmp/dev-credentials.json", "设备凭据文件（L1 之外的凭据来源；仅开发/PoC）")
	authSource := flag.String("auth-source", "file", "设备凭据来源：file 或 pg")
	pgDSN := flag.String("pg-dsn", pg.DefaultDSN, "auth-source=pg 时的业务库 DSN")
	allowAnonymous := flag.Bool("allow-anonymous", false, "⚠️ 放行全部连接（仅本地冒烟；生产绝不可开）")
	allowProjectMode := flag.Bool("allow-project-mode", false, "允许 A 档（项目级共享凭据）；ADR-008 要求显式开启")
	maxVerify := flag.Int("auth-max-concurrent", auth.DefaultPolicy().MaxConcurrentVerify, "Argon2 并发校验上限（按内存带宽定，见 03 §2.1.1）")
	genAuth := flag.String("gen-auth-file", "", "生成一份演示凭据文件后退出（值为文件路径；会打印一次明文 secret）")

	logJSON := flag.Bool("log-json", true, "日志输出为 JSON（生产）；false 为开发可读格式")
	flag.Parse()

	logger, err := newLogger(*logJSON)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化日志失败: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	if *genAuth != "" {
		if err := generateDemoCredentials(*genAuth); err != nil {
			logger.Fatal("生成演示凭据失败", zap.Error(err))
		}
		return
	}

	// 网关层（含 mochi-mqtt）使用 log/slog：mochi-mqtt 的 Hook 接口就是 slog 签名，
	// 用它可以让 broker 内部日志与业务日志共用同一条流水线。
	gwLog := newSlogLogger(*logJSON)

	metrics := new(gateway.Metrics)

	var authPoolCloser func()
	var authPool *pgxpool.Pool
	if *authSource == "pg" {
		authPool, err = pg.Open(context.Background(), pg.Config{DSN: *pgDSN})
		if err != nil {
			logger.Fatal("打开认证数据库失败", zap.Error(err))
		}
		authPoolCloser = func() { authPool.Close() }
	}
	authenticator, err := buildAuthenticator(*authSource, *authFile, authPool, *allowAnonymous, *allowProjectMode, *maxVerify, logger)
	if err != nil {
		logger.Fatal("初始化认证失败", zap.Error(err))
	}
	if authPoolCloser != nil {
		defer authPoolCloser()
	}

	// 总线不可达即拒绝启动：一个无法确认持久化的网关只会静默丢数据，
	// 而「不回 PUBACK」在设备侧表现为重传风暴 —— 两者都不该被掩盖。
	pub, err := gateway.NewNATSPublisher(*natsURL, *natsStream)
	if err != nil {
		logger.Fatal("连接 NATS 失败", zap.Error(err))
	}
	defer func() { _ = pub.Close() }()

	if err := pub.EnsureStream(gateway.StreamSpec{
		Subjects: strings.Split(*natsSubjects, ","),
		Replicas: 1,
	}); err != nil {
		logger.Fatal("确保遥测 Stream 存在失败", zap.Error(err))
	}
	lifecyclePub, err := gateway.NewNATSPublisher(*natsURL, *deviceEventsStream)
	if err != nil {
		logger.Fatal("连接设备生命周期事件通道失败", zap.Error(err))
	}
	defer func() { _ = lifecyclePub.Close() }()
	if err := lifecyclePub.EnsureStream(gateway.StreamSpec{Subjects: []string{"iot.device.>"}, Replicas: 1}); err != nil {
		logger.Fatal("确保设备生命周期 Stream 存在失败", zap.Error(err))
	}

	var node *cluster.Node
	if *clusterNodeID != "" {
		redisOpts, err := redis.ParseURL(*redisURL)
		if err != nil {
			logger.Fatal("解析集群 Redis 地址失败", zap.Error(err))
		}
		rdb := redis.NewClient(redisOpts)
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			_ = rdb.Close()
			logger.Fatal("连接集群 Redis 失败", zap.Error(err))
		}
		defer func() { _ = rdb.Close() }()

		node, err = cluster.New(context.Background(), cluster.Options{
			ID:      *clusterNodeID,
			Peers:   splitNonEmpty(*clusterPeers),
			NATSURL: *natsURL,
			Cursor:  cluster.NewRedisCursor(rdb, "gw:offline:cursor"),
			Logger:  gwLog,
		}, cluster.NewRedisLocator(rdb, "gw:client", cluster.DefaultLocatorTTL))
		if err != nil {
			logger.Fatal("初始化 A3 集群节点失败", zap.Error(err))
		}
		defer node.Close()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 计量（04 §6）：本地累加 + 按窗口批量上报。
	// 用独立 Stream 捕获 `iot.quota.usage`，与遥测流分离（计量数据不进遥测流）。
	meterAcc := metering.NewAccumulator()
	quotaPub, err := gateway.NewNATSPublisher(*natsURL, "IOT_QUOTA")
	if err != nil {
		logger.Fatal("连接计量上报通道失败", zap.Error(err))
	}
	if err := quotaPub.EnsureStream(gateway.StreamSpec{
		Subjects: []string{"iot.quota.>"},
		Replicas: 1,
	}); err != nil {
		logger.Fatal("确保计量 Stream 存在失败", zap.Error(err))
	}
	meterReporter, err := metering.NewReporter(metering.ReporterOptions{
		Accumulator: meterAcc,
		Publisher:   quotaPub,
		NodeID:      *clusterNodeID,
		Window:      *meterWindow,
		Logger:      gwLog,
	})
	if err != nil {
		logger.Fatal("构造计量上报器失败", zap.Error(err))
	}
	go meterReporter.Run(ctx)

	broker, err := gateway.New(ctx, gateway.Options{
		MQTTAddr:                 *mqttAddr,
		Publisher:                pub,
		DeviceLifecyclePublisher: lifecyclePub,
		Router:                   gateway.ContractRouter{Project: *project, Shards: *shards},
		PubackTimeout:            *pubackTimeout,
		Metrics:                  metrics,
		Log:                      gwLog,
		Authenticator:            authenticator,
		AllowAnonymous:           *allowAnonymous,
		Cluster:                  node,
		Meter:                    meterAcc,
	})
	if err != nil {
		logger.Fatal("启动 MQTT 接入失败", zap.Error(err))
	}
	if node != nil {
		if err := node.Start(broker.ClusterHook()); err != nil {
			_ = broker.Close()
			logger.Fatal("启动 A3 集群路由失败", zap.Error(err))
		}
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
			zap.Bool("auth_enabled", authenticator != nil),
			zap.Bool("project_mode_allowed", *allowProjectMode),
			zap.Int("auth_max_concurrent", *maxVerify),
			zap.Duration("puback_timeout", *pubackTimeout),
			zap.Int("shards", *shards),
			zap.String("version", buildinfo.Version),
			zap.String("commit", buildinfo.Commit),
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
	// 上报器已在上面的 ctx 取消时补发了最后一个窗口，这里释放发布通道。
	if err := quotaPub.Close(); err != nil {
		logger.Error("关闭计量上报通道失败", zap.Error(err))
	}
	logger.Info("已退出",
		zap.Int64("meter_reports", meterReporter.Metrics().ReportsTotal.Load()),
		zap.Int64("meter_counters", meterReporter.Metrics().CountersReported.Load()))
}

// buildAuthenticator 组装认证器。放行匿名时必须由部署者**显式**声明，
// 不能让「忘记配凭据文件」静默退化成「谁都能连」。
func buildAuthenticator(source, file string, pool *pgxpool.Pool, anonymous, allowProjectMode bool, maxVerify int, logger *zap.Logger) (*auth.Authenticator, error) {
	if anonymous {
		logger.Warn("⚠️ 已放行全部连接（-allow-anonymous）：此模式不得用于任何非本地环境")
		return nil, nil
	}

	var dir auth.Directory
	var size int
	if source == "pg" {
		if pool == nil {
			return nil, fmt.Errorf("auth-source=pg 需要业务库连接")
		}
		pgDir, err := auth.NewPGDirectory(pool)
		if err != nil {
			return nil, err
		}
		dir = pgDir
	} else {
		fileDir, err := auth.LoadFile(file)
		if err != nil {
			return nil, fmt.Errorf("%w\n提示：用 -gen-auth-file=%s 生成一份演示凭据，或改用 auth-source=pg", err, file)
		}
		dir, size = fileDir, fileDir.Size()
	}

	policy := auth.DefaultPolicy()
	policy.AllowProjectMode = allowProjectMode
	if maxVerify > 0 {
		policy.MaxConcurrentVerify = maxVerify
	}

	metrics := new(auth.Metrics)
	a := auth.NewAuthenticator(
		// L1 缓存包在文件目录之外；生产替换为 Redis/svc-auth 时这一层不变。
		auth.NewCachedDirectory(dir, auth.WithCacheTTL(auth.DefaultCacheTTL)),
		policy, metrics)

	logger.Info("认证已启用",
		zap.String("auth_source", source),
		zap.String("auth_file", file),
		zap.Int("devices", size),
		zap.Bool("project_mode", allowProjectMode),
		zap.Int("max_concurrent_verify", policy.MaxConcurrentVerify),
		zap.String("argon2", auth.DefaultParams.String()))
	return a, nil
}

// generateDemoCredentials 生成一份演示凭据文件，并把明文 secret 打印一次。
func generateDemoCredentials(path string) error {
	token, err := auth.GenerateSecret()
	if err != nil {
		return err
	}
	key, err := auth.GenerateSecret()
	if err != nil {
		return err
	}

	plain, err := auth.WriteCredentials(path, []auth.DeviceCredential{
		{DeviceKey: "dev-demo-perdevice", ProjectID: 1, DeviceTypeID: 55, Mode: auth.ModePerDevice},
		{DeviceKey: "dev-demo-gateway", ProjectID: 1, DeviceTypeID: 60, Mode: auth.ModePerDevice, IsGateway: true},
		{DeviceKey: "dev-demo-project", ProjectID: 1, DeviceTypeID: 55, Mode: auth.ModeProject,
			ProjectToken: token, ProjectKey: key},
	}, auth.DefaultParams)
	if err != nil {
		return err
	}

	fmt.Printf("已生成 %s（文件中只存摘要，明文仅在此展示一次）：\n\n", path)
	keys := make([]string, 0, len(plain))
	for k := range plain {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-38s %s\n", k, plain[k])
	}
	fmt.Printf("\n用法（B 档设备级）：\n  clientId=dev-demo-perdevice\n  username=dev-demo-perdevice\n  password=<上面对应的 secret>\n")
	return nil
}

// newLogger 按配置构造 zap 实例。
//
// 构造失败时返回错误，由调用方终止启动 —— 一个没有日志输出的服务不应被允许运行。
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
// 与 zap 并行存在属于临时状态：一旦统一口径，二者应合并为一条流水线；
// 此处不做「假装统一」的适配层。
func newSlogLogger(jsonFormat bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if jsonFormat {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func splitNonEmpty(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
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
