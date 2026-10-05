// Command svc-modbus-gw 轮询 Modbus TCP/RTU 点位并发布统一设备遥测信封。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/gateway"
	"github.com/SNCIC/odoo20iot/internal/modbusgw"
	"github.com/SNCIC/odoo20iot/internal/pg"
)

type config struct {
	configPath   string
	configSource string
	pgDSN        string
	projectID    int64
	natsURL      string
	natsStream   string
	logFormat    string
	reloadEvery  time.Duration
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "svc-modbus-gw 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.configPath, "config", "deploy/modbus/example.json", "Modbus 文件配置 JSON")
	flag.StringVar(&cfg.configSource, "config-source", "file", "配置来源：file 或 db")
	flag.StringVar(&cfg.pgDSN, "pg-dsn", pg.DefaultDSN, "业务库 DSN（config-source=db）")
	flag.Int64Var(&cfg.projectID, "project-id", 0, "租户 ID（config-source=db 必填）")
	flag.StringVar(&cfg.natsURL, "nats-url", "nats://100.64.0.3:28222", "NATS 地址")
	flag.StringVar(&cfg.natsStream, "nats-stream", "IOT_TELEMETRY", "遥测 Stream")
	flag.StringVar(&cfg.logFormat, "log-format", "json", "日志格式：json 或 text")
	flag.DurationVar(&cfg.reloadEvery, "reload-interval", 30*time.Second, "DB 配置刷新周期；file 配置源忽略")
	flag.Parse()
	return cfg
}

func run(cfg config) error {
	logger := newLogger(cfg.logFormat)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configs, pool, err := loadConfigs(ctx, cfg)
	if err != nil {
		return err
	}
	if pool != nil {
		defer pool.Close()
	}
	if cfg.configSource == "file" && len(configs) == 0 {
		return fmt.Errorf("没有启用的 Modbus 配置")
	}

	pub, err := gateway.NewNATSPublisher(cfg.natsURL, cfg.natsStream)
	if err != nil {
		return err
	}
	defer pub.Close()
	if err := pub.EnsureStream(gateway.StreamSpec{Subjects: []string{"iot.telemetry.>"}, Replicas: 1}); err != nil {
		return err
	}

	var wg sync.WaitGroup
	if cfg.configSource == "db" {
		store, storeErr := modbusgw.NewStore(pool)
		if storeErr != nil {
			return storeErr
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runDynamicPollers(ctx, configs, cfg.projectID, store, pub, logger, cfg.reloadEvery, &wg)
		}()
	} else {
		for _, item := range configs {
			item := item
			wg.Add(1)
			go func() { defer wg.Done(); runPoller(ctx, item, pub, logger) }()
		}
	}
	logger.Info("svc-modbus-gw 已启动", "configs", len(configs), "config_source", cfg.configSource)
	<-ctx.Done()
	wg.Wait()
	return nil
}

type managedPoller struct {
	cfg    modbusgw.Config
	cancel context.CancelFunc
}

func runDynamicPollers(ctx context.Context, initial []modbusgw.Config, projectID int64, store *modbusgw.Store, pub *gateway.NATSPublisher, logger *slog.Logger, reloadEvery time.Duration, wg *sync.WaitGroup) {
	if reloadEvery <= 0 {
		reloadEvery = 30 * time.Second
	}
	active := make(map[string]managedPoller)
	apply := func(configs []modbusgw.Config) {
		desired := make(map[string]modbusgw.Config, len(configs))
		for _, item := range configs {
			desired[item.DeviceKey] = item
		}
		for key, running := range active {
			wanted, ok := desired[key]
			if ok && reflect.DeepEqual(running.cfg, wanted) {
				continue
			}
			running.cancel()
			delete(active, key)
		}
		for key, item := range desired {
			if _, ok := active[key]; ok {
				continue
			}
			pollCtx, cancel := context.WithCancel(ctx)
			active[key] = managedPoller{cfg: item, cancel: cancel}
			wg.Add(1)
			go func() { defer wg.Done(); runPoller(pollCtx, item, pub, logger) }()
		}
	}
	apply(initial)
	ticker := time.NewTicker(reloadEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			for _, running := range active {
				running.cancel()
			}
			return
		case <-ticker.C:
			configs, err := store.List(ctx, projectID)
			if err != nil {
				logger.Warn("刷新 Modbus 配置失败，沿用当前配置", "error", err)
				continue
			}
			apply(configs)
		}
	}
}

func loadConfigs(ctx context.Context, cfg config) ([]modbusgw.Config, *pgxpool.Pool, error) {
	switch cfg.configSource {
	case "file":
		raw, err := os.ReadFile(cfg.configPath)
		if err != nil {
			return nil, nil, err
		}
		var item modbusgw.Config
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, nil, err
		}
		item.Enabled = true
		normalized, err := modbusgw.NormalizeConfig(item)
		if err != nil {
			return nil, nil, err
		}
		return []modbusgw.Config{normalized}, nil, nil
	case "db":
		if cfg.projectID <= 0 {
			return nil, nil, fmt.Errorf("config-source=db 时 -project-id 必须为正数")
		}
		pool, err := pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
		if err != nil {
			return nil, nil, err
		}
		store, err := modbusgw.NewStore(pool)
		if err != nil {
			pool.Close()
			return nil, nil, err
		}
		items, err := store.List(ctx, cfg.projectID)
		if err != nil {
			pool.Close()
			return nil, nil, err
		}
		return items, pool, nil
	default:
		return nil, nil, fmt.Errorf("未知 config-source %q：只能是 file 或 db", cfg.configSource)
	}
}

func runPoller(ctx context.Context, cfg modbusgw.Config, pub *gateway.NATSPublisher, logger *slog.Logger) {
	for {
		reader, closer, err := openReader(cfg)
		if err == nil {
			err = modbusgw.Poll(ctx, cfg, reader, func(ctx context.Context, payload map[string]any) error {
				return publish(ctx, cfg, pub, payload)
			})
		}
		if closer != nil {
			_ = closer.Close()
		}
		if ctx.Err() != nil {
			return
		}
		logger.Warn("Modbus 轮询失败，稍后重试", "device_key", cfg.DeviceKey, "transport", cfg.Transport, "error", err)
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func openReader(cfg modbusgw.Config) (modbusgw.Reader, io.Closer, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	switch cfg.Transport {
	case "", "tcp":
		return &modbusgw.Client{Endpoint: cfg.Endpoint, Timeout: timeout}, nil, nil
	case "rtu":
		transport, err := modbusgw.OpenSerial(modbusgw.SerialConfig{Path: cfg.SerialPath, BaudRate: cfg.BaudRate, DataBits: cfg.DataBits, StopBits: cfg.StopBits, Parity: cfg.Parity})
		if err != nil {
			return nil, nil, err
		}
		return &modbusgw.RTUClient{Transport: transport, Timeout: timeout}, transport, nil
	default:
		return nil, nil, fmt.Errorf("不支持的 Modbus transport %q", cfg.Transport)
	}
}

func publish(ctx context.Context, cfg modbusgw.Config, pub *gateway.NATSPublisher, payload map[string]any) error {
	trace, err := envelope.NewTraceID()
	if err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	subject, err := (gateway.ContractRouter{Shards: gateway.DefaultShards}).RouteHTTPProject(cfg.DeviceKey, "telemetry", cfg.ProjectID)
	if err != nil {
		return err
	}
	deviceID := cfg.DeviceID
	if deviceID <= 0 {
		deviceID = envelope.PlaceholderDeviceID(cfg.DeviceKey)
	}
	data, err := (envelope.Envelope{SchemaVersion: envelope.CurrentSchemaVersion, TraceID: trace, ProjectID: cfg.ProjectID, DeviceKey: cfg.DeviceKey, DeviceID: deviceID, DeviceTypeID: cfg.DeviceTypeID, Stream: "telemetry", ReceivedAt: time.Now().UTC(), Payload: body}).Encode()
	if err != nil {
		return err
	}
	return pub.Publish(ctx, subject, data)
}

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}
