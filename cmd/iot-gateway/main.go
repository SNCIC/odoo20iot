// Command iot-gateway 是 MQTT 接入网关。
//
// Phase 0 阶段它只验证两件事：构建链路可用、/healthz 可被 Compose 健康检查探活。
// 内嵌 MQTT Broker（mochi-mqtt）与三档认证 Hook 在 A1 / A2 spike 中接入；
// A2（QoS1 PUBACK 时机）是 ADR-001 的定制点，不成立则需回退 EMQX。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP 监听地址（健康检查与后续指标端点）")
	logJSON := flag.Bool("log-json", true, "日志输出为 JSON（生产）；false 为开发可读格式")
	flag.Parse()

	logger := newLogger(*logJSON)
	defer func() { _ = logger.Sync() }()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("iot-gateway 启动",
			zap.String("http_addr", *httpAddr),
			zap.String("version", buildinfo.Version),
			zap.String("commit", buildinfo.Commit),
			zap.String("build_time", buildinfo.BuildTime),
			zap.String("runtime", buildinfo.Runtime()),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("HTTP 服务异常退出", zap.Error(err))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	logger.Info("收到退出信号，开始优雅关闭")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("优雅关闭超时或失败", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("已退出")
}

func newLogger(jsonFormat bool) *zap.Logger {
	if jsonFormat {
		return zap.NewProduction()
	}
	cfg := zap.NewDevelopmentConfig()
	cfg.Encoding = "console"
	l, err := cfg.Build()
	if err != nil {
		// 开发格式构建失败不应影响启动，退回生产格式。
		return zap.NewNop()
	}
	return l
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
