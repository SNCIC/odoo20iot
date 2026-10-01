package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/dlq"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// routes 组装健康检查与指标。
func routes(app *notifier, pool *pgxpool.Pool, cfg config) http.Handler {
	mux := http.NewServeMux()

	// 存活：进程还在就 200，不依赖 PG / NATS。
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "alive"})
	})

	// 就绪：真查 PG 并检查迁移。
	//
	// 通知服务在 schema 落后时「能连上库」，但每条失败的通知都**写不进死信** ——
	// 于是表现成「通知没送到、也没留下记录」，而 readyz 却报健康。
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), cfg.readyProbe)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "PG_UNREACHABLE", "error": err.Error(),
			})
			return
		}
		pending, err := pendingMigrations(ctx, pool)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "MIGRATION_CHECK_FAILED", "error": err.Error(),
			})
			return
		}
		if len(pending) > 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "MIGRATION_PENDING", "pending": pending,
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "pg": map[string]any{"reachable": true}, "migrations": "up-to-date",
		})
	})

	// 指标：投递结果 + 按通道的成功数 + 死信量。
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		m, c := app.metrics, app.counts
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		fmt.Fprintf(w, "notify_events_received_total %d\n", c.Received.Load())
		fmt.Fprintf(w, "notify_events_delivered_total %d\n", c.Delivered.Load())
		fmt.Fprintf(w, "notify_events_skipped_no_policy_total %d\n", c.Skipped.Load())
		fmt.Fprintf(w, "notify_poison_events_total %d\n", c.Poison.Load())
		fmt.Fprintf(w, "notify_events_failed_total %d\n", c.Errors.Load())

		fmt.Fprintf(w, "notify_sent_total %d\n", m.Sent.Load())
		fmt.Fprintf(w, "notify_failed_total %d\n", m.Failed.Load())
		fmt.Fprintf(w, "notify_retries_total %d\n", m.Retries.Load())
		fmt.Fprintf(w, "notify_degraded_total %d\n", m.Degraded.Load())
		fmt.Fprintf(w, "notify_partial_total %d\n", m.Partial.Load())
		fmt.Fprintf(w, "notify_dlq_total %d\n", m.DLQTotal.Load())

		// 按**实际送达的通道**计数：降级后的成功记在主通道名下，
		// 会让「主通道彻底挂了」在指标上被抹平。
		for ch, n := range m.Snapshot() {
			fmt.Fprintf(w, "notify_delivered_by_channel_total{channel=%q} %d\n", ch, n)
		}
	})

	// 死信量按 service 暴露：死信在涨是 P2 告警的触发条件（06 §），
	// 没有这个读数就只能靠人偶尔去翻表 —— 而没人会翻。
	mux.HandleFunc("/metrics/dlq", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), cfg.readyProbe)
		defer cancel()

		n, err := dlq.New(pool).CountSince(ctx, "svc-notify", time.Now().Add(-24*time.Hour))
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "service": "svc-notify", "window_hours": 24, "count": n,
		})
	})

	return mux
}
