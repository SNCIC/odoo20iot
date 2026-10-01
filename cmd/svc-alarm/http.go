package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/nats-io/nats.go"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// routes 组装健康检查与指标。
//
// 存活与就绪**必须分开**：把 PG 探活放进 /healthz，会让 PG 抖一下
// 就把所有副本重启一遍 —— 而重启解决不了 PG 的问题，只会把状态机
// 的扫描一起打断。
func routes(app *agent, js nats.JetStreamContext, cfg config) http.Handler {
	mux := http.NewServeMux()

	// 存活：进程还在就 200，不依赖 PG / NATS。
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "alive"})
	})

	// 就绪：真打 PG，并检查**迁移是否已应用**。
	//
	// 查迁移不是多此一举：schema 落后时服务"能连上库"，但每一轮扫描都会炸，
	// 而 /readyz 却报健康 —— 那是最误导人的一种状态。
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), cfg.readyProbe)
		defer cancel()

		if err := app.pool.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "PG_UNREACHABLE", "error": err.Error(),
			})
			return
		}

		pending, err := pendingMigrations(ctx, app.pool)
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

	// 指标：业务口径（04 §2.2.1）与服务健康并排暴露。
	// §2.2.1 明确要求「不是只有技术指标」—— 只报 P99 不报误报率，
	// 会出现「系统很快但没人看告警」的失败形态。
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		m, c := app.metrics, app.counts
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		fmt.Fprintf(w, "alarm_scans_total %d\n", c.Scans.Load())
		fmt.Fprintf(w, "alarm_scan_errors_total %d\n", c.ScanErrors.Load())
		fmt.Fprintf(w, "alarm_scan_not_leader_total %d\n", c.ScanSkipped.Load())
		fmt.Fprintf(w, "alarm_triggers_total %d\n", c.Triggers.Load())
		fmt.Fprintf(w, "alarm_trigger_errors_total %d\n", c.TriggerErrors.Load())
		fmt.Fprintf(w, "alarm_poison_triggers_total %d\n", c.PoisonTriggers.Load())
		fmt.Fprintf(w, "alarm_events_published_total %d\n", c.Published.Load())
		fmt.Fprintf(w, "alarm_publish_errors_total %d\n", c.PublishErrors.Load())
		fmt.Fprintf(w, "alarm_events_republished_total %d\n", c.Republished.Load())

		fmt.Fprintf(w, "alarm_total %d\n", m.Total.Load())
		fmt.Fprintf(w, "alarm_notified_total %d\n", m.Notified.Load())
		fmt.Fprintf(w, "alarm_suppressed_total %d\n", m.Suppressed.Load())
		fmt.Fprintf(w, "alarm_resolved_total %d\n", m.Resolved.Load())
		fmt.Fprintf(w, "alarm_closed_total %d\n", m.Closed.Load())
		fmt.Fprintf(w, "alarm_batched_total %d\n", m.Batched.Load())
		fmt.Fprintf(w, "alarm_reopened_total %d\n", m.Reopened.Load())
		fmt.Fprintf(w, "alarm_storms_total %d\n", m.Storms.Load())

		// 业务口径（04 §2.2.1）。
		fmt.Fprintf(w, "alarm_effective_ratio %.4f\n", m.EffectiveRate())
		fmt.Fprintf(w, "alarm_ticket_conversion_ratio %.4f\n", m.TicketConversion())
		fmt.Fprintf(w, "alarm_merge_ratio %.4f\n", m.MergeRate())
		fmt.Fprintf(w, "alarm_avg_confirm_delay_seconds %d\n", int64(m.AvgConfirmDelay().Seconds()))
	})

	return mux
}
