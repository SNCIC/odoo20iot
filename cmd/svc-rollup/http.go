package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// routes 组装健康检查与指标。
//
// 存活与就绪**分开**：把 GreptimeDB/PG 探活塞进 /healthz，会让依赖抖一下
// 就把服务重启一遍 —— 重启解决不了依赖的问题，只会把正在进行的物化打断。
func routes(a *app) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "alive"})
	})

	// 就绪：真打 PG（并把迁移是否已应用查清楚）+ GreptimeDB 探活。
	//
	// 查迁移不是多此一举：水位表不存在时服务「能连上库」，但每一轮物化都会炸，
	// 而 /readyz 却报健康 —— 那是最误导人的一种状态。
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.readyProbe)
		defer cancel()

		if err := a.pool.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "PG_UNREACHABLE", "error": err.Error(),
			})
			return
		}
		if pending, err := pendingMigrations(ctx, a.pool); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "MIGRATION_CHECK_FAILED", "error": err.Error(),
			})
			return
		} else if len(pending) > 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "MIGRATION_PENDING", "pending": pending,
			})
			return
		}
		if err := a.gres.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "GREPTIMEDB_UNREACHABLE", "error": err.Error(),
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "pg": "up-to-date", "greptimedb": "reachable",
		})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		fmt.Fprintf(w, "rollup_ticks_total %d\n", a.counts.Ticks.Load())
		fmt.Fprintf(w, "rollup_not_leader_total %d\n", a.counts.NotLeader.Load())
		fmt.Fprintf(w, "rollup_windows_total %d\n", a.counts.Windows.Load())
		fmt.Fprintf(w, "rollup_inserts_total %d\n", a.counts.Inserts.Load())
		fmt.Fprintf(w, "rollup_errors_total %d\n", a.counts.Errors.Load())

		// 水位与滞后：物化是否跟得上，看这两个数最直接。
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.readyProbe)
		defer cancel()
		now := time.Now()
		for _, g := range a.grans {
			wm, found, err := a.store.Get(ctx, string(g))
			if err != nil || !found {
				continue
			}
			fmt.Fprintf(w, "rollup_watermark_seconds{granularity=%q} %d\n", string(g), wm.WindowStart.Unix())
			fmt.Fprintf(w, "rollup_pending_seconds{granularity=%q} %d\n", string(g), int64(now.Sub(wm.WindowStart).Seconds()))
		}
	})

	return mux
}
