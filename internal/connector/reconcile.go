package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SNCIC/odoo20iot/internal/catalog"
)

// 对账（07 §4.4「定时对账（必做）」）的默认参数。
const (
	// DefaultReconcileInterval 是对账周期（§4.4：每 15 min）。
	DefaultReconcileInterval = 15 * time.Minute

	// DefaultReconcileStaleAfter 是判定「卡住」的宽限窗口。
	//
	// 判据来自「正常情况下多久就该被处理掉」：Odoo 侧 cron 每分钟投递一次、
	// 连接器侧的 PEL 重投窗口是 30s。取 15 min 意味着连续十几轮都没被处理掉，
	// 那就不再是抖动，而是真的漏了。
	DefaultReconcileStaleAfter = 15 * time.Minute

	// DefaultReconcileBatch 是单轮单来源的处理上限。
	DefaultReconcileBatch = 200

	// ReconcileAlertStreak 是触发告警的连续遗漏轮数（§4.4「对连续两次遗漏告警」）。
	//
	// 为什么要「连续」：单轮发现遗漏很可能就是正常抖动（webhook 正在重投、
	// cron 这一分钟刚好没跑）。连续两轮都还在，说明**补投本身没生效** ——
	// 那是配置或环境问题，人必须知道。
	ReconcileAlertStreak = 2

	gapKeyOutbox = "outbox"
)

// caller 是 Reconciler 对编排层的依赖（收窄接口，便于测试注入）。
//
// 对账必须**走编排层**：它同样是在打 Odoo，不能绕开限流与熔断 ——
// 否则「一对账就把 Odoo 打满」会是最讽刺的故障。
type caller interface {
	Call(ctx context.Context, req Request, out any) error
}

// ReconcileOptions 是 Reconciler 的构造参数。
type ReconcileOptions struct {
	Caller     caller
	Publisher  Publisher
	Watermarks Watermarks
	// Models 是要对账的 Odoo 模型（C-2 水位差扫描）。为空则跳过该扫描。
	Models       []string
	Interval     time.Duration
	StaleAfter   time.Duration
	Batch        int
	Metrics      *Metrics
	Logger       *slog.Logger
	Now          func() time.Time
	ExternalRefs interface {
		ReconcileUnbound(context.Context) (int, error)
	}
	Projects catalog.ProjectLister
}

// ReconcileResult 是单轮对账的统计。
type ReconcileResult struct {
	// ① Odoo `edge.outbox` 非终态行
	OutboxStale     int // 扫到的卡住行数
	OutboxDelivered int // 补投成功数
	OutboxDead      int // 其中处于死信状态的行数
	// ② C-2 水位差
	CursorModels  int // 发现水位差的模型数
	CursorRecords int // 补投的记录数
	// 告警
	Alerts int // 因「连续多轮仍有遗漏」触发的告警数
}

// Reconciler 实现 07 §4.4 的定时对账。
//
// 它补的是两类「静默遗漏」：
//   - ① **Odoo Outbox 非终态行**：cron 没跑、或重试全部耗尽卡在那里；
//   - ② **C-2 的 `write_date` 水位差**：webhook 不持久，丢了就没人知道 ——
//     对账拿「Odoo 实际的最新位置」和「我们处理到的位置」一比，差多少补多少。
//
// §4.4 的第 ③ 项（未绑定告警待办）依赖 `t_external_ref`，该表尚未建立，故未实现。
type Reconciler struct {
	caller       caller
	pub          Publisher
	watermarks   Watermarks
	models       []string
	interval     time.Duration
	staleAfter   time.Duration
	batch        int
	metrics      *Metrics
	logger       *slog.Logger
	now          func() time.Time
	externalRefs interface {
		ReconcileUnbound(context.Context) (int, error)
	}
	projects catalog.ProjectLister

	// gapStreak 记录每个来源「连续多少轮发现遗漏」。key 为 gapKeyOutbox 或模型名。
	mu        sync.Mutex
	gapStreak map[string]int
}

// NewReconciler 构造对账器。
func NewReconciler(opts ReconcileOptions) (*Reconciler, error) {
	if opts.Caller == nil {
		return nil, fmt.Errorf("connector: 对账需要 Caller")
	}
	if opts.Publisher == nil {
		return nil, fmt.Errorf("connector: 对账需要 Publisher")
	}
	if opts.Watermarks == nil {
		opts.Watermarks = NewMemWatermarks()
	}

	r := &Reconciler{
		caller:       opts.Caller,
		pub:          opts.Publisher,
		watermarks:   opts.Watermarks,
		models:       opts.Models,
		interval:     opts.Interval,
		staleAfter:   opts.StaleAfter,
		batch:        opts.Batch,
		metrics:      opts.Metrics,
		logger:       opts.Logger,
		now:          opts.Now,
		externalRefs: opts.ExternalRefs,
		projects:     opts.Projects,
		gapStreak:    make(map[string]int, 4),
	}
	if r.interval <= 0 {
		r.interval = DefaultReconcileInterval
	}
	if r.staleAfter <= 0 {
		r.staleAfter = DefaultReconcileStaleAfter
	}
	if r.batch <= 0 {
		r.batch = DefaultReconcileBatch
	}
	if r.metrics == nil {
		r.metrics = new(Metrics)
	}
	if r.logger == nil {
		r.logger = slog.Default()
	}
	if r.now == nil {
		r.now = time.Now
	}
	return r, nil
}

// Models 返回对账的模型清单（观测用）。
func (r *Reconciler) Models() []string { return r.models }

// Run 立刻跑一轮，之后按 interval 周期执行，直到 ctx 取消。
//
// 先跑一轮再进 ticker：重启后不该等满 15 min 才第一次对账。
func (r *Reconciler) Run(ctx context.Context) error {
	r.logger.Info("对账已启动（07 §4.4）",
		"interval", r.interval, "models", r.models, "stale_after", r.staleAfter)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return nil
		}
		res, err := r.ReconcileOnce(ctx)
		if err != nil && ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.logger.Warn("对账轮次失败，等下一轮", "error", err)
		} else if res.OutboxStale > 0 || res.CursorModels > 0 {
			r.logger.Warn("对账完成：发现遗漏并补投",
				"outbox_stale", res.OutboxStale, "outbox_delivered", res.OutboxDelivered,
				"outbox_dead", res.OutboxDead, "cursor_models", res.CursorModels,
				"cursor_records", res.CursorRecords, "alerts", res.Alerts)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// ReconcileOnce 执行单轮对账（供 Run 与测试调用）。
func (r *Reconciler) ReconcileOnce(ctx context.Context) (ReconcileResult, error) {
	var res ReconcileResult

	if err := r.reconcileOutbox(ctx, &res); err != nil {
		return res, err
	}
	for _, model := range r.models {
		if scoped, ok := r.watermarks.(TenantWatermarks); ok && r.projects != nil {
			projects, err := r.projects.ListProjects(ctx)
			if err != nil {
				return res, fmt.Errorf("对账②：枚举租户: %w", err)
			}
			for _, project := range projects {
				if err := r.reconcileModelScoped(ctx, model, strconv.FormatInt(project.ID, 10), project.OdooCompanyID, scoped, &res); err != nil {
					return res, err
				}
			}
			continue
		}
		if err := r.reconcileModel(ctx, model, &res); err != nil {
			return res, err
		}
	}
	if r.externalRefs != nil {
		if _, err := r.externalRefs.ReconcileUnbound(ctx); err != nil {
			return res, fmt.Errorf("对账③：复核未绑定告警: %w", err)
		}
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// ① Odoo `edge.outbox` 非终态行
// ---------------------------------------------------------------------------

type outboxRow struct {
	TenantID       string          `json:"tenant_id"`
	EventID        string          `json:"event_id"`
	CompanyID      json.RawMessage `json:"company_id"` // Many2one 返回 [id, name]
	AggregateModel string          `json:"aggregate_model"`
	AggregateID    int64           `json:"aggregate_id"`
	Version        int32           `json:"version"`
	OccurredAt     string          `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
	State          string          `json:"state"`
	Attempts       int32           `json:"attempts"`
}

func (r *Reconciler) reconcileOutbox(ctx context.Context, res *ReconcileResult) error {
	cutoff := r.now().UTC().Add(-r.staleAfter).Format(odooDatetimeLayout)

	var rows []outboxRow
	err := r.caller.Call(ctx, Request{
		Model:   "edge.outbox",
		Method:  "search_read",
		TraceID: "reconcile",
		Params: map[string]any{
			// 「非终态」= pending | dead（§4.4 原话）。
			// 再叠加「早就该被处理掉」，否则会把正在退避重试的行也当成遗漏 ——
			// 退避是设计内的等待，不是遗漏。
			"domain": []any{
				[]any{"state", "in", []any{"pending", "dead"}},
				"|",
				[]any{"next_attempt_at", "<", cutoff},
				"&", []any{"next_attempt_at", "=", false}, []any{"create_date", "<", cutoff},
			},
			"fields": []string{
				"event_id", "company_id", "aggregate_model", "aggregate_id",
				"version", "occurred_at", "payload", "state", "attempts",
			},
			"order": "next_attempt_at asc, id asc",
			"limit": r.batch,
		},
	}, &rows)
	if err != nil {
		return fmt.Errorf("对账①：查询 edge.outbox 非终态行: %w", err)
	}

	res.OutboxStale = len(rows)
	if len(rows) == 0 {
		r.clearGap(gapKeyOutbox)
		return nil
	}

	if streak := r.noteGap(gapKeyOutbox); streak >= ReconcileAlertStreak {
		res.Alerts++
		r.logger.Error("对账①：Outbox 连续多轮存在未投递行，补投未收敛 —— 需人工介入",
			"streak", streak, "stale", len(rows),
			"提示", "先确认 Odoo 侧 cron 是否在跑（开发库 max_cron_threads=0 会让它完全不执行）")
	}

	for _, row := range rows {
		if row.State == "dead" {
			res.OutboxDead++
			// 死信是「必须有人管」的状态：补投只是救急，问题本身没解决。
			r.logger.Error("对账①：发现 Outbox 死信行，已补投但需人工排查",
				"event_id", row.EventID, "aggregate_model", row.AggregateModel,
				"aggregate_id", row.AggregateID, "attempts", row.Attempts)
		}

		ev, err := row.toEvent(r.now())
		if err != nil {
			r.logger.Error("对账①：行无法转成事件，跳过", "event_id", row.EventID, "error", err)
			continue
		}
		data, err := ev.Encode()
		if err != nil {
			r.logger.Error("对账①：事件序列化失败，跳过", "event_id", ev.EventID, "error", err)
			continue
		}
		if err := r.pub.Publish(ctx, ev.Subject(), data); err != nil {
			r.metrics.PublishErrors.Add(1)
			r.logger.Warn("对账①：补投失败", "event_id", ev.EventID, "error", err)
			continue
		}

		res.OutboxDelivered++
		r.metrics.PublishedTotal.Add(1)
		r.metrics.ReconcileRepublished.Add(1)
		r.logger.Warn("对账①：补投遗漏的 Outbox 事件",
			"event_id", ev.EventID, "subject", ev.Subject(), "state", row.State)
	}
	return nil
}

func (row outboxRow) toEvent(now time.Time) (OdooEvent, error) {
	if strings.TrimSpace(row.EventID) == "" || strings.TrimSpace(row.AggregateModel) == "" {
		return OdooEvent{}, fmt.Errorf("缺少 event_id 或 aggregate_model")
	}
	payload := row.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	occurred, _ := parseOdooTime(strings.TrimSpace(row.OccurredAt))

	return OdooEvent{
		TenantID: row.TenantID,
		// **沿用原 event_id**：若原事件其实已经到达，下游按 event_id 去重即可，
		// 补投不会变成重复。换个新 id 就等于承认「必然重复」，那去重就白做了。
		EventID:        row.EventID,
		CompanyID:      odooID(row.CompanyID),
		AggregateModel: row.AggregateModel,
		AggregateID:    row.AggregateID,
		Version:        row.Version,
		OccurredAt:     occurred,
		Payload:        payload,
		PublishedAt:    now,
	}, nil
}

// ---------------------------------------------------------------------------
// ② C-2 的 `write_date` 水位差
// ---------------------------------------------------------------------------

type c2Row struct {
	ID        int64  `json:"id"`
	WriteDate string `json:"write_date"`
}

func (r *Reconciler) reconcileModel(ctx context.Context, model string, res *ReconcileResult) error {
	return r.reconcileModelScoped(ctx, model, "", 0, nil, res)
}

func (r *Reconciler) reconcileModelScoped(ctx context.Context, model, tenantID string, companyID int64, scoped TenantWatermarks, res *ReconcileResult) error {
	gapKey := model
	if tenantID != "" {
		gapKey = tenantID + ":" + model
	}
	get := func(ctx context.Context) (Watermark, bool, error) {
		if scoped != nil {
			return scoped.GetForTenant(ctx, tenantID, model)
		}
		return r.watermarks.Get(ctx, model)
	}
	advance := func(ctx context.Context, wm Watermark) error {
		if scoped != nil {
			return scoped.AdvanceForTenant(ctx, tenantID, model, wm)
		}
		return r.watermarks.Advance(ctx, model, wm)
	}

	wm, ok, err := get(ctx)
	if err != nil {
		return fmt.Errorf("对账②：读取 %s 水位: %w", model, err)
	}
	if !ok {
		return r.baselineScoped(ctx, model, tenantID, companyID, advance)
	}

	wmStr := wm.WriteDate.UTC().Format(odooDatetimeLayout)

	var rows []c2Row
	err = r.caller.Call(ctx, Request{
		Model:   model,
		Method:  "search_read",
		TraceID: "reconcile",
		Params: map[string]any{
			// `(write_date, id) > (水位.write_date, 水位.id)`：
			// 时间戳严格更大，**或**时间戳相等但 id 更大。
			// 少了后半个条件，同一秒内写入的多条记录会永远漏在边界外。
			"domain": func() []any {
				cursorDomain := []any{
					"|",
					"&", []any{"write_date", "=", wmStr}, []any{"id", ">", wm.ID},
					[]any{"write_date", ">", wmStr},
				}
				if companyID > 0 {
					return append([]any{"&", []any{"company_id", "=", companyID}}, cursorDomain...)
				}
				return cursorDomain
			}(),
			"fields": []string{"id", "write_date"},
			"order":  "write_date asc, id asc",
			"limit":  r.batch,
		},
	}, &rows)
	if err != nil {
		return fmt.Errorf("对账②：查询 %s 水位差: %w", model, err)
	}

	if len(rows) == 0 {
		// 没有水位差 = 已经追平，滞后归零。
		//
		// 「最新记录很旧」是**数据陈旧**，不是**我们落后** —— 若把它算成滞后，
		// 一个一周没变的模型会让这个指标一直报警（实测踩过：基线落在 8 天前的
		// 记录上，指标直接报 693941 秒）。
		r.observeLag(0)
		r.clearGap(gapKey)
		return nil
	}

	// 有水位差：此刻我们确实落后了「水位到当前时刻」这么多。
	if lag := r.now().Sub(wm.WriteDate); lag > 0 {
		r.observeLag(lag)
	}

	res.CursorModels++
	if streak := r.noteGap(gapKey); streak >= ReconcileAlertStreak {
		res.Alerts++
		r.logger.Error("对账②：同一模型连续多轮存在水位差，补投未收敛 —— 需人工介入",
			"model", model, "streak", streak, "gap", len(rows))
	}

	last := wm
	republished := 0
	for _, row := range rows {
		ts, err := parseOdooTime(strings.TrimSpace(row.WriteDate))
		if err != nil {
			r.logger.Error("对账②：write_date 无法解析，跳过",
				"model", model, "id", row.ID, "raw", row.WriteDate)
			continue
		}
		next := Watermark{WriteDate: ts, ID: row.ID}
		if !next.After(last) {
			continue // 防御：水位只进不退
		}

		ev := OdooEvent{
			// 与 webhook 路径同一口径（07 §4.4.1）：那条 webhook 若其实到达过，
			// 下游按 event_id 去重即可，补投不会变成重复。
			EventID:        fmt.Sprintf("c2:%s:%d:%s", model, row.ID, row.WriteDate),
			TenantID:       tenantID,
			CompanyID:      companyID,
			AggregateModel: model,
			AggregateID:    row.ID,
			Version:        1,
			OccurredAt:     ts,
			// ⚠️ 这是**补投的桩载荷**，不是记录快照 —— 显式标注 `reconciled`，
			// 让下游知道「要细节就回 Odoo 读」，而不是误当全量字段使用。
			Payload: json.RawMessage(fmt.Sprintf(
				`{"id":%d,"write_date":%q,"reconciled":true}`, row.ID, row.WriteDate)),
			PublishedAt: r.now(),
		}

		data, err := ev.Encode()
		if err != nil {
			r.logger.Error("对账②：事件序列化失败，跳过", "model", model, "id", row.ID, "error", err)
			continue
		}
		if err := r.pub.Publish(ctx, ev.Subject(), data); err != nil {
			// 不推进水位：失败的那条下一轮还会被扫到，这才是「不漏」。
			r.metrics.PublishErrors.Add(1)
			r.logger.Warn("对账②：补投失败，水位不前进", "model", model, "id", row.ID, "error", err)
			continue
		}

		last = next
		republished++
		res.CursorRecords++
		r.metrics.PublishedTotal.Add(1)
		r.metrics.ReconcileRepublished.Add(1)
	}

	// 只把水位推进到**已成功补投**的位置。
	if last.After(wm) {
		if err := advance(ctx, last); err != nil {
			return fmt.Errorf("对账②：推进 %s 水位: %w", model, err)
		}
		r.logger.Warn("对账②：补投水位差",
			"model", model, "tenant_id", tenantID, "republished", republished, "watermark", last.WriteDate)
	}
	return nil
}

// baseline 为尚无水位的模型建立基线。
//
// ⚠️ 没有基线时若直接扫 `write_date > 0`，首轮会把**全表历史记录**当成遗漏
// 补投 —— 那是全量同步，不是对账，会把总线和下游一次打爆。
// 首次对账的正确动作是「记住现在在哪」，从这一刻起才开始负责。
func (r *Reconciler) baseline(ctx context.Context, model string) error {
	return r.baselineScoped(ctx, model, "", 0, func(ctx context.Context, wm Watermark) error {
		return r.watermarks.Advance(ctx, model, wm)
	})
}

func (r *Reconciler) baselineScoped(ctx context.Context, model, tenantID string, companyID int64, advance func(context.Context, Watermark) error) error {
	var rows []c2Row
	err := r.caller.Call(ctx, Request{
		Model:   model,
		Method:  "search_read",
		TraceID: "reconcile",
		Params: map[string]any{
			"domain": func() []any {
				if companyID > 0 {
					return []any{[]any{"company_id", "=", companyID}}
				}
				return []any{}
			}(),
			"fields": []string{"id", "write_date"},
			"order":  "write_date desc, id desc",
			"limit":  1,
		},
	}, &rows)
	if err != nil {
		return fmt.Errorf("对账②：建立 %s 基线: %w", model, err)
	}
	if len(rows) == 0 {
		r.logger.Info("对账②：模型当前无记录，暂不建立水位", "model", model)
		return nil
	}

	ts, err := parseOdooTime(strings.TrimSpace(rows[0].WriteDate))
	if err != nil {
		return fmt.Errorf("对账②：%s 基线 write_date 无法解析: %w", model, err)
	}
	wm := Watermark{WriteDate: ts, ID: rows[0].ID}
	if err := advance(ctx, wm); err != nil {
		return err
	}
	r.logger.Info("对账②：建立水位基线（避免首轮把历史记录全量当成遗漏）",
		"model", model, "tenant_id", tenantID, "watermark", wm.WriteDate, "id", wm.ID)
	return nil
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

// odooID 从 JSON-2 返回的 Many2one 值（`[id, name]` 或 `false`）里取出 id。
func odooID(raw json.RawMessage) int64 {
	if len(raw) == 0 {
		return 0
	}
	var arr []any
	if err := json.Unmarshal(raw, &arr); err == nil && len(arr) > 0 {
		if f, ok := arr[0].(float64); ok {
			return int64(f)
		}
	}
	return 0
}

// noteGap 记一次遗漏，返回连续遗漏轮数。
func (r *Reconciler) noteGap(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gapStreak[key]++
	return r.gapStreak[key]
}

// clearGap 在某一轮没有遗漏时清零（避免陈旧的连续计数把偶发抖动放大成告警）。
func (r *Reconciler) clearGap(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.gapStreak, key)
}

// observeLag 记录**当前**水位滞后（07 §4.5 的 odoo_connector_cursor_lag_seconds）。
//
// 是 gauge 不是计数器：它回答「我们现在落后多少」，追平时必须归零。
// 存历史最大值会让一次瞬时落后永久污染指标，从此失去告警价值。
func (r *Reconciler) observeLag(lag time.Duration) {
	secs := int64(lag.Seconds())
	if secs < 0 {
		secs = 0
	}
	r.metrics.CursorLagSeconds.Store(secs)
}
