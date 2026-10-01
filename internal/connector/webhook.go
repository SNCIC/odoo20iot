package connector

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// WebhookPath 是 C-2 的入口路径。
const WebhookPath = "/webhook/odoo"

// MaxWebhookBody 是 webhook 请求体上限。
//
// 必须有：这是**匿名可达**的入口（只靠 Bearer 令牌），
// 无限读入等于把内存交给调用方。
const MaxWebhookBody = 1 << 20 // 1 MiB

// webhookRequest 是 C-2 入口的请求体契约。
//
// Odoo 侧（`base.automation` → `ir.actions.server(state=webhook)`）需按此形状
// POST：路由三键用于去重与定主题，`data` 承载记录快照（原样进事件载荷）。
type webhookRequest struct {
	Model     string          `json:"model"`
	ID        int64           `json:"id"`
	WriteDate string          `json:"write_date"`
	CompanyID int64           `json:"company_id"`
	Data      json.RawMessage `json:"data"`
}

// WebhookOptions 是 C-2 入口的构造参数。
type WebhookOptions struct {
	Publisher Publisher
	Deduper   Deduper
	// Watermarks 记录「C-2 处理到哪个位置」，供 15 min 对账比对（07 §4.4）。
	//
	// 不记录水位，对账就只能建立基线、无法发现遗漏 —— 而 C-2 **不持久**
	// （§3.2：进程崩溃即丢事件且无重试），对账是它唯一的兜底。
	Watermarks Watermarks
	// Token 是调用方必须携带的 Bearer 令牌。
	//
	// **为空则整个入口禁用**：一个「匿名可往总线上写事件」的端点，
	// 比没有这个端点危险得多（任何人可伪造 Odoo 事件驱动下游动作）。
	Token   string
	Metrics *Metrics
	Logger  *slog.Logger
	Now     func() time.Time
}

// Webhook 是 07 §4.4 的 C-2 入口：Odoo postcommit → 本入口 → NATS。
//
// ⚠️ C-2 **不持久**（§3.2：进程在 commit 后崩溃即丢事件，且无重试与 DLQ），
// 因此它只应承载低价值通知，并由 15 min 定时对账兜底；
// 高价值事件必须走 C-1 Outbox。
type Webhook struct {
	pub        Publisher
	dedup      Deduper
	watermarks Watermarks
	token      string
	metrics    *Metrics
	logger     *slog.Logger
	now        func() time.Time
}

var _ http.Handler = (*Webhook)(nil)

// NewWebhook 构造 C-2 入口。
func NewWebhook(opts WebhookOptions) (*Webhook, error) {
	if opts.Publisher == nil {
		return nil, fmt.Errorf("connector: webhook 需要 Publisher")
	}
	if opts.Token == "" {
		return nil, fmt.Errorf("connector: webhook 需要 Token（留空即禁用该入口）")
	}
	w := &Webhook{
		pub:        opts.Publisher,
		dedup:      opts.Deduper,
		watermarks: opts.Watermarks,
		token:      opts.Token,
		metrics:    opts.Metrics,
		logger:     opts.Logger,
		now:        opts.Now,
	}
	if w.metrics == nil {
		w.metrics = new(Metrics)
	}
	if w.logger == nil {
		w.logger = slog.Default()
	}
	if w.now == nil {
		w.now = time.Now
	}
	return w, nil
}

// ServeHTTP 实现 http.Handler。
func (w *Webhook) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.metrics.WebhookReceived.Add(1)

	if r.Method != http.MethodPost {
		w.reject(rw, http.StatusMethodNotAllowed, "只接受 POST")
		return
	}
	if !w.authorized(r) {
		w.reject(rw, http.StatusUnauthorized, "令牌无效")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, MaxWebhookBody+1))
	if err != nil {
		w.reject(rw, http.StatusBadRequest, "读取请求体失败")
		return
	}
	if len(body) > MaxWebhookBody {
		w.reject(rw, http.StatusRequestEntityTooLarge, "请求体过大")
		return
	}

	var req webhookRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.reject(rw, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	if strings.TrimSpace(req.Model) == "" || req.ID == 0 {
		w.reject(rw, http.StatusUnprocessableEntity, "缺少 model 或 id")
		return
	}

	// 去重键取 §4.4 的口径 `(ext_model, ext_id, write_date)`。
	// 没有 write_date 时退化为 `(model, id)`：仍能拦住「同一记录重复通知」，
	// 但拦不住「同一记录被改了两次」—— 后者本该走 C-1，不该靠 webhook。
	dedupKey := fmt.Sprintf("c2dedup:%s:%d:%s", req.Model, req.ID, req.WriteDate)
	if w.dedup != nil {
		first, err := w.dedup.FirstSeen(r.Context(), dedupKey)
		switch {
		case err != nil:
			// 去重器挂了**不拦**：重复投递比丢事件轻，且下游按 event_id 还能去。
			w.logger.Warn("C-2 去重不可用，放行（下游按 event_id 兜底）",
				"key", dedupKey, "error", err)
		case !first:
			w.writeJSON(rw, http.StatusOK, map[string]any{
				"ok": true, "duplicate": true, "dedup_key": dedupKey,
			})
			return
		}
	}

	eventID := fmt.Sprintf("c2:%s:%d:%s", req.Model, req.ID, req.WriteDate)
	payload := req.Data
	if len(payload) == 0 {
		payload = json.RawMessage(body)
	}

	occurred := parseOccurredAt(req.WriteDate, w.now())
	ev := OdooEvent{
		EventID:        eventID,
		CompanyID:      req.CompanyID,
		AggregateModel: req.Model,
		AggregateID:    req.ID,
		Version:        1,
		OccurredAt:     occurred,
		Payload:        payload,
		PublishedAt:    w.now(),
	}
	data, err := ev.Encode()
	if err != nil {
		w.reject(rw, http.StatusInternalServerError, "序列化事件失败")
		return
	}

	if err := w.pub.Publish(r.Context(), ev.Subject(), data); err != nil {
		// C-2 是 fire-and-forget 路径：返回 5xx 也不会被 Odoo 重试，
		// 这条事件就丢了 —— 这正是它只能承载低价值通知的原因（§3.2），
		// 遗漏由 15 min 对账补齐（§4.4）。
		w.metrics.PublishErrors.Add(1)
		w.logger.Error("C-2 事件发布失败（待对账补齐）",
			"event_id", eventID, "subject", ev.Subject(), "error", err)
		w.reject(rw, http.StatusServiceUnavailable, "发布失败")
		return
	}

	w.advanceWatermark(r, req, occurred)

	w.metrics.PublishedTotal.Add(1)
	w.writeJSON(rw, http.StatusOK, map[string]any{
		"ok": true, "event_id": eventID, "subject": ev.Subject(),
	})
}

// advanceWatermark 记录「C-2 已处理到这里」，供 15 min 对账比对水位差（§4.4）。
//
// 失败**不让请求失败**：事件已经发出去了，最坏后果只是下一轮对账把这条
// 当成遗漏再补投一次 —— 下游按 `event_id` 去重，代价可以忽略。
func (w *Webhook) advanceWatermark(r *http.Request, req webhookRequest, occurred time.Time) {
	if w.watermarks == nil || req.WriteDate == "" {
		return
	}
	wm := Watermark{WriteDate: occurred, ID: req.ID}
	if err := w.watermarks.Advance(r.Context(), req.Model, wm); err != nil {
		w.logger.Warn("记录 C-2 水位失败（下轮对账可能重复补投一次）",
			"model", req.Model, "id", req.ID, "error", err)
	}
}

// authorized 用常数时间比较 Bearer 令牌，避免时序侧信道逐字符猜出令牌。
func (w *Webhook) authorized(r *http.Request) bool {
	raw := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(raw, prefix) {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(raw, prefix))
	return subtle.ConstantTimeCompare([]byte(got), []byte(w.token)) == 1
}

func (w *Webhook) reject(rw http.ResponseWriter, status int, msg string) {
	w.metrics.WebhookRejected.Add(1)
	w.logger.Warn("C-2 请求被拒", "status", status, "reason", msg)
	w.writeJSON(rw, status, map[string]any{"ok": false, "message": msg})
}

func (w *Webhook) writeJSON(rw http.ResponseWriter, status int, body any) {
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	rw.WriteHeader(status)
	_ = json.NewEncoder(rw).Encode(body)
}

// parseOccurredAt 解析 Odoo 的 write_date；失败时回退到 fallback（本地时刻）。
//
// 与 C-1 路径不同：webhook 的记录快照不保证带 write_date，
// 而「事件发生在什么时候」总得有个值，故在此允许回退并如实标注。
func parseOccurredAt(raw string, fallback time.Time) time.Time {
	if ts, err := parseOdooTime(strings.TrimSpace(raw)); err == nil {
		return ts
	}
	return fallback.UTC()
}
