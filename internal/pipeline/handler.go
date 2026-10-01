package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

// Result 是单条消息的处理结果，决定调用方如何确认消息。
type Result int

const (
	// ResultPersisted 已落库（本次或此前）→ ACK。
	ResultPersisted Result = iota
	// ResultDuplicate 命中 done 标记（重复交付）→ ACK 跳过。
	ResultDuplicate
	// ResultPoison 毒消息（解析失败）→ 计数后 ACK 释放，**不重投**。
	ResultPoison
	// ResultRetry 暂不可完成（processing 占用 / 落库失败 / 存储不可用）→ NAK 重试。
	ResultRetry
)

func (r Result) String() string {
	switch r {
	case ResultPersisted:
		return "persisted"
	case ResultDuplicate:
		return "duplicate"
	case ResultPoison:
		return "poison"
	case ResultRetry:
		return "retry"
	default:
		return "unknown"
	}
}

// Metrics 是管道计数器。
type Metrics struct {
	// ConsumedTotal 消费到的总线消息数。
	ConsumedTotal atomic.Int64
	// ParsedTotal 解析成功的消息数。
	ParsedTotal atomic.Int64
	// PermanentTotal 毒消息数（解析失败；按 03 §4.3 计数后 ACK 释放）。
	PermanentTotal atomic.Int64
	// FlushErrorTotal 批次落库失败次数（消息会被重投）。
	FlushErrorTotal atomic.Int64
	// RowsWritten 已确认落库的时序行数。
	RowsWritten atomic.Int64

	// ---- 幂等（03 §4.3）----

	// DuplicateTotal 命中 done 标记的重复交付数（已直接 ACK 跳过）。
	DuplicateTotal atomic.Int64
	// ContendedTotal processing 标记被占用而退避的次数。
	ContendedTotal atomic.Int64
	// IdemErrorTotal 幂等存储不可用的次数。
	IdemErrorTotal atomic.Int64
}

// HandlerConfig 是处理器的时序参数。
type HandlerConfig struct {
	// ProcessingTTL 并发互斥标记的 TTL（03 §4.3：60s，≥ AckWait × 2）。
	ProcessingTTL time.Duration
	// DoneTTL 完成标记的 TTL（03 §4.3：5min，覆盖最大重投窗口）。
	DoneTTL time.Duration
}

// Handler 是单条总线消息的处理入口（含两阶段幂等）：
// 解信封 → 解析 → done 检查 → 抢占 processing → 入批 → 落库 → 写 done。
//
// 它刻意不做 NATS 交互：消费与确认的语义（何时 ACK / NAK）留在调用方，
// 这样处理逻辑可以脱离真实总线做确定性测试。
type Handler struct {
	parser  *Parser
	batcher *Batcher[tsdb.Row]
	idem    IdempotencyStore
	metrics *Metrics
	logger  *slog.Logger
	cfg     HandlerConfig
}

// NewHandler 构造处理器。idem 为 nil 时退化为无幂等（仅单实例冒烟用，不推荐）。
func NewHandler(parser *Parser, batcher *Batcher[tsdb.Row], idem IdempotencyStore, metrics *Metrics, logger *slog.Logger) *Handler {
	if metrics == nil {
		metrics = new(Metrics)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if idem == nil {
		idem = NewMemIdempotency()
	}
	return &Handler{
		parser:  parser,
		batcher: batcher,
		idem:    idem,
		metrics: metrics,
		logger:  logger,
		cfg: HandlerConfig{
			ProcessingTTL: DefaultProcessingTTL,
			DoneTTL:       DefaultDoneTTL,
		},
	}
}

// Handle 处理一条总线消息，**阻塞到落库确认**（03 §4.3 步骤 2–7）。
//
// 返回的 Result 决定调用方如何确认总线消息：
// ACK（Persisted / Duplicate / Poison）或 NAK 重试（Retry）。
func (h *Handler) Handle(ctx context.Context, data []byte) (Result, error) {
	h.metrics.ConsumedTotal.Add(1)

	// 1) 解信封 + 物模型解析。
	env, rec, err := h.parse(data)
	if err != nil {
		h.metrics.PermanentTotal.Add(1)
		return ResultPoison, err
	}
	h.metrics.ParsedTotal.Add(1)

	key := IdempotencyKey(env.ProjectID, env.DeviceID, rec.Row.TS, rec.Seq)

	// 2①) done 命中 → 重复交付，直接 ACK 跳过（唯一允许提前 ACK 的情况）。
	done, err := h.idem.IsDone(ctx, key)
	if err != nil {
		h.metrics.IdemErrorTotal.Add(1)
		return ResultRetry, fmt.Errorf("查询幂等标记: %w", err)
	}
	if done {
		h.metrics.DuplicateTotal.Add(1)
		return ResultDuplicate, nil
	}

	// 2②) 抢占 processing：**只作并发互斥**。
	// 占用失败必须 NAK 重试，绝不能 ACK —— 否则「上一次处理尚未落库」时
	// 就会提前确认，造成永久丢失（03 §4.3 修正的第二个缺陷）。
	acquired, err := h.idem.AcquireProcessing(ctx, key, h.cfg.ProcessingTTL)
	if err != nil {
		h.metrics.IdemErrorTotal.Add(1)
		return ResultRetry, fmt.Errorf("抢占处理标记: %w", err)
	}
	if !acquired {
		h.metrics.ContendedTotal.Add(1)
		return ResultRetry, nil
	}

	// 3) 入批并等待落库。
	tk, err := h.batcher.Add(rec.Row)
	if err != nil {
		_ = h.idem.ReleaseProcessing(ctx, key)
		return ResultRetry, fmt.Errorf("入批失败: %w", err)
	}
	if err := tk.Wait(ctx); err != nil {
		h.metrics.FlushErrorTotal.Add(1)
		// 释放 processing，让重投能重新处理（TTL 也会兜底）。
		_ = h.idem.ReleaseProcessing(ctx, key)
		return ResultRetry, fmt.Errorf("落库失败: %w", err)
	}

	// 4) **持久化确认之后**才写 done（03 §4.3 步骤 7）。
	if err := h.idem.MarkDone(ctx, key, h.cfg.DoneTTL); err != nil {
		// 数据已落库、done 未写：重投会被当新消息，可能重复入库。
		// 在 at-least-once 语义下可接受（读侧按 ts 去重），但必须能被发现。
		h.metrics.IdemErrorTotal.Add(1)
		h.logger.Warn("写 done 标记失败（数据已落库，重投可能重复入库）", "key", key, "error", err)
		_ = h.idem.ReleaseProcessing(ctx, key)
	}

	h.metrics.RowsWritten.Add(1)
	return ResultPersisted, nil
}

func (h *Handler) parse(data []byte) (envelope.Envelope, Record, error) {
	env, err := envelope.Decode(data)
	if err != nil {
		// 解不出信封同样是**不可重试**的毒消息。
		return envelope.Envelope{}, Record{}, fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	rec, err := h.parser.Parse(env)
	if err != nil {
		return envelope.Envelope{}, Record{}, err
	}
	return env, rec, nil
}

// Metrics 返回计数器。
func (h *Handler) Metrics() *Metrics { return h.metrics }
