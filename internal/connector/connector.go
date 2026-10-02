package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sony/gobreaker"

	"github.com/SNCIC/odoo20iot/internal/dlq"
)

// Client 是连接器对 Odoo 的依赖，收窄为一次调用，便于注入 fake 做确定性测试。
// 生产里传 `*odoo.Client`。
type Client interface {
	Call(ctx context.Context, model, method string, params any, out any) error
}

type DLQ interface {
	Put(context.Context, dlq.Entry) error
}

type dlqSuppressedKey struct{}

func SuppressDLQ(ctx context.Context) context.Context {
	return context.WithValue(ctx, dlqSuppressedKey{}, true)
}

// Request 是一次写回 / 查询请求。
type Request struct {
	Model  string
	Method string
	Params any
	// IdempotencyKey 非空表示携带幂等键（§4.3.1）。它影响两件事：
	//   - **下游超时是否可重试**（§4.3.2「仅在携带幂等键时重试」）；
	//   - 是否走 Guard 的并发重复拦截。
	IdempotencyKey string
	// RequestHash 是规范化请求摘要（§4.3.1 第 1 步）。
	// 与 IdempotencyKey **同时非空**时 Guard 才生效。
	RequestHash string
	// TraceID 透传到对外错误（§4.3.2 统一响应体含 trace_id）。
	TraceID string
}

// Options 是连接器构造参数。
type Options struct {
	Policy  Policy
	Metrics *Metrics
	Logger  *slog.Logger
	// Guard 是幂等占位（§4.3.1）。nil 时不做并发重复拦截 ——
	// 权威账本仍在 Odoo 侧，缺它只是少了快速失败，不影响正确性。
	Guard Guard
	DLQ   DLQ
	// Sleep 注入退避等待（测试把 1s/3s/9s 压成瞬时）；nil 时用真实等待。
	Sleep func(ctx context.Context, d time.Duration) error
	// Now 注入时钟（耗时观测）。
	Now func() time.Time
}

// Connector 是 07 §4.3 的编排层。
type Connector struct {
	client  Client
	policy  Policy
	adm     *admission
	breaker *breaker
	guard   Guard
	dlq     DLQ
	metrics *Metrics
	logger  *slog.Logger
	sleep   func(context.Context, time.Duration) error
	now     func() time.Time
}

// New 构造编排层。client 为 nil 时返回错误 —— 没有下游的编排层没有意义。
func New(client Client, opts Options) (*Connector, error) {
	if client == nil {
		return nil, fmt.Errorf("connector: client 不能为 nil")
	}
	p := opts.Policy.Normalize()

	metrics := opts.Metrics
	if metrics == nil {
		metrics = new(Metrics)
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	return &Connector{
		client:  client,
		policy:  p,
		adm:     newAdmission(p),
		breaker: newBreaker(p),
		guard:   opts.Guard,
		dlq:     opts.DLQ,
		metrics: metrics,
		logger:  logger,
		sleep:   sleep,
		now:     now,
	}, nil
}

// Policy 返回生效策略（已 Normalize）。
func (c *Connector) Policy() Policy { return c.policy }

// Metrics 返回计数器。
func (c *Connector) Metrics() *Metrics { return c.metrics }

// BreakerState 返回熔断状态（观测用）。
func (c *Connector) BreakerState() gobreaker.State { return c.breaker.State() }

// Waiting 返回当前排队数（观测用）。
func (c *Connector) Waiting() int64 { return c.adm.Waiting() }

// Call 在编排保护下执行一次 Odoo 调用。
//
// 重试只覆盖「技术失败」；业务拒绝（4xx）**一次即返回** ——
// 重放一个参数错误的请求，只会让问题放大（§4.3.2）。
func (c *Connector) Call(ctx context.Context, req Request, out any) error {
	c.metrics.CallsTotal.Add(1)
	start := c.now()

	// §4.3.1 第 2 步：调用前的幂等占位，快速拦截并发重复。
	if err := c.reserve(ctx, req); err != nil {
		c.observe(start)
		return err
	}

	if err := c.adm.Acquire(ctx); err != nil {
		c.metrics.RateLimitedTotal.Add(1)
		return c.fail(req, CodeRateLimited, "本地准入拒绝（限流或排队已满）", err)
	}
	defer c.adm.Release()

	hasKey := req.IdempotencyKey != ""

	var (
		lastErr  error
		lastCode Code
	)

	for attempt := 0; ; attempt++ {
		_, err := c.breaker.Execute(func() (any, error) {
			return nil, c.client.Call(ctx, req.Model, req.Method, req.Params, out)
		})
		if err == nil {
			c.metrics.SuccessTotal.Add(1)
			c.observe(start)
			return nil
		}
		lastErr = err

		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			c.metrics.BreakerOpenTotal.Add(1)
			lastCode = CodeCircuitOpen
			break
		}

		lastCode = Classify(err)
		if !Retryable(lastCode, hasKey) || attempt >= len(c.policy.RetryBackoff) {
			break
		}

		backoff := c.policy.RetryBackoff[attempt]
		c.metrics.RetriesTotal.Add(1)
		c.logger.Warn("Odoo 调用失败，退避后重试",
			"model", req.Model, "method", req.Method, "code", lastCode,
			"attempt", attempt+1, "backoff", backoff, "error", err)

		if serr := c.sleep(ctx, backoff); serr != nil {
			c.observe(start)
			return c.fail(req, lastCode, "退避等待被取消", serr)
		}
	}

	c.observe(start)
	err := c.fail(req, lastCode, "Odoo 调用失败", lastErr)
	c.recordDLQ(ctx, req, err)
	return err
}

func (c *Connector) recordDLQ(ctx context.Context, req Request, callErr error) {
	if c.dlq == nil || req.IdempotencyKey == "" || ctx.Value(dlqSuppressedKey{}) == true {
		return
	}
	payload, err := json.Marshal(req)
	if err != nil {
		c.logger.Error("序列化 Odoo DLQ 请求失败", "model", req.Model, "method", req.Method, "error", err)
		return
	}
	entry := dlq.Entry{
		Service:        "odoo-connector",
		Subject:        req.Model + "." + req.Method,
		EntityType:     "odoo_call",
		IdempotencyKey: req.IdempotencyKey,
		Payload:        string(payload),
		Reason:         callErr.Error(),
		Attempts:       1,
		History:        []string{callErr.Error()},
		TraceID:        req.TraceID,
		CreatedAt:      c.now(),
	}
	if err := c.dlq.Put(ctx, entry); err != nil {
		c.logger.Error("写入 Odoo DLQ 失败", "model", req.Model, "method", req.Method, "error", err)
	}
}

// reserve 执行 §4.3.1 的幂等占位。
//
// 占位存储不可用时**降级放行**：Redis 只是加速层，权威账本是 Odoo 的
// `edge_idempotency`（§5.2.1）。把 Redis 故障升级成业务失败，等于让一个
// 缓存问题挡住所有写回 —— 那比偶尔让 Odoo 的唯一约束去裁决要糟得多。
func (c *Connector) reserve(ctx context.Context, req Request) error {
	if c.guard == nil || req.IdempotencyKey == "" || req.RequestHash == "" {
		return nil
	}

	res, err := c.guard.Reserve(ctx, req.IdempotencyKey, req.RequestHash)
	if err != nil {
		c.logger.Warn("幂等占位不可用，降级交由 Odoo 账本裁决",
			"key", req.IdempotencyKey, "error", err)
		return nil
	}
	if res == ReservationConflict {
		return c.fail(req, CodeIdempotencyConflict, "同键不同请求摘要", nil)
	}
	return nil
}

// fail 统一构造对外错误：只含 code / message / trace_id，原始错误留在 Err 里。
func (c *Connector) fail(req Request, code Code, msg string, err error) error {
	if code == "" {
		code = CodeUpstreamError
	}
	c.metrics.RecordError(code)
	return &Error{Code: code, Message: msg, TraceID: req.TraceID, Err: err}
}

func (c *Connector) observe(start time.Time) {
	d := c.now().Sub(start)
	c.metrics.DurationSumNanos.Add(d.Nanoseconds())
	c.metrics.DurationCount.Add(1)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
