package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SNCIC/odoo20iot/internal/dlq"
)

// RetryDelays 是重试阶梯（04 §3.3：重试 1 +500ms / 重试 2 +2s / 重试 3 +8s）。
var RetryDelays = []time.Duration{500 * time.Millisecond, 2 * time.Second, 8 * time.Second}

// DLQ 记录超过重试上限的投递。
//
// 条目类型定义在 internal/dlq 而不是这里：死信是**跨服务**的设施
// （odoo-connector 往同一张表写），把类型绑在通知包上会让别的服务
// 为了写一条死信而依赖整个通知模块。
type DLQ interface {
	Put(ctx context.Context, entry dlq.Entry) error
}

// ErrDLQUnavailable 表示「所有通道都失败，而且死信也没写进去」。
//
// 这两种情况的处置**必须不同**：前者已经留下痕迹（死信表里有条目），
// 调用方可以放心 ACK；后者什么都没有 —— 既没送到人手上，也没留下
// 「它没被送到」的记录，必须让调用方重投，否则这条告警就彻底消失了。
var ErrDLQUnavailable = errors.New("notify: 死信写入失败")

// DiscardDLQ 丢弃死信。
//
// ⚠️ **只允许测试使用**。生产用它等于把「通知没送到人手上」这件事
// 彻底抹掉痕迹 —— 一条告警既没被处理，也没有留下「它没被处理」的记录。
type DiscardDLQ struct{}

func (DiscardDLQ) Put(context.Context, dlq.Entry) error { return nil }

// ChannelHealth 配置通道健康判定（04 §2.3：短信拥塞失败率 > 30% 自动切换邮件）。
//
// 文档把这条规则写成「只对短信生效」，这里实现为**通用**的通道健康判定：
// 把规则写死成只对短信生效，等 Webhook 网关整体挂掉时就没有同样的保护，
// 而那种故障比短信拥塞更常见。
type ChannelHealth struct {
	// Threshold 是失败率阈值（0.3 = 30%）。
	Threshold float64
	// Window 是统计窗口。
	Window time.Duration
	// MinSamples 是最小样本数，样本不足时不判定。
	//
	// ⚠️ 没有这一条会出大问题：服务刚起来时第一次失败就把失败率顶到 100%，
	// 于是**首条告警永远走降级通道** —— 而那恰恰是最需要走主通道的时候。
	MinSamples int
}

// DefaultChannelHealth 是 04 §2.3 的默认口径。
func DefaultChannelHealth() ChannelHealth {
	return ChannelHealth{Threshold: 0.3, Window: 10 * time.Minute, MinSamples: 5}
}

// Request 是解析通知策略的输入。
type Request struct {
	ProjectID string
	RuleID    string
	Level     string
	Escalated bool
}

// PolicySource 提供告警的通知策略。
//
// 正式实现要读 `t_alarm_rule.notify`，并由 `t_user`/`t_role` 把通知组
// 展开成收件人；这两张表都还没建（见 09 的遗留项），故当前实现从配置文件读。
// 做成接口而不是直接读文件：等表建好时只需换实现，分发逻辑一行都不用动。
type PolicySource interface {
	Resolve(ctx context.Context, req Request) (Policy, error)
}

// Options 是分发器构造参数。
type Options struct {
	Channels map[string]Channel
	// FallbackOrder 是策略没给顺序时的默认优先级（04 §2.3：Webhook → 邮件 → 短信）。
	FallbackOrder []string
	Health        ChannelHealth
	DLQ           DLQ
	Logger        *slog.Logger
	Metrics       *Metrics
	// Now / Sleep 注入便于测试（把 +500ms/+2s/+8s 压成瞬时）。
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	// Retry 覆盖重试阶梯；nil 用 RetryDelays。
	Retry []time.Duration
}

// Dispatcher 按优先级分发通知（04 §2.3 第 3 步）。
type Dispatcher struct {
	channels map[string]Channel
	order    []string
	health   *healthTracker
	dlq      DLQ
	metrics  *Metrics
	logger   *slog.Logger
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) error
	retry    []time.Duration
}

// NewDispatcher 构造。
func NewDispatcher(opts Options) (*Dispatcher, error) {
	if len(opts.Channels) == 0 {
		return nil, fmt.Errorf("notify: 至少需要一个通道")
	}
	d := &Dispatcher{
		channels: opts.Channels,
		order:    opts.FallbackOrder,
		health:   newHealthTracker(opts.Health),
		dlq:      opts.DLQ,
		metrics:  opts.Metrics,
		logger:   opts.Logger,
		now:      opts.Now,
		sleep:    opts.Sleep,
		retry:    opts.Retry,
	}
	if d.metrics == nil {
		d.metrics = new(Metrics)
	}
	if d.dlq == nil {
		d.dlq = DiscardDLQ{}
	}
	if d.now == nil {
		d.now = time.Now
	}
	if d.sleep == nil {
		d.sleep = sleepCtx
	}
	if d.logger == nil {
		d.logger = slog.Default()
	}
	if d.retry == nil {
		d.retry = RetryDelays
	}
	return d, nil
}

// Metrics 返回指标。
func (d *Dispatcher) Metrics() *Metrics { return d.metrics }

// Dispatch 按策略分发一条通知，返回每个尝试过的通道的结果。
//
// 返回的 results 是**尝试历史**而不只是最终结果：排查「为什么这个人没收到」
// 时，需要看到「主通道试了 4 次、都失败、降级到邮件成功」，
// 而不是只看到最后那个「成功」。
func (d *Dispatcher) Dispatch(ctx context.Context, msg Message, policy Policy) ([]Result, error) {
	return d.dispatch(ctx, msg, policy, true)
}

// Replay 重新投递一条已进入 DLQ 的通知。
// 重放失败由 DLQ 重放器记录，不能在这里再次写入 DLQ，否则会形成递归死信。
func (d *Dispatcher) Replay(ctx context.Context, msg Message, policy Policy) ([]Result, error) {
	return d.dispatch(ctx, msg, policy, false)
}

func (d *Dispatcher) dispatch(ctx context.Context, msg Message, policy Policy, recordDLQ bool) ([]Result, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	dlqFailed := false
	order := policy.Channels
	if len(order) == 0 {
		order = d.order
	}

	var results []Result
	failedFrom := ""
	for _, name := range order {
		ch, ok := d.channels[name]
		if !ok {
			d.logger.Warn("策略指定的通道未注册，跳过", "channel", name)
			continue
		}

		// 通道拥塞 → 跳过并降级（04 §2.3）。
		if rate, n := d.health.congested(name, d.now()); rate > 0 {
			d.metrics.Degraded.Add(1)
			d.logger.Warn("通道拥塞，跳过并降级",
				"channel", name, "failure_rate", rate, "samples", n)
			results = append(results, Result{
				Channel:      name,
				Err:          fmt.Errorf("通道 %s 在最近窗口内失败率 %.0f%%（%d 次样本），已降级", name, rate*100, n),
				DegradedFrom: failedFrom,
			})
			if failedFrom == "" {
				failedFrom = name
			}
			continue
		}

		res := d.deliver(ctx, ch, msg, policy.Recipients[name])
		if res.OK() {
			if failedFrom != "" {
				// 靠降级发出去的必须标出来：不标的话「主通道一直失败」
				// 永远不会有人发现，直到主通道彻底没了。
				res.DegradedFrom = failedFrom
			}
			results = append(results, res)
			return results, nil
		}
		if IsPartial(res.Err) {
			// 部分投递：消息已经出去了，重试会给已收到的人重复发，
			// 降级也没必要（通道是好的）。
			d.metrics.Partial.Add(1)
			d.logger.Warn("部分收件人投递失败", "channel", name, "error", res.Err)
			results = append(results, res)
			return results, nil
		}

		results = append(results, res)
		if recordDLQ {
			if err := d.recordDLQ(ctx, msg, policy, name, res); err != nil {
				dlqFailed = true
			}
		}
		if failedFrom == "" {
			failedFrom = name
		}
	}
	err := fmt.Errorf("notify: 所有通道都失败（告警 %s）", msg.AlarmID)
	if dlqFailed {
		// 把「死信也失败了」这件事显式带出去 —— 调用方据此决定重投，
		// 而不是以为「反正进死信了」就把消息 ACK 掉。
		err = fmt.Errorf("%w: %w", ErrDLQUnavailable, err)
	}
	return results, err
}

// deliver 按重试阶梯投递一个通道（04 §3.3）。
func (d *Dispatcher) deliver(ctx context.Context, ch Channel, msg Message, recipients []string) Result {
	res := Result{Channel: ch.Name()}
	for attempt := 0; ; attempt++ {
		err := ch.Send(ctx, msg, recipients)
		res.Attempts = attempt + 1
		res.Err = err

		if err == nil {
			d.metrics.Sent.Add(1)
			d.health.record(ch.Name(), true, d.now())
			return res
		}
		// 永久失败与部分投递都不重试：
		// 前者重试无意义；后者消息已经投出去了，重试等于给已收到的人重发。
		if IsPermanent(err) || IsPartial(err) {
			d.metrics.Failed.Add(1)
			d.health.record(ch.Name(), IsPartial(err), d.now())
			return res
		}
		if attempt >= len(d.retry) {
			d.metrics.Failed.Add(1)
			d.health.record(ch.Name(), false, d.now())
			return res
		}
		d.metrics.Retries.Add(1)
		if err := d.sleep(ctx, d.retry[attempt]); err != nil {
			// ctx 被取消（进程在退出）：不再重试，把当前结果当最终结果 ——
			// 继续重试会在退出路径上再耗 10 秒，而停机时间是要计入 SLO 的。
			d.metrics.Failed.Add(1)
			d.health.record(ch.Name(), false, d.now())
			return res
		}
	}
}

func (d *Dispatcher) recordDLQ(ctx context.Context, msg Message, policy Policy, channel string, res Result) error {
	d.metrics.DLQTotal.Add(1)
	payload, err := json.Marshal(struct {
		Message Message `json:"message"`
		Policy  Policy  `json:"policy"`
		Channel string  `json:"channel"`
	}{Message: msg, Policy: policy, Channel: channel})
	if err != nil {
		return fmt.Errorf("notify: 序列化死信载荷: %w", err)
	}
	entry := dlq.Entry{
		Service:        "svc-notify",
		Subject:        channel,
		EntityType:     "notification",
		IdempotencyKey: msg.DedupKey + ":" + channel,
		Payload:        truncate(string(payload)),
		Reason:         errString(res.Err),
		Attempts:       res.Attempts,
		History:        []string{fmt.Sprintf("%s: 尝试 %d 次，最后错误：%s", channel, res.Attempts, errString(res.Err))},
		TraceID:        msg.TraceID,
		CreatedAt:      d.now(),
	}

	if err := d.dlq.Put(ctx, entry); err != nil {
		// DLQ 写失败**必须显式记错**：它意味着这条通知既没送到人、
		// 也没留下任何痕迹 —— 那是真正的静默丢失。
		d.logger.Error("写死信失败：这条通知既没送达也没有记录",
			"channel", channel, "alarm", msg.AlarmID, "error", err)
		return err
	}
	d.logger.Warn("通知进入死信并降级",
		"channel", channel, "alarm", msg.AlarmID, "attempts", res.Attempts, "error", res.Err)
	return nil
}

// healthTracker 统计各通道最近窗口内的成功率（04 §2.3 的拥塞降级）。
type healthTracker struct {
	cfg ChannelHealth
	mu  sync.Mutex
	ev  map[string][]outcome
}

type outcome struct {
	at time.Time
	ok bool
}

func newHealthTracker(cfg ChannelHealth) *healthTracker {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 0.3
	}
	if cfg.Window <= 0 {
		cfg.Window = 10 * time.Minute
	}
	if cfg.MinSamples <= 0 {
		cfg.MinSamples = 5
	}
	return &healthTracker{cfg: cfg, ev: make(map[string][]outcome)}
}

func (h *healthTracker) record(channel string, ok bool, at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ev[channel] = append(h.ev[channel], outcome{at: at, ok: ok})
}

// congested 返回 (失败率, 样本数)；失败率 > 0 表示应降级。
func (h *healthTracker) congested(channel string, at time.Time) (float64, int) {
	rate, n := h.failureRate(channel, at)
	if n < h.cfg.MinSamples || rate <= h.cfg.Threshold {
		return 0, n
	}
	return rate, n
}

func (h *healthTracker) failureRate(channel string, at time.Time) (float64, int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cutoff := at.Add(-h.cfg.Window)
	evs := h.ev[channel]
	keep := evs[:0]
	failed := 0
	for _, e := range evs {
		if e.at.Before(cutoff) {
			continue
		}
		keep = append(keep, e)
		if !e.ok {
			failed++
		}
	}
	h.ev[channel] = keep
	if len(keep) == 0 {
		return 0, 0
	}
	return float64(failed) / float64(len(keep)), len(keep)
}

// Metrics 是通知分发的计数。
type Metrics struct {
	Sent     atomic.Int64
	Failed   atomic.Int64
	Retries  atomic.Int64
	Degraded atomic.Int64
	DLQTotal atomic.Int64
	Partial  atomic.Int64

	mu        sync.Mutex
	byChannel map[string]int64
}

// RecordChannel 记录一次按通道的成功投递（供服务侧在 Dispatch 后累加）。
func (m *Metrics) RecordChannel(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.byChannel == nil {
		m.byChannel = make(map[string]int64)
	}
	m.byChannel[channel]++
}

// Snapshot 返回按通道的成功计数快照。
func (m *Metrics) Snapshot() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.byChannel))
	for k, v := range m.byChannel {
		out[k] = v
	}
	return out
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sleepCtx 是可被取消的等待（注入给测试以便把阶梯压成瞬时）。
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
