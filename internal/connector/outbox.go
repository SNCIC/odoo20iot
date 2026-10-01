package connector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// Publisher 是事件出口（NATS 的窄接口，便于确定性测试）。
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// Outbox 消费者的默认参数。
const (
	DefaultOutboxBatch = 64
	DefaultOutboxBlock = 2 * time.Second
	// DefaultIngestBackoff 是消费轮次失败后的退避。
	//
	// 必须有：Redis 挂掉时 `ReadGroup` 会立刻返回错误，
	// 不退避就是一个打满 CPU 的空转循环。
	DefaultIngestBackoff = time.Second
	// DefaultReclaimIdle 是「多久没确认就认为上一位处理者失败了」的阈值。
	//
	// 取值要显著大于一次正常发布的耗时（NATS PublishAck 通常 < 10ms），
	// 又要小到不影响重投时效；30s 在两者之间留足了余量。
	DefaultReclaimIdle = 30 * time.Second
)

// OutboxOptions 是 Outbox 消费者的构造参数。
type OutboxOptions struct {
	Store     StreamsStore
	Publisher Publisher
	// Stream 默认为 OutboxStream。
	Stream string
	// Group 默认为 DefaultConsumerGroup。
	Group string
	// Consumer 是本实例名（多副本时用于区分 PEL 归属）；默认为主机名。
	Consumer string
	// Batch 单次拉取上限，默认 DefaultOutboxBatch。
	Batch int
	// Block 阻塞等待时长，默认 DefaultOutboxBlock。
	Block time.Duration
	// Backoff 轮次失败后的退避，默认 DefaultIngestBackoff。
	Backoff time.Duration
	// ReclaimIdle 是 PEL 重投的空闲阈值，默认 DefaultReclaimIdle。
	ReclaimIdle time.Duration
	Metrics     *Metrics
	Logger      *slog.Logger
	Now         func() time.Time
}

// ConsumeResult 是单轮消费的统计。
type ConsumeResult struct {
	Reclaimed int // 从 PEL 接管重投的条数
	Fetched   int // 本轮处理的总条数（重投 + 新消息）
	Published int // 成功发布数
	Poisoned  int // 无法解析、已 ACK 丢弃数
	Failed    int // 发布失败、保留在 PEL 待重投数
}

// OutboxConsumer 把 Odoo 的 Outbox 事件从 Redis Streams 搬到 NATS（07 §4.4 C-1）。
//
// 投递语义：**至少一次**。两件事共同成立才叫至少一次：
//   - XACK 只在发布成功之后（失败就不确认）；
//   - 未确认的消息会被下一轮的 ClaimStale 捞回来重试。
//
// 只做前者不做后者，失败的消息会永远沉在 PEL 里 —— 那是「零次」，不是「至少一次」。
// 重复由事件信封里的 `event_id` 在下游去重（§4.4）。
type OutboxConsumer struct {
	store       StreamsStore
	pub         Publisher
	stream      string
	group       string
	cons        string
	batch       int
	block       time.Duration
	backoff     time.Duration
	reclaimIdle time.Duration
	metrics     *Metrics
	logger      *slog.Logger
	now         func() time.Time
}

// NewOutboxConsumer 构造 Outbox 消费者。
func NewOutboxConsumer(opts OutboxOptions) (*OutboxConsumer, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("connector: 需要 StreamsStore")
	}
	if opts.Publisher == nil {
		return nil, fmt.Errorf("connector: 需要 Publisher")
	}

	c := &OutboxConsumer{
		store:       opts.Store,
		pub:         opts.Publisher,
		stream:      opts.Stream,
		group:       opts.Group,
		cons:        opts.Consumer,
		batch:       opts.Batch,
		block:       opts.Block,
		backoff:     opts.Backoff,
		reclaimIdle: opts.ReclaimIdle,
		metrics:     opts.Metrics,
		logger:      opts.Logger,
		now:         opts.Now,
	}
	if c.stream == "" {
		c.stream = OutboxStream
	}
	if c.group == "" {
		c.group = DefaultConsumerGroup
	}
	if c.cons == "" {
		if host, err := os.Hostname(); err == nil {
			c.cons = host
		} else {
			c.cons = "odoo-connector"
		}
	}
	if c.batch <= 0 {
		c.batch = DefaultOutboxBatch
	}
	if c.block <= 0 {
		c.block = DefaultOutboxBlock
	}
	if c.backoff <= 0 {
		c.backoff = DefaultIngestBackoff
	}
	if c.reclaimIdle <= 0 {
		c.reclaimIdle = DefaultReclaimIdle
	}
	if c.metrics == nil {
		c.metrics = new(Metrics)
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c, nil
}

// Stream 返回消费的 Redis Stream 键（观测用）。
func (c *OutboxConsumer) Stream() string { return c.stream }

// Metrics 返回计数器。
func (c *OutboxConsumer) Metrics() *Metrics { return c.metrics }

// Run 建立消费组并持续搬运，直到 ctx 取消。
func (c *OutboxConsumer) Run(ctx context.Context) error {
	if err := c.store.EnsureGroup(ctx, c.stream, c.group); err != nil {
		return err
	}

	c.logger.Info("Outbox 消费已启动（C-1）",
		"stream", c.stream, "group", c.group, "consumer", c.cons,
		"batch", c.batch, "reclaim_idle", c.reclaimIdle)

	for {
		if ctx.Err() != nil {
			return nil
		}

		res, err := c.ConsumeOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Warn("消费轮次失败，退避后重试", "backoff", c.backoff, "error", err)
			if !sleepCtxOK(ctx, c.backoff) {
				return nil
			}
			continue
		}

		if res.Fetched > 0 {
			c.logger.Info("搬运一轮完成",
				"reclaimed", res.Reclaimed, "fetched", res.Fetched,
				"published", res.Published, "poisoned", res.Poisoned, "failed", res.Failed)
		}
	}
}

// ConsumeOnce 执行单轮消费（供 Run 与测试调用）。
func (c *OutboxConsumer) ConsumeOnce(ctx context.Context) (ConsumeResult, error) {
	var res ConsumeResult

	// 1) 先接管「投递出去却迟迟没确认」的消息。
	//
	// 顺序刻意的：先重投旧账再取新消息，避免在持续有新消息时
	// 让失败的那条永远排在队尾（饿死）。
	stale, err := c.store.ClaimStale(ctx, c.stream, c.group, c.cons, c.reclaimIdle, c.batch)
	if err != nil {
		c.metrics.IngestErrors.Add(1)
		return res, fmt.Errorf("接管待重投消息 %s/%s: %w", c.stream, c.group, err)
	}
	res.Reclaimed = len(stale)
	if res.Reclaimed > 0 {
		c.metrics.Reclaimed.Add(int64(res.Reclaimed))
		c.logger.Warn("接管未确认消息重投", "count", res.Reclaimed, "idle", c.reclaimIdle)
	}

	// 2) 再取从未投递过的新消息。
	entries := stale
	if remain := c.batch - len(stale); remain > 0 {
		block := c.block
		if res.Reclaimed > 0 {
			block = 0 // 手里已有活要干，别再阻塞等新消息
		}
		fresh, err := c.store.ReadGroup(ctx, c.stream, c.group, c.cons, remain, block)
		if err != nil {
			c.metrics.IngestErrors.Add(1)
			return res, fmt.Errorf("读取 %s/%s: %w", c.stream, c.group, err)
		}
		entries = append(entries, fresh...)
	}
	res.Fetched = len(entries)
	if len(entries) == 0 {
		return res, nil
	}

	// 只收集该 ACK 的 ID，末尾批量提交：中途出错时不会把「没处理完的」
	// 和「已成功的」混在一起。
	ackIDs := make([]string, 0, len(entries))

	for _, entry := range entries {
		ev, perr := parseOdooEvent(entry, c.now())
		if perr != nil {
			// 毒消息：字段缺失/格式错，重投一万次也不会变好。
			// 处理方式与 svc-quota 一致（「非法上报 → 计数后 ACK 释放」），
			// 但**必须告警** —— 静默丢一条业务事件比丢一条计量上报严重得多。
			c.metrics.Poisoned.Add(1)
			c.logger.Error("丢弃无法解析的 Outbox 事件（已 ACK）",
				"stream_id", entry.ID, "error", perr)
			ackIDs = append(ackIDs, entry.ID)
			res.Poisoned++
			continue
		}

		data, err := ev.Encode()
		if err != nil {
			c.metrics.Poisoned.Add(1)
			c.logger.Error("丢弃无法序列化的事件（已 ACK）",
				"event_id", ev.EventID, "error", err)
			ackIDs = append(ackIDs, entry.ID)
			res.Poisoned++
			continue
		}

		if err := c.pub.Publish(ctx, ev.Subject(), data); err != nil {
			// 关键：**不 ACK**。留在 PEL 里，下一轮由 ClaimStale 捞回来重投。
			c.metrics.PublishErrors.Add(1)
			c.logger.Warn("发布事件失败，保留待重投",
				"event_id", ev.EventID, "subject", ev.Subject(), "error", err)
			res.Failed++
			continue
		}

		ackIDs = append(ackIDs, entry.ID)
		res.Published++
		c.metrics.PublishedTotal.Add(1)
	}

	if err := c.store.Ack(ctx, c.stream, c.group, ackIDs...); err != nil {
		// ACK 失败 → 已发布的事件会在重投窗口后被再投一次；
		// 这是至少一次语义允许的，靠下游按 event_id 去重兜底。
		c.metrics.IngestErrors.Add(1)
		return res, fmt.Errorf("ACK %s/%s: %w", c.stream, c.group, err)
	}
	return res, nil
}

// sleepCtxOK 等待 d，返回 false 表示 ctx 已取消。
func sleepCtxOK(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
