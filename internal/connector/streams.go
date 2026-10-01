package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// StreamsStore 是 Redis Streams 的窄接口（便于注入 fake 做确定性测试）。
type StreamsStore interface {
	// EnsureGroup 幂等创建消费组（流不存在时一并创建）。
	EnsureGroup(ctx context.Context, stream, group string) error
	// ReadGroup 读取**从未投递**（`>`）的新消息；无消息时返回空切片且 err 为 nil。
	ReadGroup(ctx context.Context, stream, group, consumer string, count int, block time.Duration) ([]StreamEntry, error)
	// ClaimStale 接管「已投递但空闲超过 minIdle 仍未确认」的消息（PEL 重投）。
	ClaimStale(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int) ([]StreamEntry, error)
	// Ack 确认已成功处理的消息。
	Ack(ctx context.Context, stream, group string, ids ...string) error
}

// RedisStreams 是 StreamsStore 的 Redis 实现。
type RedisStreams struct {
	rdb redis.UniversalClient
}

var _ StreamsStore = (*RedisStreams)(nil)

// NewRedisStreams 构造 Redis Streams 存储。
func NewRedisStreams(rdb redis.UniversalClient) *RedisStreams {
	return &RedisStreams{rdb: rdb}
}

// EnsureGroup 幂等创建消费组。
//
// 起始位点取 `0` 而**不是** `$`：Outbox 在连接器上线之前就已积累事件，
// 用 `$` 会把这段积压静默跳过 —— 那正是「高价值事件不能丢」要防的事。
func (s *RedisStreams) EnsureGroup(ctx context.Context, stream, group string) error {
	err := s.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("创建消费组 %s/%s: %w", stream, group, err)
	}
	return nil
}

// ReadGroup 读取未投递的新消息。
//
// 只读 `>`（从未投递给本消费组的新消息）。**已投递未 ACK 的旧消息不走这里** ——
// 它们沉在 PEL 里，由 ClaimStale 负责捞回。
func (s *RedisStreams) ReadGroup(ctx context.Context, stream, group, consumer string, count int, block time.Duration) ([]StreamEntry, error) {
	if count <= 0 {
		return nil, nil
	}

	// ⚠️ go-redis 只在 `Block >= 0` 时下发 `BLOCK`，而 Redis 的 **`BLOCK 0`
	// 是「永久阻塞」而不是「不阻塞」** —— 直接传 0 会把消费循环挂死在这里
	// （实测：一条超时用例卡满 60s）。调用方用 block<=0 表达「别等」，
	// 这里翻译成负值，go-redis 便不下发 BLOCK，退化为非阻塞读。
	blockArg := block
	if blockArg <= 0 {
		blockArg = -1
	}

	res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    int64(count),
		Block:    blockArg,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // 阻塞窗口内无新消息，不是错误
	}
	if err != nil {
		return nil, err
	}

	var out []StreamEntry
	for _, st := range res {
		for _, m := range st.Messages {
			out = append(out, StreamEntry{ID: m.ID, Fields: toStringMap(m.Values)})
		}
	}
	return out, nil
}

// ClaimStale 用 XAUTOCLAIM 接管空闲超阈值的未确认消息。
//
// 为什么必须有：Redis Streams **不会自动重投** PEL 里的消息 ——
// 只有 XCLAIM/XAUTOCLAIM（或换 consumer 名重读）才会重新投递。
// 少了它，「发布失败不 ACK」的后果不是「稍后重试」而是「永久卡住」。
//
// `Start` 每次从 `0` 扫起：稳态下 PEL 接近空（成功的消息立刻被 ACK），
// 重复扫描的代价可以忽略；若 PEL 真的堆积，说明下游长期不可用，
// 此时更该做的是告警而不是优化扫描游标。
func (s *RedisStreams) ClaimStale(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int) ([]StreamEntry, error) {
	if count <= 0 {
		return nil, nil
	}

	msgs, _, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0",
		Count:    int64(count),
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	out := make([]StreamEntry, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, StreamEntry{ID: m.ID, Fields: toStringMap(m.Values)})
	}
	return out, nil
}

// Ack 确认消息。
func (s *RedisStreams) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.rdb.XAck(ctx, stream, group, ids...).Err()
}

// toStringMap 把 Redis 返回的松散值转成字符串表。
//
// Redis Streams 的值都是字符串，但 go-redis 解成 `any`；这里对非字符串项
// 用 fmt 兜底而不是丢弃 —— 丢字段会让下游看到「安静缺失」的载荷。
func toStringMap(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		switch t := v.(type) {
		case string:
			out[k] = t
		case []byte:
			out[k] = string(t)
		default:
			out[k] = fmt.Sprint(v)
		}
	}
	return out
}
