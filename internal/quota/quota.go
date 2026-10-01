// Package quota 实现 04 §6 的用量聚合：把网关/管道上报的计量增量
// 累加到按天分桶的计数器上。
//
// 键格式（02 §5.2）：`quota:{pid}:{metric}:{yyyymmdd}`，TTL 7d。
package quota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/metering"
)

// ErrPermanent 标记不可重试的上报（非法 JSON），调用方应计数后 ACK 释放。
var ErrPermanent = errors.New("不可重试的计量上报")

// DefaultCounterTTL 是计数器的保留时长（02 §5.2：7d）。
const DefaultCounterTTL = 7 * 24 * time.Hour

// CounterStore 是用量计数器。
type CounterStore interface {
	// IncrBy 把某租户某指标的增量累加，返回累加后的值。
	IncrBy(ctx context.Context, projectID int64, metric string, delta int64) (int64, error)
}

// CounterKey 构造计数器键（02 §5.2）。
func CounterKey(projectID int64, metric string, day time.Time) string {
	return fmt.Sprintf("quota:%d:%s:%s", projectID, metric, day.UTC().Format("20060102"))
}

// RedisCounter 是 CounterStore 的 Redis 实现。
type RedisCounter struct {
	rdb redis.UniversalClient
	ttl time.Duration
	now func() time.Time // 注入时钟，便于测试跨天分桶
}

var _ CounterStore = (*RedisCounter)(nil)

// NewRedisCounter 构造 Redis 计数器。ttl <= 0 取 DefaultCounterTTL。
func NewRedisCounter(rdb redis.UniversalClient, ttl time.Duration) *RedisCounter {
	if ttl <= 0 {
		ttl = DefaultCounterTTL
	}
	return &RedisCounter{rdb: rdb, ttl: ttl, now: time.Now}
}

// IncrBy 实现 CounterStore。
//
// INCRBY 与 EXPIRE 放在同一 pipeline：计数器是**可丢的加速层**
// （02 §5.3：配额计数降级为本地保守阈值），但键必须有 TTL，
// 否则会随租户数无限增长。
func (c *RedisCounter) IncrBy(ctx context.Context, projectID int64, metric string, delta int64) (int64, error) {
	key := CounterKey(projectID, metric, c.now())

	pipe := c.rdb.Pipeline()
	incr := pipe.IncrBy(ctx, key, delta)
	pipe.Expire(ctx, key, c.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("累加计数器 %s: %w", key, err)
	}
	return incr.Val(), nil
}

// Metrics 是聚合器计数器。
type Metrics struct {
	// ReportsTotal 处理成功的上报批次数。
	ReportsTotal atomic.Int64
	// CountersTotal 累加过的 (租户, 指标) 项数。
	CountersTotal atomic.Int64
	// PermanentTotal 不可重试的上报数（非法 JSON）。
	PermanentTotal atomic.Int64
	// Errors 计数写入失败的次数（调用方应重投）。
	Errors atomic.Int64
}

// Aggregator 把计量上报累加到计数器。
type Aggregator struct {
	store   CounterStore
	metrics *Metrics
	logger  *slog.Logger
}

// NewAggregator 构造聚合器。
func NewAggregator(store CounterStore, metrics *Metrics, logger *slog.Logger) *Aggregator {
	if metrics == nil {
		metrics = new(Metrics)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Aggregator{store: store, metrics: metrics, logger: logger}
}

// Metrics 返回计数器。
func (a *Aggregator) Metrics() *Metrics { return a.metrics }

// Apply 处理一条计量上报。
//
// 返回 ErrPermanent 表示毒消息（无法解析），调用方应计数后 ACK 释放；
// 其他错误表示暂不可用，调用方应重投（Redis 恢复后补上计数）。
func (a *Aggregator) Apply(ctx context.Context, data []byte) error {
	var report metering.UsageReport
	if err := json.Unmarshal(data, &report); err != nil {
		a.metrics.PermanentTotal.Add(1)
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	if len(report.Counters) == 0 {
		a.metrics.ReportsTotal.Add(1)
		return nil
	}

	for metric, delta := range report.Counters {
		if delta == 0 {
			continue
		}
		if _, err := a.store.IncrBy(ctx, report.ProjectID, metric, delta); err != nil {
			a.metrics.Errors.Add(1)
			return fmt.Errorf("累加 %s（project=%d）: %w", metric, report.ProjectID, err)
		}
		a.metrics.CountersTotal.Add(1)
	}

	a.metrics.ReportsTotal.Add(1)
	return nil
}
