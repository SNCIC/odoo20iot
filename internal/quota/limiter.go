package quota

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Decision struct {
	Allowed bool
	Current int64
	Limit   int64
}

// Reserve 使用 Redis 原子脚本预留配额。limit<=0 表示未配置硬限额，直接放行。
// 调用方应在真正执行有配额成本的操作前调用；脚本拒绝时不会增加计数。
func (c *RedisCounter) Reserve(ctx context.Context, projectID int64, metric string, limit, delta int64, windowStart time.Time, ttl time.Duration) (Decision, error) {
	if projectID <= 0 || metric == "" || delta < 0 || (delta == 0 && !isLatestSnapshotMetric(metric)) {
		return Decision{}, fmt.Errorf("quota: project_id、metric 和正增量必填")
	}
	if limit <= 0 {
		return Decision{Allowed: true, Limit: limit}, nil
	}
	if ttl <= 0 {
		ttl = c.ttl
	}
	format := "20060102"
	if windowStart.UTC().Day() == 1 && windowStart.UTC().Hour() == 0 {
		format = "200601"
	}
	key := fmt.Sprintf("quota:reserve:%d:%s:%s", projectID, metric, windowStart.UTC().Format(format))
	script := redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local delta = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
if current + delta > limit then return {0, current} end
local next = redis.call('INCRBY', KEYS[1], delta)
redis.call('EXPIRE', KEYS[1], ARGV[3])
return {1, next}
`)
	values, err := script.Run(ctx, c.rdb, []string{key}, delta, limit, int64(ttl.Seconds())).Int64Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("quota: 预留配额: %w", err)
	}
	if len(values) != 2 {
		return Decision{}, fmt.Errorf("quota: 预留配额返回值非法")
	}
	return Decision{Allowed: values[0] == 1, Current: values[1], Limit: limit}, nil
}

// ReplaceDaily 将已关闭日窗口的 Redis 加速桶修正为 PG 事实值。
// 仅供对账器使用；当前窗口不应调用此方法，避免覆盖并发写入。
func (c *RedisCounter) ReplaceDaily(ctx context.Context, projectID int64, metric string, day time.Time, value int64, ttl time.Duration) error {
	if projectID <= 0 || metric == "" || value < 0 {
		return fmt.Errorf("quota: 修正参数非法")
	}
	if ttl <= 0 {
		ttl = c.ttl
	}
	key := CounterKey(projectID, metric, day)
	script := redis.NewScript(`redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2]); return 1`)
	if err := script.Run(ctx, c.rdb, []string{key}, value, int64(ttl.Seconds())).Err(); err != nil {
		return fmt.Errorf("quota: 修正计数器 %s: %w", key, err)
	}
	return nil
}
