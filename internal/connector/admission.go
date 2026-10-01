package connector

import (
	"context"
	"errors"
	"sync/atomic"

	"golang.org/x/time/rate"
)

// ErrQueueFull 表示排队人数超过 Policy.MaxQueue。
//
// 文档要求「超出拒绝并告警」，而不是无限等待 —— 无限等待会把压力堆在
// 连接器自己的内存里，最后以 OOM 收场，比直接拒绝更糟。
var ErrQueueFull = errors.New("connector: 排队已满，拒绝请求")

// admission 是准入控制：令牌桶限流 + 在途并发上限 + 有界排队。
type admission struct {
	limiter  *rate.Limiter
	inFlight chan struct{}
	maxQueue int64
	waiting  atomic.Int64
}

func newAdmission(p Policy) *admission {
	return &admission{
		limiter:  rate.NewLimiter(rate.Limit(p.RatePerSecond), p.RateBurst),
		inFlight: make(chan struct{}, p.MaxInFlight),
		maxQueue: int64(p.MaxQueue),
	}
}

// Acquire 获取一次执行许可：先占排队位，再等令牌，最后占在途名额。
//
// 排队位先行是刻意的：令牌桶只限制**速率**，不限制**队列长度**；
// 若不先卡排队位，突发流量会在 Wait 里无界堆积。
func (a *admission) Acquire(ctx context.Context) error {
	if a.waiting.Add(1) > a.maxQueue {
		a.waiting.Add(-1)
		return ErrQueueFull
	}
	defer a.waiting.Add(-1)

	if err := a.limiter.Wait(ctx); err != nil {
		return err
	}

	select {
	case a.inFlight <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release 释放在途名额（必须与成功的 Acquire 配对）。
func (a *admission) Release() { <-a.inFlight }

// Waiting 返回当前排队数（观测用）。
func (a *admission) Waiting() int64 { return a.waiting.Load() }
