package querysvc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrQueueFull 表示该租户的排队位已满 → 503。
	ErrQueueFull = errors.New("querysvc: 每租户排队已满")
	// ErrQueueTimeout 表示排队等超时 → 503。
	ErrQueueTimeout = errors.New("querysvc: 排队超时")
)

// LimiterConfig 是每租户并发限制的参数。
type LimiterConfig struct {
	// MaxConcurrency 是每租户同时在途的查询数（02 §4.3 默认 20）。
	MaxConcurrency int
	// MaxQueue 是每租户的**有界**排队位（无界队列等于 OOM）。
	MaxQueue int
	// QueueTimeout 是排队等待上限。
	QueueTimeout time.Duration
	// MaxTenants 是内存里保留的租户上限（有界 map）。
	MaxTenants int
	// IdleTTL 是租户通道的空闲驱逐阈值。
	IdleTTL time.Duration
}

// DefaultLimiterConfig 对齐 02 §4.3。
func DefaultLimiterConfig() LimiterConfig {
	return LimiterConfig{
		MaxConcurrency: 20,
		MaxQueue:       40,
		QueueTimeout:   2 * time.Second,
		MaxTenants:     10_000,
		IdleTTL:        10 * time.Minute,
	}
}

type tenantLimiter struct {
	sem      chan struct{}
	waiting  atomic.Int64
	lastUsed atomic.Int64 // unix nano
}

// Limiter 是每租户的「信号量 + 有界队列」。
//
// 为什么用 PG 之外的东西？这里是**进程内**的并发保护，与 internal/connector/admission
// 同一思路：文档说「超出排队而非拒绝」，但排队必须有界，否则一次慢查询就能把内存吃光。
// 本实现不跨副本（多副本时总并发 = 副本数 × MaxConcurrency），已记入遗留。
type Limiter struct {
	cfg     LimiterConfig
	metrics *Metrics
	now     func() time.Time

	mu      sync.Mutex
	tenants map[int64]*tenantLimiter
}

// NewLimiter 构造限流器（缺省项用 DefaultLimiterConfig 补齐）。
func NewLimiter(cfg LimiterConfig, m *Metrics, now func() time.Time) *Limiter {
	def := DefaultLimiterConfig()
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = def.MaxConcurrency
	}
	if cfg.MaxQueue < 0 {
		cfg.MaxQueue = def.MaxQueue
	}
	if cfg.QueueTimeout <= 0 {
		cfg.QueueTimeout = def.QueueTimeout
	}
	if cfg.MaxTenants <= 0 {
		cfg.MaxTenants = def.MaxTenants
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = def.IdleTTL
	}
	if now == nil {
		now = time.Now
	}
	return &Limiter{cfg: cfg, metrics: m, now: now, tenants: map[int64]*tenantLimiter{}}
}

// Acquire 占一个在途名额。返回的 release 必须被调用（通常 defer）。
//
// 先尝试非阻塞获取；拿不到才进有界队列。**先试后排队**很关键：否则 MaxQueue=0
// 会把「信号量其实有空位」的请求也一并拒掉。
func (l *Limiter) Acquire(ctx context.Context, projectID int64) (func(), error) {
	tl, err := l.tenant(projectID)
	if err != nil {
		return nil, err
	}

	select {
	case tl.sem <- struct{}{}:
		return l.releaseFunc(tl), nil
	default:
	}

	if l.metrics != nil {
		l.metrics.QueueDepth.Add(1)
		defer l.metrics.QueueDepth.Add(-1)
	}

	if tl.waiting.Add(1) > int64(l.cfg.MaxQueue) {
		tl.waiting.Add(-1)
		return nil, ErrQueueFull
	}
	defer tl.waiting.Add(-1)

	waitCtx, cancel := context.WithTimeout(ctx, l.cfg.QueueTimeout)
	defer cancel()
	select {
	case tl.sem <- struct{}{}:
		return l.releaseFunc(tl), nil
	case <-waitCtx.Done():
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, ErrQueueTimeout
		}
		return nil, ctx.Err()
	}
}

func (l *Limiter) releaseFunc(tl *tenantLimiter) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			<-tl.sem
			tl.lastUsed.Store(l.now().UnixNano())
		})
	}
}

// tenant 取（或惰性创建）某租户的限流通道。
func (l *Limiter) tenant(projectID int64) (*tenantLimiter, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if tl, ok := l.tenants[projectID]; ok {
		tl.lastUsed.Store(l.now().UnixNano())
		return tl, nil
	}

	if len(l.tenants) >= l.cfg.MaxTenants {
		l.evictIdleLocked()
		if len(l.tenants) >= l.cfg.MaxTenants {
			// 在册租户打满且没有可驱逐的：新租户先吃 503，而不是把内存顶爆。
			return nil, fmt.Errorf("%w: 在册租户已达上限 %d", ErrQueueFull, l.cfg.MaxTenants)
		}
	}

	tl := &tenantLimiter{sem: make(chan struct{}, l.cfg.MaxConcurrency)}
	tl.lastUsed.Store(l.now().UnixNano())
	l.tenants[projectID] = tl
	return tl, nil
}

// evictIdleLocked 驱逐空闲租户。**绝不驱逐在途**（信号量有占用或有人在排队）。
func (l *Limiter) evictIdleLocked() {
	cutoff := l.now().Add(-l.cfg.IdleTTL).UnixNano()
	for id, tl := range l.tenants {
		if len(tl.sem) > 0 || tl.waiting.Load() > 0 {
			continue
		}
		if tl.lastUsed.Load() < cutoff {
			delete(l.tenants, id)
		}
	}
}

// Tenants 返回当前在册租户数（指标/测试用）。
func (l *Limiter) Tenants() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.tenants)
}
