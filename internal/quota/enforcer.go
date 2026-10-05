package quota

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	EnforcementReject   = "reject"
	EnforcementThrottle = "throttle"
)

var ErrExceeded = errors.New("quota: hard limit exceeded")

func ValidEnforcement(mode string) bool {
	return mode == EnforcementReject || mode == EnforcementThrottle
}

type PolicySource interface {
	Policy(context.Context, int64, string) (Policy, error)
}

type Enforcer struct {
	counter *RedisCounter
	source  PolicySource
	logger  *slog.Logger
	ttl     time.Duration
	mu      sync.Mutex
	cache   map[string]cachedPolicy
}

type cachedPolicy struct {
	policy  Policy
	expires time.Time
}

func NewEnforcer(counter *RedisCounter, source PolicySource, ttl time.Duration, logger *slog.Logger) *Enforcer {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Enforcer{counter: counter, source: source, logger: logger, ttl: ttl, cache: make(map[string]cachedPolicy)}
}

// ReserveTelemetry 在遥测进入总线前预留 msg_count。配额基础设施故障时放行，
// 但记录日志；明确返回超限时才拒绝业务请求。
func (e *Enforcer) ReserveTelemetry(ctx context.Context, projectID int64, duplicate bool) (Decision, error) {
	if duplicate || e == nil || e.counter == nil || e.source == nil {
		return Decision{Allowed: true}, nil
	}
	p, err := e.policy(ctx, projectID, "msg_count")
	if err != nil {
		e.logger.Error("读取配额策略失败，遥测降级放行", "project_id", projectID, "error", err)
		return Decision{Allowed: true}, nil
	}
	if p.HardLimit <= 0 {
		return Decision{Allowed: true, Limit: p.HardLimit}, nil
	}
	start, end := quotaWindow(time.Now().UTC(), p.Window)
	if start.IsZero() {
		return Decision{Allowed: true}, nil
	}
	decision, err := e.counter.Reserve(ctx, projectID, "msg_count", p.HardLimit, 1, start, time.Until(end)+DefaultCounterTTL)
	if err != nil {
		e.logger.Error("预留配额失败，遥测降级放行", "project_id", projectID, "error", err)
		return Decision{Allowed: true}, nil
	}
	if decision.Allowed || p.Enforcement != EnforcementThrottle {
		return decision, nil
	}
	for _, delay := range []time.Duration{25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond} {
		select {
		case <-ctx.Done():
			return Decision{}, ctx.Err()
		case <-time.After(delay):
		}
		decision, err = e.counter.Reserve(ctx, projectID, "msg_count", p.HardLimit, 1, start, time.Until(end)+DefaultCounterTTL)
		if err != nil {
			e.logger.Error("节流重试预留配额失败，遥测降级放行", "project_id", projectID, "error", err)
			return Decision{Allowed: true}, nil
		}
		if decision.Allowed {
			return decision, nil
		}
	}
	return decision, ErrExceeded
}

func (e *Enforcer) policy(ctx context.Context, projectID int64, metric string) (Policy, error) {
	key := fmt.Sprintf("%d:%s", projectID, metric)
	e.mu.Lock()
	item, ok := e.cache[key]
	e.mu.Unlock()
	if ok && time.Now().Before(item.expires) {
		return item.policy, nil
	}
	p, err := e.source.Policy(ctx, projectID, metric)
	if err != nil {
		return Policy{}, err
	}
	if p.Enforcement == "" {
		p.Enforcement = EnforcementReject
	}
	e.mu.Lock()
	e.cache[key] = cachedPolicy{policy: p, expires: time.Now().Add(e.ttl)}
	e.mu.Unlock()
	return p, nil
}
