package connector

import (
	"sync/atomic"

	"github.com/sony/gobreaker"
)

// breaker 包装 gobreaker，补上文档要求但 gobreaker 不直接支持的两点：
//
//   - 「401/403 → **立即**熔断」（07 §4.2：不等满 10 次连续失败，
//     否则失效的凭据会先把 Odoo 账号推向被锁）；
//   - 「业务拒绝（4xx）不计入熔断」—— 参数判错不代表 Odoo 不健康。
type breaker struct {
	cb *gobreaker.CircuitBreaker

	// authFailed 在探测到 401/403 时置位，供 ReadyToTrip 立即跳闸；
	// 恢复闭环（StateClosed）时清零。
	authFailed atomic.Bool

	onP1Alert func(reason string)
}

func newBreaker(p Policy) *breaker {
	b := &breaker{onP1Alert: p.BreakerOpenP1Alert}

	b.cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "odoo-connector",
		MaxRequests: 1, // 半开态只放行 1 次探测，避免恢复瞬间打回原形
		Interval:    p.BreakerInterval,
		Timeout:     p.BreakerTimeout,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			if b.authFailed.Load() {
				return true
			}
			if c.ConsecutiveFailures >= p.BreakerConsecutiveFailures {
				return true
			}
			if c.Requests >= p.BreakerMinSamples {
				return float64(c.TotalFailures)/float64(c.Requests) > p.BreakerFailureRatio
			}
			return false
		},
		IsSuccessful: func(err error) bool {
			if err == nil {
				return true
			}
			// 谓词带副作用（记录凭据失效）：gobreaker 只在 IsSuccessful 里
			// 把错误交给我们，这是唯一能同时「判定」与「留痕」的入口。
			switch Classify(err) {
			case CodeAuthRequired, CodeForbidden:
				b.authFailed.Store(true)
			}
			return !isTechnicalFailure(err)
		},
		OnStateChange: func(_ string, _, to gobreaker.State) {
			switch to {
			case gobreaker.StateOpen:
				if b.authFailed.Load() && b.onP1Alert != nil {
					b.onP1Alert("凭据失效（401/403），已立即熔断")
				}
			case gobreaker.StateClosed:
				b.authFailed.Store(false)
			}
		},
	})
	return b
}

// Execute 在有熔断保护下执行一次调用。
func (b *breaker) Execute(req func() (any, error)) (any, error) {
	return b.cb.Execute(req)
}

// State 返回当前熔断状态（观测用）。
func (b *breaker) State() gobreaker.State { return b.cb.State() }
