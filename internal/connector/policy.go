// Package connector 实现 07 §4.3 的 `odoo-connector` 编排层：
// 限流 → 排队/准入 → 熔断 → 重试 → 错误码映射。
//
// 它**不嵌入**网关或管道（07 §4.1）：作为独立服务消费 `iot.odoo.*` / Redis Streams，
// 并把写回请求编排到 `internal/odoo` 的 JSON-2 客户端上。
//
// 分工：`internal/odoo` 只管「怎么把一次 HTTP 调用发对」（鉴权头、多库路由、参数形态）；
// 本包只管「什么时候允许发、失败了要不要再发」（限流、熔断、退避、幂等占位）。
package connector

import (
	"time"
)

// 07 §4.3 的默认策略。这些数字来自文档，改动前请先改文档。
const (
	// DefaultRatePerSecond 是令牌桶速率：**保护 Odoo 的第一道闸门**
	// （对应 C2：开发库未配 workers，突发请求会把 Odoo 打满）。
	DefaultRatePerSecond = 20.0
	// DefaultRateBurst 是令牌桶桶容量。文档未给此参数：取等于速率，
	// 即最多允许 1 秒的突发，超出即平滑排队。
	DefaultRateBurst = 20

	// DefaultMaxInFlight 是最大在途请求数。
	DefaultMaxInFlight = 8
	// DefaultMaxQueue 是排队上限，超出**拒绝并告警**（不是无限等待）。
	DefaultMaxQueue = 1000

	// DefaultDialTimeout 是连接超时。
	DefaultDialTimeout = 3 * time.Second
	// DefaultReadTimeout 是读超时。Odoo 有慢查询风险，故给足但不超过 15s。
	DefaultReadTimeout = 15 * time.Second

	// DefaultBreakerConsecutiveFailures 是熔断的连续失败阈值。
	DefaultBreakerConsecutiveFailures = 10
	// DefaultBreakerFailureRatio 是熔断的失败率阈值。
	DefaultBreakerFailureRatio = 0.5
	// DefaultBreakerMinSamples 是失败率判定所需的最小样本数。
	DefaultBreakerMinSamples = 20
	// DefaultBreakerTimeout 是熔断打开后的保持时长。
	DefaultBreakerTimeout = 60 * time.Second
	// DefaultBreakerInterval 是熔断统计窗口。
	//
	// ⚠️ 文档只写了「连续 10 次失败**或**失败率 > 50% 且样本 ≥ 20」，
	// 未定义失败率的统计窗口。取 60s 是本实现的**决策**：没有窗口的失败率
	// 会在长跑后趋于稳定，永远触发不了。窗口内计数由 gobreaker 自行滚动清零。
	DefaultBreakerInterval = 60 * time.Second
)

// DefaultRetryBackoff 是重试退避序列（07 §4.3：1s/3s/9s，最多 3 次）。
//
// 长度即重试上限：**4xx 不重试**，故业务拒绝一次即返回，不会走完这个序列。
var DefaultRetryBackoff = []time.Duration{
	1 * time.Second,
	3 * time.Second,
	9 * time.Second,
}

// Policy 是编排策略。零值经 Normalize 后取上面的文档默认。
type Policy struct {
	RatePerSecond float64
	RateBurst     int

	MaxInFlight int
	MaxQueue    int

	DialTimeout time.Duration
	ReadTimeout time.Duration

	RetryBackoff []time.Duration

	BreakerConsecutiveFailures uint32
	BreakerFailureRatio        float64
	BreakerMinSamples          uint32
	BreakerTimeout             time.Duration
	BreakerInterval            time.Duration

	// BreakerOpenP1Alert 在熔断因**认证失败（401/403）**打开时置位，
	// 对应 07 §4.2「401/403 → 立即熔断 + P1 告警（不重试，避免账号被锁）」。
	// 由 OnStateChange 回调置位，供上层捞取并告警。
	BreakerOpenP1Alert func(reason string)
}

// Normalize 用文档默认补齐零值，返回可直接使用的策略。
func (p Policy) Normalize() Policy {
	if p.RatePerSecond <= 0 {
		p.RatePerSecond = DefaultRatePerSecond
	}
	if p.RateBurst <= 0 {
		p.RateBurst = DefaultRateBurst
	}
	if p.MaxInFlight <= 0 {
		p.MaxInFlight = DefaultMaxInFlight
	}
	if p.MaxQueue <= 0 {
		p.MaxQueue = DefaultMaxQueue
	}
	if p.DialTimeout <= 0 {
		p.DialTimeout = DefaultDialTimeout
	}
	if p.ReadTimeout <= 0 {
		p.ReadTimeout = DefaultReadTimeout
	}
	// 用 nil 判定而非 len == 0：显式传入的**空切片**表示「不重试」，
	// 这是一个有意义的配置（例如纯只读查询重试无益）。
	if p.RetryBackoff == nil {
		p.RetryBackoff = DefaultRetryBackoff
	}
	if p.BreakerConsecutiveFailures == 0 {
		p.BreakerConsecutiveFailures = DefaultBreakerConsecutiveFailures
	}
	if p.BreakerFailureRatio <= 0 {
		p.BreakerFailureRatio = DefaultBreakerFailureRatio
	}
	if p.BreakerMinSamples == 0 {
		p.BreakerMinSamples = DefaultBreakerMinSamples
	}
	if p.BreakerTimeout <= 0 {
		p.BreakerTimeout = DefaultBreakerTimeout
	}
	if p.BreakerInterval <= 0 {
		p.BreakerInterval = DefaultBreakerInterval
	}
	return p
}
