package connector

import "sync/atomic"

// 按错误码分桶的固定槽数（与本文件 errorCodes 一致）。
const codeSlotCount = 8

// errorCodes 是错误码分桶的固定顺序（与 07 §4.3.2 的表一致）。
var errorCodes = [codeSlotCount]Code{
	CodeAuthRequired,
	CodeForbidden,
	CodeBusinessRejected,
	CodeIdempotencyConflict,
	CodeUpstreamTimeout,
	CodeCircuitOpen,
	CodeRateLimited,
	CodeUpstreamError,
}

// Metrics 是连接器计数器，命名对齐 07 §4.5 的指标清单。
//
// 用原子计数而非 Prometheus 直方图：项目尚未引入 Prometheus 客户端，
// 且 §4.5 的 duration 直方图需要有观测后端才谈得上分位数。
// 落地 Prometheus 时，本结构即导出层的取值来源。
type Metrics struct {
	// CallsTotal 进入编排的调用总数（一次业务调用算一次，不含内部重试）。
	CallsTotal atomic.Int64
	// SuccessTotal 最终成功的调用数。
	SuccessTotal atomic.Int64
	// RetriesTotal 内部重试次数。
	RetriesTotal atomic.Int64
	// BreakerOpenTotal 熔断打开次数（对应 odoo_connector_breaker_state 的跃迁）。
	BreakerOpenTotal atomic.Int64
	// RateLimitedTotal 被本地准入（限流/排队）拒绝的次数。
	RateLimitedTotal atomic.Int64
	// DLQTotal 进死信的数量（对应 odoo_connector_dlq_total）。
	DLQTotal atomic.Int64

	// PublishedTotal 成功发布到 NATS 的 Odoo 事件数（C-1）。
	PublishedTotal atomic.Int64
	// Reclaimed 从 PEL 接管重投的事件数（持续增长说明下游长期不可用）。
	Reclaimed atomic.Int64
	// ReconcileRepublished 由对账补投的事件数（持续增长说明有静默遗漏）。
	ReconcileRepublished atomic.Int64
	// PublishErrors 发布失败、保留在 PEL 待重投的次数。
	PublishErrors atomic.Int64
	// IngestErrors Redis 侧（读取/ACK）失败次数。
	IngestErrors atomic.Int64
	// Poisoned 无法解析而丢弃的事件数（已 ACK，**必须告警**）。
	Poisoned atomic.Int64
	// WebhookReceived 收到的 C-2 webhook 请求数。
	WebhookReceived atomic.Int64
	// WebhookRejected 被拒的 C-2 webhook 请求数（鉴权/解析失败）。
	WebhookRejected atomic.Int64
	// CursorLagSeconds 是 C-2 水位的**当前**滞后秒数
	// （对应 odoo_connector_cursor_lag_seconds）。追平时归零。
	CursorLagSeconds atomic.Int64
	// DurationSumNanos / DurationCount 支撑
	// odoo_connector_request_duration_seconds 的均值观测。
	DurationSumNanos atomic.Int64
	DurationCount    atomic.Int64

	errorsByCode [codeSlotCount]atomic.Int64
}

// RecordError 按错误码计数（对应 odoo_connector_errors_total{code}）。
func (m *Metrics) RecordError(code Code) {
	for i, k := range errorCodes {
		if k == code {
			m.errorsByCode[i].Add(1)
			return
		}
	}
}

// ErrorsByCode 返回错误码 → 计数的快照。
func (m *Metrics) ErrorsByCode() map[Code]int64 {
	out := make(map[Code]int64, codeSlotCount)
	for i, k := range errorCodes {
		if v := m.errorsByCode[i].Load(); v != 0 {
			out[k] = v
		}
	}
	return out
}
