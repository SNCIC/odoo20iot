package connector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/SNCIC/odoo20iot/internal/odoo"
)

// Code 是 07 §4.3.2 约定的对外错误码。
type Code string

// 错误码集合与 07 §4.3.2 的表格一一对应。
const (
	CodeAuthRequired        Code = "AUTH_REQUIRED"
	CodeForbidden           Code = "FORBIDDEN"
	CodeBusinessRejected    Code = "BUSINESS_REJECTED"
	CodeIdempotencyConflict Code = "IDEMPOTENCY_CONFLICT"
	CodeUpstreamTimeout     Code = "UPSTREAM_TIMEOUT"
	CodeCircuitOpen         Code = "CIRCUIT_OPEN"
	CodeRateLimited         Code = "RATE_LIMITED"
	CodeUpstreamError       Code = "UPSTREAM_ERROR"
)

// Error 是连接器对外暴露的统一错误（含 code / message / trace_id）。
//
// 对外只给这三样：**禁止把 Python 堆栈或 ORM 异常原文暴露给外部**
// （技术方案 p.7）——原始错误留在 Err 里供内部日志使用，不进对外响应体。
type Error struct {
	Code    Code
	Message string
	TraceID string
	Err     error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
}

// Unwrap 保留底层错误，便于内部用 errors.Is/As 判定。
func (e *Error) Unwrap() error { return e.Err }

// Classify 把底层错误映射为对外错误码（07 §4.3.2）。
func Classify(err error) Code {
	if err == nil {
		return ""
	}

	var apiErr *odoo.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.StatusCode {
		case http.StatusUnauthorized:
			return CodeAuthRequired
		case http.StatusForbidden:
			return CodeForbidden
		case http.StatusConflict:
			return CodeIdempotencyConflict
		case http.StatusUnprocessableEntity:
			return CodeBusinessRejected
		case http.StatusGatewayTimeout:
			return CodeUpstreamTimeout
		case http.StatusTooManyRequests:
			return CodeRateLimited
		}
		if apiErr.StatusCode >= 500 {
			return CodeUpstreamError
		}
		// 其余 4xx：文档未逐一列举，按 §4.3.2 的原则
		// 「技术错误与业务拒绝必须分开」归为业务拒绝 ——
		// 关键在于它是**不可重试**的（400/404 重放多少次结果都一样）。
		return CodeBusinessRejected
	}

	if isTimeout(err) {
		return CodeUpstreamTimeout
	}
	// 连接被拒、EOF、DNS 失败等 → 上游错误，可重试。
	return CodeUpstreamError
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// isTechnicalFailure 判定错误是否属于「技术失败」（应计入熔断）。
//
// 4xx 是 Odoo 在**正常应答**，只是我们发的东西不对 —— 把它算作熔断失败，
// 会让一个参数 bug 把整条链路熔断掉。401/403 例外：那是凭据问题，
// 文档要求「立即熔断 + P1 告警」，故计入并触发跳闸。
func isTechnicalFailure(err error) bool {
	switch Classify(err) {
	case CodeBusinessRejected, CodeIdempotencyConflict:
		return false
	default:
		return true
	}
}

// Retryable 判定某个错误码是否可重试（07 §4.3：仅 5xx / 429 / 网络错误可重试，
// 4xx 一律不重试）。
//
// hasIdempotencyKey 只影响 UPSTREAM_TIMEOUT —— §4.3.2 明确
// 「下游超时**仅在携带幂等键时**重试」：没有幂等键的重试可能造成重复写入。
func Retryable(code Code, hasIdempotencyKey bool) bool {
	switch code {
	case CodeUpstreamError, CodeRateLimited:
		return true
	case CodeUpstreamTimeout:
		return hasIdempotencyKey
	default:
		return false
	}
}
