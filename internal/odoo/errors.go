package odoo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// 哨兵错误：调用方用 errors.Is 判定语义，不必自己解析状态码。
var (
	// ErrUnauthorized 认证失败（401）：API Key 无效/过期/scope 不含 rpc。
	ErrUnauthorized = errors.New("odoo: 认证失败")
	// ErrForbidden 无权限（403）：服务账号 ACL 不足。
	ErrForbidden = errors.New("odoo: 无权限")
	// ErrConflict 幂等冲突（409）：同键不同请求摘要（07 §4.3.1）。
	ErrConflict = errors.New("odoo: 幂等冲突")
	// ErrValidation 校验失败（422）：参数不合法。
	ErrValidation = errors.New("odoo: 校验失败")
)

// APIError 是 Odoo 返回的非 2xx 响应。
type APIError struct {
	StatusCode int
	Model      string
	Method     string
	Body       string // 已剥离 Python 堆栈并截断的响应摘要
}

func newAPIError(status int, model, method string, body []byte) *APIError {
	return &APIError{
		StatusCode: status,
		Model:      model,
		Method:     method,
		Body:       sanitizeErrorBody(body),
	}
}

func (e *APIError) Error() string {
	return fmt.Sprintf("odoo: %s.%s 返回 %d: %s", e.Model, e.Method, e.StatusCode, e.Body)
}

// Unwrap 把状态码映射到哨兵错误，供 errors.Is 使用。
func (e *APIError) Unwrap() error {
	switch e.StatusCode {
	case http.StatusUnauthorized:
		return ErrUnauthorized
	case http.StatusForbidden:
		return ErrForbidden
	case http.StatusConflict:
		return ErrConflict
	case http.StatusUnprocessableEntity:
		return ErrValidation
	default:
		return nil
	}
}

// Retryable 判定该响应是否应重试。
//
// 07 §4.3：**仅 5xx / 429 / 网络错误重试，4xx 一律不重试** ——
// 重试 4xx 只会放大错误（参数错的请求重放多少次都还是错的）。
func (e *APIError) Retryable() bool {
	return e.StatusCode >= 500 || e.StatusCode == http.StatusTooManyRequests
}

// IsRetryable 判定一个错误是否可重试（含网络错误）。
func IsRetryable(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Retryable()
	}
	// 网络层错误（超时、连接失败、EOF）可重试。
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return false
}

// sanitizeErrorBody 只保留 Odoo 错误响应里的 name / message，**丢弃 debug**。
//
// 技术方案 p.7 明令「禁止把 Python 堆栈或 ORM 异常原文暴露给外部」。
// 在**源头**剥离比在每个出口处提防更可靠：开发库（debug 打开）返回的
// traceback 动辄数 KB，一旦被顺手拼进响应体或日志，就是实打实的信息泄露。
//
// Odoo 的错误体形如：
//
//	{"name":"werkzeug.exceptions.Unauthorized","message":"Invalid apikey",
//	 "arguments":[...],"debug":"Traceback (most recent call last): ..."}
func sanitizeErrorBody(body []byte) string {
	var payload struct {
		Name    string `json:"name"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		if summary := strings.TrimSpace(payload.Name + " " + payload.Message); summary != "" {
			return truncate(summary, 512)
		}
	}
	// 非 JSON（HTML 错误页、网关返回的纯文本）：只能整体截断。
	return truncate(string(body), 512)
}

// truncate 按字节截断（用于日志安全的错误摘要）。
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…(截断)"
}
