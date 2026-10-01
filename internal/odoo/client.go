// Package odoo 是 Odoo 20 JSON-2 API 的 Go 适配层（ADR-011，07 §4）。
//
// 三条铁律（07 §1.2）决定了本包边界：
//   - **R3**：只用 `POST /json/2/<model>/<method>`，禁止 `/jsonrpc` `/xmlrpc`
//     （后者自 Odoo 19 弃用、计划 22 移除）；
//   - **R2**：禁止直连 PostgreSQL 写 —— 本包只走 HTTP，写入必须经 Odoo ORM；
//   - **R5**：所有对接逻辑集中在本适配层，业务代码不得直接调 Odoo。
//
// 本包是 D2 骨架：只覆盖请求构造、鉴权头、多库路由、错误映射与分页约定；
// 限流/熔断/重试编排/幂等账本/DLQ 属于 `odoo-connector` 服务（07 §4.3），不在此处。
package odoo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultTimeout 是单次请求超时。
	DefaultTimeout = 30 * time.Second
	// DefaultSearchLimit 是 search_read 的默认页大小（07 §6 S1：分页 limit=200）。
	DefaultSearchLimit = 200
	// maxBodyBytes 限制读入的响应体大小，避免异常响应打爆内存。
	maxBodyBytes = 32 << 20 // 32 MiB
)

// Config 是客户端配置。
type Config struct {
	// BaseURL 形如 `https://odoo.example.com`（不含路径；尾部斜杠可有可无）。
	BaseURL string
	// Database 是 Odoo 库名。
	//
	// **每次请求都必须携带** `X-Odoo-Database`：多库部署下缺省会静默路由到
	// 错误库（07 §2.2）—— 这是最难排查的一类故障，因此由本包强制注入，
	// 且构造时即校验非空，不允许业务代码省略。
	Database string
	// APIKey 是 `res.users.apikeys` 的密钥，scope 必须含 `rpc`。
	// 生产由 Vault 注入，禁止明文写配置文件（07 §4.2）。
	APIKey string
	// HTTPClient 可注入（测试用）；为 nil 时按 Timeout 构造。
	HTTPClient *http.Client
	// Timeout 单次请求超时（默认 30s）。
	Timeout time.Duration
}

// Client 是 Odoo JSON-2 客户端。并发安全。
type Client struct {
	base *url.URL
	cfg  Config
	http *http.Client
}

// New 构造客户端并校验必填项。
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("odoo: BaseURL 不能为空")
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("odoo: Database 不能为空（多库路由必须显式指定）")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("odoo: APIKey 不能为空")
	}

	base, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("odoo: 解析 BaseURL: %w", err)
	}
	if base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("odoo: BaseURL 需含协议与主机（如 https://odoo.example.com）")
	}

	hc := cfg.HTTPClient
	if hc == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = DefaultTimeout
		}
		hc = &http.Client{Timeout: timeout}
	}
	return &Client{base: base, cfg: cfg, http: hc}, nil
}

// Database 返回配置的库名（便于日志与断言）。
func (c *Client) Database() string { return c.cfg.Database }

// Call 调用 `POST /json/2/{model}/{method}`。
//
// params **必须是命名参数的 JSON 对象**（JSON-2 不支持位置参数，07 §2.2）；
// out 为 nil 时忽略响应体。
func (c *Client) Call(ctx context.Context, model, method string, params any, out any) error {
	if model == "" || method == "" {
		return fmt.Errorf("odoo: model 与 method 不能为空")
	}

	endpoint := *c.base
	endpoint.Path = "/json/2/" + model + "/" + method

	payload := []byte("{}")
	if params != nil {
		var err error
		if payload, err = json.Marshal(params); err != nil {
			return fmt.Errorf("odoo: 序列化参数: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("odoo: 构造请求: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	// 多库路由（07 §2.2）：必须在客户端强制注入。
	req.Header.Set("X-Odoo-Database", c.cfg.Database)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("odoo: 请求 %s.%s: %w", model, method, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return fmt.Errorf("odoo: 读取 %s.%s 响应: %w", model, method, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return newAPIError(resp.StatusCode, model, method, body)
	}
	if out == nil || len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("odoo: 解析 %s.%s 响应: %w", model, method, err)
	}
	return nil
}

// SearchReadRequest 是 `search_read` 的命名参数（JSON-2 只支持命名参数）。
type SearchReadRequest struct {
	Domain any      `json:"domain,omitempty"`
	Fields []string `json:"fields,omitempty"`
	Limit  int      `json:"limit,omitempty"`
	Offset int      `json:"offset,omitempty"`
	Order  string   `json:"order,omitempty"`
}

// SearchRead 调用 `{model}.search_read`（07 §6 S1 主数据同步的主路径）。
//
// Limit <= 0 时取 DefaultSearchLimit：不设上限的 search_read 会把整表拉回来。
func (c *Client) SearchRead(ctx context.Context, model string, req SearchReadRequest, out any) error {
	if req.Limit <= 0 {
		req.Limit = DefaultSearchLimit
	}
	return c.Call(ctx, model, "search_read", req, out)
}
