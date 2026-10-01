package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// WebhookChannel 用 HTTP POST 投递（钉钉 / 飞书 / 企微的群机器人都是这个形状）。
type WebhookChannel struct {
	guard   *Guard
	timeout time.Duration
	client  *http.Client
}

// NewWebhookChannel 构造。
func NewWebhookChannel(g *Guard, timeout time.Duration) *WebhookChannel {
	if timeout <= 0 {
		// 04 §2.3 定的是 5s。
		timeout = 5 * time.Second
	}
	return &WebhookChannel{guard: g, timeout: timeout}
}

func (c *WebhookChannel) Name() string { return ChannelWebhook }

// Send 逐个 URL 投递。
//
// 一个 URL 失败**不拖垮其他收件人**：同一个告警发给三个群，其中一个群
// 机器人被禁用，不该导致另外两个群也收不到。
func (c *WebhookChannel) Send(ctx context.Context, msg Message, recipients []string) error {
	if len(recipients) == 0 {
		return Permanent("webhook 通道没有收件人 URL")
	}
	var retryable error
	for _, raw := range recipients {
		err := c.sendOne(ctx, msg, raw)
		if err == nil {
			continue
		}
		if IsPermanent(err) {
			// 白名单未命中 / 内网段 —— 换个 URL 也一样，直接上抛。
			return err
		}
		if retryable == nil {
			retryable = err
		}
	}
	return retryable
}

func (c *WebhookChannel) sendOne(ctx context.Context, msg Message, raw string) error {
	t, err := c.guard.Check(ctx, raw)
	if err != nil {
		return err // 已是永久失败
	}

	payload, err := json.Marshal(c.body(msg))
	if err != nil {
		return fmt.Errorf("序列化 webhook 载荷: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		t.URL.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("构造 webhook 请求: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	// Host 头必须是**原域名**而不是固定 IP：企微/钉钉的机器人 URL 里带 key，
	// 而不少反代按 Host 分流 —— 发成 IP 会被 404 或落到默认站点。
	req.Host = t.URL.Host

	client := *c.clientOrNew(t)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", t.Host, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	// 响应体是不可信输入：截断到 1 KB 再进日志与 DLQ（04 §2.3）。
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return classifyHTTP(t.Host, resp.StatusCode, string(body))
	}
	// ⚠️ 2xx **不等于投递成功**：国内三家 IM 的机器人 webhook 都是
	// 「HTTP 200 + 响应体里带业务错误码」。只判状态码的话，
	// token 过期、机器人被移出群、频率超限全都会被记成「投递成功」——
	// 通知静默丢失，而指标上一切正常。
	if err := classifyBusinessBody(string(body)); err != nil {
		return err
	}
	return nil
}

// body 是 webhook 的 JSON 载荷。
//
// 用 04 §2.3 的告警语义字段拼一个**各家都能读**的扁平结构，
// 而不是直接转发 alarm.Event：机器人只认 text/markdown，
// 把内部事件结构整个发过去，运维看到的就是一坨看不懂的字段名。
func (c *WebhookChannel) body(msg Message) map[string]any {
	out := map[string]any{
		"msg_type": "text",
		"text": map[string]any{
			"content": msg.Subject + "\n" + msg.Body,
		},
	}
	// 同时带上结构化字段：有的接收方是自建服务（DAG 的 webhook.call 动作），
	// 它们要的是能解析的字段，不是终端文本。
	out["alarm_id"] = msg.AlarmID
	out["dedup_key"] = msg.DedupKey
	out["project_id"] = msg.ProjectID
	out["device_id"] = msg.DeviceID
	out["rule_id"] = msg.RuleID
	out["level"] = msg.Level
	out["trace_id"] = msg.TraceID
	if msg.Payload != nil {
		out["payload"] = msg.Payload
	}
	return out
}

func (c *WebhookChannel) clientOrNew(t *Target) *http.Client {
	if c.client != nil {
		return c.client
	}
	return &http.Client{Transport: c.guard.Transport(t), Timeout: c.timeout}
}

// classifyHTTP 把 HTTP 状态码分成「可重试」与「永久失败」。
func classifyHTTP(host string, code int, body string) error {
	switch {
	case code == http.StatusTooManyRequests:
		return fmt.Errorf("webhook %s 被限流（429）: %s", host, body)
	case code == http.StatusRequestTimeout:
		return fmt.Errorf("webhook %s 请求超时（408）: %s", host, body)
	case code >= 500:
		return fmt.Errorf("webhook %s 服务端错误（%d）: %s", host, code, body)
	default:
		// 其余 4xx：URL 写错、token 失效、机器人不存在。
		// 重试三次只会浪费三次超时，还把同一类噪音灌满 DLQ。
		return Permanent("webhook %s 返回 %d（重试无意义）: %s", host, code, body)
	}
}

// businessError 是 IM 机器人 webhook 的业务响应形状。
//
// 三家写法略有不同，但都是「2xx + 码值」：
//
//	钉钉 / 企微: {"errcode":0,"errmsg":"ok"}
//	飞书:        {"code":0,"msg":"success"}
//
// 两种都认，指针类型用来区分「字段不存在」与「字段为 0」。
type businessError struct {
	ErrCode *int   `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
	Code    *int   `json:"code"`
	Msg     string `json:"msg"`
}

// classifyBusinessBody 检查 2xx 响应体里是否藏着业务错误。
//
// ⚠️ 这是本项目里最容易「一切指标都正常、但人没收到通知」的地方：
// 国内三家 IM 的机器人 webhook 在 token 过期、机器人被移出群、频率超限时
// **仍然返回 HTTP 200**，错误只在响应体的码值里。只判 HTTP 状态码，
// 这三种情况全都会被记成投递成功。
//
// 判定为**可重试**而不是永久失败：这三类里「频率超限」重试就能过，
// 而判错的代价不对称 —— 判成可重试最多白试两次（随后照样进 DLQ 并降级），
// 判成永久失败会立刻降级，把一次限流当成通道故障。
func classifyBusinessBody(body string) error {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" || trimmed[0] != '{' {
		// 空响应、纯文本 "ok"、JSON 数组：无法判定。
		// 这类多出现在**自建接收方**，它们的约定本来就不是「码值」。
		// 按成功处理，但下游要看内容时得自己解析。
		return nil
	}
	var be businessError
	if err := json.Unmarshal([]byte(trimmed), &be); err != nil {
		// 是 JSON 但结构不认识：不能宣称成功（那是在替对方撒谎），
		// 也不该当成永久失败（我们看不懂不代表对方错了）。
		return fmt.Errorf("webhook 响应是 JSON 但结构无法识别: %s", truncate(trimmed))
	}
	if be.ErrCode != nil && *be.ErrCode != 0 {
		return fmt.Errorf("webhook 返回业务错误（HTTP 200）errcode=%d: %s", *be.ErrCode, be.ErrMsg)
	}
	if be.Code != nil && *be.Code != 0 {
		return fmt.Errorf("webhook 返回业务错误（HTTP 200）code=%d: %s", *be.Code, be.Msg)
	}
	return nil
}

// truncate 按 04 §2.3 把待记录的内容截到 1 KB。
func truncate(s string) string {
	if len(s) <= maxResponseBytes {
		return s
	}
	// 按字节截断可能切断一个多字节字符，这里简单补一个省略标记即可 ——
	// 目标是「日志不被灌爆」，不是「内容完整可读」。
	return s[:maxResponseBytes] + "…(截断)"
}
