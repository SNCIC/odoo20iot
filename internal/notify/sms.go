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

// SMSChannel 用厂商 HTTP 网关投递短信。
//
// 只支持「HTTP API + 已报备模板 + 变量」这一种形状：短信内容在运营商侧
// 必须使用报备过的模板，不能自由拼字符串。这不是设计偏好而是合规要求，
// 所以本通道**不接受任意正文**，只把告警字段当模板变量传过去 ——
// 因此它天然不可能泄漏正文里的敏感内容，也天然发不出「自定义短信」。
type SMSChannel struct {
	guard    *Guard
	endpoint string
	token    string
	sender   string
	template string
	timeout  time.Duration
	client   *http.Client
}

// NewSMSChannel 构造。
func NewSMSChannel(g *Guard, endpoint, token, sender, templateID string, timeout time.Duration) *SMSChannel {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &SMSChannel{
		guard: g, endpoint: endpoint, token: token, sender: sender,
		template: templateID, timeout: timeout,
	}
}

func (c *SMSChannel) Name() string { return ChannelSMS }

// Send 投递短信。
func (c *SMSChannel) Send(ctx context.Context, msg Message, recipients []string) error {
	if c.endpoint == "" || c.template == "" {
		// 没配网关/模板时**明确报永久失败**，而不是静默返回成功：
		// 静默成功会让「短信通道一直没配」这件事永远不暴露，
		// 直到某天有人问「为什么半夜没人打电话」。
		return Permanent("sms 通道未配置（缺网关地址或模板 ID）")
	}
	if len(recipients) == 0 {
		return Permanent("sms 通道没有收件人号码")
	}

	t, err := c.guard.Check(ctx, c.endpoint)
	if err != nil {
		return err
	}

	// 只传**模板变量**，不传自由正文（见类型注释）。
	body, err := json.Marshal(map[string]any{
		"phone_numbers": recipients,
		"sign_name":     c.sender,
		"template_id":   c.template,
		"template_param": map[string]string{
			"alarm_id": msg.AlarmID,
			"level":    msg.Level,
			"device":   msg.DeviceID,
			"subject":  msg.Subject,
			"time":     msg.At.Format(time.RFC3339),
		},
	})
	if err != nil {
		return fmt.Errorf("序列化短信载荷: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		t.URL.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("构造短信请求: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Host = t.URL.Host

	client := c.client
	if client == nil {
		client = &http.Client{Transport: c.guard.Transport(t), Timeout: c.timeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("POST 短信网关 %s: %w", t.Host, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return classifyHTTP(t.Host, resp.StatusCode, string(respBody))
	}
	// 与 IM 机器人同一个陷阱：短信厂商也普遍「HTTP 200 + 响应体里带码值」
	//（欠费、模板未报备、号码在黑名单）。故复用同一套判定，
	// 而不是每个通道各写一份 —— 一份漏了就是一个静默丢通知的口子。
	if err := classifyBusinessBody(string(respBody)); err != nil {
		return fmt.Errorf("短信网关业务错误: %w", err)
	}
	return nil
}

// SMSCongestion 判定短信通道是否拥塞（04 §2.3：失败率 > 30% 自动切换邮件）。
//
// 放在这里作为**语义说明**，判定逻辑在 Dispatcher 的通道健康统计里（通用实现）。
func SMSCongestion(reason string) bool { return strings.Contains(reason, ChannelSMS) }
