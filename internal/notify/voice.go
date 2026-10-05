package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// VoiceChannel 通过可配置的 HTTP 语音网关发起外呼。
// 网关适配保持在一个通用 JSON 契约内，避免把运营商 SDK 或密钥写死在服务中。
type VoiceChannel struct {
	guard    *Guard
	endpoint string
	token    string
	from     string
	timeout  time.Duration
	client   *http.Client
}

// NewVoiceChannel 构造语音通道。
func NewVoiceChannel(g *Guard, endpoint, token, from string, timeout time.Duration) *VoiceChannel {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &VoiceChannel{guard: g, endpoint: endpoint, token: token, from: from, timeout: timeout}
}

func (c *VoiceChannel) Name() string { return ChannelVoice }

// Send 向每个号码提交一次外呼任务。运营商异步接通不属于本服务的职责，
// HTTP 2xx 且业务码成功即视为已受理。
func (c *VoiceChannel) Send(ctx context.Context, msg Message, recipients []string) error {
	if c.endpoint == "" {
		return Permanent("voice 通道未配置语音网关地址")
	}
	if len(recipients) == 0 {
		return Permanent("voice 通道没有收件人号码")
	}
	t, err := c.guard.Check(ctx, c.endpoint)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{
		"to":          recipients,
		"from":        c.from,
		"subject":     msg.Subject,
		"text":        msg.Body,
		"alarm_id":    msg.AlarmID,
		"trace_id":    msg.TraceID,
		"project_id":  msg.ProjectID,
		"device_id":   msg.DeviceID,
		"level":       msg.Level,
		"occurred_at": msg.At.Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("序列化语音载荷: %w", err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, t.URL.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("构造语音请求: %w", err)
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
		return fmt.Errorf("POST 语音网关 %s: %w", t.Host, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return classifyHTTP(t.Host, resp.StatusCode, string(body))
	}
	if err := classifyVoiceBody(string(body)); err != nil {
		return err
	}
	return nil
}

type voiceResponse struct {
	Code    *int   `json:"code"`
	ErrCode *int   `json:"errcode"`
	Success *bool  `json:"success"`
	Message string `json:"message"`
	Msg     string `json:"msg"`
}

func classifyVoiceBody(body string) error {
	if len(body) == 0 {
		return nil
	}
	var response voiceResponse
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		return nil
	}
	if response.Success != nil && !*response.Success {
		return fmt.Errorf("voice 网关业务失败: %s", firstNonEmpty(response.Message, response.Msg))
	}
	if response.Code != nil && *response.Code != 0 {
		return fmt.Errorf("voice 网关业务失败（code=%d）: %s", *response.Code, firstNonEmpty(response.Message, response.Msg))
	}
	if response.ErrCode != nil && *response.ErrCode != 0 {
		return fmt.Errorf("voice 网关业务失败（errcode=%d）: %s", *response.ErrCode, firstNonEmpty(response.Message, response.Msg))
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return "unknown error"
}
