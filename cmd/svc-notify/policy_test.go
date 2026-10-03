package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/notify"
	"github.com/SNCIC/odoo20iot/internal/quota"
)

func TestPolicyFileSupportsEscalatedRecipients(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	data := map[string]any{
		"default": map[string]any{
			"channels": []string{notify.ChannelWebhook, notify.ChannelEmail},
			"template": "alarm",
			"recipients": map[string][]string{
				notify.ChannelWebhook: {"https://normal.example/hook"},
				notify.ChannelEmail:   {"ops@example.com"},
			},
			"escalated_recipients": map[string][]string{
				notify.ChannelWebhook: {"https://p1.example/hook"},
				notify.ChannelEmail:   {"oncall@example.com"},
			},
		},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("序列化策略失败: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("写入策略失败: %v", err)
	}

	source, err := loadPolicySource(path, notify.Policy{})
	if err != nil {
		t.Fatalf("加载策略失败: %v", err)
	}
	policy, err := source.Resolve(context.Background(), notify.Request{ProjectID: "1", Level: "critical", Escalated: true})
	if err != nil {
		t.Fatalf("解析升级策略失败: %v", err)
	}
	if got := policy.EscalatedRecipients[notify.ChannelEmail][0]; got != "oncall@example.com" {
		t.Fatalf("升级邮件收件人错误: %q", got)
	}
	if got := policy.Recipients[notify.ChannelEmail][0]; got != "ops@example.com" {
		t.Fatalf("普通邮件收件人不应被覆盖: %q", got)
	}
}

type quotaTestPolicySource struct{ request notify.Request }

func (s *quotaTestPolicySource) Resolve(_ context.Context, req notify.Request) (notify.Policy, error) {
	s.request = req
	return notify.Policy{Channels: []string{notify.ChannelWebhook}, Recipients: map[string][]string{notify.ChannelWebhook: {"https://notify.example/hook"}}}, nil
}

type quotaTestChannel struct{ message notify.Message }

func (c *quotaTestChannel) Name() string { return notify.ChannelWebhook }
func (c *quotaTestChannel) Send(_ context.Context, msg notify.Message, _ []string) error {
	c.message = msg
	return nil
}

func TestHandleQuotaEventUsesProjectPolicyAndIncludesThreshold(t *testing.T) {
	source := new(quotaTestPolicySource)
	channel := new(quotaTestChannel)
	dispatcher, err := notify.NewDispatcher(notify.Options{Channels: map[string]notify.Channel{notify.ChannelWebhook: channel}, Retry: []time.Duration{}})
	if err != nil {
		t.Fatal(err)
	}
	app := &notifier{policies: source, dispatcher: dispatcher, renderer: notify.NewRenderer(), metrics: new(notify.Metrics), counts: new(counters), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	event := quota.AlertEvent{ProjectID: 23, Metric: "api_calls", Level: "critical", Usage: 120, Limit: 100, WindowStart: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)}
	data, err := event.Data()
	if err != nil {
		t.Fatal(err)
	}
	if err := app.handleQuotaEvent(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if source.request.ProjectID != "23" || source.request.RuleID != "quota:api_calls" {
		t.Fatalf("策略请求租户或 rule 不符: %+v", source.request)
	}
	if !strings.Contains(channel.message.Body, "120") || !strings.Contains(channel.message.Body, "100") {
		t.Fatalf("通知正文缺少当前用量或阈值: %q", channel.message.Body)
	}
}
