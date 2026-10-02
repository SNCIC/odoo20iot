package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/SNCIC/odoo20iot/internal/notify"
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
