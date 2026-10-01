package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/SNCIC/odoo20iot/internal/notify"
)

// filePolicySource 从配置文件读通知策略。
//
// ⚠️ **这是过渡实现**：正式实现要读 `t_alarm_rule.notify`（04 §2.4），
// 并由 `t_user` / `t_role` 把通知组展开成收件人 —— 这几张表都还没建
// （见 09 的遗留项）。之所以先做成配置文件：**「策略从哪来」与
// 「怎么把通知可靠地送出去」是两件独立的事**，前者换了，分发逻辑一行都不用动；
// 反之如果把表结构写进分发路径，等表建好时会连带改动重试与降级。
type filePolicySource struct {
	mu  sync.RWMutex
	cfg policyFile
	// fromFile 为 false 表示用的是命令行给的默认策略（开发/联调）。
	fromFile bool
}

// policyFile 是策略文件的形状。
type policyFile struct {
	// Default 是所有未命中规则的兜底策略。
	Default *policySpec `json:"default"`
	// ByRule 按规则 ID 覆盖（优先级最高）。
	ByRule map[string]policySpec `json:"by_rule"`
	// ByLevel 按级别覆盖（次优先）。
	ByLevel map[string]policySpec `json:"by_level"`
}

type policySpec struct {
	Groups     []string            `json:"groups"`
	Channels   []string            `json:"channels"`
	Template   string              `json:"template"`
	Recipients map[string][]string `json:"recipients"`
}

func (s policySpec) toPolicy() notify.Policy {
	return notify.Policy{
		Groups:     s.Groups,
		Channels:   s.Channels,
		Template:   s.Template,
		Recipients: s.Recipients,
	}
}

// loadPolicySource 读策略文件；path 为空或文件不存在时退回 flags 给的默认策略。
//
// 退回而不是报错：本地联调时不该被迫先写一个策略文件。
// 但退回是**显式的**（fromFile=false），启动日志里会明说，
// 免得有人以为线上在跑配置文件。
func loadPolicySource(path string, fallback notify.Policy) (*filePolicySource, error) {
	s := &filePolicySource{}
	if path == "" {
		s.cfg = policyFile{Default: &policySpec{
			Channels:   fallback.Channels,
			Template:   fallback.Template,
			Recipients: fallback.Recipients,
		}}
		return s, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("svc-notify: 策略文件 %s 不存在", path)
		}
		return nil, fmt.Errorf("svc-notify: 读策略文件: %w", err)
	}
	var cfg policyFile
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("svc-notify: 解析策略文件 %s: %w", path, err)
	}
	if cfg.Default == nil && len(cfg.ByRule) == 0 && len(cfg.ByLevel) == 0 {
		return nil, fmt.Errorf("svc-notify: 策略文件 %s 里没有任何可用策略", path)
	}
	s.cfg = cfg
	s.fromFile = true
	return s, nil
}

// Resolve 实现 notify.PolicySource。
//
// 优先级：by_rule → by_level → default。规则粒度比级别粒度更具体，
// 所以先看规则：一条「数据库连接数」的 critical 告警该发给 DBA，
// 而不是发给所有 critical 的接收人。
func (s *filePolicySource) Resolve(_ context.Context, req notify.Request) (notify.Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if p, ok := s.cfg.ByRule[req.RuleID]; ok {
		return p.toPolicy(), nil
	}
	if p, ok := s.cfg.ByLevel[req.Level]; ok {
		return p.toPolicy(), nil
	}
	if s.cfg.Default != nil {
		return s.cfg.Default.toPolicy(), nil
	}
	// 没有策略是**配置缺口**，不是投递失败。标成永久失败让调用方
	// 跳过（而不是重试），也**不要**写进 DLQ —— 一个没配策略的规则
	// 会持续产生告警，把它灌进死信表会把真正的投递故障埋掉。
	return notify.Policy{}, notify.Permanent(
		"没有匹配的通知策略（rule=%s level=%s）", req.RuleID, req.Level)
}

// FromFile 报告策略是否来自配置文件（启动日志用）。
func (s *filePolicySource) FromFile() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fromFile
}
