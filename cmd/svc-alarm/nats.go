package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// natsPublisher 把告警事件写进 JetStream。
//
// 为什么必须用 JetStream 而不是 core NATS：下游 odoo-connector 需要**持久订阅**。
// core NATS 是即发即忘 —— 订阅者当时不在线，那张工单就永远没人建，
// 而且发布端会收到「发送成功」。这正是「告警发出去了但没人收到」的典型形态。
type natsPublisher struct {
	js nats.JetStreamContext
}

func (p *natsPublisher) Publish(_ context.Context, subject string, data []byte) error {
	if _, err := p.js.Publish(subject, data); err != nil {
		return fmt.Errorf("发布 %s: %w", subject, err)
	}
	return nil
}

// ensureStream 幂等地确保流存在且覆盖给定的 subject 前缀。
//
// 已存在但**不覆盖**时直接报错，而不是自动改配置：改流的 subjects 会影响
// 其他消费者，属部署动作。这里的失败信息要能一眼看出缺了哪个前缀，
// 否则表现是「发布成功但没人收到」——那种故障查起来最花时间。
func ensureStream(js nats.JetStreamContext, name string, subjects []string, maxAge time.Duration) error {
	info, err := js.StreamInfo(name)
	if err == nil {
		for _, want := range subjects {
			covered := false
			for _, have := range info.Config.Subjects {
				if have == want || strings.HasPrefix(want, strings.TrimSuffix(have, ">")) {
					covered = true
					break
				}
			}
			if !covered {
				return fmt.Errorf("流 %s 未覆盖 subject %s（现有 %v）："+
					"需在部署侧调整，本服务不自动改流的订阅范围", name, want, info.Config.Subjects)
			}
		}
		return nil
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Retention: nats.LimitsPolicy,
		Storage:   nats.FileStorage,
		MaxAge:    maxAge,
	}); err != nil {
		return fmt.Errorf("建流 %s: %w", name, err)
	}
	return nil
}
