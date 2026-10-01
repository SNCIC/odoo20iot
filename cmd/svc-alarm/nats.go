package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/SNCIC/odoo20iot/internal/natsjs"
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

// ensureStream 幂等地确保流存在，并校正**保留口径**（走共享实现：建或校 + WARN）。
//
// subjects 用**严格**策略：已存在但**不覆盖**时直接报错，而不是自动改配置 ——
// 改流的 subjects 会影响其他消费者，属部署动作。这里的失败信息要能一眼看出
// 缺了哪个前缀，否则表现是「发布成功但没人收到」——那种故障查起来最花时间。
//
// 保留口径则相反，按既定策略**自动校正并告警**：先于口径代码建出来的流会停在
// `MaxAge=0`（无限保留），服务不自愈的话它会一直涨下去。
func ensureStream(js nats.JetStreamContext, name string, subjects []string, maxAge time.Duration) error {
	return natsjs.EnsureStream(js, natsjs.StreamSpec{
		Name:           name,
		Subjects:       subjects,
		Replicas:       1,
		MaxAge:         maxAge,
		StrictSubjects: true,
	})
}
