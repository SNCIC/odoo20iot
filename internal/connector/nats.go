package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// OdooStream 是承载 `iot.odoo.>` 事件的 JetStream Stream。
const OdooStream = "IOT_ODOO"

// DefaultOdooStreamMaxAge 是事件在 Stream 里的保留时长。
//
// 取 7 天：07 §4.4 要求 Outbox 事件「可重放、可对账」，
// 而下游消费者宕机超过一周属于需要人工介入的故障，不该靠 Stream 兜底。
const DefaultOdooStreamMaxAge = 7 * 24 * time.Hour

// NATSPublisher 把 Odoo 事件发布到 NATS JetStream。
type NATSPublisher struct {
	js     nats.JetStreamContext
	stream string
}

var _ Publisher = (*NATSPublisher)(nil)

// NewNATSPublisher 构造发布器并确保目标 Stream 存在。
func NewNATSPublisher(nc *nats.Conn, stream string) (*NATSPublisher, error) {
	if nc == nil {
		return nil, fmt.Errorf("connector: 需要 NATS 连接")
	}
	if stream == "" {
		stream = OdooStream
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("获取 JetStream 上下文: %w", err)
	}
	p := &NATSPublisher{js: js, stream: stream}
	if err := p.EnsureStream(); err != nil {
		return nil, err
	}
	return p, nil
}

// Stream 返回 Stream 名（观测用）。
func (p *NATSPublisher) Stream() string { return p.stream }

// EnsureStream 幂等地保证 `iot.odoo.>` 的 Stream 存在。
func (p *NATSPublisher) EnsureStream() error {
	_, err := p.js.StreamInfo(p.stream)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrStreamNotFound):
	default:
		return fmt.Errorf("查询 Stream %s: %w", p.stream, err)
	}

	if _, err := p.js.AddStream(&nats.StreamConfig{
		Name:      p.stream,
		Subjects:  []string{OdooSubjectPrefix + ".>"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    DefaultOdooStreamMaxAge,
	}); err != nil {
		return fmt.Errorf("创建 Stream %s: %w", p.stream, err)
	}
	return nil
}

// Publish 同步等待 `PublishAck`。
//
// 只有拿到 ack 才算发布成功 —— 调用方据此决定是否 XACK。
// 「发布超时」与「发布失败」在此**不作区分**：两者都意味着持久化结果未知，
// 而此时唯一安全的动作都是「不确认、等重投」（与 A2 的 PUBACK 时序同一道理）。
func (p *NATSPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	if _, err := p.js.Publish(subject, data, nats.Context(ctx)); err != nil {
		return fmt.Errorf("发布 %s: %w", subject, err)
	}
	return nil
}
