// Package natsjs 收敛 NATS JetStream 的订阅约定。
//
// 存在的理由：订阅参数里有**两个不看源码就想不到、且后果严重**的默认行为，
// 而它们散布在 5 个服务里各写一遍 —— 每写一遍就多一次踩中的机会。
package natsjs

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Options 是持久消费者的订阅参数。
type Options struct {
	// Subject 是订阅的 subject（可含通配）。
	Subject string
	// Durable 是消费者名。同一 durable 的多个实例共享消费进度。
	Durable string
	// Stream 要绑定的流名。
	Stream string
	// AckWait 是总线等待 ACK 的上限。
	AckWait time.Duration
	// Inactive 是消费者的空闲回收阈值，**必须显式设置**（见 Subscribe 的说明）。
	// 通常取流的保留时长。
	Inactive time.Duration
	// MaxAckPending 限制同时在途未确认的消息数；<=0 表示交给服务端默认值。
	// 攒批类消费者（如 svc-pipeline）需要它来约束内存占用。
	MaxAckPending int
	// MaxDeliver 是单条消息最大投递次数；<=0 保持无限重投兼容。
	MaxDeliver int
}

// Subscription 是对持久消费者的**收窄视图**：只暴露消费循环真正需要的 Fetch。
//
// 为什么不用 `*nats.Subscription`：它带着 `Unsubscribe`，而对 JetStream 持久消费者
// 调用它会**删除消费者** —— 一个「看着像清理、实际是清空进度」的方法，本项目已在
// 多处踩过（§6 坑 42/47）。类型上不提供，就没有再误用的可能。
//
// 需要清理时交给 `nc.Close()`：消费者是服务端状态，本就不该由客户端删除。
type Subscription struct{ sub *nats.Subscription }

// Fetch 拉取最多 batch 条消息，参数透传 nats（如 nats.MaxWait）。
func (s *Subscription) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	return s.sub.Fetch(batch, opts...)
}

// Subscribe 创建一个**不会被重启清空**的持久消费者。
//
// ⚠️ 两个坑，后果都是「重启即重放整个保留窗口」：
//
//  1. **退出时不要对 durable 调 `sub.Unsubscribe()`**。
//     nats.go 里对 JetStream 订阅调用它会**删除消费者**；重启后新建的消费者
//     按 DeliverAll 从头投递 —— 流保留 24h，于是每次重启都会把 24 小时内的
//     事件重发一遍（对通知服务就是一场通知风暴，对计量服务就是重复计数）。
//     消费者是**服务端状态**，退出时只要关连接即可。本函数返回收窄的
//     `*Subscription`，调用方**拿不到** Unsubscribe。
//
//  2. **必须显式设置 `InactiveThreshold`**。NATS 2.10+ 给 durable 消费者加了
//     默认的空闲回收（5 分钟）：服务停机超过 5 分钟消费者就被自动删除，
//     于是「一个周末的停机」会变成一次全量重放。
//     设成与流的保留时长一致 —— 比它更久的空闲本来也没有东西可补。
func Subscribe(js nats.JetStreamContext, opts Options) (*Subscription, error) {
	if opts.Subject == "" || opts.Durable == "" || opts.Stream == "" {
		return nil, fmt.Errorf("natsjs: subject / durable / stream 都不能为空")
	}
	serverMaxDeliver := opts.MaxDeliver
	subOpts := []nats.SubOpt{
		nats.BindStream(opts.Stream),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.DeliverAll(),
	}
	// Existing durable consumers keep their server-side delivery policy. Update
	// it before binding so a rollout from unlimited retries is effective without
	// deleting the consumer and losing its delivery position.
	if opts.MaxDeliver > 0 {
		if info, err := js.ConsumerInfo(opts.Stream, opts.Durable); err == nil && info != nil && info.Config.MaxDeliver != opts.MaxDeliver {
			updated := info.Config
			updated.MaxDeliver = opts.MaxDeliver
			if _, err := js.UpdateConsumer(opts.Stream, &updated); err != nil {
				// Older servers/consumers reject changing MaxDeliver in place.
				// Keep the durable usable; the caller still enforces its own DLQ
				// threshold from delivery metadata.
				serverMaxDeliver = info.Config.MaxDeliver
			}
		}
	}
	if serverMaxDeliver > 0 {
		subOpts = append(subOpts, nats.MaxDeliver(serverMaxDeliver))
	} else {
		subOpts = append(subOpts, nats.MaxDeliver(-1))
	}
	if opts.AckWait > 0 {
		subOpts = append(subOpts, nats.AckWait(opts.AckWait))
	}
	if opts.Inactive > 0 {
		subOpts = append(subOpts, nats.InactiveThreshold(opts.Inactive))
	}
	if opts.MaxAckPending > 0 {
		subOpts = append(subOpts, nats.MaxAckPending(opts.MaxAckPending))
	}

	sub, err := js.PullSubscribe(opts.Subject, opts.Durable, subOpts...)
	if err != nil {
		return nil, fmt.Errorf("natsjs: 订阅 %s（stream=%s durable=%s）: %w",
			opts.Subject, opts.Stream, opts.Durable, err)
	}
	return &Subscription{sub: sub}, nil
}
