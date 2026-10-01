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
}

// Subscribe 创建一个**不会被重启清空**的持久消费者。
//
// ⚠️ 两个坑，后果都是「重启即重放整个保留窗口」：
//
//  1. **退出时不要对 durable 调 `sub.Unsubscribe()`**。
//     nats.go 里对 JetStream 订阅调用它会**删除消费者**；重启后新建的消费者
//     按 DeliverAll 从头投递 —— 流保留 24h，于是每次重启都会把 24 小时内的
//     事件重发一遍（对通知服务就是一场通知风暴，对计量服务就是重复计数）。
//     消费者是**服务端状态**，退出时只要关连接即可。故本函数返回的订阅
//     由调用方**不要** Unsubscribe，交给 `nc.Close()`。
//
//  2. **必须显式设置 `InactiveThreshold`**。NATS 2.10+ 给 durable 消费者加了
//     默认的空闲回收（5 分钟）：服务停机超过 5 分钟消费者就被自动删除，
//     于是「一个周末的停机」会变成一次全量重放。
//     设成与流的保留时长一致 —— 比它更久的空闲本来也没有东西可补。
func Subscribe(js nats.JetStreamContext, opts Options) (*nats.Subscription, error) {
	if opts.Subject == "" || opts.Durable == "" || opts.Stream == "" {
		return nil, fmt.Errorf("natsjs: subject / durable / stream 都不能为空")
	}
	subOpts := []nats.SubOpt{
		nats.BindStream(opts.Stream),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.DeliverAll(),
		nats.MaxDeliver(-1),
	}
	if opts.AckWait > 0 {
		subOpts = append(subOpts, nats.AckWait(opts.AckWait))
	}
	if opts.Inactive > 0 {
		subOpts = append(subOpts, nats.InactiveThreshold(opts.Inactive))
	}

	sub, err := js.PullSubscribe(opts.Subject, opts.Durable, subOpts...)
	if err != nil {
		return nil, fmt.Errorf("natsjs: 订阅 %s（stream=%s durable=%s）: %w",
			opts.Subject, opts.Stream, opts.Durable, err)
	}
	return sub, nil
}
