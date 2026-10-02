package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Acker 是 A2 的仲裁器，实现「设备 PUBLISH → 总线持久化 → 回 PUBACK」的同步时序。
//
// 它刻意不依赖 MQTT 库：投递与等待持久化的判据（AwaitPersist）可以独立测试，
// broker 适配只负责把结果翻译成 PUBACK / 静默不确认两种动作。
type Acker struct {
	pub     Publisher
	timeout time.Duration
	metrics *Metrics
}

// NewAcker 构造仲裁器。timeout 为等待 PublishAck 的上限（§4.4 默认 5s）。
func NewAcker(pub Publisher, timeout time.Duration, metrics *Metrics) *Acker {
	if metrics == nil {
		metrics = new(Metrics)
	}
	return &Acker{pub: pub, timeout: timeout, metrics: metrics}
}

// AwaitPersist 投递消息并同步等待持久化确认。
//
//   - 返回 nil    ⇒ 消息已持久化，调用方**必须**向设备回 PUBACK；
//   - 返回非 nil  ⇒ 结果未知，调用方**禁止**回 PUBACK（设备重传兜底）。
//
// 注意这里不做重试：在「设备 → 网关」这一段，重传机制的持有者是设备
// （QoS1 + CleanSession=false 的 inflight 窗口）。网关侧重试只会放大积压，
// 且无法解决「设备已超时但网关仍在等」的语义分裂。
func (a *Acker) AwaitPersist(ctx context.Context, subject string, payload []byte) error {
	a.metrics.PublishTotal.Add(1)

	wctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	err := a.pub.Publish(wctx, subject, payload)
	switch {
	case err == nil:
		a.metrics.PersistedTotal.Add(1)
		return nil
	case errors.Is(err, ErrPubackTimeout):
		a.metrics.TimeoutTotal.Add(1)
		return fmt.Errorf("%w: 超过 %s 未收到总线确认（subject=%s）", ErrPubackTimeout, a.timeout, subject)
	default:
		a.metrics.PublishErrorTotal.Add(1)
		return fmt.Errorf("总线投递失败（subject=%s）: %w", subject, err)
	}
}

// PublishQoS0 将 QoS0 报文送入统一总线路径。MQTT QoS0 没有 PUBACK，
// 因此失败只能记录并交回 broker 的本地路径，不能伪造设备侧成功确认。
func (a *Acker) PublishQoS0(ctx context.Context, subject string, payload []byte) error {
	a.metrics.QoS0PublishTotal.Add(1)
	wctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	if err := a.pub.Publish(wctx, subject, payload); err != nil {
		a.metrics.QoS0PublishErrorTotal.Add(1)
		return fmt.Errorf("QoS0 总线投递失败（subject=%s）: %w", subject, err)
	}
	return nil
}
