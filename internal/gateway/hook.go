package gateway

import (
	"context"
	"log/slog"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Hook 把 A2 时序接入 mochi-mqtt 的 PUBLISH 处理路径。
//
// # 机制（已核对 mochi-mqtt v2.7.9 源码 server.go:processPublish）
//
// 当 OnPublish 返回 packets.ErrRejectPacket 时，broker 立即 `return nil` ——
// **既不发送内置 PUBACK，也不投递给本地订阅者**。
// 这正是 A2 需要的挂载点：确认时机被完全交给业务代码。
//
// 因此本 hook 的两种返回都用 ErrRejectPacket，语义由「是否已显式回 PUBACK」区分：
//   - 总线已确认 → 显式写 PUBACK，然后返回 ErrRejectPacket 阻止 broker 重复确认；
//   - 总线未确认 → 直接返回 ErrRejectPacket，设备收不到 PUBACK，按 QoS1 重传兜底。
//
// 代价：当前实现会**同步阻塞该客户端的收包协程**（最长 PubackTimeout）。
// 这是「一设备一确认」语义的直接后果（§4.4 明确禁止合并等待）；对 KeepAlive ≥ 60s
// 的设备无协议影响，但 PINGREQ 的响应也会被推迟，需在 A4（5 万连接 24h）中实测。
type Hook struct {
	mqtt.HookBase

	baseCtx context.Context
	acker   *Acker
	router  SubjectRouter
	metrics *Metrics
	logger  *slog.Logger
}

var _ mqtt.Hook = (*Hook)(nil)

// NewHook 构造 A2 hook。baseCtx 决定「优雅关闭时在途等待是否立即放弃」。
func NewHook(baseCtx context.Context, acker *Acker, router SubjectRouter, metrics *Metrics, log *slog.Logger) *Hook {
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if metrics == nil {
		metrics = new(Metrics)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Hook{
		baseCtx: baseCtx,
		acker:   acker,
		router:  router,
		metrics: metrics,
		logger:  log,
	}
}

// ID 实现 mqtt.Hook。
func (h *Hook) ID() string { return "iot-sync-ack" }

// Provides 只接管收包路径：认证（A1）与 ACL 属于后续验证项，此处刻意不声明，
// 以免给出「已有接入安全能力」的错觉。
func (h *Hook) Provides(b byte) bool {
	return b == mqtt.OnPublish
}

// OnPublish 是 A2 的核心实现，详见类型注释。
func (h *Hook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	switch pk.FixedHeader.Qos {
	case 0:
		// QoS0 无确认语义，与 A2 无关：交回 broker 原生路径，不阻塞。
		return pk, nil
	case 2:
		// 端侧契约（§5）只使用 QoS0/QoS1。QoS2 一旦出现即为契约违规，
		// 且本 hook 未覆盖其 PUBREC/PUBREL 流程 —— 明确拒绝而不是静默放行，
		// 后者会让消息绕过「先持久化再确认」的语义（静默降级是更坏的失败）。
		h.metrics.UnsupportedQosTotal.Add(1)
		h.logger.Error("拒绝 QoS2 上报：超出端侧契约，A2 时序未覆盖",
			"client", cl.ID, "topic", pk.TopicName, "packet_id", pk.PacketID)
		return pk, packets.ErrRejectPacket
	}

	subject, err := h.router.Route(cl, pk)
	if err != nil {
		// 契约外的 topic 属于**不可重试**的客户端错误：不回 PUBACK 只会让设备
		// 无意义地重传同一份垃圾，因此同样拒绝该包并计入指标。
		h.metrics.UnroutableTotal.Add(1)
		h.logger.Warn("拒绝无法路由的上报", "client", cl.ID, "topic", pk.TopicName, "error", err)
		return pk, packets.ErrRejectPacket
	}

	if err := h.acker.AwaitPersist(h.baseCtx, subject, pk.Payload); err != nil {
		h.logger.Error("未回 PUBACK：总线未确认持久化，等待设备重传",
			"client", cl.ID, "topic", pk.TopicName, "subject", subject,
			"packet_id", pk.PacketID, "dup", pk.FixedHeader.Dup, "error", err)
		return pk, packets.ErrRejectPacket
	}

	if err := writePuback(cl, pk); err != nil {
		// 已持久化但确认未送达：设备会重传，下游按 msg_id 幂等去重。
		// 宁可重复也不丢 —— 与 ADR-004「遥测最终一致」一致。
		h.metrics.PubackWriteErrorTotal.Add(1)
		h.logger.Error("总线已确认但回 PUBACK 失败：设备将重传",
			"client", cl.ID, "topic", pk.TopicName, "packet_id", pk.PacketID, "error", err)
	}

	return pk, packets.ErrRejectPacket
}

// writePuback 显式向设备回 PUBACK。
//
// 字段构造刻意与 mochi-mqtt 内部 server.buildAck 保持一致，避免 MQTT 5 的
// Properties 继承语义与原生路径分叉；协议版本由 cl.WritePacket 按连接覆盖。
func writePuback(cl *mqtt.Client, pk packets.Packet) error {
	ack := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Puback,
			Qos:  0,
		},
		PacketID:   pk.PacketID,
		ReasonCode: packets.QosCodes[pk.FixedHeader.Qos].Code,
		Properties: pk.Properties,
		Created:    time.Now().Unix(),
	}
	if cl.Properties.ProtocolVersion < 5 {
		ack.Properties = packets.Properties{}
	}
	return cl.WritePacket(ack)
}
