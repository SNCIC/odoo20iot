package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/SNCIC/odoo20iot/internal/cluster"
	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/metering"
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
// Meter 是网关热路径上的计量累加器（04 §6）。
//
// 实现必须低开销：每条**成功持久化**的消息调用一次，且**不得做 IO** ——
// 累加与上报分离（上报由 internal/metering 的 Reporter 按窗口批量发出）。
type Meter interface {
	Add(projectID int64, metric string, delta int64)
}

// HookConfig 是 A2 hook 的非依赖配置。
type HookConfig struct {
	// ProjectID / DeviceTypeID 是 Phase 0 的归属占位值（见 internal/envelope）。
	ProjectID    int64
	DeviceTypeID int64
	// Meter 为 nil 时不做计量（不影响 A2 时序）。
	Meter                   Meter
	ReplyPublisher          Publisher
	ShadowReportedPublisher Publisher
	ReplyTimeout            time.Duration
	IdentityForClient       func(string) (projectID, deviceID, deviceTypeID int64, ok bool)
	RequireIdentity         bool
}

type Hook struct {
	mqtt.HookBase

	baseCtx     context.Context
	acker       *Acker
	replyAcker  *Acker
	shadowAcker *Acker
	router      SubjectRouter
	metrics     *Metrics
	logger      *slog.Logger

	// 归属占位值（Phase 0）。真实 tenant / 设备主键投影依赖 A1 注册表，
	// 见 internal/envelope 包注释。
	projectID    int64
	deviceTypeID int64

	meter             Meter
	identityForClient func(string) (projectID, deviceID, deviceTypeID int64, ok bool)
	requireIdentity   bool
}

const MaxPayloadBytes = 32 << 10

var _ mqtt.Hook = (*Hook)(nil)

// NewHook 构造 A2 hook。baseCtx 决定「优雅关闭时在途等待是否立即放弃」。
func NewHook(baseCtx context.Context, acker *Acker, router SubjectRouter, metrics *Metrics, log *slog.Logger, cfg HookConfig) *Hook {
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if metrics == nil {
		metrics = new(Metrics)
	}
	if log == nil {
		log = slog.Default()
	}
	hook := &Hook{
		baseCtx:           baseCtx,
		acker:             acker,
		router:            router,
		metrics:           metrics,
		logger:            log,
		projectID:         cfg.ProjectID,
		deviceTypeID:      cfg.DeviceTypeID,
		meter:             cfg.Meter,
		identityForClient: cfg.IdentityForClient,
		requireIdentity:   cfg.RequireIdentity,
	}
	if cfg.ReplyPublisher != nil {
		replyTimeout := cfg.ReplyTimeout
		if replyTimeout <= 0 {
			replyTimeout = DefaultPubackTimeout
		}
		hook.replyAcker = NewAcker(cfg.ReplyPublisher, replyTimeout, metrics)
	}
	if cfg.ShadowReportedPublisher != nil {
		shadowTimeout := cfg.ReplyTimeout
		if shadowTimeout <= 0 {
			shadowTimeout = DefaultPubackTimeout
		}
		hook.shadowAcker = NewAcker(cfg.ShadowReportedPublisher, shadowTimeout, metrics)
	}
	return hook
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
	// 内部注入的报文（跨节点投递、未来的服务端下发）直接放行。
	//
	// 这是 A3 倒逼出来的必要分支：跨节点投递要复用 broker 的订阅匹配，
	// 只能走 inline client 注入，而那条路径同样会经过本 hook。
	// 若不区分，注入的 QoS1 报文会被当成设备上报 —— 既会被路由回总线，
	// 又会被拒绝（ErrRejectPacket），跨节点投递直接失效。
	if cl.Net.Inline {
		return pk, nil
	}

	switch pk.FixedHeader.Qos {
	case 0:
		// QoS0 没有 PUBACK，但仍必须经过统一信封和 NATS，不能静默绕过数据总线。
		// 投递失败时保留 broker 本地路径；同时记录错误，因为设备不会重传。
		// 路由和封装在公共路径完成，避免 QoS0 与 QoS1 的归属口径分叉。
	case 2:
		// 端侧契约（§5）只使用 QoS0/QoS1。QoS2 一旦出现即为契约违规，
		// 且本 hook 未覆盖其 PUBREC/PUBREL 流程 —— 明确拒绝而不是静默放行，
		// 后者会让消息绕过「先持久化再确认」的语义（静默降级是更坏的失败）。
		h.metrics.UnsupportedQosTotal.Add(1)
		h.logger.Error("拒绝 QoS2 上报：超出端侧契约，A2 时序未覆盖",
			"client", cl.ID, "topic", pk.TopicName, "packet_id", pk.PacketID)
		return pk, packets.ErrRejectPacket
	}

	if cluster.IsCommandReplyTopic(pk.TopicName) {
		return h.onCommandReply(cl, pk)
	}
	if cluster.IsShadowReportedTopic(pk.TopicName) {
		return h.onShadowReported(cl, pk)
	}

	subject, err := h.router.Route(cl, pk)
	if err != nil {
		// 契约外的 topic 属于**不可重试**的客户端错误：不回 PUBACK 只会让设备
		// 无意义地重传同一份垃圾，因此同样拒绝该包并计入指标。
		h.metrics.UnroutableTotal.Add(1)
		h.logger.Warn("拒绝无法路由的上报", "client", cl.ID, "topic", pk.TopicName, "error", err)
		return pk, packets.ErrRejectPacket
	}

	// 包装统一信封：把归属元数据与原始报文一起发到总线（03 §2.4）。
	// 不这样做，`device_key` 会随 subject 一并丢失，下游消费者无从落库。
	data, err := h.buildEnvelopeForClient(cl.ID, pk)
	if err != nil {
		// 报文不是合法 JSON（或 topic 解析不出归属）属**不可重试**的客户端错误：
		// 与「无法路由」同类，不回 PUBACK 只会让设备无意义地重传同一份垃圾。
		h.metrics.InvalidPayloadTotal.Add(1)
		h.logger.Warn("拒绝无法封装的上报", "client", cl.ID, "topic", pk.TopicName, "error", err)
		return pk, packets.ErrRejectPacket
	}

	if pk.FixedHeader.Qos == 0 {
		if err := h.acker.PublishQoS0(h.baseCtx, subject, data); err != nil {
			h.logger.Error("QoS0 已进入统一路径但总线投递失败：设备不会重传",
				"client", cl.ID, "topic", pk.TopicName, "subject", subject, "error", err)
		} else if h.meter != nil {
			h.meter.Add(h.projectID, metering.MetricMsgCount, 1)
		}
		return pk, nil
	}

	if err := h.acker.AwaitPersist(h.baseCtx, subject, data); err != nil {
		h.logger.Error("未回 PUBACK：总线未确认持久化，等待设备重传",
			"client", cl.ID, "topic", pk.TopicName, "subject", subject,
			"packet_id", pk.PacketID, "dup", pk.FixedHeader.Dup, "error", err)
		return pk, packets.ErrRejectPacket
	}

	// 计量：只有**已持久化**的消息才计入（04 §6「每条消息可归属到 project_id」）。
	// 热路径只做一次加锁自增，无 IO。
	if h.meter != nil && !pk.FixedHeader.Dup {
		h.meter.Add(h.projectID, metering.MetricMsgCount, 1)
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

func (h *Hook) onShadowReported(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if h.shadowAcker == nil {
		return pk, packets.ErrRejectPacket
	}
	if !json.Valid(pk.Payload) || len(pk.Payload) > MaxPayloadBytes {
		h.metrics.InvalidPayloadTotal.Add(1)
		if pk.FixedHeader.Qos > 0 {
			_ = writePuback(cl, pk)
		}
		return pk, packets.ErrRejectPacket
	}
	deviceKey := cluster.DeviceKeyFromTopic(pk.TopicName)
	projectID := h.projectID
	if h.identityForClient != nil {
		resolvedProject, _, _, ok := h.identityForClient(cl.ID)
		if ok {
			projectID = resolvedProject
		} else if h.requireIdentity {
			return pk, packets.ErrRejectPacket
		}
	}
	subject, err := cluster.ShadowReportedSubject(projectID, deviceKey)
	if err != nil {
		return pk, packets.ErrRejectPacket
	}
	if pk.FixedHeader.Qos == 0 {
		if err := h.shadowAcker.PublishQoS0(h.baseCtx, subject, pk.Payload); err != nil {
			h.logger.Error("影子 reported QoS0 发布失败", "subject", subject, "error", err)
		}
		return pk, nil
	}
	if err := h.shadowAcker.AwaitPersist(h.baseCtx, subject, pk.Payload); err != nil {
		h.logger.Error("影子 reported 未持久化，等待设备重传", "subject", subject, "error", err)
		return pk, packets.ErrRejectPacket
	}
	if err := writePuback(cl, pk); err != nil {
		h.metrics.PubackWriteErrorTotal.Add(1)
	}
	return pk, packets.ErrRejectPacket
}

func (h *Hook) onCommandReply(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if h.replyAcker == nil {
		h.logger.Error("命令回执通道未配置", "topic", pk.TopicName)
		return pk, packets.ErrRejectPacket
	}
	if !json.Valid(pk.Payload) || len(pk.Payload) > MaxPayloadBytes {
		h.metrics.InvalidPayloadTotal.Add(1)
		h.logger.Warn("拒绝非法命令回执", "client", cl.ID, "topic", pk.TopicName)
		if pk.FixedHeader.Qos > 0 {
			if err := writePuback(cl, pk); err != nil {
				h.metrics.PubackWriteErrorTotal.Add(1)
			}
			return pk, packets.ErrRejectPacket
		}
		return pk, packets.ErrRejectPacket
	}
	deviceKey := cluster.DeviceKeyFromTopic(pk.TopicName)
	projectID := h.projectID
	if h.identityForClient != nil {
		resolvedProject, _, _, ok := h.identityForClient(cl.ID)
		if ok {
			projectID = resolvedProject
		} else if h.requireIdentity {
			return pk, packets.ErrRejectPacket
		}
	}
	subject, err := cluster.CommandReplySubject(projectID, deviceKey)
	if err != nil {
		return pk, packets.ErrRejectPacket
	}
	if pk.FixedHeader.Qos == 0 {
		if err := h.replyAcker.PublishQoS0(h.baseCtx, subject, pk.Payload); err != nil {
			h.logger.Error("命令回执 QoS0 发布失败", "subject", subject, "error", err)
		}
		return pk, nil
	}
	if err := h.replyAcker.AwaitPersist(h.baseCtx, subject, pk.Payload); err != nil {
		h.logger.Error("命令回执未持久化，等待设备重传", "subject", subject, "error", err)
		return pk, packets.ErrRejectPacket
	}
	if err := writePuback(cl, pk); err != nil {
		h.metrics.PubackWriteErrorTotal.Add(1)
	}
	return pk, packets.ErrRejectPacket
}

// buildEnvelope 把一次设备上报包装成总线信封并编码（03 §2.4）。
//
// device_key / stream 从 topic 解析；project_id / device_id / device_type_id
// 为 Phase 0 占位值（真实映射依赖 A1 注册表，见 internal/envelope 包注释）。
func (h *Hook) buildEnvelope(pk packets.Packet) ([]byte, error) {
	return h.buildEnvelopeForClient("", pk)
}

func (h *Hook) buildEnvelopeForClient(clientID string, pk packets.Packet) ([]byte, error) {
	deviceKey, rest := cluster.ParseDeviceTopic(pk.TopicName)
	if deviceKey == "" {
		return nil, fmt.Errorf("topic %q 解析不出 device_key", pk.TopicName)
	}
	stream, _, _ := strings.Cut(rest, "/")
	if stream == "" {
		return nil, fmt.Errorf("topic %q 解析不出 stream", pk.TopicName)
	}
	// 03 §2.4：进入管道的报文必须是合法 JSON（拒绝尾随逗号、单引号等）。
	// 等到 Marshal 才失败的话，错误信息离开现场更远、更难定位。
	if !json.Valid(pk.Payload) {
		return nil, fmt.Errorf("payload 不是合法 JSON（%d 字节）", len(pk.Payload))
	}
	if len(pk.Payload) > MaxPayloadBytes {
		return nil, fmt.Errorf("payload 超过 %d 字节上限", MaxPayloadBytes)
	}
	traceID, err := envelope.NewTraceID()
	if err != nil {
		return nil, err
	}

	projectID, deviceID, deviceTypeID := h.projectID, envelope.PlaceholderDeviceID(deviceKey), h.deviceTypeID
	if h.identityForClient != nil {
		if resolvedProject, resolvedDevice, resolvedType, ok := h.identityForClient(clientID); ok {
			projectID, deviceTypeID = resolvedProject, resolvedType
			if resolvedDevice > 0 {
				deviceID = resolvedDevice
			}
		} else if h.requireIdentity {
			return nil, fmt.Errorf("设备 %q 未找到已认证的真实身份", deviceKey)
		}
	}

	return envelope.Envelope{
		SchemaVersion: envelope.CurrentSchemaVersion,
		TraceID:       traceID,
		ProjectID:     projectID,
		DeviceKey:     deviceKey,
		DeviceID:      deviceID,
		DeviceTypeID:  deviceTypeID,
		Stream:        stream,
		ReceivedAt:    time.Now().UTC(),
		Payload:       json.RawMessage(pk.Payload),
	}.Encode()
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
