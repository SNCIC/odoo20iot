package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/auth"
	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/protocol"
	"github.com/SNCIC/odoo20iot/internal/quota"
)

// ProtocolIngestHandler 把 CoAP/TCP 适配器交给它的 IngressFrame 接入既有上行管道。
// 认证、配额、统一信封和 NATS 持久化确认只实现一次，协议服务不得自行复制。
type ProtocolIngestHandler struct {
	auth          *auth.Authenticator
	publisher     Publisher
	router        ContractRouter
	quotaEnforcer *quota.Enforcer
	logger        *slog.Logger
	timeout       time.Duration
}

type ProtocolIngestOptions struct {
	Authenticator *auth.Authenticator
	Publisher     Publisher
	Router        ContractRouter
	QuotaEnforcer *quota.Enforcer
	Logger        *slog.Logger
	Timeout       time.Duration
}

func NewProtocolIngestHandler(opts ProtocolIngestOptions) (*ProtocolIngestHandler, error) {
	if opts.Authenticator == nil || opts.Publisher == nil {
		return nil, fmt.Errorf("协议接入依赖认证器和发布器")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &ProtocolIngestHandler{auth: opts.Authenticator, publisher: opts.Publisher, router: opts.Router, quotaEnforcer: opts.QuotaEnforcer, logger: opts.Logger, timeout: opts.Timeout}, nil
}

// Handle 认证并在 NATS 持久化成功后返回。secret 始终只在内存中使用。
func (h *ProtocolIngestHandler) Handle(ctx context.Context, frame protocol.IngressFrame, secret string, remoteIP net.IP) (string, error) {
	if frame.DeviceKey == "" || frame.Stream == "" || len(frame.Payload) == 0 || len(frame.Payload) > MaxHTTPPayloadBytes || !json.Valid(frame.Payload) {
		return "", fmt.Errorf("协议上报帧非法")
	}
	clientID := frame.DeviceKey
	envelopeDeviceKey := frame.DeviceKey
	if frame.SubKey != "" {
		if frame.GatewayKey == "" {
			return "", fmt.Errorf("子设备帧缺少网关身份")
		}
		clientID = frame.GatewayKey
		envelopeDeviceKey = frame.GatewayKey + "/" + frame.SubKey
	}
	result, err := h.auth.Authenticate(ctx, auth.Request{ClientID: clientID, Username: clientID, Password: secret, RemoteIP: remoteIP.String()})
	if err != nil {
		return "", fmt.Errorf("协议设备认证失败: %w", err)
	}
	var subject string
	if frame.SubKey != "" {
		subject, err = h.router.RouteHTTPSubdevice(frame.GatewayKey, frame.SubKey, frame.Stream, result.ProjectID)
	} else {
		subject, err = h.router.RouteHTTPProject(frame.DeviceKey, frame.Stream, result.ProjectID)
	}
	if err != nil {
		return "", err
	}
	if h.quotaEnforcer != nil {
		decision, reserveErr := h.quotaEnforcer.ReserveTelemetry(ctx, result.ProjectID, false)
		if reserveErr != nil || !decision.Allowed {
			return "", fmt.Errorf("协议上报超出配额: %w", reserveErr)
		}
	}
	traceID, err := envelope.NewTraceID()
	if err != nil {
		return "", err
	}
	deviceID := result.DeviceID
	if frame.SubKey != "" {
		deviceID = envelope.PlaceholderDeviceID(envelopeDeviceKey)
	}
	data, err := (envelope.Envelope{SchemaVersion: envelope.CurrentSchemaVersion, TraceID: traceID, ProjectID: result.ProjectID, DeviceKey: envelopeDeviceKey, DeviceID: deviceID, DeviceTypeID: result.DeviceTypeID, Stream: frame.Stream, ReceivedAt: time.Now().UTC(), Payload: json.RawMessage(frame.Payload)}).Encode()
	if err != nil {
		return "", err
	}
	acker := NewAcker(h.publisher, h.timeout, new(Metrics))
	if err := acker.AwaitPersist(ctx, subject, data); err != nil {
		h.logger.Error("协议上报未持久化", "protocol", frame.Protocol, "device_key", strings.TrimSpace(envelopeDeviceKey), "error", err)
		return "", err
	}
	return traceID, nil
}
