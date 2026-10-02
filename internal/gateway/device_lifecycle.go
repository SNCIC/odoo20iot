package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

const DeviceLifecycleSubjectPrefix = "iot.device"

type DeviceLifecyclePublisher interface {
	Publish(context.Context, string, []byte) error
}

type deviceLifecycleEvent struct {
	EventID   string    `json:"event_id"`
	Event     string    `json:"event"`
	DeviceKey string    `json:"device_key"`
	Occurred  time.Time `json:"occurred_at"`
}

type DeviceLifecycleHook struct {
	mqtt.HookBase
	ctx       context.Context
	publisher DeviceLifecyclePublisher
	metrics   *Metrics
	logger    *slog.Logger

	mu       sync.Mutex
	sessions map[string]sessionState
}

type sessionState struct {
	client  *mqtt.Client
	eventID string
}

func NewDeviceLifecycleHook(ctx context.Context, publisher DeviceLifecyclePublisher, metrics *Metrics, logger *slog.Logger) *DeviceLifecycleHook {
	if ctx == nil {
		ctx = context.Background()
	}
	if metrics == nil {
		metrics = new(Metrics)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &DeviceLifecycleHook{ctx: ctx, publisher: publisher, metrics: metrics, logger: logger, sessions: make(map[string]sessionState, 1024)}
}

func (h *DeviceLifecycleHook) ID() string { return "iot-device-lifecycle" }

func (h *DeviceLifecycleHook) Provides(b byte) bool {
	return b == mqtt.OnSessionEstablished || b == mqtt.OnDisconnect
}

func (h *DeviceLifecycleHook) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	h.mu.Lock()
	if current, ok := h.sessions[cl.ID]; ok {
		if current.client == cl {
			h.mu.Unlock()
			return
		}
		// 相同 client ID 的新连接接管在线状态；旧连接的迟到断开回调
		// 由 OnDisconnect 的 client 指针校验忽略，不误发 offline。
		eventID := fmt.Sprintf("device-session:%s:%d", cl.ID, time.Now().UTC().UnixNano())
		h.sessions[cl.ID] = sessionState{client: cl, eventID: eventID}
		h.mu.Unlock()
		h.publish(cl.ID, "online", eventID+":online")
		return
	}
	eventID := fmt.Sprintf("device-session:%s:%d", cl.ID, time.Now().UTC().UnixNano())
	h.sessions[cl.ID] = sessionState{client: cl, eventID: eventID}
	h.mu.Unlock()
	h.publish(cl.ID, "online", eventID+":online")
}

func (h *DeviceLifecycleHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	h.mu.Lock()
	session, ok := h.sessions[cl.ID]
	if ok && session.client == cl {
		delete(h.sessions, cl.ID)
	}
	h.mu.Unlock()
	if ok && session.client == cl {
		h.publish(cl.ID, "offline", session.eventID+":offline")
	}
}

func (h *DeviceLifecycleHook) publish(deviceKey, event, eventID string) {
	if h.publisher == nil {
		return
	}
	payload, err := json.Marshal(deviceLifecycleEvent{
		EventID: eventID, Event: event, DeviceKey: deviceKey, Occurred: time.Now().UTC(),
	})
	if err != nil {
		h.metrics.DeviceLifecyclePublishErrors.Add(1)
		h.logger.Error("设备生命周期事件序列化失败", "device_key", deviceKey, "event", event, "error", err)
		return
	}
	if err := h.publisher.Publish(h.ctx, DeviceLifecycleSubjectPrefix+"."+event, payload); err != nil {
		h.metrics.DeviceLifecyclePublishErrors.Add(1)
		h.logger.Error("设备生命周期事件发布失败", "device_key", deviceKey, "event", event, "error", err)
		return
	}
	h.metrics.DeviceLifecyclePublished.Add(1)
}
