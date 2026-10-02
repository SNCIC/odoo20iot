package gateway

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

type lifecycleCall struct {
	subject string
	payload []byte
}

type lifecyclePublisher struct {
	mu    sync.Mutex
	calls []lifecycleCall
}

func (p *lifecyclePublisher) Publish(_ context.Context, subject string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, lifecycleCall{subject: subject, payload: append([]byte(nil), payload...)})
	return nil
}

func (p *lifecyclePublisher) snapshot() []lifecycleCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]lifecycleCall(nil), p.calls...)
}

func TestDeviceLifecycleHookPublishesOnlineAndOfflineOnce(t *testing.T) {
	pub := new(lifecyclePublisher)
	hook := NewDeviceLifecycleHook(context.Background(), pub, new(Metrics), testLogger())
	client := &mqtt.Client{ID: "dev-1"}

	hook.OnSessionEstablished(client, packets.Packet{})
	hook.OnDisconnect(client, nil, false)
	calls := pub.snapshot()
	if len(calls) != 2 {
		t.Fatalf("期望上线/下线各 1 条，得到 %d", len(calls))
	}
	if calls[0].subject != "iot.device.online" || calls[1].subject != "iot.device.offline" {
		t.Fatalf("生命周期 subject 不符: %+v", calls)
	}
	var online, offline deviceLifecycleEvent
	if err := json.Unmarshal(calls[0].payload, &online); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(calls[1].payload, &offline); err != nil {
		t.Fatal(err)
	}
	if online.DeviceKey != "dev-1" || offline.DeviceKey != "dev-1" || online.EventID == offline.EventID {
		t.Fatalf("生命周期事件字段不符: online=%+v offline=%+v", online, offline)
	}
	if hook.metrics.DeviceLifecyclePublished.Load() != 2 {
		t.Fatalf("发布计数不符: %d", hook.metrics.DeviceLifecyclePublished.Load())
	}
}

func TestDeviceLifecycleHookIgnoresStaleDisconnect(t *testing.T) {
	pub := new(lifecyclePublisher)
	hook := NewDeviceLifecycleHook(context.Background(), pub, new(Metrics), testLogger())
	oldClient := &mqtt.Client{ID: "dev-1"}
	newClient := &mqtt.Client{ID: "dev-1"}

	hook.OnSessionEstablished(oldClient, packets.Packet{})
	hook.OnSessionEstablished(newClient, packets.Packet{})
	hook.OnDisconnect(oldClient, nil, false)
	hook.OnDisconnect(newClient, nil, false)
	calls := pub.snapshot()
	if len(calls) != 3 {
		t.Fatalf("旧连接断开不应发布 offline，期望 3 条，得到 %d", len(calls))
	}
	if calls[2].subject != "iot.device.offline" {
		t.Fatalf("最后一条应为 offline，得到 %s", calls[2].subject)
	}
}
