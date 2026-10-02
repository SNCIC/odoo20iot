package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/SNCIC/odoo20iot/internal/envelope"
)

func TestBuildEnvelopeAddsTraceAndSchema(t *testing.T) {
	hook := NewHook(context.Background(), nil, fixedRouter{}, new(Metrics), slog.Default(), HookConfig{ProjectID: 1, DeviceTypeID: 55})
	data, err := hook.buildEnvelope(packets.Packet{TopicName: "v1/devices/dev-1/telemetry", Payload: []byte(`{"ts":"2026-10-02T00:00:00Z"}`)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := envelope.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != envelope.CurrentSchemaVersion || len(got.TraceID) != 32 {
		t.Fatalf("metadata = %+v", got)
	}
}

func TestBuildEnvelopeRejectsOversizedPayload(t *testing.T) {
	hook := NewHook(context.Background(), nil, fixedRouter{}, new(Metrics), slog.Default(), HookConfig{ProjectID: 1, DeviceTypeID: 55})
	payload, _ := json.Marshal(map[string]string{"data": string(make([]byte, MaxPayloadBytes))})
	if _, err := hook.buildEnvelope(packets.Packet{TopicName: "v1/devices/dev-1/telemetry", Payload: payload}); err == nil {
		t.Fatal("oversized payload unexpectedly accepted")
	}
}

func TestBuildEnvelopeUsesAuthenticatedIdentity(t *testing.T) {
	hook := NewHook(context.Background(), nil, fixedRouter{}, new(Metrics), slog.Default(), HookConfig{
		ProjectID: 1, DeviceTypeID: 55, RequireIdentity: true,
		IdentityForClient: func(clientID string) (int64, int64, int64, bool) {
			if clientID != "client-1" {
				t.Fatalf("clientID=%q", clientID)
			}
			return 9, 901, 77, true
		},
	})
	data, err := hook.buildEnvelopeForClient("client-1", packets.Packet{TopicName: "v1/devices/dev-1/telemetry", Payload: []byte(`{"ts":"2026-10-02T00:00:00Z"}`)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := envelope.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProjectID != 9 || got.DeviceID != 901 || got.DeviceTypeID != 77 {
		t.Fatalf("identity mapping = %+v", got)
	}
}
