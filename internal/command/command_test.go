package command

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SNCIC/odoo20iot/internal/cluster"
)

type fakeRouter struct{ env cluster.Envelope }

func (f *fakeRouter) RouteExternal(_ context.Context, env cluster.Envelope) error {
	f.env = env
	return nil
}

func TestSenderBuildsDeviceCommand(t *testing.T) {
	router := new(fakeRouter)
	if err := (Sender{Router: router}).Send(context.Background(), Request{DeviceKey: "dev-1", CommandKey: "reset", Payload: map[string]any{"force": true}, CorrelationID: "corr-1"}); err != nil {
		t.Fatal(err)
	}
	if router.env.Topic != "v1/devices/dev-1/cmd/reset" || router.env.Qos != 1 {
		t.Fatalf("unexpected envelope: %+v", router.env)
	}
	var body map[string]any
	if err := json.Unmarshal(router.env.Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body["correlation_id"] != "corr-1" || body["command"] != "reset" {
		t.Fatalf("unexpected command body: %+v", body)
	}
}

func TestActionRequiresDeviceAndCommand(t *testing.T) {
	if err := (Action{Sender: Sender{Router: new(fakeRouter)}}).Do(context.Background(), map[string]any{}); err == nil {
		t.Fatal("missing device and command should fail")
	}
}

func TestDecodeReply(t *testing.T) {
	reply, err := DecodeReply([]byte(`{"correlation_id":"corr-1","status":"ok"}`))
	if err != nil || reply.CorrelationID != "corr-1" || reply.Status != "ok" {
		t.Fatalf("unexpected reply: %+v, err=%v", reply, err)
	}
}
