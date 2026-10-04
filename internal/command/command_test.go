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

func TestDecodeLegacyDeviceReply(t *testing.T) {
	reply, err := DecodeReply([]byte(`{"id":"corr-2","code":0,"msg":"ok","data":{"accepted":true}}`))
	if err != nil {
		t.Fatal(err)
	}
	if reply.CorrelationID != "corr-2" || reply.Status != "acked" || reply.Error != "ok" {
		t.Fatalf("unexpected legacy reply: %+v", reply)
	}
	if string(reply.Payload) != `{"accepted":true}` {
		t.Fatalf("unexpected legacy payload: %s", reply.Payload)
	}
}

func TestDecodeLegacyFailedDeviceReply(t *testing.T) {
	reply, err := DecodeReply([]byte(`{"id":"corr-3","code":17,"msg":"busy"}`))
	if err != nil {
		t.Fatal(err)
	}
	if reply.CorrelationID != "corr-3" || reply.Status != "failed" || reply.Error != "busy" {
		t.Fatalf("unexpected failed reply: %+v", reply)
	}
}

func TestProjectFromReplySubject(t *testing.T) {
	projectID, deviceKey, err := projectFromReplySubject("iot.cmd.reply.42.dev-1")
	if err != nil || projectID != 42 || deviceKey != "dev-1" {
		t.Fatalf("unexpected subject parse: project=%d device=%q err=%v", projectID, deviceKey, err)
	}
	if _, _, err := projectFromReplySubject("iot.cmd.reply.invalid.dev-1"); err == nil {
		t.Fatal("invalid project id should fail")
	}
}
