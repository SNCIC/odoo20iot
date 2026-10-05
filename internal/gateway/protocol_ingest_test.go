package gateway

import (
	"context"
	"testing"

	"github.com/SNCIC/odoo20iot/internal/auth"
	"github.com/SNCIC/odoo20iot/internal/protocol"
)

func TestProtocolIngressRejectsInvalidFrame(t *testing.T) {
	h, err := NewProtocolIngestHandler(ProtocolIngestOptions{Authenticator: testAuthenticator(t), Publisher: &fakePublisher{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Handle(context.Background(), protocol.IngressFrame{DeviceKey: "d1", Stream: "telemetry", Payload: []byte(`{`)}, "secret", nil); err == nil {
		t.Fatal("非法 JSON 必须拒绝")
	}
}

func testAuthenticator(t *testing.T) *auth.Authenticator {
	t.Helper()
	id := &auth.Identity{ProjectID: 1, DeviceID: 2, DeviceTypeID: 3, DeviceKey: "d1", Mode: auth.ModePerDevice, Secret: auth.Digest{}}
	// 该测试只覆盖帧校验，认证器不会被非法帧调用。
	return auth.NewAuthenticator(auth.DirectoryFunc(func(context.Context, string) (*auth.Identity, error) { return id, nil }), auth.DefaultPolicy(), nil)
}
