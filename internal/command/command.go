// Package command 定义平台到设备的下行命令契约。
package command

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SNCIC/odoo20iot/internal/cluster"
)

type Router interface {
	RouteExternal(context.Context, cluster.Envelope) error
}

type Request struct {
	ProjectID     int64
	DeviceKey     string
	CommandKey    string
	Payload       any
	CorrelationID string
}

type Sender struct{ Router Router }

func (s Sender) Send(ctx context.Context, req Request) error {
	if s.Router == nil {
		return fmt.Errorf("command: router 未配置")
	}
	if strings.TrimSpace(req.DeviceKey) == "" || strings.TrimSpace(req.CommandKey) == "" {
		return fmt.Errorf("command: device_key 和 command_key 必填")
	}
	if strings.ContainsAny(req.DeviceKey, "/+#") || strings.ContainsAny(req.CommandKey, "/+#") {
		return fmt.Errorf("command: device_key 和 command_key 不能包含 MQTT 通配符或斜杠")
	}
	if req.CorrelationID == "" {
		id, err := newID()
		if err != nil {
			return err
		}
		req.CorrelationID = id
	}
	body, err := json.Marshal(map[string]any{
		"command_id":     req.CorrelationID,
		"correlation_id": req.CorrelationID,
		"command":        req.CommandKey,
		"payload":        req.Payload,
	})
	if err != nil {
		return fmt.Errorf("command: 编码 payload: %w", err)
	}
	topic := fmt.Sprintf("v1/devices/%s/cmd/%s", req.DeviceKey, req.CommandKey)
	return s.Router.RouteExternal(ctx, cluster.Envelope{Topic: topic, Payload: body, Qos: 1, DeviceKey: req.DeviceKey})
}

type Action struct{ Sender Sender }

func (Action) Name() string     { return "command.send" }
func (Action) Idempotent() bool { return false }

func (a Action) Do(ctx context.Context, params map[string]any) error {
	deviceKey, _ := params["device_key"].(string)
	commandKey, _ := params["command"].(string)
	if commandKey == "" {
		commandKey, _ = params["command_key"].(string)
	}
	var projectID int64
	switch value := params["project_id"].(type) {
	case int64:
		projectID = value
	case int:
		projectID = int64(value)
	case float64:
		projectID = int64(value)
	}
	return a.Sender.Send(ctx, Request{ProjectID: projectID, DeviceKey: deviceKey, CommandKey: commandKey, Payload: params["payload"], CorrelationID: stringValue(params["correlation_id"])})
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("command: 生成 correlation_id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
