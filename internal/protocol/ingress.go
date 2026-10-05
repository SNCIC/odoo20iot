package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// IngressFrame 是二期协议适配器交给统一网关管道的最小契约。
// 适配器只负责解传输，不负责认证、配额、统一信封和 NATS 持久化确认。
type IngressFrame struct {
	Protocol   string
	DeviceKey  string
	GatewayKey string
	SubKey     string
	Stream     string
	Payload    []byte
}

func ParseCoAPIngress(msg CoAPMessage) (IngressFrame, error) {
	parts := strings.Split(strings.Trim(msg.URIPath, "/"), "/")
	if len(parts) == 4 && parts[0] == "v1" && parts[1] == "devices" {
		return newIngressFrame("coap", parts[2], "", "", parts[3], msg.Payload)
	}
	if len(parts) == 6 && parts[0] == "v1" && parts[1] == "gateways" && parts[3] == "devices" {
		return newIngressFrame("coap", parts[4], parts[2], parts[4], parts[5], msg.Payload)
	}
	return IngressFrame{}, fmt.Errorf("coap URI-Path 不符合设备上报契约: %q", msg.URIPath)
}

func ParseTCPIngress(deviceKey string, frame []byte) (IngressFrame, error) {
	if strings.TrimSpace(deviceKey) == "" {
		return IngressFrame{}, fmt.Errorf("tcp 缺少 device_key")
	}
	var body struct {
		Stream  string          `json:"stream"`
		Payload json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(frame, &body) == nil && len(body.Payload) > 0 {
		return newIngressFrame("tcp", deviceKey, "", "", body.Stream, body.Payload)
	}
	return newIngressFrame("tcp", deviceKey, "", "", "telemetry", frame)
}

func newIngressFrame(protocol, deviceKey, gatewayKey, subKey, stream string, payload []byte) (IngressFrame, error) {
	if deviceKey == "" || stream == "" || strings.ContainsAny(deviceKey+gatewayKey+subKey+stream, "/+#.") {
		return IngressFrame{}, fmt.Errorf("%s 设备标识或 stream 非法", protocol)
	}
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || !json.Valid(payload) {
		return IngressFrame{}, fmt.Errorf("%s payload 必须是合法 JSON", protocol)
	}
	return IngressFrame{Protocol: protocol, DeviceKey: deviceKey, GatewayKey: gatewayKey, SubKey: subKey, Stream: stream, Payload: append([]byte(nil), payload...)}, nil
}
