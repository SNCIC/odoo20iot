package command

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Reply struct {
	CommandID     string          `json:"command_id"`
	CorrelationID string          `json:"correlation_id"`
	DeviceKey     string          `json:"device_key,omitempty"`
	Status        string          `json:"status"`
	Payload       json.RawMessage `json:"payload"`
	Error         string          `json:"error,omitempty"`
	ReceivedAt    time.Time       `json:"received_at,omitempty"`
}

func DecodeReply(data []byte) (Reply, error) {
	var raw struct {
		Reply
		ID   string          `json:"id"`
		Code *int            `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Reply{}, fmt.Errorf("command: 回执不是合法 JSON: %w", err)
	}
	reply := raw.Reply
	if reply.CorrelationID == "" {
		reply.CorrelationID = strings.TrimSpace(raw.ID)
	}
	if reply.Payload == nil && raw.Data != nil {
		reply.Payload = raw.Data
	}
	if reply.Error == "" {
		reply.Error = raw.Msg
	}
	if strings.TrimSpace(reply.Status) == "" && raw.Code != nil {
		if *raw.Code == 0 {
			reply.Status = "acked"
		} else {
			reply.Status = "failed"
		}
	}
	if strings.TrimSpace(reply.CorrelationID) == "" && strings.TrimSpace(reply.CommandID) == "" {
		return Reply{}, fmt.Errorf("command: 回执缺少 correlation_id 或 command_id")
	}
	if strings.TrimSpace(reply.Status) == "" {
		return Reply{}, fmt.Errorf("command: 回执缺少 status")
	}
	return reply, nil
}
