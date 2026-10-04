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
	Status        string          `json:"status"`
	Payload       json.RawMessage `json:"payload"`
	Error         string          `json:"error,omitempty"`
	ReceivedAt    time.Time       `json:"received_at,omitempty"`
}

func DecodeReply(data []byte) (Reply, error) {
	var reply Reply
	if err := json.Unmarshal(data, &reply); err != nil {
		return Reply{}, fmt.Errorf("command: 回执不是合法 JSON: %w", err)
	}
	if strings.TrimSpace(reply.CorrelationID) == "" && strings.TrimSpace(reply.CommandID) == "" {
		return Reply{}, fmt.Errorf("command: 回执缺少 correlation_id 或 command_id")
	}
	if strings.TrimSpace(reply.Status) == "" {
		return Reply{}, fmt.Errorf("command: 回执缺少 status")
	}
	return reply, nil
}
