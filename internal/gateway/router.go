package gateway

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strings"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// ErrUnroutableTopic 表示报文无法归属到任何租户流（端侧契约之外）。
var ErrUnroutableTopic = errors.New("无法路由的设备 topic")

// SubjectRouter 把设备上报 topic 映射为内部总线 subject。
type SubjectRouter interface {
	Route(cl *mqtt.Client, pk packets.Packet) (string, error)
}

// 端侧 topic 契约（docs/03-ingestion.md §5）：v1/devices/{deviceKey}/{stream}
const (
	topicRoot    = "v1"
	topicDevices = "devices"
	topicParts   = 4 // v1 / devices / {deviceKey} / {stream}
)

// ContractRouter 按端侧 topic 契约解析并映射到分片式总线 subject：
//
//	v1/devices/{deviceKey}/{stream}  →  iot.{stream}.{project}.shard.{hash(deviceKey) % Shards}
//
// ⚠️ **它不是多租户实现**：`Project` 是 Phase 0 的占位值。真实的租户投影
// （deviceKey → project_id）依赖 A1 的设备凭据注册表，该表尚未实现；
// 这里如实使用占位 project，不假装已经具备多租户能力。
//
// 分片数固定为编译期常量（P0-3 的结论 N=32），保证 Lite（1 实例）→ Standard（4 实例）
// 迁移时 subject 名不变。
type ContractRouter struct {
	Project string
	Shards  int
}

// DefaultShards 是离线/遥测流的分片数（P0-3 选型结论）。
const DefaultShards = 32

var _ SubjectRouter = ContractRouter{}

// Route 实现 SubjectRouter。
func (r ContractRouter) Route(_ *mqtt.Client, pk packets.Packet) (string, error) {
	shards := r.Shards
	if shards <= 0 {
		shards = DefaultShards
	}

	parts := strings.Split(pk.TopicName, "/")
	if len(parts) != topicParts ||
		parts[0] != topicRoot || parts[1] != topicDevices ||
		parts[2] == "" || parts[3] == "" {
		return "", fmt.Errorf("%w: %q 不符合 v1/devices/{deviceKey}/{stream}", ErrUnroutableTopic, pk.TopicName)
	}

	deviceKey, stream := parts[2], parts[3]
	shard := hashShard(deviceKey, shards)

	return fmt.Sprintf("iot.%s.%s.shard.%d", stream, r.Project, shard), nil
}

// hashShard 用 FNV-1a 做分片，保证同一设备恒定落同一分片（规则 prev 上下文与影子合并的前提）。
func hashShard(deviceKey string, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(deviceKey))
	return int(h.Sum32() % uint32(shards))
}
