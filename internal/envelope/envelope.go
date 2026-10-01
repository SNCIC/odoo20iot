// Package envelope 定义「网关 → 消息管道」的统一消息信封。
//
// # 为什么必须有它（03 §2.4 / §4.3）
//
// 网关把设备上报映射到总线 subject `iot.{stream}.{project}.shard.{n}` 后，
// **设备归属信息在 subject 里就丢了** —— subject 只含 project 与 shard，
// 不含 `device_key`。消费者因此无法知道一条消息属于哪台设备，也就无法落库。
//
// 03 §2.4「通过后包装统一信封，发布到 NATS」与 §4.3「解信封 → 校验
// tenant / trace_id / schema_version」正是为此：**原始报文必须与归属元数据
// 一起封装**，而不是裸发 payload。
//
// ⚠️ Phase 0 的占位说明（如实留白，不做假实现）：`project_id` / `device_id` /
// `device_type_id` 在真实系统中来自 A1 的设备凭据注册表（`device_key` →
// 租户与设备主键），该注册表尚未实现。当前用**稳定占位值**填充，并由网关侧
// 统一生成，保证「同一设备恒定同一 ID」；接入注册表后替换生成逻辑即可。
package envelope

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"
)

// Envelope 是网关发布到 NATS 的统一信封。
type Envelope struct {
	// 归属元数据（Phase 0 为占位值，见包注释）。
	ProjectID    int64  `json:"project_id"`
	DeviceKey    string `json:"device_key"`
	DeviceID     int64  `json:"device_id"`
	DeviceTypeID int64  `json:"device_type_id"`

	// Stream 是端侧上行流（telemetry / attributes / events / ...），
	// 取自 topic `v1/devices/{device_key}/{stream}`。
	Stream string `json:"stream"`

	// ReceivedAt 是网关接收时刻（平台侧时间轴，与端侧 ts 区分）。
	ReceivedAt time.Time `json:"received_at"`

	// Payload 是设备上报的**原始报文**（不解析）。
	//
	// 用 json.RawMessage 而非 []byte：`[]byte` 会被 base64 编码，体积膨胀 33%；
	// 而 03 §2.4 已要求进入管道的报文必须是合法 JSON，故可直接内联。
	Payload json.RawMessage `json:"payload"`
}

// Encode 序列化信封。
func (e Envelope) Encode() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("序列化设备信封（device_key=%s）: %w", e.DeviceKey, err)
	}
	return b, nil
}

// Decode 反序列化信封并校验必要字段。
//
// 校验刻意严格：缺 `device_key` 或 `payload` 的信封无法落库，
// 与其在管道深处空指针，不如在入口就拒绝。
func Decode(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("解析设备信封: %w", err)
	}
	if e.DeviceKey == "" {
		return Envelope{}, fmt.Errorf("设备信封缺少 device_key")
	}
	if len(e.Payload) == 0 {
		return Envelope{}, fmt.Errorf("设备信封缺少 payload（device_key=%s）", e.DeviceKey)
	}
	return e, nil
}

// PlaceholderDeviceID 由 device_key 派生一个稳定的占位 device_id（FNV-1a）。
//
// 只用于 Phase 0 打通链路：它保证「同一设备恒定同一 ID」，但**不是**真实主键。
// 与网关分片用的哈希同源（gateway.hashShard 亦为 FNV-1a），便于对照。
func PlaceholderDeviceID(deviceKey string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(deviceKey))
	return int64(h.Sum64() & 0x7FFF_FFFF_FFFF_FFFF) // 保持正数，避免占位 ID 为负
}
