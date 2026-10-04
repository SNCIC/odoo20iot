// Package cluster 实现 03 §1.4 的跨节点投递与 §1.5 的离线队列。
//
// # 与文档设计的一处重要分歧（实测得出，见 03 §1.4.1 ①）
//
// 03 §1.4 原方案的核心是 Redis 反向索引 `gw:route:{filter_hash}`，用它「由 topic
// 反查哪些节点有订阅」。但 MQTT 的订阅是**通配符过滤器**（`v1/devices/+/telemetry`），
// 而按 `filter_hash` 建的索引只能做**精确匹配** —— 给定一条具体 topic，
// 无法在不枚举全部 filter 的前提下找出匹配它的那些 filter。
//
// 因此本包默认采用文档自己列为「节点数 ≤ 8 时可用」的**广播模式**：
// 把消息发到每个对端节点的专属 subject，由对端用自己的订阅树判定是否命中。
// 代价是每发布放大 (N-1) 倍，实测数据见 03 §1.4.1 ②。
//
// 为什么不用「把 filter 列表拉到本地逐个匹配」：那本质就是广播，
// 却额外引入了注册表一致性问题。方向索引要真正可用，需要一个**按层级前缀索引**
// 的结构（而不是 filter 的哈希）；这是 Phase 1 的待解问题，已如实登记。
package cluster

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Envelope 是跨节点投递的信封。
//
// 它必须自带 **Origin**：接收端据此判断「这条消息是不是自己发的」，
// 以及为将来的定向模式（排除已投递节点）留出依据。
// (Origin, Seq) 共同构成投递标识，接收端用它做进程内的 best-effort 去重
// （见 dedupCache 注释）；端到端幂等仍由设备侧 msg_id 兜底。
type Envelope struct {
	// Seq 是源节点内的单调序号，与 Origin 一起构成全局唯一的投递标识。
	Seq uint64 `json:"seq"`
	// Origin 是源节点 id。**必要**：广播模式下每个节点都会收到自己的那份，
	// 靠它丢弃自环。
	Origin string `json:"origin"`

	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	Qos     byte   `json:"qos"`
	Retain  bool   `json:"retain"`

	// DeviceKey 由 topic 解析而来，供接收端快速判定「目标设备是否在本节点」，
	// 从而决定走投递还是走离线队列。为空表示该 topic 不属于设备命名空间。
	DeviceKey string `json:"device_key,omitempty"`

	// PublishedAt 是源节点发布时刻，用于观测端到端时延。
	PublishedAt time.Time `json:"published_at"`
}

// Encode 序列化信封。
func (e Envelope) Encode() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("序列化投递信封: %w", err)
	}
	return b, nil
}

// DecodeEnvelope 反序列化信封。
func DecodeEnvelope(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("解析投递信封: %w", err)
	}
	if e.Topic == "" {
		return Envelope{}, fmt.Errorf("投递信封缺少 topic")
	}
	return e, nil
}

// 设备 topic 的命名空间前缀（03 §5.1）。
const (
	devicePrefix  = "v1/devices/"
	gatewayPrefix = "v1/gateways/"
)

// ParseDeviceTopic 把设备 topic 拆成 (device_key, 剩余路径)。
//
// 覆盖设备与网关两个命名空间；非设备 topic 返回 ("", "")。
// 两个函数共用它，避免「解析规则有一份、方向判定又写一份」的漂移。
func ParseDeviceTopic(topic string) (deviceKey, rest string) {
	switch {
	case strings.HasPrefix(topic, devicePrefix):
		rest = topic[len(devicePrefix):]
	case strings.HasPrefix(topic, gatewayPrefix):
		// v1/gateways/{gw}/devices/{key}/...
		after := topic[len(gatewayPrefix):]
		gw, tail, ok := cut(after, "/")
		if !ok || gw == "" {
			return "", ""
		}
		if !strings.HasPrefix(tail, "devices/") {
			return "", ""
		}
		rest = tail[len("devices/"):]
	default:
		return "", ""
	}

	key, tail, ok := cut(rest, "/")
	if !ok || key == "" {
		return "", ""
	}
	return key, tail
}

// cut 是 strings.Cut 的简写包装（省略 found 的样板）。
func cut(s, sep string) (before, after string, found bool) {
	return strings.Cut(s, sep)
}

// DeviceKeyFromTopic 返回 topic 归属的设备；非设备 topic 返回空字符串。
func DeviceKeyFromTopic(topic string) string {
	key, _ := ParseDeviceTopic(topic)
	return key
}

func IsCommandReplyTopic(topic string) bool {
	_, rest := ParseDeviceTopic(topic)
	return rest == "cmd/reply"
}

func CommandReplySubject(projectID int64, deviceKey string) (string, error) {
	if projectID <= 0 || deviceKey == "" {
		return "", fmt.Errorf("命令回执缺少有效 project_id 或 device_key")
	}
	encodedKey := base64.RawURLEncoding.EncodeToString([]byte(deviceKey))
	return fmt.Sprintf("iot.cmd.reply.%d.%s", projectID, encodedKey), nil
}

func ParseCommandReplySubject(subject string) (int64, string, error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 5 || parts[0] != "iot" || parts[1] != "cmd" || parts[2] != "reply" {
		return 0, "", fmt.Errorf("命令回执 subject 非法: %q", subject)
	}
	projectID, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || projectID <= 0 {
		return 0, "", fmt.Errorf("命令回执 project_id 非法")
	}
	deviceKeyBytes, err := base64.RawURLEncoding.DecodeString(parts[4])
	if err != nil || len(deviceKeyBytes) == 0 {
		return 0, "", fmt.Errorf("命令回执 device_key 编码非法")
	}
	return projectID, string(deviceKeyBytes), nil
}

func ShadowReportedSubject(projectID int64, deviceKey string) (string, error) {
	if projectID <= 0 || deviceKey == "" {
		return "", fmt.Errorf("影子上报缺少有效 project_id 或 device_key")
	}
	encodedKey := base64.RawURLEncoding.EncodeToString([]byte(deviceKey))
	return fmt.Sprintf("iot.shadow.reported.%d.%s", projectID, encodedKey), nil
}

func ParseShadowReportedSubject(subject string) (int64, string, error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 5 || parts[0] != "iot" || parts[1] != "shadow" || parts[2] != "reported" {
		return 0, "", fmt.Errorf("影子上报 subject 非法: %q", subject)
	}
	projectID, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || projectID <= 0 {
		return 0, "", fmt.Errorf("影子上报 project_id 非法")
	}
	deviceKeyBytes, err := base64.RawURLEncoding.DecodeString(parts[4])
	if err != nil || len(deviceKeyBytes) == 0 {
		return 0, "", fmt.Errorf("影子上报 device_key 编码非法")
	}
	return projectID, string(deviceKeyBytes), nil
}

func IsShadowReportedTopic(topic string) bool {
	_, rest := ParseDeviceTopic(topic)
	return rest == "shadow/reported"
}

// IsDeviceDownlink 判断该 topic 是否是「平台 → 设备」方向。
//
// 方向由**设备键之后的第一段路径**决定，规则直接来自 03 §2.2 的两张白名单：
//
//	上行（设备发布）：telemetry / attributes / events / cmd/reply / shadow/reported
//	下行（设备订阅）：cmd/*（除 reply）/ cfg/* / ota/* / shadow/desired
//
// 未列出的段一律按**非下行**处理：宁可少路由，也不要把上行的东西当成下行
// 送进离线队列（那会把真正的命令挤掉，见下）。
func IsDeviceDownlink(topic string) bool {
	key, rest := ParseDeviceTopic(topic)
	if key == "" || rest == "" {
		return false
	}

	seg, tail, _ := strings.Cut(rest, "/")
	switch seg {
	case "cmd":
		return tail != "reply" // cmd/reply 是设备应答，属上行
	case "cfg", "ota":
		return true
	case "shadow":
		return tail == "desired"
	default:
		return false
	}
}
