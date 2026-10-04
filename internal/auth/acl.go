package auth

import "strings"

// ACL 是由物模型**自动生成**的设备 topic 白名单（03 §2.2），不接受手工配置。
//
// 设计要点：既能表达「允许的过滤器」，也能安全地判定「某条具体 topic 是否属于它」。
// 设备侧只做两件事 —— 发布自己命名空间下的 topic、订阅自己命名空间下的命令/配置；
// 其余一切（包括 `#`、`+/+`、`$SYS/#`）一律拒绝，防止横向越权。
type ACL struct {
	// DeviceKey 是该设备的唯一标识，用作命名空间前缀。
	DeviceKey string
	// IsGateway 为真时额外放行子设备命名空间（03 §2.2）。
	IsGateway bool

	allowPub []string
	allowSub []string
}

// 03 §2.2 规定的发布/订阅主题（占位符 {k} 为 device_key）。
var (
	pubPatterns = []string{
		"v1/devices/%s/telemetry",
		"v1/devices/%s/attributes",
		"v1/devices/%s/events",
		"v1/devices/%s/cmd/reply",
		"v1/devices/%s/shadow/reported",
		"v1/devices/%s/ota/progress",
	}
	subPatterns = []string{
		"v1/devices/%s/cmd/+",
		"v1/devices/%s/shadow/desired",
		"v1/devices/%s/cfg/+",
		"v1/devices/%s/ota/+",
	}
	// 网关型设备的子设备命名空间：v1/gateways/{k}/devices/{sub}/...
	gatewayPubPatterns = []string{
		"v1/gateways/%s/devices/+/telemetry",
		"v1/gateways/%s/devices/+/attributes",
		"v1/gateways/%s/devices/+/events",
		"v1/gateways/%s/devices/+/cmd/reply",
		"v1/gateways/%s/devices/+/shadow/reported",
		"v1/gateways/%s/devices/+/ota/progress",
	}
	gatewaySubPatterns = []string{
		"v1/gateways/%s/devices/+/cmd/+",
		"v1/gateways/%s/devices/+/shadow/desired",
		"v1/gateways/%s/devices/+/cfg/+",
		"v1/gateways/%s/devices/+/ota/+",
	}
)

// NewACL 按 identity 生成 ACL。
func NewACL(id *Identity) *ACL {
	if id == nil {
		return &ACL{}
	}

	a := &ACL{DeviceKey: id.DeviceKey, IsGateway: id.IsGateway}
	a.allowPub = fill(pubPatterns, id.DeviceKey)
	a.allowSub = fill(subPatterns, id.DeviceKey)

	if id.IsGateway {
		a.allowPub = append(a.allowPub, fill(gatewayPubPatterns, id.DeviceKey)...)
		a.allowSub = append(a.allowSub, fill(gatewaySubPatterns, id.DeviceKey)...)
	}
	return a
}

func fill(patterns []string, key string) []string {
	out := make([]string, 0, len(patterns))
	for _, p := range patterns {
		out = append(out, strings.ReplaceAll(p, "%s", key))
	}
	return out
}

// AllowedPub 返回允许发布的过滤器（只读用途：日志、控制台回显）。
func (a *ACL) AllowedPub() []string { return a.allowPub }

// AllowedSub 返回允许订阅的过滤器（只读用途）。
func (a *ACL) AllowedSub() []string { return a.allowSub }

// AllowPublish 判断设备是否可以发布到该具体 topic。
//
// write=true 时按发布白名单判定，false 时按订阅白名单判定。
// 注意这里是「具体 topic vs 过滤器」的匹配，方向不能反 ——
// 反过来会把 `#` 这类过滤器当成合法 topic 放行。
func (a *ACL) Allow(topic string, write bool) bool {
	if topic == "" || a.DeviceKey == "" {
		return false
	}
	// 设备永远不能碰 $SYS 之类的系统主题。
	if strings.HasPrefix(topic, "$") {
		return false
	}
	// 设备的订阅请求本身可能带通配符（它订阅的是过滤器），
	// 因此订阅方向要做「过滤器 vs 过滤器」包含判定；发布方向 topic 必须是具体值。
	if write {
		if strings.ContainsAny(topic, "+#") {
			return false
		}
		return anyMatch(a.allowPub, topic)
	}
	return anySubset(a.allowSub, topic)
}

func anyMatch(filters []string, topic string) bool {
	for _, f := range filters {
		if MatchTopic(topic, f) {
			return true
		}
	}
	return false
}

// anySubset 判断「请求的订阅过滤器」是否被某条白名单过滤器完全包含。
//
// 例：白名单有 v1/devices/k/cmd/+ 时，请求 v1/devices/k/cmd/set 与
// v1/devices/k/cmd/+ 都允许；请求 v1/devices/k/cmd/# 也允许（它是 ls 的子集），
// 但请求 v1/devices/# 必须拒绝。
func anySubset(filters []string, requested string) bool {
	for _, f := range filters {
		if FilterContains(f, requested) {
			return true
		}
	}
	return false
}

// MatchTopic 判断一个**具体 topic** 是否符合 MQTT topic 过滤器。
//
// 语义遵循 MQTT 3.1.1/5.0 §4.7：
//   - `+` 匹配单层，且**不匹配空层**；
//   - `#` 匹配剩余所有层（含零层），且**只能是最后一层**；
//   - `$` 开头的 topic 不被 `#` / `+` 在首层匹配（此处由 Allow 统一拒绝 `$` 主题）。
func MatchTopic(topic, filter string) bool {
	tl := strings.Split(topic, "/")
	fl := strings.Split(filter, "/")

	for i, f := range fl {
		if f == "#" {
			// 只能是最后一层；容忍 "#" 出现在末尾（"a/#" 亦匹配 "a"）。
			return i == len(fl)-1
		}
		if i >= len(tl) {
			return false
		}
		if f == "+" {
			if tl[i] == "" {
				return false // `+` 不匹配空层
			}
			continue
		}
		if f != tl[i] {
			return false
		}
	}
	return len(tl) == len(fl)
}

// FilterContains 判断过滤器 outer 是否覆盖过滤器 inner（inner ⊆ outer）。
//
// 用于订阅授权：设备请求的订阅必须落在白名单范围内。
func FilterContains(outer, inner string) bool {
	ol := strings.Split(outer, "/")
	il := strings.Split(inner, "/")

	for i, o := range ol {
		if o == "#" {
			return true // outer 到这里已经覆盖 inner 的剩余全部
		}
		if i >= len(il) {
			return false
		}
		switch o {
		case "+":
			if il[i] == "#" {
				return false // inner 用 # 覆盖了比 + 更宽的范围
			}
		default:
			if o != il[i] {
				return false
			}
		}
	}
	// outer 已走完；inner 还有剩余层，或两者等长才算覆盖。
	return len(il) == len(ol)
}
