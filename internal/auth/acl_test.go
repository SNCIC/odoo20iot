package auth

import "testing"

// TestMatchTopic 是 ACL 的地基，必须逐条对照 MQTT 4.7 的语义。
func TestMatchTopic(t *testing.T) {
	cases := []struct {
		topic  string
		filter string
		want   bool
	}{
		// 精确
		{"v1/devices/k/cmd/set", "v1/devices/k/cmd/set", true},
		{"v1/devices/k/cmd/set", "v1/devices/k/cmd/get", false},
		// 层级必须严格一致（前缀相似不算匹配）
		{"v1/devices/k/cmd", "v1/devices/k/cmd/set", false},
		{"v1/devices/k/cmd/set/extra", "v1/devices/k/cmd/set", false},
		// `+` 匹配单层
		{"v1/devices/k/cmd/set", "v1/devices/k/cmd/+", true},
		{"v1/devices/k/cmd/set/extra", "v1/devices/k/cmd/+", false},
		{"v1/devices/k/cmd", "v1/devices/k/cmd/+", false},
		// `+` 不匹配空层
		{"v1/devices/k/cmd/", "v1/devices/k/cmd/+", false},
		// `#` 匹配剩余全部（含零层）
		{"v1/devices/k/cmd/set", "v1/devices/k/#", true},
		{"v1/devices/k", "v1/devices/k/#", true},
		{"v1/devices/k/cmd/set/extra", "v1/devices/#", true},
		{"v1/devices/other/cmd", "v1/devices/k/#", false},
	}

	for _, c := range cases {
		if got := MatchTopic(c.topic, c.filter); got != c.want {
			t.Errorf("MatchTopic(%q, %q) = %v，期望 %v", c.topic, c.filter, got, c.want)
		}
	}
}

// TestFilterContains 是「订阅授权」的地基。
// 方向搞反会让设备订阅到别人的主题 —— 这是横向越权，不是小问题。
func TestFilterContains(t *testing.T) {
	cases := []struct {
		outer string
		inner string
		want  bool
	}{
		{"v1/devices/k/cmd/+", "v1/devices/k/cmd/set", true},
		{"v1/devices/k/cmd/+", "v1/devices/k/cmd/+", true},
		{"v1/devices/k/cmd/+", "v1/devices/k/cmd/#", false}, // inner 更宽
		{"v1/devices/k/cmd/+", "v1/devices/k/#", false},
		{"v1/devices/k/cmd/+", "v1/devices/#", false},
		{"v1/devices/k/cmd/+", "v1/devices/k/cmd", false},
		{"v1/devices/k/cmd/+", "v1/devices/k/cmd/set/extra", false},
		{"v1/devices/#", "v1/devices/k/cmd/set", true},
		{"v1/devices/#", "v1/devices/k/#", true},
		{"v1/devices/k/#", "v1/devices/other/#", false},
	}

	for _, c := range cases {
		if got := FilterContains(c.outer, c.inner); got != c.want {
			t.Errorf("FilterContains(%q, %q) = %v，期望 %v", c.outer, c.inner, got, c.want)
		}
	}
}

// TestACL_设备只能碰自己的命名空间 是 03 §2.2 的核心安全断言。
func TestACL_设备只能碰自己的命名空间(t *testing.T) {
	acl := NewACL(&Identity{DeviceKey: "dev-A", Mode: ModePerDevice})

	allowedPub := []string{
		"v1/devices/dev-A/telemetry",
		"v1/devices/dev-A/attributes",
		"v1/devices/dev-A/events",
		"v1/devices/dev-A/cmd/reply",
		"v1/devices/dev-A/shadow/reported",
	}
	for _, topic := range allowedPub {
		if !acl.Allow(topic, true) {
			t.Errorf("应允许发布 %s", topic)
		}
	}

	deniedPub := []string{
		"v1/devices/dev-B/telemetry", // 别的设备
		"v1/devices/dev-A/cmd/set",   // 只允许订阅，不允许发布
		"v1/devices/dev-A/shadow/desired",
		"v1/devices/dev-A/telemetry/extra", // 多一层
		"v1/devices/dev-A",                 // 少一层
		"v1/devices/+/telemetry",           // 发布带通配符
		"v1/devices/dev-A/#",
		"$SYS/broker/uptime",
		"#",
		"",
	}
	for _, topic := range deniedPub {
		if acl.Allow(topic, true) {
			t.Errorf("不应允许发布 %s", topic)
		}
	}

	allowedSub := []string{
		"v1/devices/dev-A/cmd/set",
		"v1/devices/dev-A/cmd/+",
		"v1/devices/dev-A/shadow/desired",
		"v1/devices/dev-A/cfg/+",
		"v1/devices/dev-A/ota/+",
	}
	for _, filter := range allowedSub {
		if !acl.Allow(filter, false) {
			t.Errorf("应允许订阅 %s", filter)
		}
	}

	deniedSub := []string{
		"v1/devices/dev-B/cmd/+",
		"v1/devices/dev-A/#", // 覆盖到不该订阅的层级
		"v1/devices/#",
		"#",
		"+/+",
		"v1/devices/dev-A/cmd/set/extra", // 超出 + 的宽度
		"$SYS/#",
	}
	for _, filter := range deniedSub {
		if acl.Allow(filter, false) {
			t.Errorf("不应允许订阅 %s", filter)
		}
	}
}

// TestACL_网关型设备 覆盖 03 §2.2 的子设备命名空间。
func TestACL_网关型设备(t *testing.T) {
	acl := NewACL(&Identity{DeviceKey: "gw-1", Mode: ModePerDevice, IsGateway: true})

	if !acl.Allow("v1/gateways/gw-1/devices/sub-1/telemetry", true) {
		t.Error("网关应可代发子设备遥测")
	}
	if !acl.Allow("v1/gateways/gw-1/devices/sub-1/cmd/+", false) {
		t.Error("网关应可订阅子设备命令")
	}
	if acl.Allow("v1/gateways/gw-2/devices/sub-1/telemetry", true) {
		t.Error("网关不得代发别人名下的子设备数据")
	}
	// 网关仍然只能碰自己的 v1/devices/{自身 key} 命名空间
	if acl.Allow("v1/devices/gw-2/telemetry", true) {
		t.Error("网关不得发布到其他设备的命名空间")
	}
}

func TestACL_空身份一律拒绝(t *testing.T) {
	for _, acl := range []*ACL{NewACL(nil), NewACL(&Identity{})} {
		if acl.Allow("v1/devices/k/telemetry", true) {
			t.Error("空身份不得放行任何发布")
		}
		if acl.Allow("v1/devices/k/cmd/+", false) {
			t.Error("空身份不得放行任何订阅")
		}
	}
}
