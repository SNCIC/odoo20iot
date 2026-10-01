package alarm

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseTriggerHappyPath(t *testing.T) {
	raw := []byte(`{
		"project_id":"p1","device_id":"d1","device_type_id":55,
		"rule_id":"ar_001","rule_name":"温度过高","level":"critical",
		"detect_window_s":45,"suppress_s":420,"auto_close_s":90,
		"value":{"temperature":92}
	}`)
	tr, err := ParseTrigger(raw)
	if err != nil {
		t.Fatalf("ParseTrigger: %v", err)
	}
	if tr.ProjectID != "p1" || tr.DeviceID != "d1" || tr.RuleID != "ar_001" || tr.Level != "critical" {
		t.Fatalf("得 %+v", tr)
	}
	if tr.DeviceTypeID != 55 {
		t.Fatalf("device_type_id 得 %d", tr.DeviceTypeID)
	}
	// 时序三段要真的按秒还原，而不是落到默认值 ——
	// 落默认值的表现是「观察期突然变成 60s」，很难从日志看出来。
	if tr.Timing.DetectWindow != 45*time.Second ||
		tr.Timing.Suppress != 420*time.Second ||
		tr.Timing.AutoClose != 90*time.Second {
		t.Fatalf("timing 得 %+v", tr.Timing)
	}
	if string(tr.Value) != `{"temperature":92}` {
		t.Fatalf("快照应原样带上，得 %s", tr.Value)
	}
	// 去重键只由三元组决定，快照不该影响它。
	if DedupKey(tr.ProjectID, tr.DeviceID, tr.RuleID) == "" {
		t.Fatal("去重键不该为空")
	}
}

func TestParseTriggerRejectsMissingRequiredFields(t *testing.T) {
	// 缺一个必填字段就必须拒绝，且**不设默认值**：
	// 缺租户会让去重键跨租户串味，缺级别会让通知无法分级路由。
	cases := map[string]string{
		"缺租户": `{"device_id":"d1","rule_id":"r","level":"warn"}`,
		"缺设备": `{"project_id":"p1","rule_id":"r","level":"warn"}`,
		"缺规则": `{"project_id":"p1","device_id":"d1","level":"warn"}`,
		"缺级别": `{"project_id":"p1","device_id":"d1","rule_id":"r"}`,
		"空对象": `{}`,
	}
	for name, raw := range cases {
		if _, err := ParseTrigger([]byte(raw)); err == nil {
			t.Fatalf("%s：应被拒绝", name)
		}
	}
	if _, err := ParseTrigger([]byte(`{bad json`)); err == nil {
		t.Fatal("非法 JSON 应被拒绝")
	}
}

func TestParseTriggerDefaultsTiming(t *testing.T) {
	raw := []byte(`{"project_id":"p1","device_id":"d1","rule_id":"r","level":"warn"}`)
	tr, err := ParseTrigger(raw)
	if err != nil {
		t.Fatalf("ParseTrigger: %v", err)
	}
	// 没给 timing 时应落 04 §2.1 的默认，而不是零值
	// （零值会让观察期变成 0，detected 一进来就直接 confirmed）。
	got := tr.Timing.Normalize()
	if got.DetectWindow != DefaultDetectWindow || got.Suppress != DefaultSuppress || got.AutoClose != DefaultAutoClose {
		t.Fatalf("得 %+v", got)
	}
	if len(tr.Value) != 0 {
		t.Fatalf("未给快照时不该编造，得 %s", tr.Value)
	}
}

func TestTriggerSubjectIsStable(t *testing.T) {
	// 这是跨服务契约；改名要同步 svc-rule 与消费端，故盯住它。
	if TriggerSubject != "iot.rule.alarm" {
		t.Fatalf("subject 被改了：%q", TriggerSubject)
	}
	_ = json.Valid
}
