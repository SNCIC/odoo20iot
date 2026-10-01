package alarm

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAlarmSubject(t *testing.T) {
	if got := AlarmSubject("p1"); got != "iot.alarm.p1" {
		t.Fatalf("得 %q", got)
	}
	// 空租户不该拼出末尾空 token —— 那样「租户丢了」只会在很下游才暴露。
	if got := AlarmSubject(""); got != "iot.alarm._" {
		t.Fatalf("空租户应占位，得 %q", got)
	}
	// subject 前缀是 07 §6 S3 的下游契约，改名会静默切断 S3。
	if AlarmSubjectPrefix != "iot.alarm" {
		t.Fatalf("前缀被改了，会切断 07 §6 S3：%q", AlarmSubjectPrefix)
	}
}

func TestNewEventOnlyOnEnteringActive(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := &Alarm{
		ID: "alarm-7", DedupKey: "dk", ProjectID: "p1", DeviceID: "d1",
		DeviceTypeID: 55, RuleID: "ar_001", Level: "critical",
		State: StateActive, FirstTS: base, LastTS: base.Add(time.Minute),
		ConfirmedTS: base.Add(time.Minute), NotifiedTS: base.Add(time.Minute),
		NotifyCount: 1, TriggerValue: json.RawMessage(`{"temperature":92}`),
	}

	// 进入 active 才发（07 §6 S3）。
	ev, ok := NewEvent(Decision{Alarm: a, Action: ActionNotified, Notify: true})
	if !ok {
		t.Fatal("进入 active 应产生事件")
	}
	if ev.AlarmID != "alarm-7" || ev.DedupKey != "dk" || ev.Level != "critical" {
		t.Fatalf("得 %+v", ev)
	}
	if string(ev.MetricSnapshot) != `{"temperature":92}` {
		t.Fatalf("应带上取值快照，得 %s", ev.MetricSnapshot)
	}

	// 聚合那条也一样算「进入 active」。
	if _, ok := NewEvent(Decision{Alarm: a, Action: ActionBatched, Notify: true}); !ok {
		t.Fatal("批量告警也是进入 active")
	}

	// 中间推进不能发：多发会把下游工单量放大一个数量级，
	// 而 07 §6 S3 的容量不等式是按有效告警数算的。
	for _, act := range []Action{ActionCreated, ActionObserved, ActionConfirmed, ActionSuppressed, ActionClosed, ActionReopened} {
		if _, ok := NewEvent(Decision{Alarm: a, Action: act}); ok {
			t.Fatalf("%s 不该产生对外事件", act)
		}
	}
	if _, ok := NewEvent(Decision{Action: ActionNotified, Notify: true}); ok {
		t.Fatal("没有告警实体时不该产生事件")
	}
}

func TestEventJSONUsesContractFieldNames(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := &Alarm{
		ID: "alarm-1", DedupKey: "dk", ProjectID: "p1", DeviceID: "d1",
		RuleID: "ar_001", Level: "warn", State: StateActive,
		FirstTS: base, LastTS: base, ConfirmedTS: base, NotifiedTS: base,
		NotifyCount: 1, TriggerValue: json.RawMessage(`{}`),
	}
	ev, _ := NewEvent(Decision{Alarm: a, Action: ActionNotified, Notify: true})
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// 字段名是下游契约（07 §6 S3），逐个盯住。
	for _, k := range []string{
		`"alarm_id"`, `"dedup_key"`, `"device_id"`, `"level"`,
		`"metric_snapshot"`, `"first_ts"`, `"notified_ts"`,
	} {
		if !strings.Contains(string(b), k) {
			t.Fatalf("载荷缺少 %s（下游契约要求），得 %s", k, b)
		}
	}
	// 空快照也必须是个合法 JSON 对象，不能是 null —— 下游要直接塞进 JSONB 列。
	if strings.Contains(string(b), `"metric_snapshot":null`) {
		t.Fatalf("快照不能是 null，得 %s", b)
	}
}

func TestNormalizeValueRejectsGarbage(t *testing.T) {
	if got := string(normalizeValue(nil)); got != "{}" {
		t.Fatalf("空值应写成 {}，得 %s", got)
	}
	if got := string(normalizeValue(json.RawMessage(`{bad`))); got != "{}" {
		t.Fatalf("非法 JSON 应写成 {}（否则 JSONB 列拒收，错误信息离根因很远），得 %s", got)
	}
	if got := string(normalizeValue(json.RawMessage(`{"a":1}`))); got != `{"a":1}` {
		t.Fatalf("合法 JSON 应原样保留，得 %s", got)
	}
}
