package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/alarm"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakePublisher 记录发出去的 subject 与载荷，并可注入失败。
type fakePublisher struct {
	mu       sync.Mutex
	subjects []string
	payloads [][]byte
	fail     bool
}

func (p *fakePublisher) Publish(_ context.Context, subject string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("注入的发布失败")
	}
	p.subjects = append(p.subjects, subject)
	p.payloads = append(p.payloads, data)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.subjects)
}

// memPublishStore 是 publishStore 的内存替身。
type memPublishStore struct {
	mu        sync.Mutex
	pending   []*alarm.Alarm
	published map[string]time.Time
}

func (s *memPublishStore) MarkPublished(_ context.Context, key string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published[key] = at
	kept := s.pending[:0]
	for _, a := range s.pending {
		if a.DedupKey != key {
			kept = append(kept, a)
		}
	}
	s.pending = kept
	return nil
}

func (s *memPublishStore) UnpublishedActive(context.Context) ([]*alarm.Alarm, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*alarm.Alarm(nil), s.pending...), nil
}

func (s *memPublishStore) Active(_ context.Context, states ...alarm.State) ([]*alarm.Alarm, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	want := map[alarm.State]bool{}
	for _, state := range states {
		want[state] = true
	}
	var out []*alarm.Alarm
	for _, item := range s.pending {
		if len(want) > 0 && !want[item.State] {
			continue
		}
		out = append(out, item.Clone())
	}
	return out, nil
}

func (s *memPublishStore) ClaimEscalation(_ context.Context, key string, stage int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.pending {
		if item.DedupKey == key && item.EscalationStage < stage {
			item.EscalationStage = stage
			return true, nil
		}
	}
	return false, nil
}

func newFakeAgent() (*agent, *fakePublisher, *memPublishStore) {
	pub := &fakePublisher{}
	store := &memPublishStore{published: map[string]time.Time{}}
	return &agent{
		store:   store,
		pub:     pub,
		counts:  new(counters),
		metrics: new(alarm.Metrics),
		logger:  quiet(),
		now:     func() time.Time { return time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC) },
	}, pub, store
}

func TestEscalateUnconfirmedClaimsStageAndPublishes(t *testing.T) {
	agent, pub, store := newFakeAgent()
	current := sampleAlarm("alarm-1", "dk1")
	current.NotifiedTS = agent.now().Add(-31 * time.Minute)
	store.pending = []*alarm.Alarm{current}
	agent.notifyEscalationAfter = 30 * time.Minute
	agent.p1EscalationAfter = 2 * time.Hour

	if err := agent.escalateUnconfirmed(context.Background()); err != nil {
		t.Fatalf("升级扫描失败: %v", err)
	}
	if pub.count() != 1 || pub.subjects[0] != "iot.alarm.escalation.p1" {
		t.Fatalf("首次升级应发布 escalation subject，得到 subjects=%v", pub.subjects)
	}
	if err := agent.escalateUnconfirmed(context.Background()); err != nil {
		t.Fatalf("重复升级扫描失败: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("同一升级阶段不应重复发布，得到 %d 条", pub.count())
	}
}

func sampleAlarm(id, dedup string) *alarm.Alarm {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &alarm.Alarm{
		ID: id, DedupKey: dedup, ProjectID: "p1", DeviceID: "d1",
		DeviceTypeID: 55, RuleID: "ar_001", RuleName: "温度过高", Level: "critical",
		State:   alarm.StateActive,
		FirstTS: base, LastTS: base.Add(time.Minute),
		ConfirmedTS: base.Add(time.Minute), NotifiedTS: base.Add(time.Minute),
		NotifyCount: 1,
	}
}

func TestPublishDecisionMarksPublishedOnlyOnSuccess(t *testing.T) {
	a, pub, store := newFakeAgent()
	ctx := context.Background()

	al := sampleAlarm("alarm-1", "dk1")
	d := alarm.Decision{Alarm: al, Action: alarm.ActionNotified, Notify: true}
	if !a.publishDecision(ctx, d) {
		t.Fatal("应发布成功")
	}
	if pub.count() != 1 || pub.subjects[0] != "iot.alarm.p1" {
		t.Fatalf("subject 得 %v", pub.subjects)
	}
	if _, ok := store.published["dk1"]; !ok {
		t.Fatal("发布成功才应写发布标记（补发扫描靠它判断）")
	}
	if a.counts.Published.Load() != 1 {
		t.Fatalf("published 得 %d", a.counts.Published.Load())
	}

	// ⚠️ 发布失败时**绝不能**写标记：写了的话补发扫描永远不会重试这条，
	// 一张工单就静默消失了 —— 这正是加 published_at 要防的事。
	pub.fail = true
	al2 := sampleAlarm("alarm-2", "dk2")
	if a.publishDecision(ctx, alarm.Decision{Alarm: al2, Action: alarm.ActionNotified, Notify: true}) {
		t.Fatal("注入失败时不该报告成功")
	}
	if _, ok := store.published["dk2"]; ok {
		t.Fatal("发布失败却写了标记 —— 该事件将永远不会被补发")
	}
	if a.counts.PublishErrors.Load() != 1 {
		t.Fatalf("publish_errors 得 %d", a.counts.PublishErrors.Load())
	}
}

func TestPublishDecisionSkipsNonNotify(t *testing.T) {
	a, pub, _ := newFakeAgent()
	ctx := context.Background()

	// 中间推进（观察/抑制/关闭）不该对外发事件 —— 07 §6 S3 的容量不等式
	// 是按「有效告警数」算的，多发会把工单量放大一个数量级。
	for _, act := range []alarm.Action{
		alarm.ActionCreated, alarm.ActionObserved, alarm.ActionConfirmed,
		alarm.ActionSuppressed, alarm.ActionClosed, alarm.ActionReopened,
	} {
		if a.publishDecision(ctx, alarm.Decision{Alarm: sampleAlarm("x", "dk"), Action: act}) {
			t.Fatalf("%s 不该发事件", act)
		}
	}
	if pub.count() != 0 {
		t.Fatalf("不该有任何发布，得 %v", pub.subjects)
	}
}

func TestRepublishSendsPendingThenClears(t *testing.T) {
	a, pub, store := newFakeAgent()
	ctx := context.Background()

	store.pending = []*alarm.Alarm{sampleAlarm("alarm-1", "dk1"), sampleAlarm("alarm-2", "dk2")}
	if err := a.republish(ctx); err != nil {
		t.Fatalf("republish: %v", err)
	}
	if pub.count() != 2 {
		t.Fatalf("应补发两条，得 %d", pub.count())
	}
	pending, _ := store.UnpublishedActive(ctx)
	if len(pending) != 0 {
		t.Fatalf("补发成功后不该还留在待发列表，得 %d", len(pending))
	}
	if a.counts.Republished.Load() != 2 {
		t.Fatalf("republished 得 %d", a.counts.Republished.Load())
	}

	// 再补发一轮：不应重复发（否则每 5s 都在给下游灌重复事件）。
	if err := a.republish(ctx); err != nil {
		t.Fatalf("republish: %v", err)
	}
	if pub.count() != 2 {
		t.Fatalf("空列表不该再发，得 %d", pub.count())
	}
}

func TestRepublishKeepsFailedOnesPending(t *testing.T) {
	a, pub, store := newFakeAgent()
	ctx := context.Background()

	store.pending = []*alarm.Alarm{sampleAlarm("alarm-1", "dk1")}
	pub.fail = true
	if err := a.republish(ctx); err != nil {
		t.Fatalf("republish: %v", err)
	}
	// 失败的要留在待发列表里，下一轮继续试 —— 这才叫「至少一次」。
	pending, _ := store.UnpublishedActive(ctx)
	if len(pending) != 1 {
		t.Fatalf("失败的事件必须留在待发列表，得 %d", len(pending))
	}
	if a.counts.Republished.Load() != 0 {
		t.Fatal("失败不该计入已补发")
	}

	// 恢复后应能补上。
	pub.fail = false
	if err := a.republish(ctx); err != nil {
		t.Fatalf("republish: %v", err)
	}
	pending, _ = store.UnpublishedActive(ctx)
	if len(pending) != 0 {
		t.Fatalf("恢复后应补发成功，得 %d 条仍待发", len(pending))
	}
}

func TestDispatchDoesNotPublishStorm(t *testing.T) {
	a, pub, _ := newFakeAgent()
	ctx := context.Background()

	// 风暴熔断的语义是「一条聚合通知 + P1 升级」（04 §2.2），
	// **不是**给这一条告警建工单 —— 把它当普通告警发出去，
	// 会在风暴里再制造一批工单。
	a.dispatch(ctx, []alarm.Decision{{
		Alarm:  sampleAlarm("alarm-9", "dk9"),
		Action: alarm.ActionCreated, Notify: false, Escalate: true,
		Reason: "告警风暴：单租户窗口内新增告警超阈值",
	}})
	if pub.count() != 1 || pub.subjects[0] != "iot.alarm.escalation.p1" {
		t.Fatalf("风暴决策应只发布 P1 升级事件，得 %v", pub.subjects)
	}

	// 正常进入 active 的仍要发。
	a.dispatch(ctx, []alarm.Decision{{
		Alarm: sampleAlarm("alarm-1", "dk1"), Action: alarm.ActionNotified, Notify: true,
	}})
	if pub.count() != 2 {
		t.Fatalf("正常告警应发布，得 %d", pub.count())
	}
}

func TestWildcardOfGroupsRuleSubjects(t *testing.T) {
	cases := map[string]string{
		"iot.rule.alarm": "iot.rule.>",
		"iot.rule.scene": "iot.rule.>",
		// 不足 3 段时原样返回：`iot.rule.>` 匹配不到 `iot.rule` 本身，
		// 流里不含实际 subject → 发布静默失败。
		"iot.rule": "iot.rule",
		"alarm":    "alarm",
	}
	for in, want := range cases {
		if got := wildcardOf(in); got != want {
			t.Fatalf("wildcardOf(%q)=%q，期望 %q", in, got, want)
		}
	}
}
