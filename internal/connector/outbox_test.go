package connector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// fake 实现
// ---------------------------------------------------------------------------

type fakeStreams struct {
	mu       sync.Mutex
	batches  [][]StreamEntry
	stale    [][]StreamEntry // ClaimStale 依次返回的批次（模拟 PEL 里的未确认消息）
	acked    []string
	readErr  error
	ackErr   error
	claimErr error
	ensured  bool
}

func (f *fakeStreams) EnsureGroup(_ context.Context, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensured = true
	return nil
}

func (f *fakeStreams) ReadGroup(_ context.Context, _, _, _ string, _ int, _ time.Duration) ([]StreamEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return nil, f.readErr
	}
	if len(f.batches) == 0 {
		return nil, nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}

func (f *fakeStreams) ClaimStale(_ context.Context, _, _, _ string, _ time.Duration, _ int) ([]StreamEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if len(f.stale) == 0 {
		return nil, nil
	}
	b := f.stale[0]
	f.stale = f.stale[1:]
	return b, nil
}

func (f *fakeStreams) Ack(_ context.Context, _, _ string, ids ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ackErr != nil {
		return f.ackErr
	}
	f.acked = append(f.acked, ids...)
	return nil
}

func (f *fakeStreams) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acked...)
}

type publishCall struct {
	subject string
	data    []byte
}

type fakePub struct {
	mu    sync.Mutex
	calls []publishCall
	errs  []error // 按调用序返回
}

func (p *fakePub) Publish(_ context.Context, subject string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := len(p.calls)
	p.calls = append(p.calls, publishCall{subject: subject, data: append([]byte(nil), data...)})
	if i < len(p.errs) {
		return p.errs[i]
	}
	return nil
}

func (p *fakePub) published() []publishCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]publishCall(nil), p.calls...)
}

// ---------------------------------------------------------------------------
// 构造辅助
// ---------------------------------------------------------------------------

func outboxEntry(streamID, eventID, model string) StreamEntry {
	return StreamEntry{ID: streamID, Fields: map[string]string{
		"event_id":        eventID,
		"company_id":      "1",
		"aggregate_model": model,
		"aggregate_id":    "42",
		"version":         "3",
		"occurred_at":     "2026-10-01 06:30:00",
		"payload":         `{"state":"done"}`,
	}}
}

func newTestOutbox(t *testing.T, store StreamsStore, pub Publisher) *OutboxConsumer {
	t.Helper()
	c, err := NewOutboxConsumer(OutboxOptions{
		Store:     store,
		Publisher: pub,
		Logger:    testLogger(),
		Block:     time.Millisecond, // 测试不真等
	})
	if err != nil {
		t.Fatalf("构造 Outbox 消费者失败: %v", err)
	}
	return c
}

// ---------------------------------------------------------------------------
// 事件翻译
// ---------------------------------------------------------------------------

// TestNormalizeModel 验证模型名会被归一化成单个 subject token。
func TestNormalizeModel(t *testing.T) {
	cases := map[string]string{
		"mrp.workorder":         "mrp_workorder",
		"maintenance.equipment": "maintenance_equipment",
		"res.partner":           "res_partner",
		" single ":              "single",
	}
	for in, want := range cases {
		if got := NormalizeModel(in); got != want {
			t.Errorf("NormalizeModel(%q) 期望 %q，得到 %q", in, want, got)
		}
	}
}

// TestOdooEvent_Subject 验证主题形态：`iot.odoo.{model}`。
func TestOdooEvent_Subject(t *testing.T) {
	ev := OdooEvent{AggregateModel: "mrp.workorder"}
	if got := ev.Subject(); got != "iot.odoo.mrp_workorder" {
		t.Fatalf("主题应为 iot.odoo.mrp_workorder，得到 %s", got)
	}
}

// TestParseOdooEvent_字段解析 覆盖正常路径与时间/载荷解释。
func TestParseOdooEvent_字段解析(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	ev, err := parseOdooEvent(outboxEntry("1-0", "ev-1", "mrp.workorder"), now)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if ev.EventID != "ev-1" || ev.AggregateID != 42 || ev.CompanyID != 1 || ev.Version != 3 {
		t.Fatalf("字段解析错误: %+v", ev)
	}
	// Odoo 的 naive datetime 按 UTC 解释（其内部即存 UTC）。
	want := time.Date(2026, 10, 1, 6, 30, 0, 0, time.UTC)
	if !ev.OccurredAt.Equal(want) {
		t.Fatalf("occurred_at 应为 %s（UTC），得到 %s", want, ev.OccurredAt)
	}
	if ev.PublishedAt != now {
		t.Fatalf("published_at 应为翻译时刻，得到 %s", ev.PublishedAt)
	}
	if string(ev.Payload) != `{"state":"done"}` {
		t.Fatalf("payload 应为原样 JSON，得到 %s", ev.Payload)
	}
}

// TestParseOdooEvent_拒绝非法记录 验证「永远不可能成功」的记录会被识别出来。
func TestParseOdooEvent_拒绝非法记录(t *testing.T) {
	cases := map[string]StreamEntry{
		"缺 event_id":        {ID: "1-0", Fields: map[string]string{"aggregate_model": "m"}},
		"缺 aggregate_model": {ID: "1-0", Fields: map[string]string{"event_id": "e"}},
		"payload 非 JSON": {ID: "1-0", Fields: map[string]string{
			"event_id": "e", "aggregate_model": "m", "payload": "{不是 JSON",
		}},
		"company_id 非法": {ID: "1-0", Fields: map[string]string{
			"event_id": "e", "aggregate_model": "m", "company_id": "abc",
		}},
	}
	for name, entry := range cases {
		if _, err := parseOdooEvent(entry, time.Now()); err == nil {
			t.Errorf("%s：应报错", name)
		}
	}
}

// TestParseOdooEvent_空载荷容忍 验证不带 payload 的事件仍可投递。
func TestParseOdooEvent_空载荷容忍(t *testing.T) {
	ev, err := parseOdooEvent(StreamEntry{ID: "1-0", Fields: map[string]string{
		"event_id": "e", "aggregate_model": "m",
	}}, time.Now())
	if err != nil {
		t.Fatalf("空载荷不应报错: %v", err)
	}
	if string(ev.Payload) != "{}" {
		t.Fatalf("空载荷应归一为 {}，得到 %s", ev.Payload)
	}
	if !ev.OccurredAt.IsZero() {
		t.Fatal("缺 occurred_at 时不应回退本地时间（会污染服务端时间轴）")
	}
}

// ---------------------------------------------------------------------------
// 至少一次语义
// ---------------------------------------------------------------------------

// TestOutboxConsumer_发布成功才ACK 是 C-1 的主路径断言。
func TestOutboxConsumer_发布成功才ACK(t *testing.T) {
	store := &fakeStreams{batches: [][]StreamEntry{{outboxEntry("1-0", "ev-1", "mrp.workorder")}}}
	pub := new(fakePub)
	c := newTestOutbox(t, store, pub)

	res, err := c.ConsumeOnce(context.Background())
	if err != nil {
		t.Fatalf("消费失败: %v", err)
	}
	if res.Fetched != 1 || res.Published != 1 || res.Failed != 0 || res.Poisoned != 0 {
		t.Fatalf("统计不符: %+v", res)
	}
	if got := store.ackedIDs(); len(got) != 1 || got[0] != "1-0" {
		t.Fatalf("应 ACK 流记录 1-0，得到 %v", got)
	}
	got := pub.published()
	if len(got) != 1 || got[0].subject != "iot.odoo.mrp_workorder" {
		t.Fatalf("应发布到 iot.odoo.mrp_workorder，得到 %v", got)
	}
	if c.Metrics().PublishedTotal.Load() != 1 {
		t.Fatal("应记 1 条已发布")
	}
}

// TestOutboxConsumer_发布失败不ACK 是「至少一次」的核心：
// 一旦 ACK 了没发出去的事件，它就从系统里消失了。
func TestOutboxConsumer_发布失败不ACK(t *testing.T) {
	store := &fakeStreams{batches: [][]StreamEntry{{outboxEntry("1-0", "ev-1", "m")}}}
	pub := &fakePub{errs: []error{errors.New("nats 不可达")}}
	c := newTestOutbox(t, store, pub)

	res, err := c.ConsumeOnce(context.Background())
	if err != nil {
		t.Fatalf("单条发布失败不应让整轮报错（其余消息还要继续处理）: %v", err)
	}
	if res.Failed != 1 || res.Published != 0 {
		t.Fatalf("统计不符: %+v", res)
	}
	if got := store.ackedIDs(); len(got) != 0 {
		t.Fatalf("发布失败的消息**不能** ACK，实际 ACK 了 %v", got)
	}
	if c.Metrics().PublishErrors.Load() != 1 {
		t.Fatal("应记 1 次发布失败")
	}
}

// TestOutboxConsumer_毒消息ACK并计数 验证坏记录不会卡死消费组。
func TestOutboxConsumer_毒消息ACK并计数(t *testing.T) {
	bad := StreamEntry{ID: "9-0", Fields: map[string]string{"aggregate_model": "m"}} // 缺 event_id
	store := &fakeStreams{batches: [][]StreamEntry{{bad}}}
	pub := new(fakePub)
	c := newTestOutbox(t, store, pub)

	res, err := c.ConsumeOnce(context.Background())
	if err != nil {
		t.Fatalf("毒消息不应让整轮报错: %v", err)
	}
	if res.Poisoned != 1 {
		t.Fatalf("应记 1 条毒消息，得到 %+v", res)
	}
	if got := store.ackedIDs(); len(got) != 1 || got[0] != "9-0" {
		t.Fatalf("毒消息必须 ACK（否则永远卡住消费组），得到 %v", got)
	}
	if len(pub.published()) != 0 {
		t.Fatal("毒消息不应发布")
	}
	if c.Metrics().Poisoned.Load() != 1 {
		t.Fatal("应记 1 条毒消息指标（需告警）")
	}
}

// TestOutboxConsumer_混合批次互不影响 验证一条失败不拖累同批其余消息。
func TestOutboxConsumer_混合批次互不影响(t *testing.T) {
	store := &fakeStreams{batches: [][]StreamEntry{{
		outboxEntry("1-0", "ev-ok-1", "mrp.workorder"),
		outboxEntry("2-0", "ev-fail", "mrp.workorder"),
		outboxEntry("3-0", "ev-ok-2", "stock.move"),
	}}}
	// 第 2 次发布失败。
	pub := &fakePub{errs: []error{nil, errors.New("boom"), nil}}
	c := newTestOutbox(t, store, pub)

	res, err := c.ConsumeOnce(context.Background())
	if err != nil {
		t.Fatalf("消费失败: %v", err)
	}
	if res.Published != 2 || res.Failed != 1 {
		t.Fatalf("统计不符: %+v", res)
	}
	if got := store.ackedIDs(); len(got) != 2 || got[0] != "1-0" || got[1] != "3-0" {
		t.Fatalf("应只 ACK 成功发布的两条，得到 %v", got)
	}
}

// TestOutboxConsumer_读取失败报错 验证 Redis 侧故障会被上报（供 Run 退避）。
func TestOutboxConsumer_读取失败报错(t *testing.T) {
	store := &fakeStreams{readErr: errors.New("redis 不可达")}
	c := newTestOutbox(t, store, new(fakePub))

	if _, err := c.ConsumeOnce(context.Background()); err == nil {
		t.Fatal("读取失败应报错")
	}
	if c.Metrics().IngestErrors.Load() != 1 {
		t.Fatal("应记 1 次摄取错误")
	}
}

// TestOutboxConsumer_ACK失败报错 验证「已发布但 ACK 失败」会被上报。
func TestOutboxConsumer_ACK失败报错(t *testing.T) {
	store := &fakeStreams{
		batches: [][]StreamEntry{{outboxEntry("1-0", "ev-1", "m")}},
		ackErr:  errors.New("redis 抖动"),
	}
	c := newTestOutbox(t, store, new(fakePub))

	res, err := c.ConsumeOnce(context.Background())
	if err == nil {
		t.Fatal("ACK 失败应报错")
	}
	// 事件其实已经发出去了：重启后会重投，靠下游 event_id 去重兜底。
	if res.Published != 1 {
		t.Fatalf("统计应反映已发布: %+v", res)
	}
}

// TestOutboxConsumer_PEL重投接管 是「至少一次」的另一半：
// 上一轮发布失败、没 ACK 的消息必须被下一轮捞回来重投 ——
// 否则它永远沉在 PEL 里，语义就从「至少一次」退化成「零次」。
func TestOutboxConsumer_PEL重投接管(t *testing.T) {
	store := &fakeStreams{stale: [][]StreamEntry{{outboxEntry("1-0", "ev-1", "mrp.workorder")}}}
	pub := new(fakePub)
	c := newTestOutbox(t, store, pub)

	res, err := c.ConsumeOnce(context.Background())
	if err != nil {
		t.Fatalf("消费失败: %v", err)
	}
	if res.Reclaimed != 1 || res.Published != 1 {
		t.Fatalf("应接管并发布 1 条: %+v", res)
	}
	if got := store.ackedIDs(); len(got) != 1 || got[0] != "1-0" {
		t.Fatalf("重投成功后应 ACK，得到 %v", got)
	}
	if c.Metrics().Reclaimed.Load() != 1 {
		t.Fatal("应记 1 条重投接管")
	}
}

// TestOutboxConsumer_重投失败仍不ACK 验证重投路径同样遵守「发布成功才 ACK」。
func TestOutboxConsumer_重投失败仍不ACK(t *testing.T) {
	store := &fakeStreams{stale: [][]StreamEntry{{outboxEntry("1-0", "ev-1", "m")}}}
	pub := &fakePub{errs: []error{errors.New("nats 不可达")}}
	c := newTestOutbox(t, store, pub)

	res, err := c.ConsumeOnce(context.Background())
	if err != nil {
		t.Fatalf("消费失败: %v", err)
	}
	if res.Reclaimed != 1 || res.Failed != 1 || res.Published != 0 {
		t.Fatalf("统计不符: %+v", res)
	}
	if len(store.ackedIDs()) != 0 {
		t.Fatal("重投仍失败时不能 ACK（要留给下一轮）")
	}
}

// TestOutboxConsumer_重投与新消息同轮处理 验证一批里两者互不阻塞。
func TestOutboxConsumer_重投与新消息同轮处理(t *testing.T) {
	store := &fakeStreams{
		stale:   [][]StreamEntry{{outboxEntry("1-0", "ev-old", "mrp.workorder")}},
		batches: [][]StreamEntry{{outboxEntry("2-0", "ev-new", "mrp.workorder")}},
	}
	pub := new(fakePub)
	c := newTestOutbox(t, store, pub)

	res, err := c.ConsumeOnce(context.Background())
	if err != nil {
		t.Fatalf("消费失败: %v", err)
	}
	if res.Reclaimed != 1 || res.Fetched != 2 || res.Published != 2 {
		t.Fatalf("统计不符: %+v", res)
	}
}

// TestOutboxConsumer_接管失败报错 验证 Redis 侧故障会被上报（供 Run 退避）。
func TestOutboxConsumer_接管失败报错(t *testing.T) {
	store := &fakeStreams{claimErr: errors.New("XAUTOCLAIM 失败")}
	c := newTestOutbox(t, store, new(fakePub))

	if _, err := c.ConsumeOnce(context.Background()); err == nil {
		t.Fatal("接管失败应报错")
	}
}

// TestNewOutboxConsumer_拒绝缺依赖 验证构造期就挡住误用。
func TestNewOutboxConsumer_拒绝缺依赖(t *testing.T) {
	if _, err := NewOutboxConsumer(OutboxOptions{Publisher: new(fakePub)}); err == nil {
		t.Fatal("缺 Store 应报错")
	}
	if _, err := NewOutboxConsumer(OutboxOptions{Store: new(fakeStreams)}); err == nil {
		t.Fatal("缺 Publisher 应报错")
	}
}
