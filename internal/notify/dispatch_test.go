package notify

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/dlq"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeChannel 按调用序号返回预设错误，并记录调用次数。
type fakeChannel struct {
	name string
	mu   sync.Mutex
	// errs 是各次调用的返回；超出长度后一直用最后一个。
	errs  []error
	calls int
}

func (c *fakeChannel) Name() string { return c.name }

func (c *fakeChannel) Send(context.Context, Message, []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if len(c.errs) == 0 {
		return nil
	}
	i := c.calls - 1
	if i >= len(c.errs) {
		i = len(c.errs) - 1
	}
	return c.errs[i]
}

func (c *fakeChannel) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

type dlqRecorder struct {
	mu      sync.Mutex
	entries []dlq.Entry
}

func (r *dlqRecorder) Put(_ context.Context, e dlq.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, e)
	return nil
}

func (r *dlqRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

var fixedNow = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// noSleep 把重试阶梯压成瞬时，同时记录被要求等了多久。
func noSleep(slept *[]time.Duration) func(context.Context, time.Duration) error {
	return func(_ context.Context, d time.Duration) error {
		*slept = append(*slept, d)
		return nil
	}
}

func twoChannelPolicy() Policy {
	return Policy{
		Channels: []string{ChannelWebhook, ChannelEmail},
		Recipients: map[string][]string{
			ChannelWebhook: {"https://hooks.example.com/x"},
			ChannelEmail:   {"oncall@example.com"},
		},
	}
}

func newTestDispatcher(t *testing.T, chans map[string]Channel, rec *dlqRecorder,
	health ChannelHealth, slept *[]time.Duration) *Dispatcher {
	t.Helper()
	d, err := NewDispatcher(Options{
		Channels: chans,
		Health:   health,
		DLQ:      rec,
		Logger:   quietLogger(),
		Now:      fixedNow,
		Sleep:    noSleep(slept),
	})
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	return d
}

func TestDispatchRetriesThenDegrades(t *testing.T) {
	a := &fakeChannel{name: ChannelWebhook, errs: []error{errors.New("网络抖动")}}
	b := &fakeChannel{name: ChannelEmail}
	rec := &dlqRecorder{}
	var slept []time.Duration
	d := newTestDispatcher(t,
		map[string]Channel{ChannelWebhook: a, ChannelEmail: b}, rec,
		ChannelHealth{Threshold: 1, MinSamples: 100}, &slept)

	results, err := d.Dispatch(context.Background(), testMessage(), twoChannelPolicy())
	if err != nil {
		t.Fatalf("降级到邮件后应成功: %v", err)
	}

	// 1 次 + 3 次重试（04 §3.3）。
	if a.callCount() != 4 {
		t.Fatalf("主通道应尝试 4 次，得 %d", a.callCount())
	}
	want := []time.Duration{500 * time.Millisecond, 2 * time.Second, 8 * time.Second}
	if len(slept) != len(want) {
		t.Fatalf("应有 3 次退避，得 %v", slept)
	}
	for i := range want {
		if slept[i] != want[i] {
			t.Fatalf("重试阶梯应为 500ms/2s/8s（04 §3.3），得 %v", slept)
		}
	}

	if len(results) != 2 {
		t.Fatalf("应记录两次尝试，得 %d", len(results))
	}
	if !results[1].OK() {
		t.Fatalf("第二次应成功，得 %+v", results[1])
	}
	// 降级来源必须标出来：不标的话「主通道一直失败」永远不会有人发现。
	if results[1].DegradedFrom != ChannelWebhook {
		t.Fatalf("应标出降级来源，得 %q", results[1].DegradedFrom)
	}
	if rec.count() != 1 {
		t.Fatalf("失败的通道应进 DLQ，得 %d 条", rec.count())
	}
	if e := rec.entries[0]; e.Attempts != 4 || e.TraceID != "trace-1" || e.Service != "svc-notify" {
		t.Fatalf("DLQ 条目应含重试次数与 trace_id（04 §3.3），得 %+v", e)
	}
}

func TestDispatchPermanentFailureSkipsRetries(t *testing.T) {
	a := &fakeChannel{name: ChannelWebhook, errs: []error{Permanent("token 失效")}}
	b := &fakeChannel{name: ChannelEmail}
	rec := &dlqRecorder{}
	var slept []time.Duration
	d := newTestDispatcher(t,
		map[string]Channel{ChannelWebhook: a, ChannelEmail: b}, rec,
		ChannelHealth{Threshold: 1, MinSamples: 100}, &slept)

	if _, err := d.Dispatch(context.Background(), testMessage(), twoChannelPolicy()); err != nil {
		t.Fatalf("降级后应成功: %v", err)
	}
	// 永久失败重试三次只是浪费三次超时，还把同一类噪音灌满 DLQ。
	if a.callCount() != 1 {
		t.Fatalf("永久失败不该重试，得 %d 次", a.callCount())
	}
	if len(slept) != 0 {
		t.Fatalf("不该有退避等待，得 %v", slept)
	}
}

func TestDispatchPartialStopsWithoutRetryOrDegrade(t *testing.T) {
	a := &fakeChannel{name: ChannelWebhook, errs: []error{
		&PartialError{Channel: ChannelWebhook, Rejected: []string{"bad"}, Accepted: 1},
	}}
	b := &fakeChannel{name: ChannelEmail}
	rec := &dlqRecorder{}
	var slept []time.Duration
	d := newTestDispatcher(t,
		map[string]Channel{ChannelWebhook: a, ChannelEmail: b}, rec,
		ChannelHealth{Threshold: 1, MinSamples: 100}, &slept)

	results, err := d.Dispatch(context.Background(), testMessage(), twoChannelPolicy())
	if err != nil {
		t.Fatalf("部分投递不该让整次分发失败: %v", err)
	}
	if a.callCount() != 1 {
		t.Fatalf("部分投递不该重试（会给已收到的人重发），得 %d 次", a.callCount())
	}
	if b.callCount() != 0 {
		t.Fatal("部分投递不该降级（通道其实好好的）")
	}
	if rec.count() != 0 {
		t.Fatal("部分投递不该进 DLQ")
	}
	if len(results) != 1 || !IsPartial(results[0].Err) {
		t.Fatalf("应如实记录部分投递，得 %+v", results)
	}
}

func TestDispatchSkipsCongestedChannel(t *testing.T) {
	a := &fakeChannel{name: ChannelWebhook, errs: []error{errors.New("网关整体挂了")}}
	b := &fakeChannel{name: ChannelEmail}
	rec := &dlqRecorder{}
	var slept []time.Duration
	d := newTestDispatcher(t,
		map[string]Channel{ChannelWebhook: a, ChannelEmail: b}, rec,
		ChannelHealth{Threshold: 0.3, Window: time.Minute, MinSamples: 3}, &slept)

	ctx := context.Background()
	policy := twoChannelPolicy()
	for i := 0; i < 3; i++ {
		if _, err := d.Dispatch(ctx, testMessage(), policy); err != nil {
			t.Fatalf("第 %d 轮降级到邮件后应成功: %v", i+1, err)
		}
	}

	// 攒够样本后，拥塞的通道应被**直接跳过** —— 这才是「自动切换」，
	// 而不是「每条告警都先把主通道试满 4 次」。
	before := a.callCount()
	results, err := d.Dispatch(ctx, testMessage(), policy)
	if err != nil {
		t.Fatalf("降级后应成功: %v", err)
	}
	if a.callCount() != before {
		t.Fatalf("拥塞的通道不该再被调用，得 %d → %d", before, a.callCount())
	}
	if !strings.Contains(results[0].Err.Error(), "降级") {
		t.Fatalf("应记下「因拥塞而降级」，得 %v", results[0].Err)
	}
	if b.callCount() != 4 {
		t.Fatalf("邮件通道应收到全部 4 条，得 %d", b.callCount())
	}
}

func TestDispatchAllChannelsFail(t *testing.T) {
	a := &fakeChannel{name: ChannelWebhook, errs: []error{Permanent("token 失效")}}
	b := &fakeChannel{name: ChannelEmail, errs: []error{Permanent("SMTP 认证失败")}}
	rec := &dlqRecorder{}
	var slept []time.Duration
	d := newTestDispatcher(t,
		map[string]Channel{ChannelWebhook: a, ChannelEmail: b}, rec,
		ChannelHealth{Threshold: 1, MinSamples: 100}, &slept)

	if _, err := d.Dispatch(context.Background(), testMessage(), twoChannelPolicy()); err == nil {
		t.Fatal("所有通道都失败时必须返回错误，不能装作成功")
	}
	if rec.count() != 2 {
		t.Fatalf("两个失败通道都应留下 DLQ，得 %d", rec.count())
	}
}

func TestReplayFailureDoesNotWriteDLQ(t *testing.T) {
	a := &fakeChannel{name: ChannelWebhook, errs: []error{Permanent("webhook unavailable")}}
	rec := &dlqRecorder{}
	var slept []time.Duration
	d := newTestDispatcher(t,
		map[string]Channel{ChannelWebhook: a}, rec,
		ChannelHealth{Threshold: 1, MinSamples: 100}, &slept)

	if _, err := d.Replay(context.Background(), testMessage(), Policy{
		Channels:   []string{ChannelWebhook},
		Recipients: map[string][]string{ChannelWebhook: {"https://example.test"}},
	}); err == nil {
		t.Fatal("重放失败时必须返回错误")
	}
	if rec.count() != 0 {
		t.Fatalf("重放失败不应再次写入 DLQ，得 %d 条", rec.count())
	}
}

func TestDispatchRespectsPolicyOrder(t *testing.T) {
	a := &fakeChannel{name: ChannelWebhook}
	b := &fakeChannel{name: ChannelEmail}
	rec := &dlqRecorder{}
	var slept []time.Duration
	d := newTestDispatcher(t,
		map[string]Channel{ChannelWebhook: a, ChannelEmail: b}, rec,
		ChannelHealth{Threshold: 1, MinSamples: 100}, &slept)

	// 策略把邮件排在前面：应按策略走，而不是按注册顺序。
	policy := twoChannelPolicy()
	policy.Channels = []string{ChannelEmail, ChannelWebhook}
	if _, err := d.Dispatch(context.Background(), testMessage(), policy); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if b.callCount() != 1 || a.callCount() != 0 {
		t.Fatalf("应按策略顺序（邮件优先），得 email=%d webhook=%d", b.callCount(), a.callCount())
	}
}

func TestDispatchRejectsInvalidPolicy(t *testing.T) {
	a := &fakeChannel{name: ChannelWebhook}
	rec := &dlqRecorder{}
	var slept []time.Duration
	d := newTestDispatcher(t, map[string]Channel{ChannelWebhook: a}, rec,
		ChannelHealth{}, &slept)

	// 通道没有收件人 = 配置错误，重试无意义，必须当场拒。
	if _, err := d.Dispatch(context.Background(), testMessage(),
		Policy{Channels: []string{ChannelWebhook}}); err == nil {
		t.Fatal("没有收件人的策略应被拒")
	}
	if a.callCount() != 0 {
		t.Fatal("策略不合法时不该调用通道")
	}
}
