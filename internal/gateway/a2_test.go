package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/SNCIC/odoo20iot/internal/envelope"
)

// ---------- 测试替身 ----------

type publishCall struct {
	subject string
	payload []byte
}

// fakePublisher 是可控的 Publisher：能阻塞、能报错、能记录调用，
// 用来在**确定性时序**下检验 A2 的两种分支，而不依赖真实 NATS。
type fakePublisher struct {
	mu     sync.Mutex
	calls  []publishCall
	block  chan struct{}
	err    error
	closed bool
}

type discardLifecyclePublisher struct{}

func (discardLifecyclePublisher) Publish(context.Context, string, []byte) error { return nil }

var _ Publisher = (*fakePublisher)(nil)

func (f *fakePublisher) Publish(ctx context.Context, subject string, payload []byte) error {
	f.mu.Lock()
	f.calls = append(f.calls, publishCall{subject: subject, payload: append([]byte(nil), payload...)})
	block, err := f.block, f.err
	f.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			// 与 NATSPublisher 的归一化一致：超时统一报 ErrPubackTimeout。
			return fmt.Errorf("%w: %w", ErrPubackTimeout, ctx.Err())
		}
	}
	return err
}

func (f *fakePublisher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakePublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakePublisher) lastCall() (publishCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return publishCall{}, false
	}
	return f.calls[len(f.calls)-1], true
}

func (f *fakePublisher) setBlock(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block = ch
}

func (f *fakePublisher) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// testLogger 默认丢弃日志（测试输出只保留断言结果）；
// 置 IOT_TEST_VERBOSE_LOG=1 可把网关内部日志打到 stderr，便于定位失败原因。
func testLogger() *slog.Logger {
	if os.Getenv("IOT_TEST_VERBOSE_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestBroker 起一个监听 127.0.0.1:0 的 broker，返回其地址。
func newTestBroker(t *testing.T, pub Publisher, timeout time.Duration) (*Broker, *Metrics) {
	t.Helper()
	return newTestBrokerWithRouter(t, pub, timeout, ContractRouter{Project: "spike"})
}

func newTestBrokerWithRouter(t *testing.T, pub Publisher, timeout time.Duration, router SubjectRouter) (*Broker, *Metrics) {
	t.Helper()

	metrics := new(Metrics)
	b, err := New(context.Background(), Options{
		MQTTAddr:                 "127.0.0.1:0",
		Publisher:                pub,
		DeviceLifecyclePublisher: discardLifecyclePublisher{},
		Router:                   router,
		PubackTimeout:            timeout,
		Metrics:                  metrics,
		Log:                      testLogger(),
		// 这组用例验证的是 QoS1 确认时序，与认证无关；
		// 放行匿名必须显式声明（见 Options.AllowAnonymous）。
		AllowAnonymous:     true,
		EnableInlineClient: true,
	})
	if err != nil {
		t.Fatalf("构造 broker 失败: %v", err)
	}

	b.Serve()
	t.Cleanup(func() { _ = b.Close() })
	return b, metrics
}

// ---------- A2 验证 ----------

// TestA2_PubackOnlyAfterPersist 是 A2 的主判据：
// **总线未确认之前，网关不得回 PUBACK。**
func TestA2_PubackOnlyAfterPersist(t *testing.T) {
	pub := &fakePublisher{block: make(chan struct{})}
	broker, metrics := newTestBroker(t, pub, 5*time.Second)

	dev := dialTestDevice(t, broker.Addr(), "dev-1")
	dev.connect("dev-1")

	id := dev.publish("v1/devices/dev-1/telemetry", []byte(`{"ts":"2026-10-01T00:00:00.000Z"}`), false)

	// 总线尚未确认 —— 设备必须收不到 PUBACK。
	dev.assertNoPuback(300 * time.Millisecond)

	if got := metrics.PublishTotal.Load(); got != 1 {
		t.Fatalf("期望 1 次总线投递，得到 %d", got)
	}
	if got := metrics.PersistedTotal.Load(); got != 0 {
		t.Fatalf("总线未确认时 PersistedTotal 应为 0，得到 %d", got)
	}

	// 放行总线确认。
	close(pub.block)

	if err := dev.awaitPuback(id, 3*time.Second); err != nil {
		t.Fatalf("总线确认后仍未收到 PUBACK: %v", err)
	}
	if got := metrics.PersistedTotal.Load(); got != 1 {
		t.Fatalf("期望 PersistedTotal=1，得到 %d", got)
	}
	if got := metrics.TimeoutTotal.Load(); got != 0 {
		t.Fatalf("不应有超时，得到 %d", got)
	}

	// 落点 subject 必须符合契约映射（分片由 device key 决定，恒定）。
	call, ok := pub.lastCall()
	if !ok {
		t.Fatal("总线未收到任何投递")
	}
	if want := fmt.Sprintf("iot.telemetry.spike.shard.%d", hashShard("dev-1", DefaultShards)); call.subject != want {
		t.Fatalf("subject 映射错误：期望 %s，得到 %s", want, call.subject)
	}
}

// TestA2_TimeoutSuppressesPuback 验证 §4.4 的兜底路径：
// 等待 PublishAck 超时 → **不回 PUBACK**，且计入 gw_puback_timeout_total。
func TestA2_TimeoutSuppressesPuback(t *testing.T) {
	pub := &fakePublisher{block: make(chan struct{})} // 永不放行
	broker, metrics := newTestBroker(t, pub, 200*time.Millisecond)

	dev := dialTestDevice(t, broker.Addr(), "dev-2")
	dev.connect("dev-2")

	dev.publish("v1/devices/dev-2/telemetry", []byte(`{"n":1}`), false)

	// 超时窗口（200ms）之后仍不得有 PUBACK。
	dev.assertNoPuback(700 * time.Millisecond)

	if got := metrics.TimeoutTotal.Load(); got != 1 {
		t.Fatalf("期望 TimeoutTotal=1，得到 %d", got)
	}
	if got := metrics.PersistedTotal.Load(); got != 0 {
		t.Fatalf("超时不是持久化，PersistedTotal 应为 0，得到 %d", got)
	}

	// 设备重传（DUP=1）：这次总线放行，应收到 PUBACK。
	pub.setBlock(nil)
	id := dev.publish("v1/devices/dev-2/telemetry", []byte(`{"n":1}`), true)
	if err := dev.awaitPuback(id, 3*time.Second); err != nil {
		t.Fatalf("重传后仍未收到 PUBACK: %v", err)
	}

	// at-least-once：总线侧看到 2 次投递，去重责任在下游（msg_id）。
	if got := pub.callCount(); got != 2 {
		t.Fatalf("期望总线收到 2 次投递（含重传），得到 %d", got)
	}
}

// TestA2_GatewayCrashBeforePuback_MessageNotLost 注入「网关崩溃」：
// 设备 PUBLISH 后网关在回 PUBACK 前死亡 —— 消息绝不能因为「设备以为已送达」而丢失。
//
// 崩溃的模拟方式：关闭整个 broker（等价于进程被杀）。设备观察到连接断开后
// 重连到新网关实例并带 DUP=1 重传，消息最终被总线接收。
func TestA2_GatewayCrashBeforePuback_MessageNotLost(t *testing.T) {
	pub := &fakePublisher{block: make(chan struct{})}

	// 超时给 1s：测试关心的是「崩溃前没回 PUBACK」，不必等满生产默认的 5s。
	broker1, _ := newTestBroker(t, pub, time.Second)
	dev := dialTestDevice(t, broker1.Addr(), "dev-3")
	dev.connect("dev-3")

	payload := []byte(`{"ts":"2026-10-01T00:00:00.000Z","seq":7}`)
	dev.publish("v1/devices/dev-3/events", payload, false)

	// 确认网关此刻尚未回 PUBACK（阻塞在等待总线确认）。
	dev.assertNoPuback(200 * time.Millisecond)

	// —— 注入崩溃 ——
	if err := broker1.Close(); err != nil {
		t.Fatalf("关闭 broker 失败: %v", err)
	}

	// 网关重启：新实例（新端口，等价于 LB 换节点），总线确认恢复可用。
	pub.setBlock(nil)
	broker2, metrics2 := newTestBroker(t, pub, 5*time.Second)

	dev2 := dialTestDevice(t, broker2.Addr(), "dev-3")
	dev2.connect("dev-3")

	// 设备侧重传（CleanSession=true 下由应用层重发；语义上就是 QoS1 的 DUP 重传）。
	id := dev2.publish("v1/devices/dev-3/events", payload, true)
	if err := dev2.awaitPuback(id, 3*time.Second); err != nil {
		t.Fatalf("重传后仍未收到 PUBACK: %v", err)
	}

	if metrics2.PersistedTotal.Load() != 1 {
		t.Fatalf("重启后的网关实例应确认 1 条，得到 %d", metrics2.PersistedTotal.Load())
	}

	// 消息没有丢：总线侧拿到了（且是至少一次）。
	call, ok := pub.lastCall()
	if !ok {
		t.Fatal("总线最终未收到消息 —— 数据丢失")
	}
	// 总线载荷是**统一信封**（03 §2.4）：归属元数据 + 原始报文。
	// 若只发裸 payload，device_key 会随 subject 一并丢失，下游消费者无从落库。
	env, err := envelope.Decode(call.payload)
	if err != nil {
		t.Fatalf("总线载荷不是合法信封: %v", err)
	}
	if env.DeviceKey != "dev-3" || env.Stream != "events" {
		t.Fatalf("信封归属元数据错误: device_key=%s stream=%s", env.DeviceKey, env.Stream)
	}
	if !bytes.Equal(env.Payload, payload) {
		t.Fatalf("信封内原始报文被改写：期望 %s，得到 %s", payload, env.Payload)
	}
	if call.subject != fmt.Sprintf("iot.events.spike.shard.%d", hashShard("dev-3", DefaultShards)) {
		t.Fatalf("subject 映射错误: %s", call.subject)
	}
}

// TestA2_Control_NativeBrokerAcksImmediately 是对照实验：不装载 A2 hook 的原生
// broker 会**立即**回 PUBACK。
//
// 它的作用是证明上面的「未收到 PUBACK」断言不是假阳性 —— 同样的读路径确实能收到
// PUBACK，只是被 A2 hook 刻意推迟/抑制了。同时它也量化了 A2 的代价：
// 网关在收包路径上多了一次同步等待。
func TestA2_Control_NativeBrokerAcksImmediately(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}

	server := mqtt.New(&mqtt.Options{Logger: testLogger()})
	if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("装载认证 hook 失败: %v", err)
	}
	if err := server.AddListener(listeners.NewNet("ctl", ln)); err != nil {
		t.Fatalf("装载监听器失败: %v", err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve() }()
	t.Cleanup(func() { _ = server.Close(); <-serveErr })

	dev := dialTestDevice(t, ln.Addr().String(), "ctl-dev")
	dev.connect("ctl-dev")

	id := dev.publish("v1/devices/ctl-dev/telemetry", []byte(`{}`), false)
	if err := dev.awaitPuback(id, time.Second); err != nil {
		t.Fatalf("原生 broker 应立即回 PUBACK（该失败意味着读路径本身有问题）: %v", err)
	}
}

// TestA2_UnroutableTopicRejected 确认契约外的 topic 被明确拒绝，而不是被静默放行。
func TestA2_UnroutableTopicRejected(t *testing.T) {
	pub := &fakePublisher{}
	broker, metrics := newTestBroker(t, pub, time.Second)

	dev := dialTestDevice(t, broker.Addr(), "dev-4")
	dev.connect("dev-4")

	dev.publish("garbage/topic", []byte(`{}`), false)
	dev.assertNoPuback(300 * time.Millisecond)

	if got := metrics.UnroutableTotal.Load(); got != 1 {
		t.Fatalf("期望 UnroutableTotal=1，得到 %d", got)
	}
	if pub.callCount() != 0 {
		t.Fatalf("契约外 topic 不应进入总线，却投递了 %d 次", pub.callCount())
	}
}

// TestA2_Qos2Rejected 确认 QoS2 被拒绝而不是静默绕过 A2 时序。
func TestA2_Qos2Rejected(t *testing.T) {
	pub := &fakePublisher{}
	broker, metrics := newTestBroker(t, pub, time.Second)

	dev := dialTestDevice(t, broker.Addr(), "dev-5")
	dev.connect("dev-5")

	// testDevice.publish 固定用 QoS1，这里手工构造契约外的 QoS2 报文。
	dev.sendQos2("v1/devices/dev-5/telemetry", []byte(`{}`), 99)

	dev.assertNoPuback(300 * time.Millisecond)

	if got := metrics.UnsupportedQosTotal.Load(); got != 1 {
		t.Fatalf("期望 UnsupportedQosTotal=1，得到 %d", got)
	}
	if pub.callCount() != 0 {
		t.Fatalf("QoS2 不应进入总线，却投递了 %d 次", pub.callCount())
	}
}

// TestA2_内部注入报文必须放行 是 A3 倒逼出来的回归用例。
//
// 跨节点投递复用 inline client 注入，那条路径同样经过 OnPublish。
// 若这里把注入的 QoS1 报文当成设备上报（路由 + 拒绝），跨节点投递会静默失效。
func TestA2_内部注入报文必须放行(t *testing.T) {
	pub := &fakePublisher{}
	broker, metrics := newTestBroker(t, pub, time.Second)

	if err := broker.Server.Publish("v1/devices/dev-x/telemetry", []byte(`{"n":1}`), false, 1); err != nil {
		t.Fatalf("注入失败: %v", err)
	}

	if got := metrics.PublishTotal.Load(); got != 0 {
		t.Fatalf("注入的报文不应进入 A2 时序（会被路由回总线并拒绝），实际进入 %d 次", got)
	}
	if got := pub.callCount(); got != 0 {
		t.Fatalf("注入的报文不应被投到总线，实际 %d 次", got)
	}
}

// ---------- Acker 单元测试 ----------

func TestAcker_AwaitPersist(t *testing.T) {
	t.Run("确认成功", func(t *testing.T) {
		pub := &fakePublisher{}
		a := NewAcker(pub, time.Second, new(Metrics))

		if err := a.AwaitPersist(context.Background(), "s", nil); err != nil {
			t.Fatalf("期望成功，得到 %v", err)
		}
		if a.metrics.PersistedTotal.Load() != 1 {
			t.Fatal("PersistedTotal 未累加")
		}
	})

	t.Run("超时", func(t *testing.T) {
		pub := &fakePublisher{block: make(chan struct{})}
		a := NewAcker(pub, 50*time.Millisecond, new(Metrics))

		err := a.AwaitPersist(context.Background(), "s", nil)
		if !errors.Is(err, ErrPubackTimeout) {
			t.Fatalf("期望 ErrPubackTimeout，得到 %v", err)
		}
		if a.metrics.TimeoutTotal.Load() != 1 {
			t.Fatal("TimeoutTotal 未累加")
		}
		if a.metrics.PersistedTotal.Load() != 0 {
			t.Fatal("超时不应计入 PersistedTotal")
		}
	})

	t.Run("总线报错", func(t *testing.T) {
		pub := &fakePublisher{err: errors.New("boom")}
		a := NewAcker(pub, time.Second, new(Metrics))

		err := a.AwaitPersist(context.Background(), "s", nil)
		if err == nil || errors.Is(err, ErrPubackTimeout) {
			t.Fatalf("期望普通投递错误，得到 %v", err)
		}
		if a.metrics.PublishErrorTotal.Load() != 1 {
			t.Fatal("PublishErrorTotal 未累加")
		}
	})
}

func TestAcker_PublishQoS0(t *testing.T) {
	pub := &fakePublisher{}
	m := new(Metrics)
	a := NewAcker(pub, time.Second, m)
	if err := a.PublishQoS0(context.Background(), "iot.telemetry.p1", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("QoS0 投递失败: %v", err)
	}
	if m.QoS0PublishTotal.Load() != 1 || m.QoS0PublishErrorTotal.Load() != 0 {
		t.Fatalf("QoS0 指标不符: total=%d errors=%d", m.QoS0PublishTotal.Load(), m.QoS0PublishErrorTotal.Load())
	}
	if pub.callCount() != 1 {
		t.Fatalf("QoS0 应进入总线一次，实际 %d", pub.callCount())
	}
}

// ---------- 路由单元测试 ----------

func TestContractRouter(t *testing.T) {
	r := ContractRouter{Project: "p1", Shards: 8}

	t.Run("分片稳定", func(t *testing.T) {
		got, err := r.Route(nil, packetWithTopic("v1/devices/dk-1/telemetry"))
		if err != nil {
			t.Fatalf("不应报错: %v", err)
		}
		want := fmt.Sprintf("iot.telemetry.p1.shard.%d", hashShard("dk-1", 8))
		if got != want {
			t.Fatalf("期望 %s，得到 %s", want, got)
		}
		again, _ := r.Route(nil, packetWithTopic("v1/devices/dk-1/telemetry"))
		if again != got {
			t.Fatalf("同一设备两次映射不一致: %s vs %s", got, again)
		}
	})

	t.Run("拒绝非法 topic", func(t *testing.T) {
		for _, topic := range []string{
			"", "/", "v1/devices", "v1/devices/", "v1/devices//telemetry",
			"v2/devices/dk/telemetry", "v1/device/dk/telemetry", "v1/devices/dk",
			"v1/devices/dk/telemetry/extra",
		} {
			if _, err := r.Route(nil, packetWithTopic(topic)); !errors.Is(err, ErrUnroutableTopic) {
				t.Fatalf("topic %q 应被拒绝，得到 %v", topic, err)
			}
		}
	})
}
