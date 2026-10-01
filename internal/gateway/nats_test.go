package gateway

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/nats-io/nats.go"
)

// A2 的真实总线验证。默认跳过（`go test ./...` 不应依赖外部中间件），显式开启：
//
//	IOT_NATS_URL=nats://100.64.0.3:28222 go test ./internal/gateway -run TestA2_RealNATS -v
//	# 等价：make test-nats
//
// 与 fakePublisher 的用例相比，它验证的是更强的一条命题：
// **PUBACK 不仅是「等到了某个信号」，而且该信号确实意味着消息此刻可被读回**。
const (
	a2Stream      = "IOT_A2_TEST"
	a2Captured    = "a2test.messages"
	a2NotCaptured = "a2test.not-captured"
)

// fixedRouter 是真实总线用例专用的路由：把上报固定投到一个测试专属 subject。
//
// 为什么不用 ContractRouter：JetStream **禁止跨 Stream 的 subject 重叠**，
// 而开发栈里存在网关自建的 `iot.telemetry.>`。测试若也用 `iot.telemetry.*`
// 就会与它冲突。subject 映射本身由 TestContractRouter 单独覆盖，此处不需要重复。
type fixedRouter struct{ subject string }

var _ SubjectRouter = fixedRouter{}

func (r fixedRouter) Route(*mqtt.Client, packets.Packet) (string, error) { return r.subject, nil }

// TestA2_RealNATS_PubackAfterPersist 验证「先持久化、再确认」在真实 JetStream 上成立。
func TestA2_RealNATS_PubackAfterPersist(t *testing.T) {
	url := natsURL(t)
	js, cleanup := prepareA2Stream(t, url)
	defer cleanup()

	pub, err := NewNATSPublisher(url, a2Stream)
	if err != nil {
		t.Fatalf("构造 NATSPublisher 失败: %v", err)
	}
	defer pub.Close()

	if err := pub.EnsureStream(StreamSpec{Subjects: []string{a2Captured}, Replicas: 1}); err != nil {
		t.Fatalf("确保 Stream 存在失败: %v", err)
	}

	// 订阅必须晚于 Stream 创建：无匹配 Stream 时 SubscribeSync 会直接报错。
	sub, err := js.SubscribeSync(a2Captured)
	if err != nil {
		t.Fatalf("订阅校验流失败: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	broker, metrics := newTestBrokerWithRouter(t, pub, DefaultPubackTimeout, fixedRouter{a2Captured})

	dev := dialTestDevice(t, broker.Addr(), "dev-nats")
	dev.connect("dev-nats")

	payload := []byte(`{"ts":"2026-10-01T00:00:00.000Z","seq":1,"v":3.14}`)

	start := time.Now()
	id := dev.publish("v1/devices/dev-nats/telemetry", payload, false)
	if err := dev.awaitPuback(id, 3*time.Second); err != nil {
		t.Fatalf("未收到 PUBACK: %v", err)
	}
	latency := time.Since(start)

	// PUBACK 之后消息必须已可读回 —— 这就是「已持久化」的可观测定义。
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("PUBACK 已回但消息读不回，说明确认早于持久化: %v", err)
	}
	// 断言「原始遥测载荷在信封里」，而不是与它字节相等：
	// 网关会把遥测包进统一信封（project_id / device_key / payload…，见 03 §4），
	// 字节相等的写法在信封落地那天就过期了 —— 而这条用例默认被跳过
	//（需要 IOT_NATS_URL），于是它悄悄红着也没人发现。这类「环境门控的用例」
	// 最容易腐烂：不跑就不会红，不红就没人修。
	var env struct {
		DeviceKey string          `json:"device_key"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(msg.Data, &env); err != nil {
		t.Fatalf("NATS 载荷不是合法的信封 JSON: %v（原文 %s）", err, msg.Data)
	}
	if env.DeviceKey != "dev-nats" {
		t.Fatalf("信封里的 device_key 不对：期望 dev-nats，得到 %q", env.DeviceKey)
	}
	if string(env.Payload) != string(payload) {
		t.Fatalf("信封里的原始载荷不一致：期望 %s，得到 %s", payload, env.Payload)
	}

	if got := metrics.PersistedTotal.Load(); got != 1 {
		t.Fatalf("期望 PersistedTotal=1，得到 %d", got)
	}
	if got := metrics.TimeoutTotal.Load(); got != 0 {
		t.Fatalf("不应有超时，得到 %d", got)
	}

	// §4.4 的吞吐假设：本地 NATS 的 PublishAck 往返通常 < 1ms。
	t.Logf("设备 PUBLISH → PUBACK 往返 %s（含 JetStream 落盘 + 一次本地 RTT）", latency)
}

// TestA2_RealNATS_SubjectNotInStream_NoPuback 是**配置错误**的负向验证：
// 当业务 subject 未被任何 Stream 捕获时，JetStream 返回 `no response from stream`，
// 网关必须按「未确认」处理并拒绝回 PUBACK。
//
// 这条用例的价值：证明**配置失误不会退化成静默丢数据** ——
// 设备会持续重传，问题以可见的重传风暴暴露，而不是数据消失。
func TestA2_RealNATS_SubjectNotInStream_NoPuback(t *testing.T) {
	url := natsURL(t)
	_, cleanup := prepareA2Stream(t, url)
	defer cleanup()

	pub, err := NewNATSPublisher(url, a2Stream)
	if err != nil {
		t.Fatalf("构造 NATSPublisher 失败: %v", err)
	}
	defer pub.Close()

	if err := pub.EnsureStream(StreamSpec{Subjects: []string{a2Captured}, Replicas: 1}); err != nil {
		t.Fatalf("确保 Stream 存在失败: %v", err)
	}

	broker, metrics := newTestBrokerWithRouter(t, pub, time.Second, fixedRouter{a2NotCaptured})

	dev := dialTestDevice(t, broker.Addr(), "dev-nats-misconf")
	dev.connect("dev-nats-misconf")

	dev.publish("v1/devices/dev-nats-misconf/telemetry", []byte(`{}`), false)
	dev.assertNoPuback(2 * time.Second)

	if got := metrics.PersistedTotal.Load(); got != 0 {
		t.Fatalf("subject 未被 Stream 捕获时不应确认，PersistedTotal=%d", got)
	}
	if got := metrics.PublishErrorTotal.Load(); got != 1 {
		t.Fatalf("应记为总线投递失败（no response from stream），得到 %d", got)
	}
}

// TestA2_RealNATSUnreachable_NoPuback 验证总线不可达时**绝不回 PUBACK**。
func TestA2_RealNATSUnreachable_NoPuback(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过")
	}

	// 127.0.0.1:1 上不会有服务监听。
	pub, err := NewNATSPublisher("nats://127.0.0.1:1", a2Stream)
	if err != nil {
		// 连接阶段即失败，同样满足「无法确认 ⇒ 不回 PUBACK」的结论。
		t.Logf("NATS 不可达，连接阶段即失败（符合预期）: %v", err)
		return
	}
	defer pub.Close()

	broker, metrics := newTestBroker(t, pub, 500*time.Millisecond)

	dev := dialTestDevice(t, broker.Addr(), "dev-nats-down")
	dev.connect("dev-nats-down")

	dev.publish("v1/devices/dev-nats-down/telemetry", []byte(`{}`), false)
	dev.assertNoPuback(1500 * time.Millisecond)

	if metrics.PersistedTotal.Load() != 0 {
		t.Fatal("总线不可达时不应确认任何消息")
	}
}

func natsURL(t *testing.T) string {
	t.Helper()

	url := os.Getenv("IOT_NATS_URL")
	if url == "" {
		t.Skip("未设置 IOT_NATS_URL，跳过真实 NATS 验证（见文件头部的运行方式）")
	}
	return url
}

// prepareA2Stream 返回一个独立的校验侧 JetStream 上下文，并清理测试用的 Stream。
func prepareA2Stream(t *testing.T, url string) (nats.JetStreamContext, func()) {
	t.Helper()

	nc, err := nats.Connect(url, nats.Name("a2-verifier"))
	if err != nil {
		t.Fatalf("校验连接 NATS 失败: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatalf("校验侧初始化 JetStream 失败: %v", err)
	}

	// 清理上次残留，保证本次断言只看到本次的数据。
	if err := js.DeleteStream(a2Stream); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		nc.Close()
		t.Fatalf("清理旧 Stream 失败: %v", err)
	}

	return js, func() {
		_ = js.DeleteStream(a2Stream)
		nc.Close()
	}
}

// TestEnsureStreamReconcilesRetention 锁住一个**项目级缺陷**的修复。
//
// 缺陷：EnsureStream 原先只在 Stream「不存在」时创建，于是**先于 03 §4.2 口径**
// 建出来的流会一直是**无限保留**（`MaxAge=0`）—— 磁盘随写入无上限增长，
// 同时也是「重启即重放」的放大器（保留窗口越长，一次误删消费者的代价越大）。
func TestEnsureStreamReconcilesRetention(t *testing.T) {
	url := natsURL(t)
	js, cleanup := prepareA2Stream(t, url)
	defer cleanup()

	// 造一个「旧口径」的流：既没有 MaxAge，也没有 MaxMsgsPerSubject。
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: a2Stream, Subjects: []string{a2Captured}, Storage: nats.FileStorage,
	}); err != nil {
		t.Fatalf("建旧口径 Stream: %v", err)
	}

	pub, err := NewNATSPublisher(url, a2Stream)
	if err != nil {
		t.Fatalf("构造 NATSPublisher: %v", err)
	}
	defer pub.Close()

	spec := StreamSpec{Subjects: []string{a2Captured}, Replicas: 1}
	if err := pub.EnsureStream(spec); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	info, err := js.StreamInfo(a2Stream)
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if info.Config.MaxAge != DefaultStreamMaxAge {
		t.Fatalf("MaxAge 应被校正为 %s，得 %s —— 无限保留会让磁盘无上限增长",
			DefaultStreamMaxAge, info.Config.MaxAge)
	}
	if info.Config.MaxMsgsPerSubject != DefaultStreamMaxMsgsPerSubject {
		t.Fatalf("MaxMsgsPerSubject 应被校正为 %d，得 %d",
			DefaultStreamMaxMsgsPerSubject, info.Config.MaxMsgsPerSubject)
	}

	// 幂等：口径已一致时再调一次不应报错、也不应改动。
	if err := pub.EnsureStream(spec); err != nil {
		t.Fatalf("二次 EnsureStream 应幂等: %v", err)
	}
}
