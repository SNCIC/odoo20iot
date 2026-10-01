package natsjs

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func natsURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("IOT_NATS_URL")
	if url == "" {
		t.Skip("未设置 IOT_NATS_URL，跳过真实 NATS 测试")
	}
	return url
}

// withStream 建一个临时流并发布一条消息，测试结束自动删流。
func withStream(t *testing.T) (nats.JetStreamContext, string, string) {
	t.Helper()
	nc, err := nats.Connect(natsURL(t), nats.Name("natsjs-test"))
	if err != nil {
		t.Fatalf("连接 NATS: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	stream := fmt.Sprintf("TEST_NATSJS_%d", time.Now().UnixNano()%1_000_000)
	subject := stream + ".evt"
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: stream, Subjects: []string{subject}, Storage: nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("建流 %s: %v", stream, err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(stream) })
	if _, err := js.Publish(subject, []byte("one")); err != nil {
		t.Fatalf("发布: %v", err)
	}
	return js, stream, subject
}

// TestSubscribeKeepsDurableAcrossRestart 锁住一个**项目级缺陷**的修复。
//
// 缺陷：退出时对 JetStream 订阅调 `sub.Unsubscribe()` 会**删除消费者**。
// 服务每次退出都删、每次启动都新建，于是重启把整个保留窗口重放一遍 ——
// 对通知服务是一场通知风暴，对计量服务是重复计数。
func TestSubscribeKeepsDurableAcrossRestart(t *testing.T) {
	js, stream, subject := withStream(t)
	const durable = "keep"
	opts := Options{Subject: subject, Durable: durable, Stream: stream,
		AckWait: 5 * time.Second, Inactive: time.Hour}

	// 第一轮：消费并确认。
	sub1, err := Subscribe(js, opts)
	if err != nil {
		t.Fatalf("首次订阅: %v", err)
	}
	msgs, err := sub1.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("首轮应取到 1 条，得 %d 条 / %v", len(msgs), err)
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ACK: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// 模拟重启：**不** Unsubscribe，直接再建一个同 durable 的订阅。
	sub2, err := Subscribe(js, opts)
	if err != nil {
		t.Fatalf("重启后再订阅: %v", err)
	}

	// 断言 1：消费者仍在，且**进度被保留**（ack_floor 不为 0）。
	info, err := js.ConsumerInfo(stream, durable)
	if err != nil {
		t.Fatalf("重启后消费者不该消失: %v", err)
	}
	if info.AckFloor.Consumer != 1 {
		t.Fatalf("消费进度应被保留（ack_floor=1），得 %d —— "+
			"进度丢了就意味着重启会重放", info.AckFloor.Consumer)
	}

	// 断言 2：不重放。
	if got, err := sub2.Fetch(1, nats.MaxWait(1200*time.Millisecond)); err == nil && len(got) > 0 {
		t.Fatalf("重启后不该重放已确认的消息（拿到 %d 条）", len(got))
	}
}

// TestUnsubscribeDeletesDurableAndReplays 把**坑本身**钉住。
//
// 少了这条，上面那个用例在「重放不再发生」时会永远通过，
// 而它保护的东西已经没人守了 —— 测试最常见的失效方式是变得空洞。
//
// 注意这里**故意用原生 `js.PullSubscribe`** 而不是 `Subscribe`：后者的返回值
// 被收窄成 `*Subscription`，根本不提供 Unsubscribe。本用例要演示的正是
// 「若绕过 natsjs 直接用原生 API，会发生什么」。
func TestUnsubscribeDeletesDurableAndReplays(t *testing.T) {
	js, stream, subject := withStream(t)
	const durable = "trap"

	sub, err := js.PullSubscribe(subject, durable,
		nats.BindStream(stream),
		nats.ManualAck(), nats.AckExplicit(), nats.DeliverAll(), nats.MaxDeliver(-1),
		nats.InactiveThreshold(time.Hour),
	)
	if err != nil {
		t.Fatalf("订阅: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("应取到 1 条，得 %d 条 / %v", len(msgs), err)
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ACK: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// ⚠️ 这一步就是坑：对 JetStream 订阅 Unsubscribe 会删掉消费者。
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	if _, err := js.ConsumerInfo(stream, durable); err == nil {
		t.Fatal("Unsubscribe 之后消费者竟然还在 —— 若 NATS 改了行为，" +
			"「重启即重放」这条结论与 natsjs 的注释都要重新核对")
	}

	// 消费者已被删除，重建后按 DeliverAll 从头投递：那条**已 ACK** 的消息会再来一次。
	replay, err := Subscribe(js, Options{Subject: subject, Durable: durable, Stream: stream,
		AckWait: 5 * time.Second, Inactive: time.Hour})
	if err != nil {
		t.Fatalf("重建订阅: %v", err)
	}
	if got, err := replay.Fetch(1, nats.MaxWait(1500*time.Millisecond)); err != nil || len(got) == 0 {
		t.Fatal("删掉消费者后重建本应重放（这就是「重启即重放整个保留窗口」的机制），" +
			"却一条都没拿到")
	}
}

func TestSubscribeValidatesOptions(t *testing.T) {
	// 参数缺失必须在**发请求之前**就报出来，而不是等 NATS 返回一个
	// 含糊的 subject/stream 错误。
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(300*time.Millisecond))
	if err != nil {
		t.Skipf("本地无 NATS，跳过参数校验路径: %v", err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()

	for _, opts := range []Options{
		{Durable: "d", Stream: "s"},
		{Subject: "x", Stream: "s"},
		{Subject: "x", Durable: "d"},
	} {
		if _, err := Subscribe(js, opts); err == nil {
			t.Fatalf("%+v 应被拒", opts)
		}
	}
}
