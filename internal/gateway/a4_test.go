package gateway

import (
	"testing"
	"time"
)

// 本文件验证 03 §4.4.1 边界 1 的代价：
//
//	A2 的「等 NATS PublishAck 再回 PUBACK」在**该连接的收包协程内同步执行**，
//	因此同一连接上的 PINGREQ 会被排在 PUBLISH 之后 —— PINGRESP 被推迟，
//	推迟上限 = PubackTimeout。
//
// 这组用例把「代价」从注释变成可执行断言，且不依赖真实 NATS：
// 用 fakePublisher 的 block 通道精确控制「总线确认何时到达」。

// TestA4_PublishBlocksPingreq 是核心证据：同一连接上，PUBLISH 处理期间的 PINGREQ
// 不会得到响应；总线放行后，先 PUBACK、后 PINGRESP（顺序即「收包协程被占据」）。
func TestA4_PublishBlocksPingreq(t *testing.T) {
	pub := &fakePublisher{block: make(chan struct{})}
	broker, _ := newTestBroker(t, pub, 5*time.Second)

	dev := dialTestDevice(t, broker.Addr(), "dev-ping")
	dev.connect("dev-ping")

	// 基线：无 PUBLISH 在途时，PINGREQ 立即得到 PINGRESP。
	// 它的作用是排除「读路径本身有问题」导致的假阳性。
	dev.pingreq()
	if err := dev.awaitPingresp(time.Second); err != nil {
		t.Fatalf("基线 PINGRESP 未到达（读路径本身有问题）: %v", err)
	}

	// 一条 QoS1 上报：总线阻塞，PUBACK 不会来。
	id := dev.publish("v1/devices/dev-ping/telemetry", []byte(`{"n":1}`), false)

	// 同一连接立即发 PINGREQ：它排在 PUBLISH 之后，整个窗口内不应有任何响应。
	dev.pingreq()
	dev.assertNoPacket(400 * time.Millisecond)

	// 放行总线：先 PUBACK，再 PINGRESP。
	close(pub.block)
	if err := dev.awaitPuback(id, time.Second); err != nil {
		t.Fatalf("放行后 PUBACK 未到达: %v", err)
	}
	if err := dev.awaitPingresp(time.Second); err != nil {
		t.Fatalf("放行后 PINGRESP 未到达: %v", err)
	}
}

// TestA4_PublishBlockIsPerConnection 澄清阻塞的**范围**：只影响发起 PUBLISH 的那条连接。
// 其他连接有自己的收包协程，PINGREQ 照常响应 —— 阻塞不是全局的。
func TestA4_PublishBlockIsPerConnection(t *testing.T) {
	pub := &fakePublisher{block: make(chan struct{})}
	broker, _ := newTestBroker(t, pub, 5*time.Second)

	blocked := dialTestDevice(t, broker.Addr(), "dev-blocked")
	blocked.connect("dev-blocked")
	other := dialTestDevice(t, broker.Addr(), "dev-other")
	other.connect("dev-other")

	// 连接 A 上报并阻塞。
	id := blocked.publish("v1/devices/dev-blocked/telemetry", []byte(`{}`), false)
	blocked.pingreq()
	blocked.assertNoPacket(400 * time.Millisecond)

	// 连接 B 的 PINGREQ 不受影响。
	other.pingreq()
	if err := other.awaitPingresp(time.Second); err != nil {
		t.Fatalf("其他连接的 PINGRESP 被误阻塞（阻塞不应该是全局的）: %v", err)
	}

	close(pub.block)
	if err := blocked.awaitPuback(id, time.Second); err != nil {
		t.Fatalf("放行后 PUBACK 未到达: %v", err)
	}
}

// TestA4_PublishTimeoutDelaysPingresp 覆盖最坏情形：总线超时（不回 PUBACK）时，
// PINGRESP 的推迟上限就是 PubackTimeout —— 这是设备侧 keepalive 判定的直接输入。
func TestA4_PublishTimeoutDelaysPingresp(t *testing.T) {
	const pubackTimeout = 400 * time.Millisecond
	pub := &fakePublisher{block: make(chan struct{})} // 永不放行 → 走超时分支
	broker, metrics := newTestBroker(t, pub, pubackTimeout)

	dev := dialTestDevice(t, broker.Addr(), "dev-ping-timeout")
	dev.connect("dev-ping-timeout")

	dev.publish("v1/devices/dev-ping-timeout/telemetry", []byte(`{}`), false)

	sentAt := time.Now()
	dev.pingreq()

	// 超时窗口内不应有任何响应（PUBACK 不会来，PINGRESP 也被挡住）。
	dev.assertNoPacket(pubackTimeout / 2)

	// 超时之后 PINGRESP 才会到达。
	if err := dev.awaitPingresp(3 * time.Second); err != nil {
		t.Fatalf("超时后 PINGRESP 未到达: %v", err)
	}
	elapsed := time.Since(sentAt)

	if got := metrics.TimeoutTotal.Load(); got != 1 {
		t.Fatalf("应记录 1 次总线超时，得到 %d", got)
	}
	// PINGRESP 必须体现阻塞：至少被推迟到超过半个超时窗口。
	if elapsed < pubackTimeout/2 {
		t.Fatalf("PINGRESP 仅 %s 就到达，未体现阻塞（期望接近 %s）", elapsed, pubackTimeout)
	}
	t.Logf("总线超时情形：PINGREQ → PINGRESP 实测 %s（PubackTimeout=%s）",
		elapsed.Round(10*time.Millisecond), pubackTimeout)
}
