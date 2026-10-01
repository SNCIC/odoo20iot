package natsjs

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// newStreamFixture 建一个临时流并返回校验侧 js；测试结束自动删流。
func newStreamFixture(t *testing.T, cfg *nats.StreamConfig) (nats.JetStreamContext, string) {
	t.Helper()

	nc, err := nats.Connect(natsURL(t), nats.Name("natsjs-stream-test"))
	if err != nil {
		t.Fatalf("连接 NATS: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	name := cfg.Name
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("建流 %s: %v", name, err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(name) })
	return js, name
}

// TestEnsureStreamReconcilesRetention 锁住一个**项目级缺陷**的修复。
//
// 缺陷：各服务原先都写「不存在才创建」，于是**先于口径代码建出来的流**
// 永远停在旧配置 —— 典型是 `MaxAge=0`（无限保留）：磁盘随写入无上限增长，
// 且是「重启即重放」的放大器（保留窗口越长，一次误删消费者的代价越大）。
func TestEnsureStreamReconcilesRetention(t *testing.T) {
	natsURL(t)
	name := fmt.Sprintf("TEST_NATSJS_RECON_%d", time.Now().UnixNano()%1_000_000)
	subject := name + ".evt"
	js, name := newStreamFixture(t, &nats.StreamConfig{
		Name: name, Subjects: []string{subject}, Storage: nats.FileStorage,
	})

	spec := StreamSpec{
		Name: name, Subjects: []string{subject}, Replicas: 1,
		MaxAge: 24 * time.Hour, MaxMsgsPerSubject: 1000, Discard: nats.DiscardNew,
	}
	if err := EnsureStream(js, spec); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	info, err := js.StreamInfo(name)
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if info.Config.MaxAge != 24*time.Hour {
		t.Fatalf("MaxAge 应被校正为 24h，得 %s —— 无限保留会让磁盘无上限增长",
			info.Config.MaxAge)
	}
	if info.Config.MaxMsgsPerSubject != 1000 {
		t.Fatalf("MaxMsgsPerSubject 应被校正为 1000，得 %d", info.Config.MaxMsgsPerSubject)
	}
	if info.Config.Discard != nats.DiscardNew {
		t.Fatalf("Discard 应被校正为 DiscardNew，得 %v", info.Config.Discard)
	}

	// 幂等：口径已一致时再调一次不应报错、也不应改动。
	if err := EnsureStream(js, spec); err != nil {
		t.Fatalf("二次 EnsureStream 应幂等: %v", err)
	}
}

// TestEnsureStreamLeavesZeroFieldsAlone 验证「零值 = 不约束」。
//
// 这对**既存**流尤其重要：服务不该拿默认值去覆盖部署侧的显式配置。
func TestEnsureStreamLeavesZeroFieldsAlone(t *testing.T) {
	natsURL(t)
	name := fmt.Sprintf("TEST_NATSJS_ZERO_%d", time.Now().UnixNano()%1_000_000)
	subject := name + ".evt"
	js, name := newStreamFixture(t, &nats.StreamConfig{
		Name: name, Subjects: []string{subject}, Storage: nats.FileStorage,
		MaxAge: 7 * 24 * time.Hour, Discard: nats.DiscardOld,
	})

	// spec 只声明 subjects，保留口径全为零值。
	if err := EnsureStream(js, StreamSpec{Name: name, Subjects: []string{subject}}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	info, err := js.StreamInfo(name)
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if info.Config.MaxAge != 7*24*time.Hour {
		t.Fatalf("零值不该改既存的 MaxAge（7d），得 %s", info.Config.MaxAge)
	}
	if info.Config.Discard != nats.DiscardOld {
		t.Fatalf("零值不该改既存的 Discard，得 %v", info.Config.Discard)
	}
}

// TestEnsureStreamStrictSubjects 验证严格的订阅范围策略：
// 既存流不覆盖期望 subject 时**报错而不是自动改**（改 subjects 影响其他消费者）。
func TestEnsureStreamStrictSubjects(t *testing.T) {
	natsURL(t)
	name := fmt.Sprintf("TEST_NATSJS_STRICT_%d", time.Now().UnixNano()%1_000_000)
	js, name := newStreamFixture(t, &nats.StreamConfig{
		Name: name, Subjects: []string{name + ".a"}, Storage: nats.FileStorage,
	})

	err := EnsureStream(js, StreamSpec{
		Name: name, Subjects: []string{name + ".b"}, StrictSubjects: true,
	})
	if err == nil {
		t.Fatal("既存流不覆盖期望 subject 时应报错，而不是自动改订阅范围")
	}
}

// TestEnsureStreamValidates 覆盖参数校验：缺名或空 subjects 必须在**发请求前**拒绝。
func TestEnsureStreamValidates(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(300*time.Millisecond))
	if err != nil {
		t.Skipf("本地无 NATS，跳过参数校验路径: %v", err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()

	if err := EnsureStream(js, StreamSpec{Subjects: []string{"x"}}); err == nil {
		t.Fatal("缺 stream 名应被拒")
	}
	if err := EnsureStream(js, StreamSpec{Name: "x"}); err == nil {
		t.Fatal("空 subjects 应被拒")
	}
}

// TestSameStringSet 覆盖 subjects 比较的边界。
//
// 顺序不同**不算**漂移：NATS 返回 subjects 的顺序不保证与写入一致，
// 若按切片逐位比较，每次启动都会误判成漂移并重建一次配置。
func TestSameStringSet(t *testing.T) {
	cases := []struct {
		a, b []string
		want bool
	}{
		{nil, nil, true},
		{[]string{"a"}, []string{"a"}, true},
		{[]string{"a", "b"}, []string{"b", "a"}, true},
		{[]string{"a"}, []string{"b"}, false},
		{[]string{"a"}, []string{"a", "b"}, false},
		{[]string{"a", "a"}, []string{"a"}, false},
	}
	for _, c := range cases {
		if got := sameStringSet(c.a, c.b); got != c.want {
			t.Errorf("sameStringSet(%v, %v) = %v，期望 %v", c.a, c.b, got, c.want)
		}
	}
}

// TestSubjectCovers 钉住 03 §6 坑 40 的边界：`foo.>` 覆盖 `foo.bar`，
// 但**不覆盖 `foo` 本身**（`>` 至少匹配一层）。
func TestSubjectCovers(t *testing.T) {
	cases := []struct {
		h, w string
		want bool
	}{
		{"iot.telemetry.>", "iot.telemetry.1.dev", true},
		{"iot.telemetry.>", "iot.telemetry.a", true},
		{"iot.telemetry.>", "iot.telemetry", false}, // 坑 40：不覆盖自身
		{"iot.telemetry.>", "iot.other.a", false},
		{"iot.telemetry.a", "iot.telemetry.a", true},
		{"iot.telemetry.a", "iot.telemetry.b", false},
	}
	for _, c := range cases {
		if got := subjectCovers(c.h, c.w); got != c.want {
			t.Errorf("subjectCovers(%q, %q) = %v，期望 %v", c.h, c.w, got, c.want)
		}
	}
}
