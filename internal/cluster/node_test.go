package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// A3 的验收场景（06 Phase 0 表）：
//
//	| 集群路由原型 | 3 节点跨节点投递正确；节点被 kill 后消息不丢（含离线流） |
//
// 默认跳过（需要真实 NATS）：IOT_NATS_URL=nats://... go test ./internal/cluster -v
func natsURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("IOT_NATS_URL")
	if url == "" {
		t.Skip("未设置 IOT_NATS_URL，跳过集群用例（见 09-handoff.md §5.2）")
	}
	return url
}

// fakeLocal 记录被注入到「本地 broker」的消息，并可被要求失败。
//
// hasClient 默认为 true：它表示「这台设备就连在本节点」。
// 接收端会**再确认一次**（防止设备刚断开），所以这个开关必须能表达真值，
// 否则所有跨节点消息都会被兜底到离线队列 —— 第一版就踩了这个坑。
type fakeLocal struct {
	mu        sync.Mutex
	got       []Envelope
	hasClient bool
	failFirst int // >0 时前 N 次 Inject 返回错误（用于验证「注入失败不 ACK」）
}

func newFakeLocal() *fakeLocal { return &fakeLocal{hasClient: true} }

func (f *fakeLocal) Inject(env Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFirst > 0 {
		f.failFirst--
		return errors.New("注入失败（测试注入）")
	}
	f.got = append(f.got, env)
	return nil
}

func (f *fakeLocal) HasClient(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hasClient
}

func (f *fakeLocal) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

func (f *fakeLocal) topics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.got))
	for _, e := range f.got {
		out = append(out, e.Topic)
	}
	return out
}

// 每次运行用独立的流名与节点名：NATS 的 Stream/Consumer 是全局元数据，
// 复用名字会让上一次运行的 durable 消费位点污染本次结果。
type testCluster struct {
	url    string
	runID  string
	loc    *MemLocator
	nodes  map[string]*Node
	locals map[string]*fakeLocal
}

func newTestCluster(t *testing.T, peers ...string) *testCluster {
	t.Helper()

	url := natsURL(t)
	runID := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)

	tc := &testCluster{
		url:    url,
		runID:  runID,
		loc:    NewMemLocator(),
		nodes:  map[string]*Node{},
		locals: map[string]*fakeLocal{},
	}

	for _, id := range peers {
		nodeID := id + "-" + runID
		loc := newFakeLocal()
		tc.locals[id] = loc
		tc.nodes[id] = tc.startNode(t, nodeID, peers, runID, loc)
	}

	t.Cleanup(func() {
		nc, err := nats.Connect(url)
		if err == nil {
			js, jerr := nc.JetStream()
			if jerr == nil {
				_ = js.DeleteStream("IOT_ROUTE_" + runID)
				_ = js.DeleteStream("IOT_OFFLINE_" + runID)
			}
			nc.Close()
		}
	})
	return tc
}

func (tc *testCluster) startNode(t *testing.T, nodeID string, peerIDs []string, runID string, loc *fakeLocal) *Node {
	t.Helper()

	peers := make([]string, 0, len(peerIDs))
	for _, p := range peerIDs {
		peers = append(peers, p+"-"+runID)
	}

	n, err := New(context.Background(), Options{
		ID:            nodeID,
		Peers:         peers,
		NATSURL:       tc.url,
		RouteStream:   "IOT_ROUTE_" + runID,
		RoutePrefix:   "iot.route",
		OfflineStream: "IOT_OFFLINE_" + runID,
		OfflinePrefix: "offline.shard",
		Shards:        8,
		AckWait:       time.Second, // 测试里让重投尽快发生
		Cursor:        NewMemCursor(),
		Logger:        slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}, tc.loc)
	if err != nil {
		t.Fatalf("构造节点 %s 失败: %v", nodeID, err)
	}

	if err := n.Start(loc); err != nil {
		t.Fatalf("启动节点 %s 的消费循环失败: %v", nodeID, err)
	}
	t.Cleanup(n.Close)
	return n
}

// ---------------------------------------------------------------------------
// ① 跨节点投递正确
// ---------------------------------------------------------------------------

func TestCluster_定向路由到持有设备的节点(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	ctx := context.Background()

	const devKey = "dev-1001"
	if err := tc.loc.Bind(ctx, devKey, tc.nodes["n2"].ID()); err != nil {
		t.Fatalf("登记设备位置失败: %v", err)
	}

	// 设备在 n2，n1 产生下行命令 → 应定向投到 n2。
	if err := tc.nodes["n1"].Route(ctx, Envelope{
		Topic:   "v1/devices/" + devKey + "/cmd/set",
		Payload: []byte(`{"speed":3}`),
		Qos:     1,
	}); err != nil {
		t.Fatalf("路由失败: %v", err)
	}

	if !waitFor(func() bool { return tc.locals["n2"].count() == 1 }, 3*time.Second) {
		t.Fatalf("n2 未收到消息（实际 %d 条）", tc.locals["n2"].count())
	}
	if tc.locals["n1"].count() != 0 {
		t.Error("n1 不应收到自己发起的跨节点消息")
	}
	if tc.locals["n3"].count() != 0 {
		t.Error("n3 与本次投递无关，不应收到消息")
	}
	if got := tc.nodes["n1"].Metrics().RoutedDirect.Load(); got != 1 {
		t.Errorf("应为 1 次定向转发，得到 %d", got)
	}
}

// TestCluster_设备在本节点不跨节点 覆盖 03 §1.4 的第 3 条（本地命中零网络开销）。
func TestCluster_设备在本节点不跨节点(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2")
	ctx := context.Background()

	const devKey = "dev-2002"
	if err := tc.loc.Bind(ctx, devKey, tc.nodes["n1"].ID()); err != nil {
		t.Fatalf("登记设备位置失败: %v", err)
	}

	if err := tc.nodes["n1"].Route(ctx, Envelope{
		Topic: "v1/devices/" + devKey + "/cmd/set",
	}); err != nil {
		t.Fatalf("路由失败: %v", err)
	}

	if got := tc.nodes["n1"].Metrics().RoutedLocal.Load(); got != 1 {
		t.Errorf("应记为本地命中，得到 %d", got)
	}
	if tc.locals["n2"].count() != 0 {
		t.Error("本地命中时不应产生跨节点消息")
	}
}

// ---------------------------------------------------------------------------
// ② 节点被 kill 后消息不丢
// ---------------------------------------------------------------------------

// TestCluster_节点停机期间的待投消息在恢复后送达 模拟「节点被 kill」。
func TestCluster_节点停机期间的待投消息在恢复后送达(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2")
	ctx := context.Background()

	const devKey = "dev-3003"
	if err := tc.loc.Bind(ctx, devKey, tc.nodes["n2"].ID()); err != nil {
		t.Fatalf("登记设备位置失败: %v", err)
	}

	// 先发一条验证通道正常。
	if err := tc.nodes["n1"].Route(ctx, Envelope{Topic: "v1/devices/" + devKey + "/cmd/set"}); err != nil {
		t.Fatalf("路由失败: %v", err)
	}
	if !waitFor(func() bool { return tc.locals["n2"].count() == 1 }, 3*time.Second) {
		t.Fatal("基线消息未送达")
	}

	// —— 注入「节点被 kill」——
	tc.nodes["n2"].Stop()

	const during = 10
	for i := 0; i < during; i++ {
		if err := tc.nodes["n1"].Route(ctx, Envelope{
			Topic:   "v1/devices/" + devKey + "/cmd/set",
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("停机期间路由失败（不应失败：NATS 仍在）: %v", err)
		}
	}

	// 停机期间确实没被消费。
	time.Sleep(300 * time.Millisecond)
	if got := tc.locals["n2"].count(); got != 1 {
		t.Fatalf("停机期间不应有投递，实际累计 %d 条", got)
	}

	// —— 恢复 ——
	if err := tc.nodes["n2"].Start(tc.locals["n2"]); err != nil {
		t.Fatalf("重启消费循环失败: %v", err)
	}

	if !waitFor(func() bool { return tc.locals["n2"].count() == 1+during }, 10*time.Second) {
		t.Fatalf("恢复后未补齐停机期间的消息：期望 %d 条，实际 %d 条",
			1+during, tc.locals["n2"].count())
	}
}

// TestCluster_注入失败的消息会被重投 验证「不 ACK 即重投」这条语义。
//
// 它是「消息不丢」的最后一道防线：任何注入失败（本地出错、进程将死）
// 都会让消息留在流里，而不是被默默丢掉。
func TestCluster_注入失败的消息会被重投(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2")
	ctx := context.Background()

	const devKey = "dev-4004"
	if err := tc.loc.Bind(ctx, devKey, tc.nodes["n2"].ID()); err != nil {
		t.Fatalf("登记设备位置失败: %v", err)
	}

	// 让 n2 的前两次注入失败 —— 消息不应被 ACK，因而会被重投。
	tc.locals["n2"].mu.Lock()
	tc.locals["n2"].failFirst = 2
	tc.locals["n2"].mu.Unlock()

	if err := tc.nodes["n1"].Route(ctx, Envelope{Topic: "v1/devices/" + devKey + "/cmd/set"}); err != nil {
		t.Fatalf("路由失败: %v", err)
	}

	if !waitFor(func() bool { return tc.locals["n2"].count() == 1 }, 15*time.Second) {
		t.Fatalf("重投未把消息送达（注入成功率应为 1/3），实际 %d 条",
			tc.locals["n2"].count())
	}
	if got := tc.nodes["n2"].Metrics().AckFailures.Load(); got < 2 {
		t.Errorf("应记录至少 2 次注入失败，得到 %d", got)
	}
}

// ---------------------------------------------------------------------------
// ③ 离线流
// ---------------------------------------------------------------------------

func TestCluster_设备离线时落离线队列并可回放(t *testing.T) {
	tc := newTestCluster(t, "n1")
	ctx := context.Background()

	const devKey = "dev-5005" // 不登记 → 不在线

	for i := 0; i < 3; i++ {
		if err := tc.nodes["n1"].Route(ctx, Envelope{
			Topic:   "v1/devices/" + devKey + "/cmd/set",
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("路由失败: %v", err)
		}
	}

	if got := tc.nodes["n1"].Metrics().OfflineQueued.Load(); got != 3 {
		t.Fatalf("设备离线时应写入 3 条离线消息，实际 %d 条", got)
	}

	// 回放：模拟设备重连。
	var got []Envelope
	n, err := tc.nodes["n1"].ReplayOffline(ctx, devKey, func(env Envelope) error {
		got = append(got, env)
		return nil
	})
	if err != nil {
		t.Fatalf("回放失败: %v", err)
	}
	if n != 3 || len(got) != 3 {
		t.Fatalf("应回放 3 条，实际 %d/%d", n, len(got))
	}

	// 再次回放必须是空的：游标已推进。
	var again []Envelope
	n2, err := tc.nodes["n1"].ReplayOffline(ctx, devKey, func(env Envelope) error {
		again = append(again, env)
		return nil
	})
	if err != nil {
		t.Fatalf("二次回放失败: %v", err)
	}
	if n2 != 0 || len(again) != 0 {
		t.Fatalf("游标应已推进，二次回放应为空，实际 %d 条", len(again))
	}
}

// TestCluster_回放失败不推进游标 保证「至少一次」而不是「最多一次」。
func TestCluster_回放失败不推进游标(t *testing.T) {
	tc := newTestCluster(t, "n1")
	ctx := context.Background()

	const devKey = "dev-6006"
	for i := 0; i < 3; i++ {
		if err := tc.nodes["n1"].Route(ctx, Envelope{Topic: "v1/devices/" + devKey + "/cmd/set"}); err != nil {
			t.Fatalf("路由失败: %v", err)
		}
	}

	// 第二条投递失败 → 游标应停在第一条。
	var delivered int
	_, err := tc.nodes["n1"].ReplayOffline(ctx, devKey, func(Envelope) error {
		delivered++
		if delivered == 2 {
			return errors.New("投递失败（测试注入）")
		}
		return nil
	})
	if err == nil {
		t.Fatal("第二条投递失败时应返回错误")
	}

	// 再回放一次：应从未确认的那条继续（第 2 条），而不是从头或跳过。
	var got int
	if _, err := tc.nodes["n1"].ReplayOffline(ctx, devKey, func(Envelope) error {
		got++
		return nil
	}); err != nil {
		t.Fatalf("续传回放失败: %v", err)
	}
	if got != 2 {
		t.Fatalf("应从第 2 条续传 2 条（共 3 条），实际 %d 条", got)
	}
}

// ---------------------------------------------------------------------------
// ④ 方向判定与设备键解析（纯逻辑，无需 NATS）
// ---------------------------------------------------------------------------

func TestIsDeviceDownlink(t *testing.T) {
	down := []string{
		"v1/devices/dev-A/cmd/set",
		"v1/devices/dev-A/cmd/ota",
		"v1/devices/dev-A/cfg/report_interval",
		"v1/devices/dev-A/ota/announce",
		"v1/devices/dev-A/shadow/desired",
		"v1/gateways/gw-1/devices/sub-1/cmd/set",
	}
	up := []string{
		"v1/devices/dev-A/telemetry",
		"v1/devices/dev-A/attributes",
		"v1/devices/dev-A/events",
		"v1/devices/dev-A/cmd/reply", // 应答是上行
		"v1/devices/dev-A/shadow/reported",
		"iot.telemetry.spike.shard.1", // 非设备命名空间
		"$SYS/broker/uptime",
	}

	for _, topic := range down {
		if !IsDeviceDownlink(topic) {
			t.Errorf("%s 是下行，应返回 true", topic)
		}
	}
	for _, topic := range up {
		if IsDeviceDownlink(topic) {
			t.Errorf("%s 是上行（或非设备命名空间），应返回 false", topic)
		}
	}
}

func TestDeviceKeyFromTopic(t *testing.T) {
	cases := map[string]string{
		"v1/devices/dev-A/telemetry":               "dev-A",
		"v1/devices/dev-A/cmd/set":                 "dev-A",
		"v1/gateways/gw-1/devices/sub-1/telemetry": "sub-1",
		"v1/devices/":                              "",
		"v1/devices":                               "",
		"iot.telemetry.spike.shard.1":              "",
		"$SYS/broker/uptime":                       "",
		"v1/gateways/gw-1/devices/":                "",
	}
	for topic, want := range cases {
		if got := DeviceKeyFromTopic(topic); got != want {
			t.Errorf("DeviceKeyFromTopic(%q) = %q，期望 %q", topic, got, want)
		}
	}
}

// TestNode_离线分片稳定 保证同一设备恒定落同一分片（03 §4.2.1 的前提）。
func TestNode_离线分片稳定(t *testing.T) {
	n := &Node{opts: Options{OfflinePrefix: "offline.shard", Shards: 32}}

	first := n.ShardOf("dev-A")
	for i := 0; i < 100; i++ {
		if got := n.ShardOf("dev-A"); got != first {
			t.Fatalf("同一设备的分片应稳定，%d != %d", got, first)
		}
	}
	if s := n.ShardOf("dev-A"); s < 0 || s >= 32 {
		t.Fatalf("分片号越界: %d", s)
	}
	if got := n.OfflineSubject("dev-A"); got != fmt.Sprintf("offline.shard.%d.dev-A", first) {
		t.Fatalf("离线 subject 不符合预期: %s", got)
	}
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// ---------------------------------------------------------------------------
// ⑤ 接收端去重（(Origin, Seq)）
// ---------------------------------------------------------------------------

// TestCluster_去重拦截重复投递 验证同一条路由消息被 redelivery 时只注入一次。
//
// 不启动 consumeLoop，直接调用 handleRouteMessage 模拟「同一条 (Origin, Seq)
// 被投递两次」——这正是 NATS 在「注入成功但 Ack 未确认」时会发生的情形。
func TestCluster_去重拦截重复投递(t *testing.T) {
	url := natsURL(t)
	runID := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)

	n, err := New(context.Background(), Options{
		ID:            "dedup-" + runID,
		NATSURL:       url,
		RouteStream:   "IOT_ROUTE_" + runID,
		OfflineStream: "IOT_OFFLINE_" + runID,
		Shards:        8,
		Cursor:        NewMemCursor(),
		Logger:        slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}, NewMemLocator())
	if err != nil {
		t.Fatalf("构造节点失败: %v", err)
	}
	t.Cleanup(func() {
		n.Close()
		nc, err := nats.Connect(url)
		if err == nil {
			js, jerr := nc.JetStream()
			if jerr == nil {
				_ = js.DeleteStream("IOT_ROUTE_" + runID)
				_ = js.DeleteStream("IOT_OFFLINE_" + runID)
			}
			nc.Close()
		}
	})

	loc := newFakeLocal() // hasClient=true：目标设备在本节点
	n.local = loc         // 直接注入本地，不启动 consumeLoop（本测试只关心接收端去重）

	env := Envelope{Origin: "n1", Seq: 1, Topic: "v1/devices/dev-1/cmd/set", DeviceKey: "dev-1"}
	data, err := env.Encode()
	if err != nil {
		t.Fatalf("编码信封失败: %v", err)
	}

	// 同一条消息到达两次。
	n.handleRouteMessage(context.Background(), &nats.Msg{Data: data})
	n.handleRouteMessage(context.Background(), &nats.Msg{Data: data})

	if got := loc.count(); got != 1 {
		t.Fatalf("重复投递应被去重，实际注入 %d 次", got)
	}
	if got := n.Metrics().DedupHit.Load(); got != 1 {
		t.Fatalf("应命中 1 次去重，实际 %d 次", got)
	}
	if got := n.Metrics().DeliveredLocal.Load(); got != 1 {
		t.Fatalf("应注入 1 次，实际 %d 次", got)
	}
}
