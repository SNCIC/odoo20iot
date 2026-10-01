package gateway

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/SNCIC/odoo20iot/internal/cluster"
)

// fakeNode 记录 ClusterHook 对集群节点的调用，让 Hook 的单测不依赖 NATS/Redis。
type fakeNode struct {
	mu           sync.Mutex
	routes       []cluster.Envelope
	binds        []string
	unbinds      []string
	replays      []string
	replayResult int
	replayErr    error
}

var _ nodeAPI = (*fakeNode)(nil)

func (f *fakeNode) Route(_ context.Context, env cluster.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.routes = append(f.routes, env)
	return nil
}

func (f *fakeNode) Bind(_ context.Context, deviceKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.binds = append(f.binds, deviceKey)
	return nil
}

func (f *fakeNode) Unbind(_ context.Context, deviceKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unbinds = append(f.unbinds, deviceKey)
	return nil
}

func (f *fakeNode) ReplayOffline(_ context.Context, deviceKey string, _ func(cluster.Envelope) error) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replays = append(f.replays, deviceKey)
	return f.replayResult, f.replayErr
}

func (f *fakeNode) routeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.routes)
}

func (f *fakeNode) lastRoute() (cluster.Envelope, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.routes) == 0 {
		return cluster.Envelope{}, false
	}
	return f.routes[len(f.routes)-1], true
}

func (f *fakeNode) bindCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.binds)
}

func (f *fakeNode) unbindCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.unbinds)
}

func (f *fakeNode) replayCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.replays)
}

// TestClusterHook_OnPublished_SkipsInline 验证 inline 报文不会被再次路由——
// 这是跨节点注入不转圈的根基（A3 防循环）。
func TestClusterHook_OnPublished_SkipsInline(t *testing.T) {
	node := &fakeNode{}
	h := NewClusterHook(context.Background(), node, nil, new(Metrics), testLogger())

	cl := &mqtt.Client{ID: "inline"}
	cl.Net.Inline = true

	h.OnPublished(cl, packetWithTopic("v1/devices/dev-1/cmd/set"))

	if got := node.routeCount(); got != 0 {
		t.Fatalf("inline 报文不应再次路由，实际路由 %d 次", got)
	}
}

// TestClusterHook_OnPublished_RoutesOnlyDeviceDownlink 验证只有设备下行 topic 才路由。
func TestClusterHook_OnPublished_RoutesOnlyDeviceDownlink(t *testing.T) {
	node := &fakeNode{}
	h := NewClusterHook(context.Background(), node, nil, new(Metrics), testLogger())
	cl := &mqtt.Client{ID: "dev-1"}

	// 上行（telemetry）不路由。
	h.OnPublished(cl, packetWithTopic("v1/devices/dev-1/telemetry"))
	if got := node.routeCount(); got != 0 {
		t.Fatalf("上行不应路由，实际 %d 次", got)
	}

	// 下行（cmd/set）路由。
	h.OnPublished(cl, packetWithTopic("v1/devices/dev-1/cmd/set"))
	if got := node.routeCount(); got != 1 {
		t.Fatalf("下行应路由 1 次，实际 %d 次", got)
	}

	env, ok := node.lastRoute()
	if !ok {
		t.Fatal("未取到路由信封")
	}
	if env.Topic != "v1/devices/dev-1/cmd/set" {
		t.Fatalf("路由 topic 错误: %s", env.Topic)
	}
	if string(env.Payload) != "" {
		t.Fatalf("空载荷不应被改写: %q", env.Payload)
	}
}

// TestClusterHook_Inject_PublishesInline 做端到端验证：
// Inject 通过 inline client 注入，本地订阅者能收到消息，且注入本身不再触发路由。
func TestClusterHook_Inject_PublishesInline(t *testing.T) {
	node := &fakeNode{}

	server := mqtt.New(&mqtt.Options{Logger: testLogger(), InlineClient: true})
	if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("装载认证放行 hook 失败: %v", err)
	}
	h := NewClusterHook(context.Background(), node, server, new(Metrics), testLogger())
	if err := server.AddHook(h, nil); err != nil {
		t.Fatalf("装载集群 hook 失败: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	if err := server.AddListener(listeners.NewNet("mqtt", ln)); err != nil {
		t.Fatalf("装载监听器失败: %v", err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve() }()
	t.Cleanup(func() { _ = server.Close(); <-serveErr })

	dev := dialTestDevice(t, ln.Addr().String(), "dev-1")
	dev.connect("dev-1")
	if code := dev.subscribe("v1/devices/dev-1/cmd/set", 1); code >= 0x80 {
		t.Fatalf("订阅失败，SUBACK 返回码 %d", code)
	}

	// 注入一条跨节点下行命令。
	env := cluster.Envelope{
		Topic:   "v1/devices/dev-1/cmd/set",
		Payload: []byte(`{"speed":3}`),
		Qos:     1,
	}
	if err := h.Inject(env); err != nil {
		t.Fatalf("注入失败: %v", err)
	}

	// 设备必须收到该下行消息。
	got, err := dev.read(3 * time.Second)
	if err != nil {
		t.Fatalf("设备未收到注入的消息: %v", err)
	}
	if got.FixedHeader.Type != packets.Publish {
		t.Fatalf("期望 PUBLISH，得到报文类型 %d", got.FixedHeader.Type)
	}
	if got.TopicName != "v1/devices/dev-1/cmd/set" {
		t.Fatalf("topic 错误: %s", got.TopicName)
	}
	if string(got.Payload) != `{"speed":3}` {
		t.Fatalf("载荷错误: %s", got.Payload)
	}

	// 注入路径不得再次触发路由（防循环）。
	if got := node.routeCount(); got != 0 {
		t.Fatalf("inline 注入不应触发路由，实际 %d 次", got)
	}
}

// TestClusterHook_SessionLifecycle 覆盖连接建立/订阅/断开的绑定、回放与解绑时序，
// 以及「同一连接只回放一次」的防重。
func TestClusterHook_SessionLifecycle(t *testing.T) {
	node := &fakeNode{}
	h := NewClusterHook(context.Background(), node, nil, new(Metrics), testLogger())
	cl := &mqtt.Client{ID: "dev-1"}

	// 干净会话：建立时只绑定，不回放（此时还没有订阅）。
	h.OnSessionEstablished(cl, packets.Packet{Connect: packets.ConnectParams{Clean: true}})
	if got := node.bindCount(); got != 1 {
		t.Fatalf("建立会话应绑定 1 次，实际 %d 次", got)
	}
	if got := node.replayCount(); got != 0 {
		t.Fatalf("干净会话建立时不应回放，实际 %d 次", got)
	}

	// 首次订阅触发回放。
	h.OnSubscribed(cl, packets.Packet{}, nil)
	if got := node.replayCount(); got != 1 {
		t.Fatalf("首次订阅应回放 1 次，实际 %d 次", got)
	}

	// 重复订阅不重复回放。
	h.OnSubscribed(cl, packets.Packet{}, nil)
	if got := node.replayCount(); got != 1 {
		t.Fatalf("同一连接不应重复回放，实际 %d 次", got)
	}

	// 断开解绑。
	h.OnDisconnect(cl, nil, false)
	if got := node.unbindCount(); got != 1 {
		t.Fatalf("断开应解绑 1 次，实际 %d 次", got)
	}
}

// TestClusterHook_PersistentSessionReplaysOnEstablish 验证持久会话（Clean=false）
// 在会话建立时（订阅已恢复）立即回放离线消息。
func TestClusterHook_PersistentSessionReplaysOnEstablish(t *testing.T) {
	node := &fakeNode{}
	h := NewClusterHook(context.Background(), node, nil, new(Metrics), testLogger())
	cl := &mqtt.Client{ID: "dev-1"}

	h.OnSessionEstablished(cl, packets.Packet{Connect: packets.ConnectParams{Clean: false}})

	if got := node.bindCount(); got != 1 {
		t.Fatalf("应绑定 1 次，实际 %d 次", got)
	}
	if got := node.replayCount(); got != 1 {
		t.Fatalf("持久会话建立时应回放 1 次，实际 %d 次", got)
	}
}
