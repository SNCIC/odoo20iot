package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

// Redis 跨节点用例：验证 RedisLocator（设备位置共享）与 RedisCursor（离线游标共享）
// 在多节点下语义正确。默认跳过（依赖真实 Redis），显式开启：
//
//	IOT_NATS_URL=nats://100.64.0.3:28222 IOT_REDIS_URL=redis://100.64.0.3:28637/0 \
//	  go test ./internal/cluster -run TestCluster_Redis -count=1 -v
//
// MemLocator/MemCursor 只在单节点部署下正确（见 locator.go 注释），
// 这里的用例正是把「跨节点必须用 Redis」这条从注释变成可执行的断言。

func redisURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("IOT_REDIS_URL")
	if url == "" {
		t.Skip("未设置 IOT_REDIS_URL，跳过 Redis 跨节点用例（见文件头部说明）")
	}
	return url
}

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	opts, err := redis.ParseURL(redisURL(t))
	if err != nil {
		t.Fatalf("解析 Redis 地址失败: %v", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("连接 Redis 失败: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// startRedisNode 构造一个使用 Redis Locator/Cursor 的节点并启动消费循环。
// loc / cur 由两个节点**共享**（同一 Redis + 同一前缀），这正是跨节点共享的落点。
func startRedisNode(t *testing.T, nodeID, runID, url string, loc Locator, cur CursorStore, peers []string, local Local) *Node {
	t.Helper()

	n, err := New(context.Background(), Options{
		ID:            nodeID,
		Peers:         peers,
		NATSURL:       url,
		RouteStream:   "IOT_ROUTE_" + runID,
		RoutePrefix:   "iot.route",
		OfflineStream: "IOT_OFFLINE_" + runID,
		OfflinePrefix: "offline.shard",
		Shards:        8,
		AckWait:       time.Second,
		Cursor:        cur,
		Logger:        slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}, loc)
	if err != nil {
		t.Fatalf("构造节点 %s 失败: %v", nodeID, err)
	}
	if err := n.Start(local); err != nil {
		t.Fatalf("启动节点 %s 失败: %v", nodeID, err)
	}
	t.Cleanup(n.Close)
	return n
}

func cleanupCluster(t *testing.T, url, runID string, redisKeys ...string) {
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
}

// TestCluster_RedisLocator跨节点定向路由 验证设备位置存在 Redis 时，
// 任一节点都能查到并定向转发到持有该设备的节点。
func TestCluster_RedisLocator跨节点定向路由(t *testing.T) {
	url := natsURL(t)
	rdb := newTestRedis(t)
	runID := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)

	loc := NewRedisLocator(rdb, "gw:test:"+runID+":loc", time.Minute)
	cur := NewRedisCursor(rdb, "gw:test:"+runID+":cur")

	const devKey = "dev-1"
	cleanupCluster(t, url, runID)
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(),
			"gw:test:"+runID+":loc:"+devKey,
			"gw:test:"+runID+":cur:"+devKey).Err()
	})

	n1local, n2local := newFakeLocal(), newFakeLocal()
	n1 := startRedisNode(t, "n1-"+runID, runID, url, loc, cur, []string{"n2-" + runID}, n1local)
	n2 := startRedisNode(t, "n2-"+runID, runID, url, loc, cur, []string{"n1-" + runID}, n2local)

	ctx := context.Background()

	// 设备绑定到 n2：位置写进 Redis。
	if err := loc.Bind(ctx, devKey, n2.ID()); err != nil {
		t.Fatalf("登记设备位置失败: %v", err)
	}

	// n1 产生下行 → 查 Redis 得 n2 → 定向转发。
	if err := n1.Route(ctx, Envelope{
		Topic:   "v1/devices/" + devKey + "/cmd/set",
		Payload: []byte(`{"speed":3}`),
	}); err != nil {
		t.Fatalf("路由失败: %v", err)
	}

	if !waitFor(func() bool { return n2local.count() == 1 }, 3*time.Second) {
		t.Fatalf("n2 未收到跨节点消息，实际 %d 条", n2local.count())
	}
	if n1local.count() != 0 {
		t.Error("n1 不应收到自己发起的跨节点消息")
	}
	if got := n1.Metrics().RoutedDirect.Load(); got != 1 {
		t.Errorf("应定向转发 1 次，实际 %d 次", got)
	}
}

// TestCluster_RedisCursor跨节点回放续传 验证离线游标存在 Redis 时，
// 设备重连到**另一节点**不会重复回放已投递的离线消息。
func TestCluster_RedisCursor跨节点回放续传(t *testing.T) {
	url := natsURL(t)
	rdb := newTestRedis(t)
	runID := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)

	loc := NewRedisLocator(rdb, "gw:test:"+runID+":loc", time.Minute)
	cur := NewRedisCursor(rdb, "gw:test:"+runID+":cur")

	const devKey = "dev-2"
	cleanupCluster(t, url, runID)
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(),
			"gw:test:"+runID+":loc:"+devKey,
			"gw:test:"+runID+":cur:"+devKey).Err()
	})

	n1local, n2local := newFakeLocal(), newFakeLocal()
	n1 := startRedisNode(t, "n1-"+runID, runID, url, loc, cur, []string{"n2-" + runID}, n1local)
	n2 := startRedisNode(t, "n2-"+runID, runID, url, loc, cur, []string{"n1-" + runID}, n2local)

	ctx := context.Background()

	// 设备离线（未登记位置）：n1 产生 3 条下行 → 落离线队列。
	for i := 0; i < 3; i++ {
		if err := n1.Route(ctx, Envelope{
			Topic:   "v1/devices/" + devKey + "/cmd/set",
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("路由失败: %v", err)
		}
	}
	if got := n1.Metrics().OfflineQueued.Load(); got != 3 {
		t.Fatalf("应写入 3 条离线消息，实际 %d 条", got)
	}

	// 设备重连到 n2，回放 3 条，游标推进到 3（写入共享 Redis）。
	var first []Envelope
	if n, err := n2.ReplayOffline(ctx, devKey, func(env Envelope) error {
		first = append(first, env)
		return nil
	}); err != nil || n != 3 {
		t.Fatalf("n2 回放应得 3 条，实际 %d/%d，err=%v", n, len(first), err)
	}

	// 设备再重连到 n1：游标已跨节点共享，不应重复回放。
	var second []Envelope
	replayed, err := n1.ReplayOffline(ctx, devKey, func(env Envelope) error {
		second = append(second, env)
		return nil
	})
	if err != nil {
		t.Fatalf("n1 续传回放失败: %v", err)
	}
	if replayed != 0 || len(second) != 0 {
		t.Fatalf("游标应已跨节点推进，n1 不应重复回放，实际 %d 条", len(second))
	}
}
