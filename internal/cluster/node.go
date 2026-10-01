package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// Local 是集群节点对**本地 broker** 的依赖。
//
// 集群层不直接依赖 mochi：投递与在线判定都是 gateway 的职责，
// 这样路由逻辑可以脱离 broker 单独测试。
type Local interface {
	// Inject 把跨节点/离线消息注入本地 broker，复用其订阅匹配与投递。
	Inject(env Envelope) error
	// HasClient 判断该设备是否连在本节点。
	HasClient(deviceKey string) bool
}

// Options 是节点配置。
type Options struct {
	// ID 是本节点 id，必须全局唯一且稳定（重启后不变）。
	ID string
	// Peers 是其他节点的 id 列表。
	Peers []string

	NATSURL string

	// RouteStream / RoutePrefix 是路由通道。
	RouteStream string
	RoutePrefix string

	// OfflineStream / OfflinePrefix / Shards 是离线队列。
	OfflineStream string
	OfflinePrefix string
	Shards        int

	// AckWait 是路由消息的确认超时。**它决定了「节点被 kill」后多久重投**，
	// 因此测试要把「恢复」放在这个窗口之后，才能证明重投真的发生。
	AckWait time.Duration

	// Cursor 保存离线队列的投递游标（按设备）。生产用 Redis。
	Cursor CursorStore

	Logger  *slog.Logger
	Metrics *Metrics
}

// CursorStore 保存「某设备的离线消息投递到哪了」。
//
// 它必须是**跨节点共享**的：设备重连可能落到任意节点（03 §1.5）。
type CursorStore interface {
	Get(ctx context.Context, deviceKey string) (uint64, error)
	Set(ctx context.Context, deviceKey string, seq uint64) error
}

// Metrics 是集群层的计数器。
type Metrics struct {
	RoutedDirect   atomic.Int64 // 定向转发到其他节点
	RoutedBroad    atomic.Int64 // 广播兜底
	RoutedLocal    atomic.Int64 // 目标就在本节点，无需跨节点
	DeliveredLocal atomic.Int64 // 消费到并成功注入本地
	AckFailures    atomic.Int64 // 注入失败导致不 ACK（会重投）
	OfflineQueued  atomic.Int64 // 写入离线队列
	OfflineReplay  atomic.Int64 // 离线回放成功条数
	OfflineDropped atomic.Int64 // 离线回放失败条数
	SelfDropped    atomic.Int64 // 收到自己的广播，丢弃
	DedupHit       atomic.Int64 // 接收端按 (Origin, Seq) 去重命中的条数
}

// Node 是一个网关节点在集群里的身份。
type Node struct {
	opts    Options
	locator Locator

	nc *nats.Conn
	js nats.JetStreamContext

	seq atomic.Uint64

	// dedup 是接收端去重缓存：拦截 NATS redelivery 导致的重复注入。
	dedup *dedupCache

	mu       sync.Mutex
	local    Local
	consume  context.CancelFunc
	consumed chan struct{}
	stopped  bool

	metrics *Metrics
	logger  *slog.Logger
}

// New 连接 NATS 并准备好路由与离线两条通道。
func New(ctx context.Context, opts Options, locator Locator) (*Node, error) {
	if opts.ID == "" {
		return nil, errors.New("节点 id 不能为空")
	}
	if locator == nil {
		return nil, errors.New("需要 Locator：跨节点投递依赖设备位置")
	}
	if opts.RouteStream == "" {
		opts.RouteStream = "IOT_ROUTE"
	}
	if opts.RoutePrefix == "" {
		opts.RoutePrefix = "iot.route"
	}
	if opts.OfflineStream == "" {
		opts.OfflineStream = "IOT_OFFLINE"
	}
	if opts.OfflinePrefix == "" {
		opts.OfflinePrefix = "offline.shard"
	}
	if opts.Shards <= 0 {
		opts.Shards = 32
	}
	if opts.AckWait <= 0 {
		opts.AckWait = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = new(Metrics)
	}
	if opts.Cursor == nil {
		opts.Cursor = NewMemCursor()
	}

	nc, err := nats.Connect(opts.NATSURL, nats.Name("iot-gateway-"+opts.ID), nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("节点 %s 连接 NATS: %w", opts.ID, err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("初始化 JetStream: %w", err)
	}

	n := &Node{opts: opts, locator: locator, nc: nc, js: js, metrics: opts.Metrics, logger: opts.Logger, dedup: newDedupCache()}
	if err := n.ensureStreams(); err != nil {
		nc.Close()
		return nil, err
	}
	return n, nil
}

// ensureStreams 幂等地准备路由流与离线流。
func (n *Node) ensureStreams() error {
	route := &nats.StreamConfig{
		Name:      n.opts.RouteStream,
		Subjects:  []string{n.opts.RoutePrefix + ".>"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    time.Hour, // 路由消息只需存活到被消费或重投
		Replicas:  1,
	}
	if _, err := n.js.StreamInfo(route.Name); errors.Is(err, nats.ErrStreamNotFound) {
		if _, err := n.js.AddStream(route); err != nil {
			return fmt.Errorf("创建路由流 %s: %w", route.Name, err)
		}
	}

	// 离线流：**一个流承载全部设备**，分片体现在 subject 层级
	//（`offline.shard.{i}.{device_key}`），而不是「一个分片一个流」。
	// 这样流数量与设备数、分片数都解耦，元数据规模恒定 —— 实测见 03 §1.4.1 ③。
	offline := &nats.StreamConfig{
		Name:              n.opts.OfflineStream,
		Subjects:          []string{n.opts.OfflinePrefix + ".>"},
		Storage:           nats.FileStorage,
		Retention:         nats.LimitsPolicy,
		MaxAge:            DefaultOfflineTTL,
		MaxMsgsPerSubject: DefaultOfflineMaxPerDevice,
		Replicas:          1,
	}
	if _, err := n.js.StreamInfo(offline.Name); errors.Is(err, nats.ErrStreamNotFound) {
		if _, err := n.js.AddStream(offline); err != nil {
			return fmt.Errorf("创建离线流 %s: %w", offline.Name, err)
		}
	}
	return nil
}

// 03 §1.5 的离线队列口径。
const (
	DefaultOfflineTTL          = 24 * time.Hour
	DefaultOfflineMaxPerDevice = 1000
)

// Metrics 返回计数器。
func (n *Node) Metrics() *Metrics { return n.metrics }

// ShardOf 返回 deviceKey 的离线分片号（与 P0-3 的分片常量同源：稳定哈希）。
func (n *Node) ShardOf(deviceKey string) int {
	return hashShard(deviceKey, n.opts.Shards)
}

// OfflineSubject 返回该设备的离线 subject。
func (n *Node) OfflineSubject(deviceKey string) string {
	return fmt.Sprintf("%s.%d.%s", n.opts.OfflinePrefix, n.ShardOf(deviceKey), deviceKey)
}

// RouteSubject 返回目标节点的路由 subject。
func (n *Node) RouteSubject(nodeID string) string {
	return n.opts.RoutePrefix + "." + nodeID
}

// Start 启动路由消费循环。deliver 之外的一切（在线判定、离线落盘）由本节点自理。
func (n *Node) Start(local Local) error {
	if local == nil {
		return errors.New("需要 Local：集群层不直接依赖 broker")
	}

	n.mu.Lock()
	if n.consume != nil {
		n.mu.Unlock()
		return errors.New("消费循环已启动")
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.local = local
	n.consume = cancel
	n.stopped = false
	n.consumed = make(chan struct{})
	done := n.consumed
	n.mu.Unlock()

	go func() {
		defer close(done)
		n.consumeLoop(ctx)
	}()
	return nil
}

// Stop 停止消费循环并等待退出。
//
// **未 ACK 的路由消息会在 AckWait 之后重投** —— 这正是「节点被 kill 不丢消息」
// 的机制；本方法模拟的是优雅停止，与 kill 的差别只在于是否等 ACK。
func (n *Node) Stop() {
	n.mu.Lock()
	cancel, done := n.consume, n.consumed
	n.consume, n.stopped = nil, true
	n.mu.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// Close 关闭 NATS 连接。
func (n *Node) Close() {
	n.Stop()
	if n.nc != nil {
		n.nc.Close()
	}
}

// consumeLoop 用 durable pull consumer 消费本节点的路由 subject。
//
// 选 pull 而不是 push：拉取节奏可控，且「注入失败就不 ACK」的语义直白，
// 不会出现 push 模式下 prefetch 把未处理消息堆在内存里的问题。
func (n *Node) consumeLoop(ctx context.Context) {
	durable := "gw-route-" + n.opts.ID
	subject := n.RouteSubject(n.opts.ID)

	sub, err := n.js.PullSubscribe(subject, durable,
		nats.BindStream(n.opts.RouteStream),
		nats.ManualAck(),
		nats.AckWait(n.opts.AckWait),
		nats.MaxDeliver(-1), // 永不放弃：宁可重投也不能丢
	)
	if err != nil {
		n.logger.Error("订阅路由通道失败", "node", n.opts.ID, "subject", subject, "error", err)
		return
	}
	defer func() { _ = sub.Unsubscribe() }()

	n.logger.Info("路由消费已就绪", "node", n.opts.ID, "subject", subject, "durable", durable)

	for {
		if ctx.Err() != nil {
			return
		}

		msgs, err := sub.Fetch(64, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.Canceled) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			n.logger.Warn("拉取路由消息失败", "node", n.opts.ID, "error", err)
			continue
		}

		for _, m := range msgs {
			n.handleRouteMessage(ctx, m)
		}
	}
}

// handleRouteMessage 处理一条路由消息：注入本地，成功才 ACK。
func (n *Node) handleRouteMessage(ctx context.Context, m *nats.Msg) {
	env, err := DecodeEnvelope(m.Data)
	if err != nil {
		// 报文无法解析属于**不可重试**的错误：一直不 ACK 只会堵住队列。
		n.logger.Error("丢弃无法解析的路由消息", "node", n.opts.ID, "error", err)
		_ = m.Term()
		return
	}

	if env.Origin == n.opts.ID {
		// 广播模式下的自环，丢弃。
		n.metrics.SelfDropped.Add(1)
		_ = m.Ack()
		return
	}

	// 接收端去重：同一条 (Origin, Seq) 已成功处理过（NATS redelivery），
	// 直接确认而不重复注入。只覆盖进程内的 redelivery，见 dedupCache 注释。
	if n.dedup.seen(env.Origin, env.Seq) {
		n.metrics.DedupHit.Add(1)
		_ = m.Ack()
		return
	}

	n.mu.Lock()
	local := n.local
	n.mu.Unlock()

	// 目标设备不在本节点 → 落离线队列（03 §1.5），而不是丢弃。
	if env.DeviceKey != "" && (local == nil || !local.HasClient(env.DeviceKey)) {
		if err := n.EnqueueOffline(ctx, env); err != nil {
			// 离线落盘失败必须**不 ACK**：宁可重投也不能丢。
			n.metrics.AckFailures.Add(1)
			n.logger.Error("写入离线队列失败，稍后重投",
				"node", n.opts.ID, "device_key", env.DeviceKey, "error", err)
			return
		}
		n.dedup.mark(env.Origin, env.Seq)
		_ = m.Ack()
		return
	}

	if local == nil {
		n.metrics.AckFailures.Add(1)
		return
	}

	if err := local.Inject(env); err != nil {
		n.metrics.AckFailures.Add(1)
		n.logger.Warn("注入本地失败，稍后重投", "node", n.opts.ID, "topic", env.Topic, "error", err)
		return
	}

	n.dedup.mark(env.Origin, env.Seq)
	n.metrics.DeliveredLocal.Add(1)
	_ = m.Ack()
}

// Route 是发布侧的入口：决定这条消息要不要、以及发给谁。
//
// 三条路径：
//  1. **目标设备在本节点**（或消息不属于设备命名空间且本节点有订阅者）→ 不跨节点；
//  2. **目标设备在别的节点** → 定向转发到该节点（1 条消息）；
//  3. **设备不在线** → 直接落离线队列；
//  4. **非设备命名空间** → 广播兜底（应用侧订阅数量少，代价可接受）。
func (n *Node) Route(ctx context.Context, env Envelope) error {
	origin := env.Origin
	if origin == "" {
		env.Origin = n.opts.ID
	}
	if env.PublishedAt.IsZero() {
		env.PublishedAt = time.Now()
	}
	env.Seq = n.seq.Add(1)
	if env.DeviceKey == "" {
		env.DeviceKey = DeviceKeyFromTopic(env.Topic)
	}

	if env.DeviceKey == "" {
		return n.broadcast(ctx, env)
	}

	node, online, err := n.locator.NodeOf(ctx, env.DeviceKey)
	if err != nil {
		// 位置查询失败不能当成「设备不在线」——那会把在线消息误判成离线。
		return fmt.Errorf("查询设备位置 %s: %w", env.DeviceKey, err)
	}
	if !online {
		return n.EnqueueOffline(ctx, env)
	}
	if node == n.opts.ID {
		n.metrics.RoutedLocal.Add(1)
		return nil // 本地投递由 broker 的正常路径完成
	}

	n.metrics.RoutedDirect.Add(1)
	return n.publishTo(ctx, node, env)
}

// broadcast 把消息发给所有对端（非设备命名空间的兜底路径）。
func (n *Node) broadcast(ctx context.Context, env Envelope) error {
	for _, peer := range n.opts.Peers {
		if peer == n.opts.ID {
			continue
		}
		n.metrics.RoutedBroad.Add(1)
		if err := n.publishTo(ctx, peer, env); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) publishTo(ctx context.Context, nodeID string, env Envelope) error {
	data, err := env.Encode()
	if err != nil {
		return err
	}
	// 同步等待 PublishAck：跨节点投递是 at-least-once，
	// 「发出去就不管」会让节点故障时的丢失无法察觉。
	if _, err := n.js.Publish(n.RouteSubject(nodeID), data, nats.Context(ctx)); err != nil {
		return fmt.Errorf("转发到节点 %s: %w", nodeID, err)
	}
	return nil
}

// EnqueueOffline 把消息写入设备的离线队列。
func (n *Node) EnqueueOffline(ctx context.Context, env Envelope) error {
	if env.DeviceKey == "" {
		return errors.New("无法确定 device_key，不能入离线队列")
	}
	data, err := env.Encode()
	if err != nil {
		return err
	}
	if _, err := n.js.Publish(n.OfflineSubject(env.DeviceKey), data, nats.Context(ctx)); err != nil {
		return fmt.Errorf("写入离线队列 %s: %w", env.DeviceKey, err)
	}
	n.metrics.OfflineQueued.Add(1)
	return nil
}

// ReplayOffline 把设备的离线消息按顺序回放给 deliver，并推进游标。
//
// 游标**先写 Redis 再投递**是不行的（投递失败就永久丢），因此这里：
// 逐条投递 → 投递成功才推进游标。失败即停，下次重连继续 —— 至少一次。
func (n *Node) ReplayOffline(ctx context.Context, deviceKey string, deliver func(Envelope) error) (int, error) {
	from, err := n.opts.Cursor.Get(ctx, deviceKey)
	if err != nil {
		return 0, fmt.Errorf("读取离线游标 %s: %w", deviceKey, err)
	}

	subject := n.OfflineSubject(deviceKey)
	sub, err := n.js.PullSubscribe(subject, "",
		nats.BindStream(n.opts.OfflineStream),
		nats.StartSequence(from+1),
		nats.AckNone(), // 游标由我们自己推进，不走 JetStream 的 ACK
	)
	if err != nil {
		return 0, fmt.Errorf("订阅离线队列 %s: %w", subject, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	var (
		count   int
		lastSeq = from
	)

	for {
		msgs, err := sub.Fetch(128, nats.MaxWait(300*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				break // 拉空即回放结束
			}
			return count, fmt.Errorf("拉取离线消息: %w", err)
		}

		for _, m := range msgs {
			env, err := DecodeEnvelope(m.Data)
			if err != nil {
				n.metrics.OfflineDropped.Add(1)
				continue // 坏消息跳过，否则会永远卡住回放
			}
			if err := deliver(env); err != nil {
				n.metrics.OfflineDropped.Add(1)
				return count, fmt.Errorf("回放离线消息失败（游标停在 %d，下次继续）: %w", lastSeq, err)
			}

			if meta, err := m.Metadata(); err == nil {
				lastSeq = meta.Sequence.Stream
				if err := n.opts.Cursor.Set(ctx, deviceKey, lastSeq); err != nil {
					return count, fmt.Errorf("推进离线游标 %s: %w", deviceKey, err)
				}
			}
			n.metrics.OfflineReplay.Add(1)
			count++
		}
	}
	return count, nil
}

// Bind / Unbind 代理到位置注册表，供 gateway 的连接生命周期调用。
func (n *Node) Bind(ctx context.Context, deviceKey string) error {
	return n.locator.Bind(ctx, deviceKey, n.opts.ID)
}

// Unbind 代理到位置注册表。
func (n *Node) Unbind(ctx context.Context, deviceKey string) error {
	return n.locator.Unbind(ctx, deviceKey, n.opts.ID)
}

// ID 返回本节点 id。
func (n *Node) ID() string { return n.opts.ID }

// MemCursor 是 CursorStore 的内存实现。
//
// ⚠️ 只在单节点部署下正确：设备重连到别的节点时读不到游标，
// 会重复回放。多节点必须用 Redis（03 §1.5「会话元数据持久化到 Redis」）。
type MemCursor struct {
	mu   sync.Mutex
	seqs map[string]uint64
}

// NewMemCursor 构造内存游标。
func NewMemCursor() *MemCursor {
	return &MemCursor{seqs: make(map[string]uint64, 1024)}
}

// Get 实现 CursorStore。
func (c *MemCursor) Get(_ context.Context, deviceKey string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seqs[deviceKey], nil
}

// Set 实现 CursorStore。
func (c *MemCursor) Set(_ context.Context, deviceKey string, seq uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seqs[deviceKey] = seq
	return nil
}

// hashShard 与网关的分片常量同源（03 §4.2.1：同一设备恒定落同一分片）。
func hashShard(key string, shards int) int {
	if shards <= 0 {
		shards = 32
	}
	h := fnv1a(key)
	return int(h % uint32(shards))
}

func fnv1a(s string) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}
