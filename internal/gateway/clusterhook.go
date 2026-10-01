package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/SNCIC/odoo20iot/internal/cluster"
)

// nodeAPI 是 ClusterHook 对集群节点的依赖，收窄为它实际用到的四个方法。
//
// 抽象成接口而非直接持有 *cluster.Node，是为了让 Hook 的确定性单测
// 不依赖真实 NATS/Redis —— 测试注入一个记录调用的 fake 即可。
// 生产里 *cluster.Node 天然满足该接口。
type nodeAPI interface {
	Route(ctx context.Context, env cluster.Envelope) error
	Bind(ctx context.Context, deviceKey string) error
	Unbind(ctx context.Context, deviceKey string) error
	ReplayOffline(ctx context.Context, deviceKey string, deliver func(cluster.Envelope) error) (int, error)
}

// ClusterHook 是集群节点在本地 broker 上的接入点，实现 cluster.Local 与
// 三个 mochi Hook：
//
//   - `OnPublished`      把本地发出的**下行**消息交给集群层路由；
//   - `OnSessionEstablished` / `OnSubscribed`  登记设备位置并回放离线消息；
//   - `OnDisconnect`     清除设备位置。
//
// 为什么不路由上行：见 cluster.IsDeviceDownlink 的注释 —— 那不只是省流量，
// 而是防止「设备离线时自己的遥测被写进自己的离线队列」。
type ClusterHook struct {
	mqtt.HookBase

	ctx     context.Context
	node    nodeAPI
	server  *mqtt.Server
	metrics *Metrics
	logger  *slog.Logger

	// replayed 记录「本次连接是否已回放过离线消息」。
	//
	// 需要它是因为回放时机有两个：持久会话在会话建立时（订阅已恢复），
	// 干净会话在首次订阅之后（否则投递时还没有订阅者）。两者都要，但要防重。
	mu       sync.Mutex
	replayed map[string]bool
}

var (
	_ mqtt.Hook     = (*ClusterHook)(nil)
	_ cluster.Local = (*ClusterHook)(nil)
)

// NewClusterHook 构造集群 Hook。
func NewClusterHook(ctx context.Context, node nodeAPI, server *mqtt.Server, metrics *Metrics, log *slog.Logger) *ClusterHook {
	if metrics == nil {
		metrics = new(Metrics)
	}
	if log == nil {
		log = slog.Default()
	}
	return &ClusterHook{
		ctx:      ctx,
		node:     node,
		server:   server,
		metrics:  metrics,
		logger:   log,
		replayed: make(map[string]bool, 1024),
	}
}

// ID 实现 mqtt.Hook。
func (h *ClusterHook) ID() string { return "iot-cluster-route" }

// Provides 声明接管的 Hook 方法。
func (h *ClusterHook) Provides(b byte) bool {
	switch b {
	case mqtt.OnPublished, mqtt.OnSessionEstablished, mqtt.OnSubscribed, mqtt.OnDisconnect:
		return true
	default:
		return false
	}
}

// ---------- cluster.Local ----------

// Inject 把跨节点/离线消息注入本地 broker，复用其订阅匹配。
//
// 走 inline client：这是我们唯一能让消息「像本地发布一样」进入投递路径的办法，
// 也正因如此 A2 的 hook 必须显式放行 inline 报文（见 Hook.OnPublish）。
func (h *ClusterHook) Inject(env cluster.Envelope) error {
	if err := h.server.Publish(env.Topic, env.Payload, env.Retain, env.Qos); err != nil {
		return fmt.Errorf("注入本地 broker（topic=%s）: %w", env.Topic, err)
	}
	return nil
}

// HasClient 判断设备是否连在本节点。
func (h *ClusterHook) HasClient(deviceKey string) bool {
	_, ok := h.server.Clients.Get(deviceKey)
	return ok
}

// ---------- mochi hooks ----------

// OnPublished 把本地产生的下行消息交给集群层路由。
func (h *ClusterHook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	// inline 报文来自其他节点的注入或离线回放 —— 已经路由过了，不能再转一圈。
	if cl.Net.Inline {
		return
	}
	if !cluster.IsDeviceDownlink(pk.TopicName) {
		return // 上行由 svc-pipeline 直接消费 NATS，网关不做跨节点转发
	}

	env := cluster.Envelope{
		Topic:   pk.TopicName,
		Payload: append([]byte(nil), pk.Payload...),
		Qos:     pk.FixedHeader.Qos,
		Retain:  pk.FixedHeader.Retain,
	}
	if err := h.node.Route(h.ctx, env); err != nil {
		// 路由失败不能影响本地投递（已经完成了），但要能被发现。
		h.metrics.ClusterRouteFailTotal.Add(1)
		h.logger.Error("跨节点路由失败",
			"topic", pk.TopicName, "origin", cl.ID, "error", err)
	}
}

// OnSessionEstablished 登记设备位置；持久会话在此时回放离线消息（订阅已恢复）。
func (h *ClusterHook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	if err := h.node.Bind(h.ctx, cl.ID); err != nil {
		h.metrics.ClusterRouteFailTotal.Add(1)
		h.logger.Error("登记设备位置失败", "client", cl.ID, "error", err)
		return
	}

	if !pk.Connect.Clean {
		h.replayOnce(cl.ID)
	}
}

// OnSubscribed 在干净会话首次订阅后回放离线消息。
func (h *ClusterHook) OnSubscribed(cl *mqtt.Client, _ packets.Packet, _ []byte) {
	h.replayOnce(cl.ID)
}

// OnDisconnect 清除设备位置。
func (h *ClusterHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	h.mu.Lock()
	delete(h.replayed, cl.ID)
	h.mu.Unlock()

	if err := h.node.Unbind(h.ctx, cl.ID); err != nil {
		h.metrics.ClusterRouteFailTotal.Add(1)
		h.logger.Warn("清除设备位置失败", "client", cl.ID, "error", err)
	}
}

// replayOnce 保证一次连接只回放一次。
func (h *ClusterHook) replayOnce(clientID string) {
	h.mu.Lock()
	if h.replayed[clientID] {
		h.mu.Unlock()
		return
	}
	h.replayed[clientID] = true
	h.mu.Unlock()

	n, err := h.node.ReplayOffline(h.ctx, clientID, h.Inject)
	if err != nil {
		h.metrics.ClusterRouteFailTotal.Add(1)
		h.logger.Error("回放离线消息失败", "client", clientID, "error", err)
		return
	}
	if n > 0 {
		h.metrics.ClusterOfflineReplayTotal.Add(int64(n))
		h.logger.Info("已回放离线消息", "client", clientID, "count", n)
	}
}
