// Package gateway 实现 MQTT 接入网关的准入与确认语义。
//
// 当前范围 = Phase 0 的 **A2 验证项**（ADR-001 的关键定制点，docs/03-ingestion.md §4.4）：
// **网关必须在内部事件总线确认持久化之后，才向设备回 PUBACK。**
// 若该语义在 mochi-mqtt 上不成立，ADR-001（内嵌 broker + NATS 总线）即不成立，
// 需回退 EMQX（备选路径见 ADR-001）。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// ErrPubackTimeout 表示「等待总线持久化确认」超时。
//
// 注意：它不是普通的可重试错误，而是 A2 的**兜底判据** ——
// 收到该错误的调用方必须放弃回 PUBACK，把重传责任交还给设备。
var ErrPubackTimeout = errors.New("等待 PublishAck 超时")

// Publisher 把消息投递到内部事件总线，并**同步等待持久化确认**。
//
// 契约（A2 的判据，不可弱化）：
//   - 返回 nil   ⇒ 消息已被总线持久化（JetStream PublishAck 已到达），可以回 PUBACK；
//   - 返回 error ⇒ 持久化结果**未知**，禁止回 PUBACK，由设备侧重传兜底。
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Close() error
}

// NATSPublisher 是 Publisher 的 NATS JetStream 实现（ADR-006）。
//
// 遵循 ADR-010：纯 Go 网络客户端，无 cgo 链接。
type NATSPublisher struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	stream string
}

var _ Publisher = (*NATSPublisher)(nil)

// NewNATSPublisher 连接 NATS 并准备 JetStream 上下文。
//
// 不做重连上限：网关与 NATS 同宿主，断连重试是常态而非异常，
// 上限会让网关在 NATS 滚动重启后永久失能。
func NewNATSPublisher(url, stream string, opts ...nats.Option) (*NATSPublisher, error) {
	opts = append([]nats.Option{
		nats.Name("iot-gateway"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}, opts...)

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("连接 NATS %q: %w", url, err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("初始化 JetStream: %w", err)
	}

	return &NATSPublisher{nc: nc, js: js, stream: stream}, nil
}

// EnsureStream 幂等地保证目标 Stream 存在。
//
// 遥测 Stream 的副本数按租户 RPO 要求设定（§4.4：要求 RPO=0 的租户用 Replicas=3）。
// 本方法只负责 Phase 0 的可用性，正式的分档配置属于部署期职责。
func (p *NATSPublisher) EnsureStream(subjects []string, replicas int) error {
	_, err := p.js.StreamInfo(p.stream)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrStreamNotFound):
	default:
		return fmt.Errorf("查询 Stream %q: %w", p.stream, err)
	}

	_, err = p.js.AddStream(&nats.StreamConfig{
		Name:      p.stream,
		Subjects:  subjects,
		Replicas:  replicas,
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
	})
	if err != nil {
		return fmt.Errorf("创建 Stream %q: %w", p.stream, err)
	}
	return nil
}

// Publish 同步等待 PublishAck。
//
// 三类失败被归一化为 ErrPubackTimeout：「NATS 自身超时」「ctx 到期」「ctx 被取消」
// （后者发生在网关优雅关闭时）。它们对调用方的含义完全一致 ——
// **持久化结果未知，禁止回 PUBACK** —— 因此收敛成一个判据，不让调用方去猜。
func (p *NATSPublisher) Publish(ctx context.Context, subject string, payload []byte) error {
	_, err := p.js.Publish(subject, payload, nats.Context(ctx))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrTimeout),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return fmt.Errorf("%w（subject=%s）: %w", ErrPubackTimeout, subject, err)
	default:
		return fmt.Errorf("jetstream publish %q: %w", subject, err)
	}
}

// Close 优雅关闭连接（Drain 会先冲刷在途请求）。
func (p *NATSPublisher) Close() error {
	if p.nc == nil {
		return nil
	}
	return p.nc.Drain()
}

// Metrics 是 A2 时序的计数器集合。
//
// 命名对齐 docs/03-ingestion.md §4.4 与 §4.5 的指标口径；
// 导出为 Prometheus 文本格式（见 WriteProm），不引入 metrics 客户端依赖。
type Metrics struct {
	// PublishTotal 进入 A2 时序的 QoS1 上报数（含重传）。
	PublishTotal atomic.Int64
	// PersistedTotal 已收到总线持久化确认的数（= 已回 PUBACK）。
	PersistedTotal atomic.Int64
	// TimeoutTotal 等待 PublishAck 超时的数（未回 PUBACK）。
	TimeoutTotal atomic.Int64
	// PublishErrorTotal 总线报错的数（未回 PUBACK）。
	PublishErrorTotal atomic.Int64
	// PubackWriteErrorTotal 总线已确认但回写 PUBACK 失败的数（设备将重传）。
	PubackWriteErrorTotal atomic.Int64
	// UnroutableTotal 无法归属到租户流的报文数。
	UnroutableTotal atomic.Int64
	// UnsupportedQosTotal 超出端侧契约的 QoS（当前为 QoS2）报文数。
	UnsupportedQosTotal atomic.Int64

	// ---- 认证与 ACL（06 §4：gw_connect_fail_total / gw_auth_cache_hit_ratio）----

	// AuthSuccessTotal 认证通过的连接数。
	AuthSuccessTotal atomic.Int64
	// ConnectFailTotal 认证失败的连接数（按 reason 细分见下）。
	ConnectFailTotal atomic.Int64
	// ACLDeniedTotal 被 ACL 拒绝的收发操作数。
	ACLDeniedTotal atomic.Int64

	failMu      sync.Mutex
	failReasons map[string]int64
}

// recordConnectFail 按原因累加连接失败数。
func (m *Metrics) recordConnectFail(reason string) {
	m.failMu.Lock()
	defer m.failMu.Unlock()
	if m.failReasons == nil {
		m.failReasons = make(map[string]int64, 8)
	}
	m.failReasons[reason]++
}

// connectFailReasons 返回按原因分类的连接失败快照。
func (m *Metrics) connectFailReasons() map[string]int64 {
	m.failMu.Lock()
	defer m.failMu.Unlock()

	out := make(map[string]int64, len(m.failReasons))
	for k, v := range m.failReasons {
		out[k] = v
	}
	return out
}

// WriteProm 以 Prometheus 文本格式导出计数器。
func (m *Metrics) WriteProm(w io.Writer) {
	writeMetric(w, "gw_publish_total", "进入 A2 时序的 QoS1 上报数", m.PublishTotal.Load())
	writeMetric(w, "gw_persisted_total", "已收到总线持久化确认的数（已回 PUBACK）", m.PersistedTotal.Load())
	writeMetric(w, "gw_puback_timeout_total", "等待 PublishAck 超时数（未回 PUBACK，设备将重传）", m.TimeoutTotal.Load())
	writeMetric(w, "gw_publish_error_total", "总线报错数（未回 PUBACK）", m.PublishErrorTotal.Load())
	writeMetric(w, "gw_puback_write_error_total", "回写 PUBACK 失败数", m.PubackWriteErrorTotal.Load())
	writeMetric(w, "gw_unroutable_total", "无法路由的上报数", m.UnroutableTotal.Load())
	writeMetric(w, "gw_unsupported_qos_total", "超出端侧契约的 QoS 报文数", m.UnsupportedQosTotal.Load())

	writeMetric(w, "gw_auth_success_total", "设备认证通过的连接数", m.AuthSuccessTotal.Load())
	writeMetric(w, "gw_connect_fail_total", "设备认证失败的连接数（见 reason 维度）", m.ConnectFailTotal.Load())
	writeMetric(w, "gw_acl_denied_total", "被 ACL 拒绝的收发操作数", m.ACLDeniedTotal.Load())

	// 按原因分类（06 §4 的 gw_connect_fail_total{reason}）。固定顺序输出，
	// 便于人工比对与抓取。
	reasons := m.connectFailReasons()
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = fmt.Fprintf(w, "gw_connect_fail_total{reason=%q} %d\n", k, reasons[k])
	}
}

func writeMetric(w io.Writer, name, help string, v int64) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
}
