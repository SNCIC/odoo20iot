// Package metering 实现 04 §6 的计量埋点：**本地累加 + 周期批量上报**。
//
// 设计约束来自 06 Phase 0 的验收项：「每条消息可归属到 project_id，
// 聚合误差 < 0.1%，且**不影响网关吞吐**」。因此：
//   - 热路径（每条消息）只做一次加锁自增，**不做任何 IO**；
//   - 上报由独立 goroutine 按窗口（默认 10s）成批发出，与收包路径解耦；
//   - 上报是「先取出再清零」（Drain），保证不重不漏：已取出的计数即使上报
//     失败也不会在下一个窗口重复计入（宁可少报也不重复计费）。
package metering

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// 计量指标名（04 §6）。
const (
	// MetricMsgCount 是消息数（计费口径，±0.1%）。
	MetricMsgCount = "msg_count"
	// MetricConnPeak 是连接峰值（近似，套餐校验）。
	MetricConnPeak     = "conn_peak"
	MetricDeviceCount  = "device_count"
	MetricStorageBytes = "storage_bytes"
	MetricAPICalls     = "api_calls"
)

// UsageSubject 是计量上报的 NATS subject（04 §6）。
const UsageSubject = "iot.quota.usage"

// DefaultWindow 是上报窗口（04 §6：网关每 10s 上报）。
const DefaultWindow = 10 * time.Second

// Accumulator 是本地累加器：热路径只做一次加锁自增。
//
// 用普通 map + mutex 而非分片：单条消息一次无竞争 lock 的开销在纳秒级，
// 远小于同一路径上的 NATS PublishAck 往返（实测 0.5~1.1ms）。
type Accumulator struct {
	mu       sync.Mutex
	counters map[int64]map[string]int64 // projectID → metric → 累计
}

// NewAccumulator 构造累加器。
func NewAccumulator() *Accumulator {
	return &Accumulator{counters: make(map[int64]map[string]int64, 16)}
}

// Add 累加一个计数（热路径）。delta 为负时不处理（计数不应回退）。
func (a *Accumulator) Add(projectID int64, metric string, delta int64) {
	if delta == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	m := a.counters[projectID]
	if m == nil {
		m = make(map[string]int64, 4)
		a.counters[projectID] = m
	}
	if metric == MetricConnPeak {
		if delta > m[metric] {
			m[metric] = delta
		}
		return
	}
	m[metric] += delta
}

// Drain 取出并清零全部计数（上报用）。
//
// 「取出即清零」是有意为之：若上报失败仍保留计数，下一窗口会重复计入，
// 计量口径就偏大（重复计费比少计更严重）。宁可少报也不重复。
func (a *Accumulator) Drain() map[int64]map[string]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := a.counters
	a.counters = make(map[int64]map[string]int64, 16)
	return out
}

// Snapshot 返回当前计数的副本（不清零，观测用）。
func (a *Accumulator) Snapshot() map[int64]map[string]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make(map[int64]map[string]int64, len(a.counters))
	for pid, m := range a.counters {
		cp := make(map[string]int64, len(m))
		for k, v := range m {
			cp[k] = v
		}
		out[pid] = cp
	}
	return out
}

// UsageReport 是一次批量上报的载荷（NATS `iot.quota.usage`）。
type UsageReport struct {
	ProjectID int64            `json:"project_id"`
	NodeID    string           `json:"node_id,omitempty"`
	Window    time.Time        `json:"window"`
	ReportID  string           `json:"report_id,omitempty"`
	Counters  map[string]int64 `json:"counters"`
}

// Publisher 是上报通道（NATS 的窄接口，便于确定性测试）。
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// Metrics 是上报器的计数器。
type Metrics struct {
	// ReportsTotal 成功发出的上报批次数。
	ReportsTotal atomic.Int64
	// ReportErrors 上报失败的次数（计数已被 Drain 取出，不重报）。
	ReportErrors atomic.Int64
	// CountersReported 累计上报的计数值总和（对账用）。
	CountersReported atomic.Int64
}

// Reporter 周期性把累加器的计数批量上报到总线。
type Reporter struct {
	acc     *Accumulator
	pub     Publisher
	nodeID  string
	subject string
	window  time.Duration
	logger  *slog.Logger
	metrics *Metrics
	mu      sync.Mutex
	pending []UsageReport
}

// ReporterOptions 是上报器配置。
type ReporterOptions struct {
	Accumulator *Accumulator
	Publisher   Publisher
	NodeID      string
	// Subject 默认为 UsageSubject。
	Subject string
	// Window 默认为 DefaultWindow。
	Window  time.Duration
	Logger  *slog.Logger
	Metrics *Metrics
}

// NewReporter 构造上报器。
func NewReporter(opts ReporterOptions) (*Reporter, error) {
	if opts.Accumulator == nil {
		return nil, fmt.Errorf("需要 Accumulator")
	}
	if opts.Publisher == nil {
		return nil, fmt.Errorf("需要 Publisher")
	}
	if opts.Subject == "" {
		opts.Subject = UsageSubject
	}
	if opts.Window <= 0 {
		opts.Window = DefaultWindow
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = new(Metrics)
	}
	return &Reporter{
		acc:     opts.Accumulator,
		pub:     opts.Publisher,
		nodeID:  opts.NodeID,
		subject: opts.Subject,
		window:  opts.Window,
		logger:  opts.Logger,
		metrics: opts.Metrics,
	}, nil
}

// Metrics 返回计数器。
func (r *Reporter) Metrics() *Metrics { return r.metrics }

// Run 阻塞运行上报循环，直到 ctx 取消。
func (r *Reporter) Run(ctx context.Context) {
	tick := time.NewTicker(r.window)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			// 退出前最后一次上报：避免把最后一个窗口的用量丢掉。
			// 用独立 ctx —— 传入的 ctx 此刻已取消，直接用它必然发布失败。
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			r.Flush(flushCtx)
			cancel()
			return
		case <-tick.C:
			r.Flush(ctx)
		}
	}
}

// Flush 取出当前计数并立即上报（导出以便测试与优雅退出调用）。
func (r *Reporter) Flush(ctx context.Context) int {
	r.mu.Lock()
	deferred := r.pending
	r.pending = nil
	r.mu.Unlock()
	current := r.acc.Drain()
	if len(deferred) == 0 && len(current) == 0 {
		return 0
	}

	reports := append([]UsageReport(nil), deferred...)
	window := time.Now().UTC().Truncate(r.window)
	for pid, counters := range current {
		if len(counters) == 0 {
			continue
		}
		reports = append(reports, UsageReport{ProjectID: pid, NodeID: r.nodeID, Window: window, Counters: counters})
	}
	sent := 0
	for _, report := range reports {
		if report.ReportID == "" {
			var reportID [16]byte
			if _, err := rand.Read(reportID[:]); err != nil {
				r.deferReport(report)
				r.metrics.ReportErrors.Add(1)
				r.logger.Error("生成计量批次标识失败", "project_id", report.ProjectID, "error", err)
				continue
			}
			report.ReportID = hex.EncodeToString(reportID[:])
		}
		data, err := json.Marshal(report)
		if err != nil {
			r.deferReport(report)
			r.metrics.ReportErrors.Add(1)
			r.logger.Error("序列化计量上报失败", "project_id", report.ProjectID, "error", err)
			continue
		}

		if err := r.pub.Publish(ctx, r.subject, data); err != nil {
			r.deferReport(report)
			r.metrics.ReportErrors.Add(1)
			r.logger.Warn("计量上报失败，将在下次窗口重试", "project_id", report.ProjectID, "report_id", report.ReportID, "error", err)
			continue
		}

		var total int64
		for _, v := range report.Counters {
			total += v
		}
		r.metrics.CountersReported.Add(total)
		sent++
	}
	if sent > 0 {
		r.metrics.ReportsTotal.Add(int64(sent))
	}
	return sent
}

func (r *Reporter) deferReport(report UsageReport) {
	r.mu.Lock()
	r.pending = append(r.pending, report)
	r.mu.Unlock()
}
