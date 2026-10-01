package querysvc

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Metrics 是查询服务的计数与瞬时量（手写 Prometheus 文本，沿用本项目既有风格）。
type Metrics struct {
	Requests   atomic.Int64
	Errors     atomic.Int64
	DevicesOK  atomic.Int64
	SeriesOK   atomic.Int64
	InFlight   atomic.Int64
	QueueDepth atomic.Int64
	Rejected   atomic.Int64 // 队列满/超时
	TimedOut   atomic.Int64
	Slow       atomic.Int64 // 慢查询（> SlowQueryThreshold）
	CapHit     atomic.Int64
	SourceRaw  atomic.Int64
	Source1m   atomic.Int64
	Source1h   atomic.Int64
}

// Render 输出 Prometheus 文本格式。
func (m *Metrics) Render(w io.Writer) {
	fmt.Fprintf(w, "querysvc_requests_total %d\n", m.Requests.Load())
	fmt.Fprintf(w, "querysvc_errors_total %d\n", m.Errors.Load())
	fmt.Fprintf(w, "querysvc_devices_ok_total %d\n", m.DevicesOK.Load())
	fmt.Fprintf(w, "querysvc_series_ok_total %d\n", m.SeriesOK.Load())
	fmt.Fprintf(w, "querysvc_in_flight %d\n", m.InFlight.Load())
	fmt.Fprintf(w, "querysvc_queue_depth %d\n", m.QueueDepth.Load())
	fmt.Fprintf(w, "querysvc_rejected_busy_total %d\n", m.Rejected.Load())
	fmt.Fprintf(w, "querysvc_timed_out_total %d\n", m.TimedOut.Load())
	fmt.Fprintf(w, "querysvc_slow_queries_total %d\n", m.Slow.Load())
	fmt.Fprintf(w, "querysvc_cap_hit_total %d\n", m.CapHit.Load())
	fmt.Fprintf(w, "querysvc_source_raw_total %d\n", m.SourceRaw.Load())
	fmt.Fprintf(w, "querysvc_source_rollup_1m_total %d\n", m.Source1m.Load())
	fmt.Fprintf(w, "querysvc_source_rollup_1h_total %d\n", m.Source1h.Load())
}
