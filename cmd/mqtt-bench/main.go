// Command mqtt-bench 是 Phase 0 · A4 的网关容量压测工具。
//
// 它回答 06 Phase 0 表的验收问题：**单节点能否稳定维持 5 万 MQTT 长连接
// （24h 无内存泄漏）** —— 这同时是 ADR-001 决策点「网关 1 个月内达不到
// 5 万连接稳定即回退 EMQX」的判据；并覆盖 03/06 §3.2 的**接入确认延迟
// P99 < 100 ms** 与发布吞吐。
//
// 覆盖范围：
//   - 阶段 1：连接建立 + 保持 + 资源采样 + 泄漏趋势判定；
//   - 阶段 2：QoS1 发布路径（周期发布）+ PUBACK 延迟分布；
//   - 阶段 3：背靠背发布压**吞吐**，并给出接入确认延迟 P50/P95/P99 与 SLO 判定。
//
// 用法（在 devbox 内）：
//
//	# 连接容量（阶段 1）
//	go run ./cmd/mqtt-bench -broker tcp://127.0.0.1:11883 -conn 50000 -rate 2000 -duration 24h
//	# 吞吐 + 接入确认延迟（阶段 3，背靠背）
//	go run ./cmd/mqtt-bench -conn 200 -duration 30s -publish-interval 0
//
// 重要：压测客户端与被测网关**应分处不同主机**。5 万连接下工具自身也是资源大户，
// 同机运行会把工具的开销算进网关的账上。本工具默认静音客户端库日志，避免刷屏。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	cfg := parseFlags()

	// 连接失败时客户端库会刷屏；默认静音，-verbose 打开。
	if !cfg.verbose {
		mqtt.ERROR = log.New(io.Discard, "", 0)
		mqtt.CRITICAL = log.New(io.Discard, "", 0)
		mqtt.WARN = log.New(io.Discard, "", 0)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newBench(cfg).run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "\n压测失败: %v\n", err)
		os.Exit(1)
	}
}

// config 是压测参数。
type config struct {
	broker          string
	conn            int
	rate            int
	duration        time.Duration
	interval        time.Duration
	workers         int
	clientPref      string
	username        string
	password        string
	keepalive       time.Duration
	connTimeout     time.Duration
	publishInterval time.Duration
	pubTimeout      time.Duration
	verbose         bool
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.broker, "broker", "tcp://127.0.0.1:11883", "MQTT broker 地址")
	flag.IntVar(&cfg.conn, "conn", 50000, "目标连接数")
	flag.IntVar(&cfg.rate, "rate", 2000, "每秒建连数；<=0 或 >1000000 视为不限速")
	flag.DurationVar(&cfg.duration, "duration", 0, "保持时长（0 = 保持到收到信号）")
	flag.DurationVar(&cfg.interval, "interval", 30*time.Second, "资源采样与状态输出间隔")
	flag.IntVar(&cfg.workers, "workers", 256, "并发建连的 worker 数")
	flag.StringVar(&cfg.clientPref, "client-prefix", "bench", "ClientID 前缀（多实例并发跑时需各自不同）")
	flag.StringVar(&cfg.username, "username", "", "可选：MQTT 用户名")
	flag.StringVar(&cfg.password, "password", "", "可选：MQTT 密码")
	flag.DurationVar(&cfg.keepalive, "keepalive", 60*time.Second, "MQTT KeepAlive 周期")
	flag.DurationVar(&cfg.connTimeout, "connect-timeout", 15*time.Second, "单连接建立超时")
	flag.DurationVar(&cfg.publishInterval, "publish-interval", -1, "每连接的发布周期（QoS1）：-1=关闭，0=背靠背尽可能快，>0=周期")
	flag.DurationVar(&cfg.pubTimeout, "pub-timeout", 5*time.Second, "单次发布等待 PUBACK 的超时")
	flag.BoolVar(&cfg.verbose, "verbose", false, "输出客户端库日志（默认静音）")
	flag.Parse()

	if cfg.conn <= 0 {
		fmt.Fprintln(os.Stderr, "-conn 必须为正数")
		os.Exit(2)
	}
	if cfg.workers <= 0 {
		cfg.workers = 1
	}
	if cfg.interval <= 0 {
		cfg.interval = 30 * time.Second
	}
	return cfg
}

// sample 是一次资源采样。
type sample struct {
	at         time.Duration
	current    int64
	lost       int64
	goroutines int
	heapAlloc  uint64
	heapInuse  uint64
	sys        uint64
}

// bench 是压测运行体。
type bench struct {
	cfg config

	mu      sync.Mutex
	clients []mqtt.Client

	attempted atomic.Int64
	connected atomic.Int64
	failed    atomic.Int64
	lost      atomic.Int64
	current   atomic.Int64
	connectNS atomic.Int64

	// 发布路径（publish-interval >= 0 时启用）。
	pubSeq    atomic.Int64 // 遥测报文的 seq（模拟设备侧单调递增）
	pubTotal  atomic.Int64
	pubOK     atomic.Int64
	pubFailed atomic.Int64
	pubLatSum atomic.Int64
	pubLatMax atomic.Int64
	pubStart  atomic.Int64 // 首个发布的时刻（UnixNano）：吞吐率按「发布期」算
	pubEnd    atomic.Int64 // 末次发布的时刻
	pubHist   [numLatencyBuckets]atomic.Int64
	pubWG     sync.WaitGroup

	// ctx 是保持期上下文，发布循环依赖它退出。
	ctx context.Context

	samples []sample
}

func newBench(cfg config) *bench { return &bench{cfg: cfg} }

func (b *bench) run(ctx context.Context) error {
	started := time.Now()
	fmt.Printf("mqtt-bench 启动：broker=%s conn=%d rate=%d workers=%d duration=%s publish=%s\n",
		b.cfg.broker, b.cfg.conn, b.cfg.rate, b.cfg.workers,
		humanDuration(b.cfg.duration), publishDesc(b.cfg.publishInterval))

	// 保持期 ctx：duration 到期或收到信号时取消；发布循环也依赖它退出。
	holdCtx, cancel := holdContext(ctx, b.cfg.duration)
	defer cancel()
	b.ctx = holdCtx

	// 1) 建连（按 rate 爬坡）。
	b.rampUp(holdCtx)

	// 2) 保持到 duration 到期或收到信号。
	b.hold(holdCtx, started)

	// 3) 停发布、断开、出报告。
	b.pubWG.Wait()
	b.shutdown()
	b.report(time.Since(started))
	return nil
}

// holdContext 用 duration 派生上下文（duration<=0 表示保持到收到信号）。
func holdContext(parent context.Context, duration time.Duration) (context.Context, context.CancelFunc) {
	if duration > 0 {
		return context.WithTimeout(parent, duration)
	}
	return context.WithCancel(parent)
}

// rampUp 按 rate 发放许可、由 workers 并发建立连接。
func (b *bench) rampUp(ctx context.Context) {
	permits := make(chan struct{}, b.cfg.workers)

	// 许可发放：限速时按 1/rate 的间隔发放。
	go func() {
		defer close(permits)
		fast := b.cfg.rate <= 0 || b.cfg.rate > 1_000_000
		var tick *time.Ticker
		if !fast {
			tick = time.NewTicker(time.Second / time.Duration(b.cfg.rate))
			defer tick.Stop()
		}
		for i := 0; i < b.cfg.conn; i++ {
			if tick != nil {
				select {
				case <-tick.C:
				case <-ctx.Done():
					return
				}
			}
			select {
			case permits <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < b.cfg.workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range permits {
				b.connectOne()
			}
		}()
	}
	wg.Wait()
}

// connectOne 建立一条连接；使用独立的 ClientID 与 ClientOptions（ClientOptions 无 Clone）。
func (b *bench) connectOne() {
	seq := b.attempted.Add(1)
	clientID := fmt.Sprintf("%s-%d", b.cfg.clientPref, seq)
	c := mqtt.NewClient(b.clientOptions(clientID))

	t0 := time.Now()
	tok := c.Connect()
	if !tok.WaitTimeout(b.cfg.connTimeout) || tok.Error() != nil {
		b.failed.Add(1)
		c.Disconnect(0)
		return
	}

	b.connectNS.Add(time.Since(t0).Nanoseconds())
	b.connected.Add(1)
	b.current.Add(1)

	b.mu.Lock()
	b.clients = append(b.clients, c)
	b.mu.Unlock()

	// 发布路径：每连接一个循环（publish-interval >= 0 时启用）。
	if b.cfg.publishInterval >= 0 {
		b.pubWG.Add(1)
		go func() {
			defer b.pubWG.Done()
			b.publishLoop(b.ctx, c, clientID)
		}()
	}
}

func (b *bench) clientOptions(id string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(b.cfg.broker)
	opts.SetClientID(id)
	opts.SetKeepAlive(b.cfg.keepalive)
	opts.SetConnectTimeout(b.cfg.connTimeout)
	opts.SetCleanSession(true)
	// 压测自己控制生命周期：关掉自动重连与连接重试，避免「失败被掩盖成重连」。
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)
	opts.SetOrderMatters(false)
	// 连接被动丢失时递减活跃计数 —— 这是「网关是否在踢连接」的可观测点。
	opts.SetConnectionLostHandler(func(mqtt.Client, error) {
		b.lost.Add(1)
		b.current.Add(-1)
	})
	if b.cfg.username != "" {
		opts.SetUsername(b.cfg.username)
	}
	if b.cfg.password != "" {
		opts.SetPassword(b.cfg.password)
	}
	return opts
}

// hold 在保持期内周期采样并输出状态。
func (b *bench) hold(ctx context.Context, started time.Time) {
	tick := time.NewTicker(b.cfg.interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s := b.sampleNow(started)
			b.samples = append(b.samples, s)
			fmt.Printf("[%s] 活跃=%d 丢失=%d 发布=%d/%d goroutine=%d heap=%s inuse=%s sys=%s\n",
				s.at.Round(time.Second), s.current, s.lost,
				b.pubOK.Load(), b.pubTotal.Load(), s.goroutines,
				humanBytes(s.heapAlloc), humanBytes(s.heapInuse), humanBytes(s.sys))
		}
	}
}

func (b *bench) sampleNow(started time.Time) sample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return sample{
		at:         time.Since(started),
		current:    b.current.Load(),
		lost:       b.lost.Load(),
		goroutines: runtime.NumGoroutine(),
		heapAlloc:  m.HeapAlloc,
		heapInuse:  m.HeapInuse,
		sys:        m.Sys,
	}
}

// shutdown 并发断开全部连接。
func (b *bench) shutdown() {
	b.mu.Lock()
	clients := b.clients
	b.clients = nil
	b.mu.Unlock()

	var wg sync.WaitGroup
	sem := make(chan struct{}, b.cfg.workers)
	for _, c := range clients {
		wg.Add(1)
		sem <- struct{}{}
		go func(c mqtt.Client) {
			defer wg.Done()
			defer func() { <-sem }()
			c.Disconnect(0)
		}(c)
	}
	wg.Wait()
}

func (b *bench) report(elapsed time.Duration) {
	attempted, connected, failed := b.attempted.Load(), b.connected.Load(), b.failed.Load()

	fmt.Printf("\n===== A4 · 连接压测报告 =====\n")
	fmt.Printf("broker                : %s\n", b.cfg.broker)
	fmt.Printf("目标连接数            : %d\n", b.cfg.conn)
	fmt.Printf("建连 尝试/成功/失败   : %d / %d / %d\n", attempted, connected, failed)
	fmt.Printf("连接后被动丢失        : %d\n", b.lost.Load())
	fmt.Printf("建连速率上限          : %d/s\n", b.cfg.rate)
	fmt.Printf("保持时长              : %s\n", elapsed.Round(time.Second))
	if connected > 0 {
		fmt.Printf("平均建连耗时          : %s\n", time.Duration(b.connectNS.Load()/connected).Round(time.Microsecond))
	}

	b.reportLeak()
	b.reportPublish()
}

// reportLeak 用「前 1/4 vs 后 1/4 的 HeapAlloc 中位数」判断泄漏趋势。
// 用中位数而非单点/均值：后者会被 GC 时机带偏（见 09-handoff 坑 31）。
func (b *bench) reportLeak() {
	if len(b.samples) < 4 {
		fmt.Printf("泄漏趋势              : 样本不足（%d 个），跳过；请用更小的 -interval 或更长的 -duration\n", len(b.samples))
		return
	}

	q := len(b.samples) / 4
	early := medianHeap(b.samples[:q])
	late := medianHeap(b.samples[len(b.samples)-q:])
	growth := float64(int64(late)-int64(early)) / float64(early) * 100

	fmt.Printf("HeapAlloc 前 1/4 中位  : %s\n", humanBytes(early))
	fmt.Printf("HeapAlloc 后 1/4 中位  : %s\n", humanBytes(late))
	fmt.Printf("增长                  : %.1f%%\n", growth)
	fmt.Printf("判定                  : %s\n", leakVerdict(growth))
}

// reportPublish 输出发布路径统计（吞吐 + 接入确认延迟分布），并对照 §3.2 的 P99 SLO。
func (b *bench) reportPublish() {
	if b.cfg.publishInterval < 0 {
		return
	}
	total, ok, failed := b.pubTotal.Load(), b.pubOK.Load(), b.pubFailed.Load()

	fmt.Printf("\n--- 发布路径（QoS1，%s）---\n", publishDesc(b.cfg.publishInterval))
	fmt.Printf("发布 总数/成功/失败    : %d / %d / %d\n", total, ok, failed)
	if ok == 0 {
		return
	}

	// 吞吐率用「发布期」（首末发布之间）而非总时长，避免把建连时间算进去。
	if start, end := b.pubStart.Load(), b.pubEnd.Load(); end > start {
		period := time.Duration(end - start)
		fmt.Printf("发布吞吐              : %.0f msg/s（发布期 %s）\n",
			float64(ok)/period.Seconds(), period.Round(time.Second))
	}

	hist := b.histSnapshot()
	p50 := quantileFromHist(hist, ok, 0.50)
	p95 := quantileFromHist(hist, ok, 0.95)
	p99 := quantileFromHist(hist, ok, 0.99)

	fmt.Printf("接入确认延迟 avg/max  : %s / %s\n",
		time.Duration(b.pubLatSum.Load()/ok).Round(time.Microsecond),
		time.Duration(b.pubLatMax.Load()).Round(time.Microsecond))
	fmt.Printf("接入确认延迟 P50/P95/P99（桶上界）: %s / %s / %s\n",
		p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))

	// 对照 03/06 §3.2：接入确认延迟 P99 < 100 ms（网关收包 → 发布到 NATS）。
	const sloP99 = 100 * time.Millisecond
	if p99 <= sloP99 {
		fmt.Printf("SLO 判定（P99 < %s）: ✅ 达标\n", sloP99)
	} else {
		fmt.Printf("SLO 判定（P99 < %s）: ❌ 未达标（P99 落在 %s 桶）\n", sloP99, p99)
	}
}

// leakVerdict 给出启发式趋势判定。稳态下 HeapAlloc 本就有波动，
// 这里的结论只用于提示方向，不能替代人工看完整曲线。
func leakVerdict(growthPct float64) string {
	switch {
	case growthPct < 5:
		return "稳态（无泄漏迹象）"
	case growthPct < 20:
		return "轻微增长（结合曲线判断，可能是缓存预热）"
	default:
		return "⚠️ 明显增长（疑似泄漏，需排查）"
	}
}

func medianHeap(samples []sample) uint64 {
	vals := make([]uint64, len(samples))
	for i, s := range samples {
		vals[i] = s.heapAlloc
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[len(vals)/2]
}

func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := uint64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanDuration(d time.Duration) string {
	if d <= 0 {
		return "∞（保持到信号）"
	}
	return d.String()
}

func publishDesc(d time.Duration) string {
	switch {
	case d < 0:
		return "关闭"
	case d == 0:
		return "背靠背（尽可能快）"
	default:
		return "每连接 " + d.String()
	}
}

// ---------- 发布路径 ----------

// latencyBoundsNS 是 PUBACK 延迟直方图的桶上界（纳秒）。
// 用固定桶而非留存全部样本：5 万连接 24h 会产生上亿次发布，全量留存不现实。
var latencyBoundsNS = [12]int64{
	100e3, 200e3, 500e3, // 0.1 / 0.2 / 0.5 ms
	1e6, 2e6, 5e6, // 1 / 2 / 5 ms
	1e7, 5e7, // 10 / 50 ms
	1e8, 5e8, 1e9, 5e9, // 100 ms / 500 ms / 1 s / 5 s
}

const numLatencyBuckets = 13 // 12 个上界 + 1 个「超上界」桶

// publishLoop 按 publish-interval 发布 QoS1 遥测。
// interval > 0 为周期发布；interval == 0 为**背靠背**（尽可能快，用于压吞吐）。
func (b *bench) publishLoop(ctx context.Context, c mqtt.Client, deviceKey string) {
	topic := "v1/devices/" + deviceKey + "/telemetry"

	var tick *time.Ticker
	if b.cfg.publishInterval > 0 {
		tick = time.NewTicker(b.cfg.publishInterval)
		defer tick.Stop()
	}

	for ctx.Err() == nil {
		if tick != nil {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
		// 背靠背时 publishOnce 阻塞等 PUBACK，天然限速，不会空转。
		b.publishOnce(c, topic, b.telemetryPayload())
	}
}

// telemetryPayload 生成一条符合端侧契约的遥测报文（03 §5.2）：
// `{ts, seq, data}`，指标集与 internal/tsdb·BenchMetrics 一致，
// 便于端到端验证「网关 → 管道 → GreptimeDB」整条链路。
func (b *bench) telemetryPayload() []byte {
	seq := b.pubSeq.Add(1)
	return []byte(fmt.Sprintf(
		`{"ts":%q,"seq":%d,"data":{"temperature":25.0,"humidity":60.0,"pressure":1013.0,"voltage":3.70,"running":true}}`,
		time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), seq))
}

func (b *bench) publishOnce(c mqtt.Client, topic string, payload []byte) {
	t0 := time.Now()
	b.pubStart.CompareAndSwap(0, t0.UnixNano())
	b.pubEnd.Store(t0.UnixNano())

	b.pubTotal.Add(1)
	tok := c.Publish(topic, 1, false, payload)
	if !tok.WaitTimeout(b.cfg.pubTimeout) || tok.Error() != nil {
		b.pubFailed.Add(1)
		return
	}

	b.pubOK.Add(1)
	b.recordLatency(time.Since(t0))
}

// recordLatency 记录一次 PUBACK 延迟：样本和、最大值与直方图桶。
func (b *bench) recordLatency(d time.Duration) {
	ns := d.Nanoseconds()
	b.pubLatSum.Add(ns)
	for {
		cur := b.pubLatMax.Load()
		if ns <= cur || b.pubLatMax.CompareAndSwap(cur, ns) {
			break
		}
	}
	b.pubHist[bucketOf(d)].Add(1)
}

func bucketOf(d time.Duration) int {
	ns := d.Nanoseconds()
	for i, bound := range latencyBoundsNS {
		if ns < bound {
			return i
		}
	}
	return numLatencyBuckets - 1
}

func (b *bench) histSnapshot() []int64 {
	out := make([]int64, numLatencyBuckets)
	for i := range b.pubHist {
		out[i] = b.pubHist[i].Load()
	}
	return out
}

// quantileFromHist 从直方图估计分位数，返回对应桶的上界（近似值）。
func quantileFromHist(hist []int64, total int64, q float64) time.Duration {
	target := int64(float64(total) * q)
	var acc int64
	for i, c := range hist {
		acc += c
		if acc >= target {
			if i < len(latencyBoundsNS) {
				return time.Duration(latencyBoundsNS[i])
			}
			return time.Duration(latencyBoundsNS[len(latencyBoundsNS)-1])
		}
	}
	return time.Duration(latencyBoundsNS[len(latencyBoundsNS)-1])
}
