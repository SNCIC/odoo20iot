// Command tsdb-bench 是 Phase 0 · B1 的压测工具：对比时序表模型 A / B。
//
// 用法（在 devbox 内）：
//
//	go run ./cmd/tsdb-bench -plan all -report tmp/b1-report.md
//
// 它回答一个问题：**GreptimeDB 的 JSON 字段方案能否达到 06 文档的 Phase 0 验收线**
// （写入 5 万 points/s；查询满足 06 §3.4 的 API 延迟 SLO），
// 以及它与「按设备类型宽表」的量化差距。
//
// 被测组合是三组而不是两组：A 方案的 JSON 列在 PG 协议下**只有两种写入编码可用**
// （`\x` 十六进制参数 / 内联字面量，见 greptimedb.JSONEncoding），两者代价不同，
// 必须分开度量才能回答「A 方案到底能不能用」。
//
// 设计约束（避免得出误导性结论）：
//   - 所有组合写入**完全相同的数据**（同一份 Row 序列），只有存储布局与编码不同；
//   - 查询前做一致性校验：同一窗口下 JSON 与宽表必须逐点相等；
//   - 明确记录数据集规模与并发度，所有数字都是「本机 + 本数据集」的实测值。
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
	"github.com/SNCIC/odoo20iot/internal/tsdb/greptimedb"
)

func main() {
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\n压测失败: %v\n", err)
		os.Exit(1)
	}
}

// variant 是被测组合：表模型 × JSON 写入编码。
type variant struct {
	Key   string
	Label string
	Plan  tsdb.Plan
	Enc   greptimedb.JSONEncoding
}

func defaultVariants() []variant {
	return []variant{
		{Key: "json", Label: "A · JSON（\\x 参数）", Plan: tsdb.PlanJSON, Enc: greptimedb.JSONHexParam},
		{Key: "json-literal", Label: "A · JSON（内联字面量）", Plan: tsdb.PlanJSON, Enc: greptimedb.JSONInlineLiteral},
		{Key: "wide", Label: "B · 宽表", Plan: tsdb.PlanWide},
	}
}

func selectVariants(keys []string) ([]variant, error) {
	all := defaultVariants()
	var out []variant

	for _, k := range keys {
		found := false
		for _, v := range all {
			if v.Key == k {
				out = append(out, v)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("未知的被测组合 %q（可选：json / json-literal / wide / all）", k)
		}
	}
	return out, nil
}

// plans 返回去重后的表模型顺序（查询阶段按表模型跑，不按组合跑 ——
// 两个 A 组合写的是同一张表）。
func plans(vs []variant) []tsdb.Plan {
	var out []tsdb.Plan
	seen := map[tsdb.Plan]bool{}
	for _, v := range vs {
		if !seen[v.Plan] {
			seen[v.Plan] = true
			out = append(out, v.Plan)
		}
	}
	return out
}

type config struct {
	dsn         string
	variants    []variant
	projectID   int64
	deviceType  int64
	recreate    bool
	fleet       int
	fleetEvery  time.Duration
	hot         int
	hotEvery    time.Duration
	span        time.Duration
	hotSpan     time.Duration
	writers     int
	batchRows   int
	batchWait   time.Duration
	queryIters  int
	skipLoad    bool
	skipQuery   bool
	reportPath  string
	instanceTag string
	valueModel  valueModel
}

func parseFlags() config {
	var (
		planFlag = flag.String("plan", "all", "被测组合，逗号分隔：json / json-literal / wide / all")
		cfg      = config{}
	)

	flag.StringVar(&cfg.dsn, "dsn", "postgres://greptime:greptime@100.64.0.3:28403/public", "GreptimeDB PG wire DSN")
	flag.Int64Var(&cfg.projectID, "project", 1, "租户 ID（查询强制等值条件的取值）")
	flag.Int64Var(&cfg.deviceType, "device-type", 55, "设备类型 ID（A 方案的标签列）")
	flag.BoolVar(&cfg.recreate, "recreate", true, "每个组合加载前删表重建（保证起点一致）")
	flag.IntVar(&cfg.fleet, "fleet-devices", 300, "车队设备数（常规上报频率）")
	flag.DurationVar(&cfg.fleetEvery, "fleet-interval", 30*time.Second, "车队上报间隔")
	flag.IntVar(&cfg.hot, "hot-devices", 12, "高频设备数（密集曲线，1Hz 量级）")
	flag.DurationVar(&cfg.hotEvery, "hot-interval", 1*time.Second, "高频设备上报间隔")
	flag.DurationVar(&cfg.span, "span", 24*time.Hour, "车队设备的数据时间跨度")
	flag.DurationVar(&cfg.hotSpan, "hot-span", 6*time.Hour, "高频设备的数据时间跨度")
	flag.IntVar(&cfg.writers, "writers", 4, "并发写连接数（模拟多分片消费者）")
	flag.IntVar(&cfg.batchRows, "batch-rows", 1000, "攒批行数阈值（02 §4.2）")
	flag.DurationVar(&cfg.batchWait, "batch-wait", 200*time.Millisecond, "攒批等待上限（02 §4.2）")
	flag.IntVar(&cfg.queryIters, "query-iters", 30, "每个查询模式的采样次数")
	flag.BoolVar(&cfg.skipLoad, "skip-load", false, "跳过写入阶段（复用已有数据）")
	flag.BoolVar(&cfg.skipQuery, "skip-query", false, "跳过查询阶段")
	flag.StringVar(&cfg.reportPath, "report", "", "把 Markdown 报告写到该路径")
	flag.StringVar(&cfg.instanceTag, "instance", "devbox compose · GreptimeDB standalone（共享宿主）", "被测实例标签（写入报告）")
	var modelFlag = flag.String("value-model", string(modelCorrelated), "指标取值的时间相关性：correlated（默认，近真实）/ random / linear")

	flag.Parse()

	keys := strings.Split(*planFlag, ",")
	if *planFlag == "all" {
		keys = []string{"json", "json-literal", "wide"}
	}
	variants, err := selectVariants(keys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	cfg.variants = variants

	model, err := parseValueModel(*modelFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	cfg.valueModel = model
	return cfg
}

func run(ctx context.Context, cfg config) error {
	store, err := greptimedb.Open(ctx, cfg.dsn, int32(cfg.writers+2))
	if err != nil {
		return err
	}
	defer store.Close()

	ds := dataset{
		ValueModel:   cfg.valueModel,
		ProjectID:    cfg.projectID,
		DeviceTypeID: cfg.deviceType,
		Fleet:        cfg.fleet,
		FleetEvery:   cfg.fleetEvery,
		Hot:          cfg.hot,
		HotEvery:     cfg.hotEvery,
		Span:         cfg.span,
		HotSpan:      cfg.hotSpan,
		// 固定数据末端时刻：保证两次运行生成完全相同的时间戳，结果可比。
		End: time.Now().UTC().Truncate(time.Second),
	}

	rep := &report{
		Instance:    cfg.instanceTag,
		ValueModel:  string(cfg.valueModel),
		DSN:         redactDSN(cfg.dsn),
		Dataset:     ds.describe(),
		TotalRows:   ds.Rows(),
		TotalPoints: ds.Rows() * int64(len(tsdb.BenchMetrics)),
		Variants:    cfg.variants,
		Writers:     cfg.writers,
		BatchRows:   cfg.batchRows,
		BatchWait:   cfg.batchWait,
		QueryIters:  cfg.queryIters,
		GreptimeVer: greptimeVersion(ctx, store),
		Load:        map[string]*loadResult{},
	}

	if !cfg.skipLoad {
		for _, v := range cfg.variants {
			if _, err := loadVariant(ctx, store, v, cfg, ds, rep); err != nil {
				return fmt.Errorf("组合 %s 写入阶段: %w", v.Key, err)
			}
		}
	}

	if !cfg.skipQuery {
		if err := verifyConsistency(ctx, store, ds, rep); err != nil {
			return err
		}
		if err := queryPlans(ctx, store, cfg, ds, rep); err != nil {
			return err
		}
	}

	// 存储统计必须落盘后再读：写入刚结束时数据几乎全在 memtable，
	// disk_size 读到的是残余，字节/行会虚高数倍。前后各读一次，把失真的幅度
	// 一并写进报告 —— 这样读者不必相信我们，只看两列差多少。
	if pre, err := store.TableStats(ctx); err == nil {
		rep.TableStatsPreFlush = pre
	}

	if !cfg.skipLoad {
		for _, plan := range plans(cfg.variants) {
			if err := store.FlushTable(ctx, plan); err != nil {
				rep.Notes = append(rep.Notes, "flush 失败，存储统计可能失真: "+err.Error())
			}
		}
	}

	if stats, err := store.TableStats(ctx); err == nil {
		rep.TableStats = stats
	} else {
		rep.Notes = append(rep.Notes, "存储统计不可用（information_schema 字段可能随版本变动）: "+err.Error())
	}

	out := rep.render()
	fmt.Print(out)

	if cfg.reportPath != "" {
		if err := os.WriteFile(cfg.reportPath, []byte(out), 0o644); err != nil {
			return fmt.Errorf("写报告 %s: %w", cfg.reportPath, err)
		}
		fmt.Fprintf(os.Stderr, "\n报告已写入 %s\n", cfg.reportPath)
	}
	return nil
}

// ---------- 数据集 ----------

// dataset 描述一份确定性数据：设备被分成「车队」与「高频」两类。
//
// 两类都必要：高频设备提供密集曲线（曲线与聚合查询的真实成本来源），
// 车队设备提供真实的序列基数（影响写入与压缩）。
type dataset struct {
	ValueModel   valueModel
	ProjectID    int64
	DeviceTypeID int64
	Fleet        int
	FleetEvery   time.Duration
	Hot          int
	HotEvery     time.Duration
	Span         time.Duration
	HotSpan      time.Duration
	End          time.Time
}

// series 是一条设备的时间序列（设备 + 行数 + 步长 + 末端时刻）。
type series struct {
	deviceID int64
	interval time.Duration
	count    int
	end      time.Time
}

func (d dataset) series() []series {
	out := make([]series, 0, d.Fleet+d.Hot)

	for i := 0; i < d.Fleet; i++ {
		out = append(out, series{
			deviceID: int64(1_000_000 + i),
			interval: d.FleetEvery,
			count:    int(d.Span / d.FleetEvery),
			end:      d.End,
		})
	}
	for i := 0; i < d.Hot; i++ {
		out = append(out, series{
			deviceID: int64(2_000_000 + i),
			interval: d.HotEvery,
			count:    int(d.HotSpan / d.HotEvery),
			end:      d.End,
		})
	}
	return out
}

// Rows 返回总行数。
func (d dataset) Rows() int64 {
	var n int64
	for _, s := range d.series() {
		n += int64(s.count)
	}
	return n
}

func (d dataset) describe() string {
	return fmt.Sprintf(
		"车队 %d 台 × %s × %s（%.0f 行/台）；高频 %d 台 × %s × %s（%.0f 行/台）；数据末端 %s",
		d.Fleet, d.FleetEvery, d.Span, d.Span.Seconds()/d.FleetEvery.Seconds(),
		d.Hot, d.HotEvery, d.HotSpan, d.HotSpan.Seconds()/d.HotEvery.Seconds(),
		d.End.Format(time.RFC3339))
}

// hotDeviceIDs 返回高频设备 ID（密集曲线查询的目标）。
func (d dataset) hotDeviceIDs() []int64 {
	out := make([]int64, 0, d.Hot)
	for i := 0; i < d.Hot; i++ {
		out = append(out, int64(2_000_000+i))
	}
	return out
}

// appendRows 把该序列的第 from..from+n 行追加到 buf。
func (s series) appendRows(buf []tsdb.Row, d dataset, from, n int) []tsdb.Row {
	start := s.end.Add(-time.Duration(s.count) * s.interval)
	for i := from; i < from+n && i < s.count; i++ {
		buf = append(buf, tsdb.Row{
			TS:           start.Add(time.Duration(i) * s.interval),
			ProjectID:    d.ProjectID,
			DeviceID:     s.deviceID,
			DeviceTypeID: d.DeviceTypeID,
			Values:       values(s.deviceID, i, d.ValueModel),
		})
	}
	return buf
}

// valueModel 决定指标取值的时间相关性。
//
// **这个开关比它看起来重要**：列存压缩对「相邻取值是否相关」极度敏感，
// 而二者正是 A（JSON 文本）与 B（宽表原生列）差异最大的地方。同一个数据集、
// 同一份代码，只换取值模型，A/B 存储占用比可以从 0.94× 摆到 3.3×（见 02 §4.1.1）。
// 因此任何存储结论都必须声明所用的取值模型。
type valueModel string

const (
	// modelCorrelated 是有漂移 + 噪声的自相关序列，最接近真实传感器读数，默认。
	modelCorrelated valueModel = "correlated"
	// modelRandom 是纯随机取值（下界：列存拿不到任何 delta 收益）。
	modelRandom valueModel = "random"
	// modelLinear 是周期序列（上界：delta 编码几乎压到 1 字节/点，不真实）。
	modelLinear valueModel = "linear"
)

func parseValueModel(s string) (valueModel, error) {
	switch valueModel(s) {
	case modelCorrelated, modelRandom, modelLinear:
		return valueModel(s), nil
	default:
		return "", fmt.Errorf("未知的取值模型 %q（可选：correlated / random / linear）", s)
	}
}

// values 生成确定性的指标取值。
//
// 不使用随机源：同样的输入永远得到同样的值，各组合写入内容严格可比、可复现。
// 但哈希必须**充分混合**（splitmix64），线性形式会退化成周期序列。
func values(deviceID int64, i int, model valueModel) []tsdb.Value {
	h := mix64(uint64(deviceID)<<32 ^ uint64(uint32(i)))
	t := float64(i)

	// 分钟级 + 小时级的慢分量，模拟真实被测量的漂移。
	slow := math.Sin(t/1800.0)*0.5 + math.Sin(t/240.0)*0.15

	f := func(scale, base float64) float64 {
		h = mix64(h)
		noise := float64(h%1_000_000)/1_000_000.0 - 0.5

		switch model {
		case modelLinear:
			return base + float64(h%10_000)/10_000.0*scale
		case modelRandom:
			return base + (noise+0.5)*scale
		default: // modelCorrelated
			return base + scale*(0.5+0.6*slow+0.2*noise)
		}
	}

	return []tsdb.Value{
		tsdb.Number(f(10, 20)), // temperature 20~30
		tsdb.Number(f(30, 40)), // humidity    40~70
		tsdb.Number(f(20, 95)), // pressure    95~115
		tsdb.Number(f(5, 3.5)), // voltage     3.5~8.5
		tsdb.Bool(h%2 == 0),    // running
	}
}

// mix64 是 splitmix64 的终结器，用于把顺序输入打散成雪崩分布。
func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// ---------- 写入阶段 ----------

type loadResult struct {
	Rows      int64
	Points    int64
	Batches   int64
	Wall      time.Duration
	FlushLats []time.Duration
}

func loadVariant(ctx context.Context, store *greptimedb.Store, v variant, cfg config, ds dataset, rep *report) (*loadResult, error) {
	if cfg.recreate {
		if err := store.DropTable(ctx, v.Plan); err != nil {
			return nil, err
		}
	}
	if err := store.CreateTable(ctx, v.Plan); err != nil {
		return nil, err
	}

	all := ds.series()
	fmt.Printf("[%s] 开始写入：%d 条序列 / %d 行 ...\n", v.Key, len(all), ds.Rows())

	var (
		start    = time.Now()
		rowsDone atomic.Int64
		batches  atomic.Int64
		errCount atomic.Int64
		firstErr atomic.Value
	)

	lats := make([][]time.Duration, cfg.writers)
	var wg sync.WaitGroup

	for w := 0; w < cfg.writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()

			var (
				buf       = make([]tsdb.Row, 0, cfg.batchRows)
				lastFlush = time.Now()
				mine      []time.Duration
			)

			flush := func() {
				if len(buf) == 0 {
					return
				}
				t0 := time.Now()
				err := store.InsertRowsWith(ctx, v.Plan, v.Enc, buf)
				mine = append(mine, time.Since(t0))

				if err != nil {
					errCount.Add(1)
					firstErr.CompareAndSwap(nil, err)
				} else {
					rowsDone.Add(int64(len(buf)))
				}
				batches.Add(1)
				buf = buf[:0]
				lastFlush = time.Now()
			}

			for i := w; i < len(all); i += cfg.writers {
				s := all[i]
				for off := 0; off < s.count; {
					n := min(cfg.batchRows, s.count-off)
					buf = s.appendRows(buf, ds, off, n)
					off += n

					if len(buf) >= cfg.batchRows || time.Since(lastFlush) >= cfg.batchWait {
						flush()
					}
				}
				flush()
			}
			flush()
			lats[w] = mine
		}(w)
	}
	wg.Wait()

	wall := time.Since(start)
	if err := firstErr.Load(); err != nil {
		return nil, fmt.Errorf("%d 个批次失败，首个错误: %w", errCount.Load(), err.(error))
	}

	var allLats []time.Duration
	for _, ls := range lats {
		allLats = append(allLats, ls...)
	}
	sort.Slice(allLats, func(i, j int) bool { return allLats[i] < allLats[j] })

	res := &loadResult{
		Rows:      rowsDone.Load(),
		Points:    rowsDone.Load() * int64(len(tsdb.BenchMetrics)),
		Batches:   batches.Load(),
		Wall:      wall,
		FlushLats: allLats,
	}
	fmt.Printf("[%s] 写入完成：%d 行 / %s（%.0f 行/s，%.0f points/s）\n",
		v.Key, res.Rows, wall.Round(time.Millisecond),
		float64(res.Rows)/wall.Seconds(), float64(res.Points)/wall.Seconds())

	rep.Load[v.Key] = res
	return res, nil
}

// ---------- 一致性校验 ----------

// verifyConsistency 是 B1 结论成立的前提：A、B 两方案必须给出逐点相同的查询结果。
// 若此步失败，任何性能对比都没有意义（在读两个不同的东西）。
func verifyConsistency(ctx context.Context, store *greptimedb.Store, ds dataset, rep *report) error {
	dev := ds.hotDeviceIDs()[0]
	q := tsdb.RangeQuery{
		ProjectID: ds.ProjectID,
		DeviceIDs: []int64{dev},
		Since:     ds.End.Add(-1 * time.Hour),
		Metric:    "temperature",
		Limit:     100000,
	}

	jsonPts, err := store.SelectRange(ctx, tsdb.PlanJSON, q)
	if err != nil {
		return fmt.Errorf("一致性校验（JSON）: %w", err)
	}
	widePts, err := store.SelectRange(ctx, tsdb.PlanWide, q)
	if err != nil {
		return fmt.Errorf("一致性校验（宽表）: %w", err)
	}

	if len(jsonPts) != len(widePts) {
		return fmt.Errorf("一致性校验失败：JSON 返回 %d 点，宽表返回 %d 点", len(jsonPts), len(widePts))
	}
	for i := range jsonPts {
		if !jsonPts[i].TS.Equal(widePts[i].TS) || jsonPts[i].DeviceID != widePts[i].DeviceID {
			return fmt.Errorf("一致性校验失败：第 %d 行的键不一致（%s vs %s）", i, jsonPts[i].TS, widePts[i].TS)
		}
		if d := jsonPts[i].Value - widePts[i].Value; d > 1e-9 || d < -1e-9 {
			return fmt.Errorf("一致性校验失败：第 %d 行取值不一致（%v vs %v）", i, jsonPts[i].Value, widePts[i].Value)
		}
	}

	rep.Consistency = fmt.Sprintf(
		"通过：单设备近 1h `temperature` 曲线，两方案均返回 **%d 点**，时间戳 / device_id / 取值逐点相等（误差 < 1e-9）",
		len(jsonPts))
	fmt.Printf("一致性校验通过（%d 点逐点相等）\n", len(jsonPts))
	return nil
}

// ---------- 查询阶段 ----------

// queryCase 是一个查询模式（对应 02 §4.3 的一行）。
type queryCase struct {
	Name    string
	Desc    string
	Devices int
	Window  time.Duration
	Bucket  time.Duration
}

var queryCases = []queryCase{
	{Name: "Q1 单设备近 1h 曲线", Desc: "单设备 + 1h 明细，逐点返回", Devices: 1, Window: time.Hour},
	{Name: "Q2 单设备 6h 分桶聚合", Desc: "单设备 + 5min 分桶 AVG/MAX", Devices: 1, Window: 6 * time.Hour, Bucket: 5 * time.Minute},
	{Name: "Q3 多设备近 1h 曲线", Desc: "10 设备 + 1h 明细（§4.3 上限 50）", Devices: 10, Window: time.Hour},
	{Name: "Q4 多设备 24h 分桶聚合", Desc: "10 设备 + 5min 分桶 AVG/MAX", Devices: 10, Window: 24 * time.Hour, Bucket: 5 * time.Minute},
}

func queryPlans(ctx context.Context, store *greptimedb.Store, cfg config, ds dataset, rep *report) error {
	hot := ds.hotDeviceIDs()
	allDevices := append(append([]int64{}, hot...), int64(1_000_000), int64(1_000_001), int64(1_000_002))

	for _, c := range queryCases {
		n := min(c.Devices, len(allDevices))
		devices := allDevices[:n]

		for _, plan := range plans(cfg.variants) {
			lats := make([]time.Duration, 0, cfg.queryIters)
			var rowCount int

			for i := 0; i < cfg.queryIters; i++ {
				t0 := time.Now()
				var err error
				if c.Bucket > 0 {
					var rows []tsdb.BucketRow
					rows, err = store.SelectBuckets(ctx, plan, tsdb.BucketQuery{
						ProjectID: ds.ProjectID,
						DeviceIDs: devices,
						Since:     ds.End.Add(-c.Window),
						Bucket:    c.Bucket,
						Metric:    "temperature",
					})
					rowCount = len(rows)
				} else {
					var pts []tsdb.SeriesPoint
					pts, err = store.SelectRange(ctx, plan, tsdb.RangeQuery{
						ProjectID: ds.ProjectID,
						DeviceIDs: devices,
						Since:     ds.End.Add(-c.Window),
						Metric:    "temperature",
						Limit:     500000,
					})
					rowCount = len(pts)
				}
				if err != nil {
					return fmt.Errorf("查询 %s（%s）: %w", c.Name, plan, err)
				}
				lats = append(lats, time.Since(t0))
			}

			sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
			rep.Queries = append(rep.Queries, queryResult{
				Case: c,
				Plan: plan,
				Rows: rowCount,
				P50:  pct(lats, 0.50),
				P95:  pct(lats, 0.95),
				P99:  pct(lats, 0.99),
				Max:  lats[len(lats)-1],
			})
			fmt.Printf("[%s] %s：返回 %d 行，P50=%s P95=%s P99=%s\n",
				plan, c.Name, rowCount,
				pct(lats, 0.50).Round(time.Microsecond),
				pct(lats, 0.95).Round(time.Microsecond),
				pct(lats, 0.99).Round(time.Microsecond))
		}
	}
	return nil
}

// ---------- 工具 ----------

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func greptimeVersion(ctx context.Context, store *greptimedb.Store) string {
	var v string
	if err := store.Pool().QueryRow(ctx, "SELECT version()").Scan(&v); err != nil {
		return "未知"
	}
	return v
}

func redactDSN(dsn string) string {
	// 只保留主机与库名，避免把口令写进报告。
	at := strings.LastIndex(dsn, "@")
	proto := strings.Index(dsn, "://")
	if at < 0 || proto < 0 {
		return dsn
	}
	return dsn[:proto+3] + "***@" + dsn[at+1:]
}
