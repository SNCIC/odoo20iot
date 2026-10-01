package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
	"github.com/SNCIC/odoo20iot/internal/tsdb/greptimedb"
)

// 验收线（06-operations.md）：Phase 0 交付表与 §3.4 的 SLO。
const (
	// targetPointsPerSec 是 06「物模型解析 + GreptimeDB 写入」的验收值（5 万 points/s）。
	targetPointsPerSec = 50000
	// targetQueryP95 对齐 06 §3.4「API 延迟 95% 请求 < 200 ms」——
	// 查询是 API 链路里最重的一段，用整条 API 的预算来卡它是**保守**的。
	targetQueryP95 = 200 * time.Millisecond
)

type queryResult struct {
	Case queryCase
	Plan tsdb.Plan
	Rows int
	P50  time.Duration
	P95  time.Duration
	P99  time.Duration
	Max  time.Duration
	// 受保护入口（QuerySeries）的结果元数据；未保护用例为零值。
	Protected   bool
	Granularity tsdb.Granularity
	Bucket      time.Duration
	CapHit      bool
}

type report struct {
	Instance    string
	ValueModel  string
	DSN         string
	GreptimeVer string
	Dataset     string
	TotalRows   int64
	TotalPoints int64
	Variants    []variant
	Writers     int
	BatchRows   int
	BatchWait   time.Duration
	QueryIters  int
	Consistency string
	Load        map[string]*loadResult
	Queries     []queryResult
	TableStats  map[string]greptimedb.TableStat
	// TableStatsPreFlush 是 flush 前读到的统计，用作对照：
	// 未落盘时 disk_size 只是残余，字节/行会虚高数倍。
	TableStatsPreFlush map[string]greptimedb.TableStat
	Notes              []string
}

func (r *report) render() string {
	if r.Load == nil {
		r.Load = map[string]*loadResult{}
	}

	var b strings.Builder
	b.WriteString("# B1 · GreptimeDB 表模型压测报告\n\n")
	fmt.Fprintf(&b, "> 自动生成于 %s ｜ 实例：%s ｜ GreptimeDB %s ｜ `%s`\n\n",
		time.Now().Format(time.RFC3339), r.Instance, r.GreptimeVer, r.DSN)

	b.WriteString("## 1. 数据集与参数\n\n")
	fmt.Fprintf(&b, "- 数据集：%s\n", r.Dataset)
	fmt.Fprintf(&b, "- 总规模：**%d 行 / %d points**（每行 %d 个指标，对齐 06 §5 的容量口径）\n",
		r.TotalRows, r.TotalPoints, len(tsdb.BenchMetrics))
	fmt.Fprintf(&b, "- 写入并发：%d 条连接；攒批：%d 行或 %s 先到即刷（02 §4.2）\n", r.Writers, r.BatchRows, r.BatchWait)
	fmt.Fprintf(&b, "- 查询采样：每个模式 %d 次\n", r.QueryIters)
	fmt.Fprintf(&b, "- **取值模型：`%s`**（决定时间相关性，直接左右列存压缩比；见 02 §4.1.1）\n", r.ValueModel)
	fmt.Fprintf(&b, "- 被测组合：%s\n", r.variantKeys())

	b.WriteString("\n## 2. 写入吞吐\n\n")
	b.WriteString("| 组合 | 行数 | points | 墙钟 | 行/s | **points/s** | 批次数 | flush P50 | flush P95 | flush P99 |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, v := range r.Variants {
		res, ok := r.Load[v.Key]
		if !ok {
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %s | %.0f | **%.0f** | %d | %s | %s | %s |\n",
			v.Label, res.Rows, res.Points, res.Wall.Round(time.Millisecond),
			float64(res.Rows)/res.Wall.Seconds(), float64(res.Points)/res.Wall.Seconds(),
			res.Batches,
			pct(res.FlushLats, 0.50).Round(time.Microsecond),
			pct(res.FlushLats, 0.95).Round(time.Microsecond),
			pct(res.FlushLats, 0.99).Round(time.Microsecond))
	}
	fmt.Fprintf(&b, "\n验收线：Phase 0 要求 **%d points/s**（06 交付表「物模型解析 + GreptimeDB 写入」）。\n\n", targetPointsPerSec)
	for _, v := range r.Variants {
		res, ok := r.Load[v.Key]
		if !ok {
			continue
		}
		got := float64(res.Points) / res.Wall.Seconds()
		fmt.Fprintf(&b, "- %s：%.0f points/s → %s（%.2f× 验收线）\n",
			v.Label, got, verdict(got >= targetPointsPerSec), got/targetPointsPerSec)
	}
	if best, ok := r.bestJSON(); ok {
		if wide, ok := r.Load["wide"]; ok {
			bestRes := r.Load[best.Key]
			wideRate := float64(wide.Points) / wide.Wall.Seconds()
			if wideRate > 0 {
				fmt.Fprintf(&b, "\n> A 方案最优写入路径（%s）相对 B 方案（宽表）的吞吐比：**%.2f×**\n",
					best.Label, (float64(bestRes.Points)/bestRes.Wall.Seconds())/wideRate)
			}
		}
	}

	if len(r.TableStats) > 0 {
		b.WriteString("\n## 3. 存储占用（落盘后）\n\n")
		b.WriteString("| 表 | region 行数 | disk | sst | index | memtable | 字节/行（落盘后） | 字节/行（未落盘） |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, plan := range plans(r.Variants) {
			st, ok := r.TableStats[tsdb.TableName(plan)]
			if !ok {
				continue
			}
			fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %.2f | %.2f |\n",
				tsdb.TableName(plan), st.Rows, humanBytes(st.DiskSize), humanBytes(st.SSTSize),
				humanBytes(st.IndexSize), humanBytes(st.MemtableSz),
				bytesPerRow(st), bytesPerRow(r.TableStatsPreFlush[tsdb.TableName(plan)]))
		}
		b.WriteString("\n> 统计前对每张表执行了 `ADMIN FLUSH_TABLE`。**「未落盘」列是刻意的对照**：" +
			"写入刚结束时数据几乎全在 memtable，此时读 `disk_size` 会得到虚高数倍的字节/行。" +
			"任何压缩比结论都必须先落盘 —— 这两列的差值就是这个坑的幅度。\n" +
			"> 两个 A 组合写的是同一张 `telemetry` 表，无法分别统计存储占用（写入吞吐仍分别度量）。\n")

		if a, okA := r.TableStats[tsdb.TableName(tsdb.PlanJSON)]; okA {
			if w, okW := r.TableStats[tsdb.TableName(tsdb.PlanWide)]; okW {
				ba, bw := bytesPerRow(a), bytesPerRow(w)
				if bw > 0 {
					fmt.Fprintf(&b, "> 落盘后 A 方案占用是 B 方案的 **%.2f×**（%.2f vs %.2f 字节/行）。\n", ba/bw, ba, bw)
				}
			}
		}
	}

	if r.Consistency != "" {
		b.WriteString("\n## 4. 一致性校验（结论前提）\n\n")
		fmt.Fprintf(&b, "%s\n", r.Consistency)
	}

	if len(r.Queries) > 0 {
		b.WriteString("\n## 5. 查询延迟（对齐 02 §4.3 的查询模式）\n\n")
		b.WriteString("| 查询 | 方案 | 返回行数 | P50 | P95 | P99 | max |\n")
		b.WriteString("|---|---|---:|---:|---:|---:|---:|\n")
		for _, q := range r.Queries {
			fmt.Fprintf(&b, "| %s | %s | %d | %s | %s | %s | %s |\n",
				q.Case.Name, planLabel(q.Plan), q.Rows,
				q.P50.Round(time.Microsecond), q.P95.Round(time.Microsecond),
				q.P99.Round(time.Microsecond), q.Max.Round(time.Microsecond))
		}

		b.WriteString("\n### 5.1 方案差距（宽表 ÷ JSON，> 1 表示宽表更快）\n\n")
		b.WriteString("| 查询 | P50 比值 | P95 比值 | P99 比值 |\n|---|---:|---:|---:|\n")
		for _, c := range queryCases {
			j, okJ := r.findQuery(c.Name, tsdb.PlanJSON)
			w, okW := r.findQuery(c.Name, tsdb.PlanWide)
			if !okJ || !okW {
				continue
			}
			fmt.Fprintf(&b, "| %s | %.2f× | %.2f× | %.2f× |\n",
				c.Name, ratio(w.P50, j.P50), ratio(w.P95, j.P95), ratio(w.P99, j.P99))
		}

		fmt.Fprintf(&b, "\n验收线：P95 < %s（06 §3.4 API 延迟 SLO）。\n\n", targetQueryP95)
		for _, plan := range plans(r.Variants) {
			var worst time.Duration
			var worstCase string
			for _, q := range r.Queries {
				if q.Plan == plan && q.P95 > worst {
					worst, worstCase = q.P95, q.Case.Name
				}
			}
			fmt.Fprintf(&b, "- %s：最差 P95 = %s（%s）→ %s\n",
				planLabel(plan), worst.Round(time.Microsecond), worstCase, verdict(worst < targetQueryP95))
		}

		// §5.2 展示 B1 补充项（1）的落地效果：同一 Q3 形态，未保护 vs 受保护。
		b.WriteString("\n### 5.2 明细限行前后对比（Q3 形态：10 设备 × 1h × 1Hz ≈ 3.6 万行）\n\n")
		b.WriteString("| 方案 | 入口 | 返回行数 | 粒度 | 桶宽 | P50 | P95 | P99 | 判定 |\n")
		b.WriteString("|---|---|---:|---|---:|---:|---:|---:|---|\n")
		for _, plan := range plans(r.Variants) {
			if raw, ok := r.findQuery("Q3 多设备近 1h 曲线", plan); ok {
				fmt.Fprintf(&b, "| %s | 未保护 SelectRange | %d | raw | — | %s | %s | %s | %s |\n",
					planLabel(plan), raw.Rows,
					raw.P50.Round(time.Microsecond), raw.P95.Round(time.Microsecond),
					raw.P99.Round(time.Microsecond), verdict(raw.P95 < targetQueryP95))
			}
			if prot, ok := r.findQuery("Q3P 多设备近 1h 曲线（受保护）", plan); ok {
				fmt.Fprintf(&b, "| %s | 受保护 QuerySeries | %d | %s | %s | %s | %s | %s | %s |\n",
					planLabel(plan), prot.Rows, prot.Granularity, prot.Bucket,
					prot.P50.Round(time.Microsecond), prot.P95.Round(time.Microsecond),
					prot.P99.Round(time.Microsecond), verdict(prot.P95 < targetQueryP95))
			}
		}
		b.WriteString("\n> 受保护入口命中 5000 行上限后**自动降采样**（不静默截断）：返回行数被桶数学压到 ≤5000，" +
			"代价是粒度从逐点变为时间桶均值；`Granularity` / `Bucket` / `CapHit` 随结果返回，" +
			"调用方可据此引导异步导出。落地细节与边界见 `02 §4.3.1.1`。\n")
	}

	if len(r.Notes) > 0 {
		b.WriteString("\n## 6. 备注\n\n")
		for _, n := range r.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
	}

	b.WriteString("\n> **数字口径**：全部为本机、本数据集下的实测值，不是承诺值。" +
		"被测实例是共享宿主的单机 standalone，与生产形态（3 副本集群 + 独立 NVMe）不同，" +
		"绝对吞吐不可外推；**同一实例内的相对比值**才是本报告用于选型的依据。\n")

	return b.String()
}

func (r *report) variantKeys() string {
	keys := make([]string, 0, len(r.Variants))
	for _, v := range r.Variants {
		keys = append(keys, v.Key)
	}
	return strings.Join(keys, ", ")
}

// bestJSON 返回 A 方案中吞吐最高的组合。
func (r *report) bestJSON() (variant, bool) {
	var best variant
	var bestRate float64
	found := false

	for _, v := range r.Variants {
		if v.Plan != tsdb.PlanJSON {
			continue
		}
		res, ok := r.Load[v.Key]
		if !ok {
			continue
		}
		rate := float64(res.Points) / res.Wall.Seconds()
		if !found || rate > bestRate {
			best, bestRate, found = v, rate, true
		}
	}
	return best, found
}

func (r *report) findQuery(name string, plan tsdb.Plan) (queryResult, bool) {
	for _, q := range r.Queries {
		if q.Case.Name == name && q.Plan == plan {
			return q, true
		}
	}
	return queryResult{}, false
}

func bytesPerRow(st greptimedb.TableStat) float64 {
	if st.Rows <= 0 {
		return 0
	}
	return float64(st.DiskSize) / float64(st.Rows)
}

func planLabel(p tsdb.Plan) string {
	switch p {
	case tsdb.PlanJSON:
		return "A · JSON 字段"
	case tsdb.PlanWide:
		return "B · 宽表"
	default:
		return string(p)
	}
}

func verdict(ok bool) string {
	if ok {
		return "**达标**"
	}
	return "**不达标**"
}

func ratio(a, b time.Duration) float64 {
	if b <= 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
