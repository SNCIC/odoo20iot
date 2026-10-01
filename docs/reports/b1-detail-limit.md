# B1 补充项（1）· 明细查询限行与自适应降采样

> 汇编于 2026-10-01 ｜ 实例：devbox compose · GreptimeDB standalone（共享宿主） ｜ GreptimeDB 1.2.1 ｜ `postgres://***@100.64.0.3:28403/public`

本报告是 **B1 补充项（1）** 的落地证据（需求见 `02-domain-and-data.md` §4.3.1）。数字来自
2026-10-01T17:35+08:00 的一次 `make b1-bench` 运行（含受保护用例 Q3P）与一次**一次性探测诊断**
（在该次运行留下的数据集上测量，方法见 §3）。B1 基线报告 `b1-tsdb-bench.md` **不改动** ——
它记录的是「限行前」的原始测得值，两者互补。

## 1. 判定

- **Q3 形态（10 设备 × 1h × 1Hz ≈ 3.6 万行明细）**：未保护入口两方案 P95 都超线（JSON 257.4ms / 宽表 224.3ms，
  验收线 P95 < 200ms）；经受保护入口 `QuerySeries` **自动降采样**后，JSON **152.4ms**、宽表 **106.7ms**，**双双达标**。
- 返回行数由 **35990 → 3610**（≤ 上限 5000），粒度从逐点标注为 `downsampled`、桶宽 `10s`。
- 结论：**明细限行解决了「多设备 × 长时间逐点明细」这一形态的超线问题**，与表模型选择无关。

## 2. 实现

`internal/tsdb`（契约，无方言）+ `internal/tsdb/greptimedb`（适配层）：

| 组件 | 作用 |
|---|---|
| `SeriesQuery.Normalize(now)` | 校验保护规则（project_id>0、设备 1..50、指标白名单、Since<Until、回溯 ≤90d、Limit ∈[1,5000]）并补默认值 |
| `AdaptiveBucket(span, devices, rowCap)` | 在「设备数 × 桶数 ≤ rowCap」下求最小桶宽，向上取整到可读阶梯（1/2/5/10/15/30s、1/2/5/10/15/30min、1/2/3/6/12/24h） |
| `Store.QuerySeries` | Normalize → **廉价探测** → 未超限返回原始明细；超限按 `AdaptiveBucket` 降采样返回，带 `Granularity/Bucket/CapHit` |

**边界语义**：`Since` 严格（`ts > Since`）、`Until` 含（`ts <= Until`），`Until` 零值=不设上界。
**不静默截断**：降采样是对「返回行数上限」的替代答案；若聚合结果仍超限，直接报错而不是截断。
**防绕过**：裸查询改名为 `SelectRangeUnprotected` / `SelectBucketsUnprotected`（仅压测/诊断），
业务侧唯一读入口是 `QuerySeries`。

## 3. 为什么探测用「只取常量的 LIMIT」

一次性诊断在该次运行留下的 **1.12M 行 `telemetry`** 上，对 10 台高频设备近 1h 窗口（30 次采样）测得：

| 候选 | 行数 | P50 | P95 |
|---|---:|---:|---:|
| a 现行探测：`值 + ORDER BY + LIMIT 5001` | 5001 | 89.1ms | 95.7ms |
| b `值 + LIMIT 5001`（无 ORDER BY） | 5001 | 61.5ms | 68.3ms |
| **c `SELECT 1 ... LIMIT 5001`（无值、无排序，采用）** | 5001 | **18.3ms** | **20.4ms** |
| d `COUNT(1)` 全量 | 1 | 18.0ms | 19.2ms |
| e 聚合：10s 桶 AVG/MAX | 3610 | 110.7ms | 115.3ms |

**读法**：探测的职责只是「超没超」，不解析指标、不排序，因此 P95 从 **95.7ms 砍到 20.4ms**；
超限路径的总成本因此从 ≈211ms 降到 ≈135ms，把余量从 ~0 拉到 ~65ms。选 c 而非 d（COUNT）：c 在
超限时于第 5001 行处**尽早终止**，对更大的窗口更友好；d 每次都全量扫描匹配行。

> 该诊断依赖压测留下的数据集，属**一次性测量**，未入库为常驻用例；复现方式：`make b1-bench` 后
> 对 `telemetry` 跑上表 SQL（设备 `2000000..2000009`、窗口取 `max(ts)-1h`）。

## 4. 前后对比（`make b1-bench` §5.2，30 次采样）

| 方案 | 入口 | 返回行数 | 粒度 | 桶宽 | P50 | P95 | P99 | 判定 |
|---|---|---:|---|---:|---:|---:|---:|---|
| A · JSON 字段 | 未保护 `SelectRange` | 35990 | raw | — | 244.9ms | **257.4ms** | 258.2ms | ❌ |
| A · JSON 字段 | 受保护 `QuerySeries` | 3610 | downsampled | 10s | 145.6ms | **152.4ms** | 153.4ms | ✅ |
| B · 宽表 | 未保护 `SelectRange` | 35990 | raw | — | 210.6ms | **224.3ms** | 224.4ms | ❌ |
| B · 宽表 | 受保护 `QuerySeries` | 3610 | downsampled | 10s | 102.2ms | **106.7ms** | 107.8ms | ✅ |

验收线：P95 < 200ms（06 §3.4 API 延迟 SLO）。

## 5. 集成测试（小表 fixture，`make test-tsdb`）

`internal/tsdb/greptimedb/query_test.go` 在一个独立的 3.6 万行 fixture 上验证语义：

- `TestQuerySeries_DownsamplesAndMeetsSLO`：粒度 `downsampled`、`CapHit=true`、桶宽 `10s`、
  每时间桶恰含 10 台设备；受保护查询 P95 **131.5ms**、廉价探测单独 P95 **20.3ms**（均 < 200ms）。
- `TestQuerySeries_SmallStaysRaw`：单设备 10min → 600 点，粒度 `raw`、`CapHit=false`、不带桶。
- `TestQuerySeries_UnprotectedBaseline`：同形态未保护路径返回 36000 行、P95 209.6ms（对照）。

## 6. 未验证项（不要当成「已验证」）

| # | 未验证 | 影响与后续 |
|---|---|---|
| 1 | **并发查询** | 本节为串行采样；02 §4.3 要求单租户并发上限 20，需补测并发相互影响 |
| 2 | **超限后的第二条聚合查询**在大窗口下的绝对延迟 | 本文只压了 1h/24h；50 设备 × 90d 的组合只有桶数学上界（≤5000 行），未实测 |
| 3 | **多指标投影（≤4）** | 当前 `SeriesQuery` 是单指标；`MaxProjectedMetrics=4` 只作了契约常量，端点为后续批次 |
| 4 | 快路径（未超限）的**双查询**开销 | 未超限需「探测 + 取数」两次往返；小结果下未单独压测 |
| 5 | **预聚合表路由** | 本项与预聚合表正交：限行治「明细形态」，预聚合治「多设备 × 长跨度聚合」（Q4 JSON P95 196ms 仍骑线） |

## 7. 数字口径

全部为本机、本数据集下的实测值，不是承诺值。被测实例是共享宿主的单机 standalone，
与生产形态（3 副本集群 + 独立 NVMe）不同，绝对延迟不可外推；**同一实例内的相对改善**
才是本报告用于判定的依据。
