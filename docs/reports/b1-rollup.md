# B1 补充项（2）· 预聚合表与跨度路由

> 实测于 2026-10-01 ｜ 实例：devbox compose · GreptimeDB standalone（共享宿主） ｜ GreptimeDB 1.2.1 ｜ `postgres://***@100.64.0.3:28403/public`

本报告是 **B1 补充项（2）** 的落地证据（需求见 `02-domain-and-data.md` §7）。数字来自
`make test-rollup`（`IOT_GREPTIMEDB_DSN` + `IOT_PERF_ASSERT` 门控的真实库集成测试）。
B1 基线报告 `b1-tsdb-bench.md` 与上一份 `b1-detail-limit.md` **不改动**。

## 1. 判定

- **多设备 × 长跨度聚合（Q4 形态）**：在同一份 10 设备 × 6h × 1Hz（21.6 万行）fixture 上，
  原始路径 P95 **432.7ms**，经跨度路由改走 1m 预聚合表后 P95 **67.2ms** —— **6.4× 提速**，且稳定落在 P95<200ms 预算内。
- **再聚合无损**：预聚合存的 `sum/max/count` 与直接在原始数据上聚合**逐设备逐指标相等**（数值与布尔指标都验证，误差 <1e-9）。
- **幂等**：同一窗口重跑两次，行数与取值均不变 —— 预聚合表的覆盖写（非 append_mode）成立。
- 结论：**预聚合把「多设备 × 长跨度聚合」这一 A 方案唯一真实短板拉回预算内**，无需换宽表。

## 2. 实现

| 组件 | 落点 |
|---|---|
| 粒度与数据源契约 | `internal/tsdb/schema.go`：`Rollup`（1m/1h）、`Source`（raw_json/raw_wide/rollup_1m/rollup_1h）、纯函数 `RouteSource(plan, span)` |
| 表结构 | `telemetry_1m` / `telemetry_1h`：**长表** `(ts, project_id, device_id, device_type_id, "metric", "sum", "max", "count")`，主键 `(project_id, device_id, "metric")`，**不设 append_mode** |
| 物化 | `greptimedb.Store.RollupWindow`：每指标一条 `INSERT INTO <rollup> SELECT date_bin(...), ... SUM/MAX/COUNT FROM telemetry WHERE ts >= $1 AND ts < $2 GROUP BY ...` |
| 调度 | **`cmd/svc-rollup`**：定时增量，单飞靠 PG 咨询锁；水位落 PG `t_rollup_watermark`（迁移 `0005`） |
| 路由 | `greptimedb.QuerySeries` 按跨度选源：≤6h 原始 / 6h–30d 1m / >30d 1h；只对 JSON 方案路由（宽表聚合已达标） |
| 再聚合 | agg 源：`AVG→SUM("sum")/NULLIF(SUM("count"),0)`、`MAX→MAX("max")`、`COUNT→SUM("count")`，加 `"metric"='<k>'` 过滤 |

**为什么存 `sum/count` 而不是 `avg`**：粗桶（5min）由细桶（1m）再聚合时，「对 avg 再平均」是错的，
只有 `SUM(sum)/SUM(count)` 才与直接在原始数据上聚合逐桶相等。守恒测试正是钉住这一点。

**为什么长表**：预聚合现在对**所有租户强制**，若按指标建列，物模型每加一个指标就要 DDL ——
那就把 A 方案「变更零成本」这个核心卖点丢掉了。长表下 `metric` 是行值，加指标零 DDL。

## 3. 开工探针（`internal/tsdb/greptimedb/capability_test.go`）

全部在临时表 `probe_*` 上实测（不碰 telemetry），六项**全部通过，无一需要回退方案**：

| # | 未知 | 结论 |
|---|---|---|
| P-a | `INSERT … SELECT` 里能否用绑定参数 `$1/$2` | ✅ 可用（含 `date_bin` + `SUM/MAX/COUNT` + `GROUP BY`） |
| P-b | 非 append 表重写同 `(主键, ts)` 是覆盖还是追加 | ✅ **覆盖**（行数不变、取值更新）→ rollup 幂等由表结构保证 |
| P-c | `json_get("metrics",'running',0.0)` 对 JSON 布尔 | ✅ 返回数值 `1` → 布尔指标与数值指标同一套聚合 |
| P-d | rollup 表形态是否接受 `WITH ('ttl'=…)` | ✅ 接受（1m 用 `35d`、1h 用 `365d`） |
| P-e | `metric` 是否保留字 | ⚠️ **是**（裸写报 `Cannot use keyword 'metric'`）→ DDL/SQL 一律 `"metric"` |
| P-f | 全 NULL 分组的 `SUM` 语义、`NULLIF` 是否可用 | ✅ 写出行、`sum/max=NULL`、`count=0`；`NULLIF` 可用 |

> 另一条只读结论（HTTP SQL 探针）：`json_object`/`json_build_object`/`to_string` 不存在、
> `CAST(x AS JSON)` 不支持、`json_get` 首参必须是列 —— 这排除了「预聚合表沿用 JSON 列」的方案，
> 是本次改用原生列的直接原因。

## 4. 正确性与幂等（真实库）

- fixture：10 设备 × 6h × 1Hz = **216,000 行**（窗口 2026-10-01T03:57Z ~ 09:57Z）。
- 物化：360 个 1m 窗口 × 5 指标 = **1800 条 INSERT**，单线程测试循环耗时 **87.3s**。
- `TestRollup_Integration/Conservation`：`temperature`（数值）与 `running`（布尔）在抽检窗口上，
  10 台设备的 `sum/max/count` 与原始聚合**逐项相等**（误差 <1e-9）。
- `TestRollup_Integration/Idempotent`：重跑同一窗口后行数仍 **50**、`sum` 仍 **1510**（不变）。

## 5. 路由提速（Q4 形态，20 次采样）

| 路径 | 数据源 | 返回行数 | 桶宽 | P95 |
|---|---|---:|---:|---:|
| 原始（`SelectBucketsUnprotected`） | `telemetry` | 730 | 5m | **432.7ms** |
| 受保护路由（`QuerySeries`，跨度 24h） | `telemetry_1m` | 730 | 5m | **67.2ms** |

`QuerySeries` 返回 `Source=rollup_1m`、`Granularity=downsampled`、`Bucket=5m`、`CapHit=false`（跨度路由不是
「明细超上限」，来源由 `Source` 表达）。两条路径返回**同样的 730 行**。

## 6. 未验证项（不要当成「已验证」）

| # | 未验证 | 影响与后续 |
|---|---|---|
| 1 | **全量规模下的原始基线** | 本 fixture 是隔离表（仅 21.6 万行、写入后未落盘/未 compaction），故原始路径 432.7ms **与 B1 Q4 的 196ms 不可直接比**；这里可信的是**同一 fixture 内的相对改善**。上生产需在真实体量上复测 |
| 2 | **1h 表路由（>30d）** | 本次只压了 1m 路径；1h 路径逻辑相同但未单独实测延迟 |
| 3 | **迟到数据回修** | 窗口在 `lag`（=2×宽度）之后不再修正；回填更早历史需 `-backfill-since` |
| 4 | **首次水位回看范围** | 默认只回看 24h；更早历史不自动回填，>30d 查询可能命中空的 1h 表（运维步骤） |
| 5 | **多副本调度** | 单飞靠 PG 咨询锁，仅单副本验证；多副本竞争未压测 |
| 6 | **宽表方案的预聚合** | 本批只物化 JSON 方案（宽表聚合已达标，不路由） |
| 7 | **物化吞吐** | 测试为单线程串行，1800 条 INSERT 用 87s；生产增量（每分钟 ~60 窗口）足够，但未做批量优化与并发写入压测 |

## 7. 数字口径

全部为本机、本数据集下的实测值，不是承诺值。被测实例是共享宿主的单机 standalone，
与生产形态（3 副本集群 + 独立 NVMe）不同，绝对延迟不可外推；**同一实例内的相对改善**
才是本报告用于判定的依据。fixture 的原始基线尤其受「表内数据量」影响（见 §6 第 1 项）。
