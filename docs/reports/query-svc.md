# 只读查询服务 `svc-query` · 验收记录

> 实测于 2026-10-01 ｜ 实例：devbox（宿主 Docker Compose）｜ PG `100.64.0.3:28543/odoo20iot` ｜ GreptimeDB `100.64.0.3:28403/public`

本报告记录 **控制面主数据（迁移 0006）+ 只读查询 API** 的落地与验收（需求：`02 §3` / §4.3）。
命令：`make test-catalog`、`make test-query`、`make test-query-e2e`，以及一次真实二进制的 curl 走查。

## 1. 判定

- **迁移 0006** 应用成功：`t_project` / `t_device_type` / `t_device`（16 个 HASH 分区、复合外键、3 个 CHECK）落地。
- **租户隔离**：设备与设备类型必须同租户由**复合外键在 DB 层**保证；查询 API 拒绝请求里的 `project_id`（只认令牌的 `tenant`）。
- **查询链路端到端可用**：真实 PG + 真实 GreptimeDB 下，`/api/v1/series` 返回逐点明细与分桶聚合，元数据（`granularity`/`source`/`bucket`/`cap_hit`）如实回传。
- **鉴权**：无令牌 401、跨租户设备 403、指标越界 422、租户参数 400 —— 全部由机器可读错误码表达。
- **保护生效**：每租户并发上限 20 + 有界排队（溢出 503）；慢查询 >3s 记指标 + WARN。

## 2. 落地范围

| 层 | 落点 |
|---|---|
| 控制面表 | `internal/pg/migrations/0006_control_plane_master_data.sql`（PK 修正为 `(id, project_id)`、**不启用 RLS**，理由见迁移注释与 `02 §3.5`） |
| 主数据访问 | `internal/catalog`：契约 + `PGStore` + `MemStore` + `secret.go`（Argon2id 摘要编解码，复用 `internal/auth`） |
| 开发种子 | `cmd/iot-seed`：幂等 upsert 租户/类型/设备；**仅新建或 `-rotate` 时生成凭据**；凭据写 `tmp/`（0600，含明文） |
| 认证 | `internal/apiauth`：JWT（ES256/RS256、`iss` 允许列表、`aud`、**强制 `tenant`**、`jti` 吊销 fail-closed）+ 开发静态令牌 + `RequireAuth` 中间件 |
| 查询服务 | `internal/querysvc`：`SeriesReader` 接口、稳定错误码、每租户限流、handler；`cmd/svc-query` 装配 |
| 时序读路径 | 复用 `greptimedb.Store.QuerySeries`；新增 `SeriesQuery.Bucket`（显式分桶 → `Granularity=aggregated`） |

## 3. HTTP 契约与状态映射

`GET /api/v1/devices?device_type_id&status&q&cursor&limit` → `{ok, devices[], next_cursor}`
`GET /api/v1/series?device_ids&metric&since&until&bucket&limit` → `{ok, granularity, source, bucket, cap_hit, points[]|buckets[]}`

| 情形 | 状态 | code |
|---|---|---|
| 参数非法 / 时间或时长格式错 | 400 | `INVALID_ARGUMENT` |
| 请求里出现 `project_id` | 400 | `TENANT_NOT_ALLOWED` |
| 令牌缺失/非法/过期/签名错/缺 tenant/缺 jti/已吊销 | 401 | `UNAUTHENTICATED` |
| 设备不存在或不属于本租户（**同一响应，不泄露存在性**） | 403 | `FORBIDDEN` |
| 违反查询保护（设备数/指标白名单/时间范围/桶宽/行数） | 422 | `UNPROCESSABLE`（带 `rule`） |
| 回溯超 90 天 | 422 | `UNPROCESSABLE` + `export_required:true` |
| 每租户排队满/超时 | 503 | `QUERY_BUSY` + `Retry-After` |
| 认证依赖（吊销表/JWKS）不可用 | 503 | `AUTH_UNAVAILABLE`（fail-closed） |
| 查询超时 | 504 | `QUERY_TIMEOUT` |
| 其余 | 500 | `INTERNAL`（细节只进日志） |

## 4. 验收证据

### 4.1 真实库测试

- `make test-catalog`（真 PG + 临时库）：分区数=16、`pk_device` 含 `project_id`、**RLS 未启用**、
  upsert 幂等且 `rotate=false` 不覆盖凭据、keyset 分页、`DeviceIDsOwned` 排除他租户与软删、
  **跨租户挂类型被外键拒绝**、重复 `project_key` 被拒。**7/7 通过**。
- `make test-query`（无真实依赖）：JWT 各失败形态（含 `alg=none`/`HS256`/错 `aud`/缺 `tenant`/
  已吊销/**吊销依赖故障 fail-closed**）、开发令牌、限流器（并发上限、队列有界、空闲驱逐、`-race`）、
  handler 全状态码。**全绿**。
- `make test-query-e2e`（真 PG + 真 GreptimeDB）：种子 2 台设备 + 240 行遥测 →
  `/api/v1/devices` 2 台、`/api/v1/series` 原始 240 点（`source=raw_json`）、显式 `bucket=1m`
  返回 `granularity=aggregated`、`project_id` 参数 400、未知设备 403。**通过**。

### 4.2 真实二进制 curl 走查

```
$ go run ./cmd/svc-query -auth-mode dev -dev-token devtoken -dev-project-id 1
$ curl -s http://127.0.0.1:18094/readyz
{"greptimedb":"reachable","ok":true,"pg":"up-to-date"}

$ curl -s -H 'Authorization: Bearer devtoken' 'http://127.0.0.1:18094/api/v1/devices?limit=2'
{"ok":true,"devices":[{"id":1001,"device_key":"dev-1","name":"种子设备 dev-1","device_type_id":1,
 "status":"active","auth_mode":"per_device","online":false,"tags":{"seed":true},"version":2, ...}],
 "next_cursor":"1002"}

$ curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18094/api/v1/devices
401

$ curl -s -H 'Authorization: Bearer devtoken' 'http://127.0.0.1:18094/api/v1/devices?project_id=999'
{"ok":false,"code":"TENANT_NOT_ALLOWED","message":"租户只能来自令牌，不能作为查询参数"}

$ curl -s -H 'Authorization: Bearer devtoken' '.../api/v1/series?device_ids=1001,1002&metric=temperature&since=<1h前>&until=<now>'
{"ok":true,"granularity":"raw","source":"raw_json","cap_hit":false,
 "points":[{"ts":"2026-10-01T10:32:00Z","device_id":1001,"value":20}, ...]}

$ curl -s -H 'Authorization: Bearer devtoken' '.../api/v1/series?device_ids=1001&metric=temperature&bucket=1m&since=<1h前>&until=<now>'
{"ok":true,"granularity":"aggregated","source":"raw_json","bucket":"1m0s","cap_hit":false,
 "buckets":[{"bucket":"2026-10-01T10:32:00Z","device_id":1001,"avg":24.5,"max":29,"count":60}, ...]}

$ curl -s -H 'Authorization: Bearer devtoken' '.../api/v1/series?device_ids=1001&metric=not_a_metric'
{"ok":false,"code":"UNPROCESSABLE","message":"指标 \"not_a_metric\" 不在物模型白名单内","rule":"metric"}
```

> 设备密钥只在 `cmd/iot-seed` 的 `tmp/iot-seed-creds.json` 里（0600、含明文、已被 `.gitignore` 的 `tmp/` 覆盖）；
> `/api/v1/devices` 的响应**从不包含** `secret_hash`（有用例钉住）。

## 5. 未验证 / 未实现（不要当成「已完成」）

| # | 项 | 说明 |
|---|---|---|
| 1 | **RLS** | 未启用；租户隔离目前只在应用层（catalog 首参 + Normalize + 拒绝 query 参数） |
| 2 | **最新值（Redis Write-Through）** | 未实现；`/api/v1/series` 是唯一的读接口 |
| 3 | **历史导出** | 未实现；只有 `export_required` 提示 |
| 4 | **多指标投影（≤4）** | 未实现；`MaxProjectedMetrics=4` 只是 tsdb 契约常量，API 是单指标 |
| 5 | **Odoo 设备/类型同步** | 未实现；`cmd/iot-seed` 是开发替身（Odoo 才是真相源） |
| 6 | **按设备类型的物模型校验** | 未实现；仍是 tsdb 内的全局 5 指标白名单 |
| 7 | **慢查询自动降级** | 只有指标 + WARN，没有自动切预聚合表 |
| 8 | **每租户速率限制 / 读写连接池分离** | 未实现；只有并发上限（进程内，多副本会放大） |
| 9 | **前端** | Vue 3 + TypeScript 控制台已完成 M1.1-M1.3，并由 `svc-query` 通过 Go embed 提供；M2 的 Odoo 业务闭环与经营融合看板待后端 Odoo API。 |
| 10 | **生产 ID / JWKS 轮转 / scope 逐端点校验** | 均未做；JWT 只校验、不签发 |

## 6. 数字口径

本报告是**功能验收**，不是性能报告。延迟相关结论（P95<200ms、行数上限、跨度路由）见
`b1-tsdb-bench.md` / `b1-detail-limit.md` / `b1-rollup.md`；本服务未做并发/吞吐压测。

## 7. 2026-10-02 产品化增量

已完成：

- `svc-query` 支持用户级 systemd 常驻运行，配置从受保护的 EnvironmentFile 注入。
- 查询服务启动时可幂等创建遥测表和 `telemetry_1m` / `telemetry_1h` 预聚合表。
- 增加 HTTP 安全响应头，生产 TLS 下自动发送 HSTS。
- 增加 `/api/v1/series/multi?metrics=a,b`，最多同时投影 4 个指标。
- 增加 `/api/v1/export` CSV 导出接口，前端设备详情页提供“导出 CSV”。
- 局域网和 Tailscale 入口由独立用户级 systemd 代理自动恢复。

仍需生产化：

- JWT/OIDC 身份源和用户管理尚未在本仓库内签发；当前服务仍可用开发 Token 验收。
- 真实 Odoo S1/S3 场景、数据库备份恢复演练、A4 连接压测和真实飞书发送测试不在本次执行范围内。
