# 09 · 交接与工作约定

> **这份文档的作用**：让**新会话 / 新同事**读完这一份就能接着干，**不依赖任何对话上下文**。
> 每完成一个阶段、或改变工作方式时，**必须回来更新它**。

---

## 1. 一句话现状

**Phase 0（技术验证）三个 P0 决策点全部通过**：
- **A2（QoS1 PUBACK 时机）** —— ADR-001 的定制点成立，**不必回退 EMQX**（`03` §4.4.1）；
- **B1（GreptimeDB JSON vs 宽表）** —— JSON 方案写入达标 3.8×，**维持 JSON 为默认，不切宽表**（`02` §4.1.1）；
- **C1（`expr` 条件引擎）** —— 编译期校验与 P99 均达标，但**校验必须自建 AST 层**（`04` §1.2.1）。

**Phase 1 已开工**：**A1 设备认证**（三档 + 物模型 ACL + Argon2 容量实测）已完成；
**A3 集群路由**（跨节点投递 + 离线队列 + 接收端去重 + 生产启动装配）已完成；
**消息管道 `svc-pipeline`**（统一信封 + 物模型解析 + 攒批 + GreptimeDB 写入 + 落库后 ACK + 两阶段幂等）MVP 已完成并端到端跑通；
**A5 计量埋点**（网关热路径累加 + 窗口批量上报 + Redis 用量计数器）原型已完成，计数误差实测为 0；
**D2/D3 Odoo 集成骨架**（Go 侧 JSON-2 客户端 + Odoo 侧 `sn_edge_integration`）已完成、**已在 odoo20 库安装验证**并提交推送；
**`odoo-connector`** 已完成 07 §4.3 编排层（限流/准入/熔断/重试/幂等占位）、§4.4 的**两条事件入口**（C-1 Outbox→NATS、C-2 webhook→NATS）与 **15min 定时对账**（Outbox 非终态行补投 + C-2 水位差补投）；Odoo 侧 Outbox cron 投递也已接通。均端到端实测通过（真实 Odoo、真实 Redis、真实 NATS）。

**Phase 2 已开工**：**L2 自研 DAG 编排执行器**（04 §1.3 六种节点 + 编译期约束校验 + 三种错误策略）已完成；
**L3 告警五态 FSM**（04 §2.1-2.2 五态 + 去重/静默/风暴/根因抑制 + 聚合判定 + 告警质量指标）已完成。
**PG 地基**（`internal/pg` 连接池 + 带校验和的迁移器、`cmd/iot-migrate`、`t_alarm_active` 非分区活跃索引表、`alarm.PGStore`）已完成，并已对业务库 `odoo20iot` 应用迁移；
**`svc-alarm` 服务**（双通道推进 + 5s 扫描 + PG 咨询锁互斥 + 至少一次发布）已完成，并对真 PG + 真 NATS 跑通端到端。
**`svc-notify` 三通道通知**（Webhook / 邮件 / 短信 + SSRF 防护 + 降级 + 重试阶梯 + 死信 `t_dlq`）已完成，并对真 NATS 跑通端到端。

**「重启即重放」缺陷已全部修净**：`svc-notify` / `svc-alarm` / `svc-pipeline` / `svc-quota` / `internal/cluster` 五处统一走
`internal/natsjs`；且 `natsjs.Subscribe` 的返回值已收窄为**不暴露 `Unsubscribe`**，使该缺陷在类型上**无法再被写出来**
（坑 42/47）。`svc-quota` 的用量重复计数已用真 NATS 演练复现并消除（见 §4 第 21 项）。

下一步：A4 连接压测待有干净环境后再跑；Odoo 侧 S1/S3 集成场景端到端；
B1 的明细查询限行与预聚合表（P0 性能项）。

---

## 2. 环境事实（均已实测，不要凭推测）

### 2.1 拓扑

```
[任意设备] ──Tailscale──► 100.64.0.3（别名 xfusion-163）
                              │
                宿主 xfusion-163 · Ubuntu 24.04.3 · 64C / 62G / 7.3TB
                 ├── Docker：dify(80/443)、n8n(5678)、监控(3001/3002/9090/9093)
                 └── ★ odoo20iot 开发栈（本项目，28xxx 端口）
                              │
                devbox 容器 · Ubuntu 26.04.1 · 限 16C / 16G · systemd · 容器内不跑 Docker
                 ├── odoo@odoo20tbb  → :8070   ← 集成目标
                 ├── odoo@odoo19wsd  → :8069
                 ├── postgresql@18   → 127.0.0.1:5432（PG 18.6）
                 └── cc-connect      → :9810 / :9820
```

**关键**：`/home/xfusion/projects` 在**宿主与 devbox 内是同一份**（bind mount）。

> **注意 RTT**：从外部设备经 Tailscale 访问服务器约 **111 ms（DERP 中继）**。
> 这个延迟**不进** `odoo-connector` ↔ Odoo 的路径（两者同在服务器上），因此不影响延迟预算。

### 2.2 关键路径

| 用途 | 路径 |
|---|---|
| **本项目（唯一真相源）** | `/home/xfusion/projects/odoo20iot` |
| Odoo 20 源码 / 自定义模块 | `/home/xfusion/projects/odoo/odoo20tbb/{odoo,addons}` |
| Odoo 20 配置 | `/home/xfusion/etc/odoo/odoo20tbb.conf`（**在 devbox 容器内**） |
| Odoo 20 日志 | `/home/xfusion/logs/odoo20tbb.log` |
| Odoo 环境约定（**必读**） | `/home/xfusion/projects/AGENTS.md` |
| devbox 设计说明 | `/home/xfusion/devbox/README.md` |

### 2.3 工具链

| 工具 | 位置 / 值 | 说明 |
|---|---|---|
| **Go 1.27.1** | devbox `/usr/local/go`，软链 `/usr/local/bin/go` | **必须以 `xfusion` 用户运行**：`docker exec -u xfusion devbox ...`。⚠️ 若直接 `docker` 报 `permission denied`，**不要以为是没权限** —— `xfusion` 在 `docker` 组(984)里，但**启动 agent 的会话其进程组列表可能不含 984**（`id -nG` 看不到 docker）。用 `sg docker -c '...'` 让它按 `/etc/group` 重新解析即可（`devbox/README.md` 也是这么写的）。另：devbox 的 sshd 是 `PubkeyAuthentication no`，**只能密码登录**，不要走 SSH |
| `GOPROXY` | `https://goproxy.cn,direct` | **官方 `proxy.golang.org` 不可达**，必须走镜像 |
| `GOTOOLCHAIN` | `local` | 禁止自动下载其他工具链 |
| git | devbox 2.53.0 | 提交身份 `Gavin <963645882@qq.com>` |
| Docker / Compose | 宿主 29.8.1 / v5.5.1 | 镜像源已配国内镜像（daocloud / 1panel） |
| Odoo 20 | devbox `odoo@odoo20tbb` | 容器内 `127.0.0.1:8070`，Tailscale `100.64.0.3:9070` |
| `mochi-mqtt/server/v2` | **`v2.7.9`（必须锁版本）** | A2 的挂载点依赖其 `processPublish` 对 `packets.ErrRejectPacket` 的处理（见 `03` §4.4.1 边界 5） |
| **NATS 消费者** | 统一走 `internal/natsjs.Subscribe` | 它返回**收窄的 `*natsjs.Subscription`（只暴露 `Fetch`）**，调用方拿不到 `Unsubscribe`，写错会编译失败（见 §6 坑 42/47）；`InactiveThreshold` 必须显式设（默认空闲回收 5 分钟，坑 43）。**直接用原生 `js.PullSubscribe` 属于绕过约定**，仅在 `cluster.ReplayOffline` 的临时消费者处允许 |
| **NATS Stream** | 统一走 `internal/natsjs.EnsureStream` | **建或校**：既存流若与 spec 漂移则 `UpdateStream` + WARN（「只建不校」会让先于口径代码建出来的流永远停在 `MaxAge=0`，见 §4 第 22 项）。零值表示**不约束**（不拿默认值覆盖部署配置）；`StrictSubjects` 用于「不自动改订阅范围」的场景 |
| `nats-io/nats.go` | `v1.54.0` | 纯 Go，无 cgo（ADR-010） |
| **PostgreSQL 18.6** | 项目栈 `iot-postgres`（宿主容器），`100.64.0.3:28543` | **业务库 `odoo20iot`**，角色 `iot`（凭据见 `deploy/compose/docker-compose.yml`）。⚠️ devbox 内的 `postgresql@18`（`127.0.0.1:5432`）是 **Odoo 的库实例**（`odoo20`/`erp_dev`），**不要混用** —— 见 §6 坑 39 |
| `jackc/pgx/v5` | `v5.11.0` | 访问 GreptimeDB 的 PostgreSQL wire 端点；纯 Go。**注意 `Ping()` 与 simple protocol 都不可用**，见 §6 坑 19 |
| GreptimeDB | **`1.2.1`**（compose 里是 `:latest`，**上生产前必须锁版本**） | JSON 能力边界按 1.2.1 实测，见 `02` §4.1.1 ④ |
| `expr-lang/expr` | `v1.17.8` | 规则条件 DSL。**热路径必须用 `rules.Runner` 复用 VM**，否则 P99 骑线（`04` §1.2.1 ⑤） |

### 2.4 端口规划（全部避开宿主已占用）

宿主已占用：`80/443`（dify-nginx）、`3001`（grafana）、`3002`（uptime-kuma）、`5678`（n8n）、
`8060`、`9090`（prometheus）、`9093`（alertmanager）、`9069/9070/9072/9073`（devbox Odoo）、
`9910/9920`（cc-connect）、`2222`（devbox ssh）。

本项目统一使用 **28xxx 段**，且**只绑定 Tailscale IP `100.64.0.3`**（局域网 `192.168.127.163` 上不暴露）：

| 服务 | 容器内 | Tailscale 发布 |
|---|---|---|
| PostgreSQL 18 | 5432 | `100.64.0.3:28543` |
| Redis 7 | 6379 | `100.64.0.3:28637` |
| NATS 2（JetStream） | 4222 / 8222 | `100.64.0.3:28222` / `28224` |
| GreptimeDB | 4000 / 4001 / 4003 | `100.64.0.3:28400` / `28401` / `28403` |

> **副作用**：Tailscale 未启动时本栈无法启动（绑定的是 Tailscale IP）。
> **开发凭据**：PG `iot / iot_dev_only_change_me`，库 `odoo20iot`。

---

## 3. 工作约定（**最容易出错的部分，务必遵守**）

### 3.1 Git

- **真相源是服务器** `/home/xfusion/projects/odoo20iot`。
- **git 操作一律在 devbox 内执行**（SSH 密钥与别名都在那里）：

  ```bash
  docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && git status'
  ```

- **remote 必须使用专用别名**（每个仓库一把 deploy key）：

  ```
  git@github-odoo20iot:SNCIC/odoo20iot.git
  ```

  ⚠️ 写成 `git@github.com:...` 会命中 `github.com` 别名 → 使用 wsd 的 key → push 报
  `ERROR: Repository not found`。**这个坑已经踩过一次。**

- **第二个仓库：Odoo 自定义模块（D3）**。`sn_edge_integration` **不在本仓库**，
  而在 `/home/xfusion/projects/odoo/odoo20tbb/addons/`（独立仓 `SNCIC/odoo20tbb`，
  远端别名 `github-odoo20tbb`）。改动该模块时 Git 操作要指向那个目录与该别名，
  **不要误提到本仓库**；反之亦然。

- 提交身份 `Gavin <963645882@qq.com>`；风格 `type(scope): 中文摘要——补充说明`，正文写背景 / 实现要点 / 验证结论。
- 直接提交推 `main`，不开分支、不走 PR（沿用 `AGENTS.md`）。

### 3.2 远程命令的引号规则（**踩过多次**）

执行层 shell 会 **吞掉 `$`**，并且 **不支持 `\"` 转义**。

**可用**：外层双引号 + 内层只用单引号

```bash
ssh xfusion-163 "docker exec -u xfusion devbox bash -lc 'go version'"
```

**不可用**：`\"...\"`、内层再嵌单引号、`$VAR`、`$(...)`

需要复杂脚本时：**本地写脚本文件 → `scp` 到服务器 → 执行**。

### 3.3 与 Windows 本地副本的关系（**需要同步的内容**）

编辑工具绑定在 IDE 工作区根目录。早期工作区是 Windows 的 `d:/odoo/odoo20iot`，因此**在那边产生过一份副本**。

| 位置 | 状态 | 处置 |
|---|---|---|
| `d:/odoo/odoo20iot`（Windows） | 内容与服务器一致，但停留在旧 commit `0a54067`，且带未提交改动 | **对齐或删除**：`git fetch && git reset --hard origin/main`，或直接删目录 |

**服务器上没有任何内容依赖本地副本** —— 全部已入库并推送到 GitHub。

> ⚠️ 这条断言本身出过一次偏差（2026-10-01 接管时发现）：`origin/main` 实际停在 `cc86e81`（A1），
> 而本地已领先 **6 个 commit**（Phase 0 收尾 + Phase 2 一~五批），即「已全部推送」当时**不成立**。
> 已于接管当日补推，现远端为 `85610dc`。
> **教训**：这类断言不能靠印象维护 —— 它每次都会被写文档时的乐观情绪带偏。
> 判断应以 `git log --oneline origin/main..HEAD`（应为空）或 `git ls-remote origin main` 为准，而不是历史叙述。

**目标工作方式**：IDE 工作区直接指向服务器目录 `/home/xfusion/projects/odoo20iot`，
此后不存在第二份副本。

> ⚠️ **绝不允许出现两个真相源。** 这正是本项目文档阶段反复出错的根因
> （见 `odoo20iot-revision-checklist.md` 的「摘要与权威源并存 → 摘要必然漂移」）。

### 3.4 换行符

仓库用 `.gitattributes` 固定 **LF**。任何在 Windows 侧产生、经 CRLF 转换的文件，提交前必须归一化：

```bash
# 归一化全部被跟踪文件，并刷新 stat 缓存（git add -A 不会产生任何暂存内容）
git ls-files | xargs sed -i 's/\r//' && git add -A
```

> **注意**：`git checkout -- .` **清不掉** CRLF —— 因为 git 对 `text=auto` 文件的内容比较是归一化的，
> 它认为工作区"没有变化"，于是根本不重写文件。

---

## 4. 已完成（含证据）

| # | 项 | 证据 |
|---|---|---|
| 1 | Go 1.27.1 装入 devbox | `go version` → `go1.27.1 linux/amd64`；模块下载实测通过 goproxy.cn |
| 2 | 仓库建立并推送 | 初始 `0a54067` → `8057ad5` → `2a0382a`；**2026-10-01 接管时补推**了未推的 6 个 commit（`cc86e81..dec28f0`），当前远端 `main = 85610dc`（`git log origin/main..HEAD` 为空） |
| 3 | 开发栈四件套运行中 | 全部 `healthy`；**从 devbox 侧**逐项验证：GreptimeDB HTTP(28400) + PG-wire(28403) + 我们的 PG(28543) + NATS(28224) + Redis(28637) |
| 4 | 代码质量门禁通过 | `go build ./...`、`go vet ./...`、`go test ./...` 全绿 |
| 5 | Odoo 现状审计勘误 | `07-odoo-integration.md` §2.5 / §2.6 已按**服务器实际配置**重写（原审计基于已废弃的 Windows 旧布局快照，8 项里 5 项误判） |
| 9 | **A1 · 设备认证（P1）** | **完成**。`internal/auth` 实现三档认证（A 档默认关闭）、物模型驱动 ACL、L1 缓存 + fail-closed 降级、失败计数与黑名单、过载与凭据失败分离；`internal/gateway/authhook.go` 接入 broker，**端到端 5 个用例**（凭据判定 / ACL 订阅 / 越权发布断连 / 网关子设备命名空间 / 断连回收）。容量实测：单次 Argon2 **116ms**、冷启动 **51 次/秒**；**文档参数被否决**，改为 `t=3 m=32MiB p=2` + 并发 8。见 `03` §2.1.1 |
| 8 | **C1 · 规则条件引擎（P0）** | **通过**。`internal/rules` 实现编译期校验（AST + 物模型类型表）、41 个白名单函数、编译缓存与 `Runner`。实测求值 **P50 0.60~0.81 µs / P99 0.98~1.74 µs**（验收线 2 µs，6 轮；余量 1.15~2.04×，**生产机型需复测**）；编译缓存命中 121 ns / 0 分配。核心发现：**`expr.Env` 的类型化环境覆盖不了小写物模型键**，校验必须自建（`04` §1.2.1） |
| 7 | **B1 · 时序表模型（P0）** | **通过**。`cmd/tsdb-bench` 在 GreptimeDB 1.2.1 上对比 A（JSON）/ B（宽表）：写入 19.1 万 / 16.1 万 points/s（验收线 5 万，内联字面量路径 64.0 万）；查询单设备全部达标，多设备明细两方案**都**超线 → 结论「维持 JSON 默认」。含**前置一致性校验**（两方案 3599 点逐点相等）。结论与边界 `02` §4.1.1；原始报告 `docs/reports/b1-tsdb-bench.md` |
| 6 | **A2 · QoS1 PUBACK 时机（P0）** | **通过**。`mochi-mqtt` v2.7.9 可在「等待 NATS `PublishAck` 后再回 PUBACK」下工作：`OnPublish` 返回 `packets.ErrRejectPacket` 阻止自动 PUBACK，业务侧 `cl.WritePacket(ack)` 显式确认。实测往返 **0.52 ~ 1.12 ms**（5 次采样，中位 ≈ 0.92 ms）。结论与边界见 `03-ingestion.md` §4.4.1；代码 `internal/gateway`；用例 `a2_test.go`（含**崩溃注入**与**对照实验**）、`nats_test.go`（真实 JetStream，PUBACK 后消息可读回） |
| 10 | **A3 · 集群路由（P1）** | **完成（核心实现 + 端到端实测）**。`internal/cluster` 实现设备位置 Locator + 定向路由 + 非设备广播兜底、离线分片队列与游标回放、接收端 `(Origin, Seq)` 去重；`internal/gateway` 的 `ClusterHook`/inline 注入有确定性单测；`cmd/iot-gateway` 已接入显式集群装配（`-cluster-node-id` + `-redis-url`）。文档 `03` §1.4 已从「filter 反向索引」勘误为「设备位置查询」。真实 NATS/Redis 端到端用例（`redis_test.go`：RedisLocator 跨节点定向路由 + RedisCursor 跨节点回放续传）已实测通过（`IOT_NATS_URL` + `IOT_REDIS_URL` 触发） |
| 11 | **统一信封（Phase 0 缺口修复）** | **完成**。修掉一个实现缺口：网关原先只把**裸 payload** 发到 NATS，`device_key` 随 subject 丢失，下游无从落库。现新增 `internal/envelope`（归属元数据 + 原始报文，payload 用 `json.RawMessage` 内联避免 base64 膨胀），网关 `buildEnvelope` 在发布前封装（非法 JSON → `gw_invalid_payload` 拒绝）；`03` §2.4/§4.3 的「统一信封」由注释变成实现 |
| 12 | **消息管道 `svc-pipeline`（P0 · 物模型解析 + GreptimeDB 写入 + 幂等）** | **完成（MVP）**。`internal/pipeline`：`Parser`（信封 → `Record`，ISO8601 校验、缺失指标记 Null、毒消息标 `ErrPermanent`）+ `Batcher`（**或**语义 200ms/1000 行、非阻塞入批 + `Ticket.Wait`）+ **两阶段幂等**（`processing` 60s 仅并发互斥、`done` 5min 才放行 ACK；Redis 实现 + 内存实现）+ `Handler`（解信封 → done 检查 → 抢占 processing → 入批 → 落库 → 写 done）；`cmd/svc-pipeline` 消费 `iot.telemetry.>` durable consumer，**落库后 ACK**、批次失败 NAK 重投、毒消息计数后 ACK 释放、按 `concurrency` 并发处理。**端到端实测**：230 条遥测全链路落库；**幂等探针**（两条完全相同信封）→ `duplicate=921 / rows_written=1 / idem_errors=0`，GreptimeDB 增量**恰为 1** |
| 13 | **A5 · 计量埋点（P0 · 计量埋点原型）** | **完成（原型）**。`internal/metering`：`Accumulator`（热路径**只做一次加锁自增、无 IO**）+ `Reporter`（窗口批量上报 `iot.quota.usage`，**取出即清零**：宁可少报也不重复计费）+ 退出前补发最后窗口；网关 `Hook` 在**已持久化**后累加 `msg_count`（`gateway.Meter` 窄接口）；`internal/quota` + `cmd/svc-quota` 消费上报 → Redis `INCRBY quota:{pid}:{metric}:{yyyymmdd}`（TTL 7d）。**端到端实测**：网关累加 → 上报 → 累加，Redis 计数器 `quota:1:msg_count:20261001 = 192` 与实发 **192 条完全一致**（误差 0%，验收线 <0.1%）。**遗留**：每分钟落 PG（按 `(metric, ts_minute)` 幂等）、每小时对账、配额限流与分级预警、设备数/存储量/API 调用数三类指标 |
| 14 | **D2/D3 · Odoo 集成骨架（P1）** | **完成（骨架）**。**D2**（Go，本仓库）：`internal/odoo` —— JSON-2 客户端（`POST /json/2/{model}/{method}`、Bearer 鉴权、**强制注入 `X-Odoo-Database`**、命名参数、`search_read` 分页），错误码 → 哨兵错误（401/403/409/422）+ `IsRetryable`（**仅 5xx/429/网络错误可重试，4xx 不重试**，07 §4.3）；httptest 5 组用例全过。**D3**（Odoo，⚠️ **在另一仓库 `SNCIC/odoo20tbb` 的 `addons/`**）：`sn_edge_integration` 骨架 —— `edge.idempotency`（ADR-015 五态 + `UNIQUE(company_id, integration_name, idempotency_key)`，**只有 SUCCEEDED 才回放**）、`edge.outbox`（事务内事件 + `event_id` 唯一）、`/api/iot/v1/health`。**已在 odoo20 库实测安装通过**（`Module loaded in 2.18s`、两个 `_uniq` 约束 `contype=u` 落地、`ir_access` 记录生成、health 返回 200），并已提交推送（`fdf6a52`）。**安装期查出两处 Odoo 20 不兼容**（按 19 及更早的写法会直接失败或静默失效）：① 访问权文件必须叫 `security/ir.access.csv`（`ir.model.access` 已由 `ir.access` 取代）、列改为 `operation` 字母组合，写旧名会在加载时 `KeyError` **中断安装**；② 唯一约束必须用 `models.Constraint`（`_sql_constraints` 仅打印告警、**约束不生效**）。完整清单见 07 §7.2 |
| 15 | **`odoo-connector` 编排层 + 事件入口 + 定时对账（P1 · 07 §4.3 / §4.4）** | **完成（骨架 + 真实 Odoo 实测）**。`internal/connector`：令牌桶限流（20 req/s）+ 有界排队准入（在途 8 / 队列 1000，超出拒绝并告警）+ 熔断（`gobreaker`；连续 10 次失败**或**失败率 > 50% 且样本 ≥ 20 → 打开 60s；**401/403 立即跳闸**；**业务 4xx 不计入**）+ 退避重试（1s/3s/9s，最多 3 次；**仅 5xx/429/网络错误**，且**超时仅在携带幂等键时重试**）+ 错误码映射（§4.3.2 八码）+ 幂等占位（§4.3.1 第 2 步，Redis Lua 原子「查—比—写」；**占位不可用则降级放行**，权威账本仍在 Odoo `edge.idempotency`）+ 超时（连接 3s / 读 15s）。**`cmd/odoo-connector`** 骨架：`/healthz` + `/readyz`（真打 Odoo）+ `/metrics`。**实测**：对真实 Odoo 用无效凭据 → `/readyz` 返回 `AUTH_REQUIRED`、随即触发 P1 熔断、再次返回 `CIRCUIT_OPEN` 且不再触达 Odoo。**顺带修一处信息泄露**：`odoo.APIError` 现在剥离 Odoo 返回的 Python traceback（技术方案 p.7 明令禁止外泄）。**事件入口已完成（§4.4）**。**C-1**：消费 Odoo Outbox 投递到 Redis Stream `odoo:outbox` 的事件（消费组 `odoo-connector`），翻译为 `iot.odoo.{model}` 发布到 NATS `IOT_ODOO`。**「至少一次」由两件事共同成立**：XACK 只在发布成功之后 **+** 未确认消息由 `XAUTOCLAIM` 接管重投 —— 只做前者不做后者，失败消息会永远沉在 PEL 里，语义是「零次」而非「至少一次」。**C-2**：`POST /webhook/odoo`（Bearer 鉴权，**无令牌即不注册该路由**；请求体上限 1 MiB），按 `(model, id, write_date)` 去重，去重器故障时**放行**（重复比丢失轻，下游按 `event_id` 还能去）。**实测端到端**：XADD 一条 Outbox 事件 + POST 一次 webhook → NATS 收到 `iot.odoo.stock_move` / `iot.odoo.maintenance_equipment`，消费组待确认为 0；同一 webhook 重投返回 `duplicate:true` 且不重复发布；未授权返回 401。**查出并修掉两个真实缺陷**：① 漏了 PEL 重投（见上）；② `XReadGroup` 传 `Block: 0` 被 go-redis 下发为 **`BLOCK 0`＝永久阻塞**，会把消费循环挂死（已译为负值＝非阻塞，并有回归用例）。**Odoo 侧 cron 投递已接通**：`sn_edge_integration` 新增 `data/edge_outbox_cron.xml`（每分钟）与 `edge.outbox` 的投递逻辑 —— **先 XADD 成功才置 delivered**；失败按 5/15/60/300/900/1800/3600 秒退避，连续 10 次进**死信**；**Redis 整体不可达则整轮失败、不消耗 attempts**（逐条失败会让一次宕机把整批事件推成死信，反而丢数据）。依赖 python 包 `redis`（已装入 `odoo20` venv 8.1.0，并写入 manifest 的 `external_dependencies`）；地址与流名走系统参数 `edge.outbox.redis_url` / `edge.outbox.stream`。**实测**（odoo shell，含真实 cron 路径 `method_direct_trigger()`）：正常投递 → `delivered`；走真实 cron → `delivered` 且事件入 Redis；Redis 不可达 → 抛错且 `attempts` 保持 0；单条失败（WRONGTYPE）→ `attempts=1` + 退避 + `last_error` 记真实原因；连续 10 次 → `state=dead`。**全链路已打通**：Odoo 业务 → Outbox → cron → Redis Streams → connector → NATS `IOT_ODOO`（`messages:1`、`lag:0`、PEL 0）。**又查出两处 Odoo 20 API 变更**：① `ir.config_parameter` 的 `get_param`/`set_param` 已被类型化访问器 `get_str`/`set_str` 等取代（沿用旧名会在 **cron 运行时**才 `AttributeError`，属性面板上看不出来）；② `ir.cron` 不再有独立 `code` 字段，需经 `ir_actions_server_id` 委托 `ir.actions.server`。**定时对账已完成（§4.4）**：`Reconciler` 每 15 min 跑两路扫描 —— ① 补投 Odoo `edge.outbox` 的非终态行（domain 带宽限窗口，**退避中的行不算遗漏**；死信也补投并标记需人工排查）；② 比对 C-2 水位差并补投（水位是 `(write_date, id)` **二元组**，只用时间戳会漏掉同秒写入的多条记录；**首次对账只建基线**，否则会把全表历史当遗漏一次打爆下游）；**连续两轮仍有遗漏才告警**（单轮很可能是抖动）。补投**沿用原 `event_id`**，原事件若其实已到达，由下游去重 —— 换个新 id 就等于承认必然重复。水位由 webhook 推进、对账比对，两侧共用 Redis，并在 Lua 里保证**只进不退**（水位被拉回去会重复处理一整段）。**实测**（真实 Odoo/Redis/NATS，3s 间隔加速）：首轮建基线；造一条卡住的 Outbox 行 → `对账①：补投遗漏的 Outbox 事件`；改一条 `res.partner` 但**不发 webhook** → `对账②：补投水位差`（这正是 C-2 会静默丢事件的场景）；连续多轮未收敛 → ERROR 告警并提示检查 `max_cron_threads`。**顺带修一个我自己的调用 bug**：`-log-json false` 因 Go flag 的布尔语义导致**其后所有参数被静默丢弃**（不报错的配置错误），已改为 `-log-format text|json`。**遗留**：主数据增量拉取与游标（Redis+PG）、死信 `t_dlq` + 按 `entity_type` 聚合告警、对账第 ③ 项（未绑定待办，依赖尚未建立的 `t_external_ref`） |
| 16 | **L2 · 自研 DAG 编排执行器（P1 · 04 §1.3）** | **完成（Phase 2 第一批）**。`internal/dag`：六种节点（condition / action / delay / parallel / branch / end）全实现；**编译期强制校验**按 04 §1.3 的约束表逐条落地 —— 节点数 ≤32、深度 ≤8、环检测（Kahn 拓扑序，同时给出稳定遍历序）、单节点超时 ≤5s、delay ≤5min、并行 ≤5、**动作白名单**、**变量引用仅 `$path.to.value` 且禁止拼接**；并沿用 `rules.CompileError` 的取向**一次报全所有问题**（04 §1.6「保存即阻断」）。**condition 复用 L1 的 expr**（`rules.Compiler` 编译 + `rules.Runner` 求值），不另建表达式引擎 —— 这是 04 §1.3「禁止第二套编排」的落点。执行侧：整体 30s / 单节点 5s 两级超时**分开上报**（单节点慢不该被误报成整个 DAG 被中断）、三种 `error_policy`（drop / retry / dlq）、指数与固定退避、不幂等动作重试告警、变量解析失败**不消耗重试**（配置错误重试无意义）。动作注册表要求执行体是编译进二进制的 Go 函数（「新增动作 = 写 Go 代码 + 注册 + 上线」）。**38 个用例全过**。**如实留白三处文档缺口**（已在代码注释与本表标注）：① `action` 的 `next` 在 04 §1.5 示例中未给，本实现取「缺省即链到此为止」；② `branch` 的 `cases[]` 未定义字段名，取 `{expr, next}`；③ **不支持 join（多分支汇聚）** —— 04 §1.3 只定义了并发度、没定义汇聚语义（全等？任一？），凭空定一个会与用户预期不一致，故编译期对「分支可达同一节点」给出**警告**（重复执行对 `notify.send` 这类不幂等动作就是重复通知）。**遗留**：`window.*` 窗口算子（滑动/滚动窗口由 svc-rule 维护）、规则索引与拓扑匹配过滤、`error_policy=dlq` 的 DLQ 落库、样例试跑（3 组输入校验输出与耗时 <5ms）、灰度与原子生效（NATS `iot.ctrl.rule_reload.{project}` + 版本防御） |
| 17 | **L3 · 告警五态 FSM + 去重/聚合/抑制（P1 · 04 §2.1-2.2）** | **完成（Phase 2 第二批）**。`internal/alarm` 是**纯状态机**（不做 IO：落库走 `Store` 抽象、通知由调用方按 `Decision` 执行），故状态迁移可确定性穷举。**五态 FSM**：`idle→detected→confirmed→active→resolved→idle`，时序参数随规则的 `timing`（`detect_window_s`/`suppress_s`/`auto_close_s`，缺省 60s/10min/5min）；**推进双通道**（`Observe` 事件驱动 + `Tick` 定时扫描）共享同一套 advance 逻辑 —— 两条路径各写一套是最难查的分裂。**去重**：`sha1` 键保证「同一设备同一规则只允许一个活跃告警」；**抑制**：`active` 期间重复触发只更新 `last_ts`、静默窗口只记录不通知、风暴熔断（`>500/1min`，**区分「刚跨阈值」与「熔断中」**，否则第 501 条之后每条都会再发一次风暴通知，在风暴里再制造风暴）、根因抑制（父告警活跃则子告警自动抑制）；**聚合**：按「同 `device_type` 下**不同设备**数」判定（同一台刷 10 次不算 10 台，否则一台设备抖动会被误判成批量故障）；**乐观锁**按 04 §2.5 用 CAS（`Update(expected)` + 重读重试 3 次），冲突耗尽则上抛而不是静默吞掉。**告警质量指标**（§2.2.1 要求业务口径而非只有技术指标）：有效告警占比 / 工单转化率 / 合并率 / 风暴次数 / 确认时长。**21 个用例全过（含 `-race`）**。**如实留白四处**：① 批量告警**实体**与通知合并路径（需 `t_alarm` 批量字段，本批只做判定与计数）；② §2.2.1 要的是确认时长**中位数**，本包只维护和与计数故给均值，中位数需从 PG 算；③ 键为 `sha1(p+d+r)` 的文档字面写法本实现加了 `0x00` 分隔（裸拼接会让 `("a","bc","d")` 与 `("ab","c","d")` 撞键 → 不同设备的告警互相压制，见代码注释）；④ 04 §2.1 的 ASCII 图与同节表对 `active` 恢复的去向**互相矛盾**，本实现取表（`active+恢复→resolved`，否则 `resolved` 与 `auto_close_s` 都成死代码）。**遗留**：PG `Store` 实现与启动重建、多副本 `dedup_key` 分片、`svc-alarm` 服务入口与 5s 扫描循环、三条通知通道（svc-notify）与未确认升级（30min/2h） |
| 18 | **PG 地基（P1 · 02 §3 / 04 §2.5）** | **完成**。新增 `internal/pg`：连接池（走 Unix socket 的 peer 认证，**开发环境不引入新凭据**；用 `Ping` 真做连通性检查 —— 真 PG 上它可用，与 §6 坑 19 那条 GreptimeDB 限制不是一回事）+ **迁移执行器**（`schema_migrations` 版本表 + **校验和** + 每个迁移单独事务 + `pg_advisory_lock` 串行化）。新增 `cmd/iot-migrate`（迁移是**部署动作**，不藏进某个服务的启动流程；`-check` 有待应用迁移则非零退出，可做 CI 门禁）+ `internal/pg/pgtest`（跨包共用的「建临时库 → 跑迁移 → 删库」基建，避免各写一遍各错一遍）。**首个迁移 `0001_alarm_active`**：`t_alarm_active` 是**非分区表**，`PRIMARY KEY (dedup_key)` 提供**跨时间的全局唯一** —— 文档里的 `t_alarm` 按 `first_ts` RANGE 分区，而 02 §3.4 自己写明「分区表上的唯一索引只保证**分区内**唯一」，落它上面跨月重叠就会放过第二条，而 04 §2.2 要求「同一设备同一规则只允许一个活跃告警」。表上还有 `ck_alarm_state`（五态白名单，**刻意不含 idle**：idle 由「行不存在」表达）与 `ck_alarm_ts_order`（时间倒挂会让报表出现「确认时长为负」，倒查不到写入点）。`internal/alarm.PGStore` 实现 `Store`：CAS 落在 `WHERE dedup_key=$1 AND state=$2` + `RowsAffected`（即 04 §2.5 的乐观锁）；`Active()` **显式 `ORDER BY state_ts`** 让扫描顺序可复现（无序 Store 会让同一轮里父/子告警的求值顺序不确定）；`casUpdate` **不碰身份字段** —— 写脏了会让告警在无人察觉时改挂到另一台设备。**真库实测**：`internal/pg` 6 例（含并发迁移被 advisory lock 串行化、校验和漂移被检出、两条 CHECK 真拦得住）、`alarm` 的 7 例 PGStore（含 8 并发 `Observe` 收敛为一行、CAS 拒绝陈旧写、**换个引擎实例仍能从 PG 接着推进**＝04 §2.5 的「启动时重建」、timing 的 JSONB 往返、`GetByID` 支撑跨重启的根因抑制）。**⚠️ 过程中查出一次「假通过」**：`pgxpool.Config.ConnString()` 会**原样返回最初传入的字符串**，改完 `ConnConfig.Database` 再取它拿不到新库名 —— 我的「临时库」DSN 一直等于基准 DSN，6 个用例全打在 `iot` 上、**第一次还全绿**，第二次才因残留数据暴露。已加「连上后核对 `current_database()`，不符即当场失败」的自检（见 §6 坑 37/38）。**如实留白**：`t_alarm`（分区记录表）与主数据表（`t_device`/`t_alarm_rule`）未建 —— 它的 `device_id`/`rule_id` 是 BIGINT 代理键，而那些表还不存在，现在建一张谁也合法写不进去的表没有意义；`svc-alarm` 入口与 5s 扫描循环、多副本 `dedup_key` 分片未实现 |
| 19 | **`svc-alarm` 服务（P1 · 04 §2）** | **完成（骨架 + 真实栈端到端）**。`cmd/svc-alarm` 把引擎接成可运行服务：**双通道推进**（`Observe` 消费 `iot.rule.alarm` 保时效 + 每 5s 定时扫描兜底，04 §2.1）、**扫描互斥**用 PG 咨询锁（`pg_try_advisory_lock`，**会话级**故持有专用连接 —— 用 `pool.Exec` 会让取锁与解锁落到不同连接上，表现为「锁取到了永远放不掉」且**没有任何报错**）、`/healthz` + `/readyz`（真查 PG **并检查迁移是否已应用** —— schema 落后时服务「能连上库」但每轮扫描都失败，而 readyz 报健康是最误导人的状态）+ `/metrics`（**业务口径与服务健康并排**，直接落 §2.2.1 的要求）。**发布做成至少一次**：`Tick` 把状态推到 `active` 后若 NATS 发布失败，这条告警**永远不会再发**（状态已是 active，之后的重复触发都被抑制），工单会静默丢失；故新增迁移 `0003` 的 `published_at`，**发布成功才写标记**，每轮扫描按 `published_at IS NULL` 补发，重复由下游 S3 的幂等键与 10min 合并窗口兜住。**端到端实测**（真 PG + 真 NATS，1s 扫描加速）：发一条 `iot.rule.alarm` → 2s 观察期 → 扫描推进到 `active` → 发布 `iot.alarm.p_e2e` → JetStream `IOT_ALARM` `messages=1`、`/metrics` 的 `alarm_events_published_total=1`、`alarm_avg_confirm_delay_seconds=2`（**观察期确实生效**，不是落默认 60s）、`published_at` 已落库；测试数据与流已清理。**查出并修一处静默失效**：`wildcardOf("iot.rule")` 原会生成 `iot.rule.>`，而**它匹配不到 `iot.rule` 本身** —— 流的 subjects 不含实际要发布的 subject 时，发布静默失败（或报一个看不出原因的 `no response from stream`）。**⚠️ 过程中发现我用错了数据库**：业务库一直建在 devbox 内那个**与 Odoo 共用**的 `postgresql@18`（`odoo20`/`erp_dev` 在里面），而它本该是本项目栈独立的 `iot-postgres`（`100.64.0.3:28543` / 库 `odoo20iot`）；已改默认 DSN、迁移打到正确的库、删除误建的库（见 §6 坑 39）。**如实留白**：通知策略解析与模板渲染（§2.3 第 1、2 步，需 `t_alarm_rule.notify`）、未确认升级（30min → 上级 / 2h → P1）、`dedup_key` 哈希分片（§2.5）、静默窗口的配置源（`Silences` 恒为空集，语义是「没有窗口」而非「静默全部」） |
| 20 | **`svc-notify` 三通道通知（P1 · 04 §2.3）** | **完成**。`internal/notify`：**三条通道**（Webhook / 邮件 / 短信 —— 04 §2.3 列了四档含语音，而 06 的 Phase 2 验收项写的是「Webhook / 邮件 / 短信，3 通道」，语音如实留白）+ **SSRF 出站防护**（白名单 + 禁内网段 + **DNS 固定解析**防 rebinding；统一出口代理未部署，故四项做了三项，差异写明在包注释里）+ **分发器**（按优先级 + 降级、**重试阶梯 500ms/2s/8s**、永久失败不重试、**部分投递**作为第三种结果）+ **模板渲染**（`text/template` + `levelZh`/`timeFmt` 过滤器）+ **通道健康降级**（失败率 > 30% 直接跳过并切下一通道；文档写的是「只对短信生效」，这里做成通用 —— Webhook 网关整体挂掉比短信拥塞更常见）+ **死信**（迁移 0004 的 `t_dlq`：按月分区 + 幂等建分区函数）。`cmd/svc-notify` 消费 `iot.alarm.>`，暴露 `/healthz` + `/readyz`（真查 PG 与迁移）+ `/metrics`。**查出并修四处真实缺陷**：① **`sub.Unsubscribe()` 会删掉 JetStream 消费者** —— 服务每次退出都删、每次启动都新建，于是重启即**重放整个保留窗口**（流保留 24h，等于每次重启把全天告警重发一遍）。实测：不发任何新事件、仅优雅重启，服务重放了已确认的事件（`received` 从 0 变 1）；修法是退出只关连接 + 显式 `InactiveThreshold`（NATS 2.10+ 对 durable 消费者有**默认 5 分钟空闲回收**，一次周末停机同样触发重放）。已抽成 `internal/natsjs` 并加**含反证的回归用例**。② SSRF 网段表里的 `::ffff:0:0/96` 被 Go 规范化成 **`0.0.0.0/0`＝匹配全部 IPv4**，所有 webhook 都会被拦掉（正是用例抓住的）。③ **2xx 里藏业务错误码**：钉钉/企微/飞书与短信厂商在 token 失效、机器人被移出群、模板未报备时**仍返回 HTTP 200**，只判状态码会把它们记成投递成功（通知静默丢失，而指标上一切正常）。**端到端实测**（真 NATS + 本地接收端）：正常投递 → 接收端收到带 `Host` 头与结构化字段的 POST、`levelZh` 把 `critical` 渲染成「严重」、`payload` 带上取值快照；把接收端改成 `{"errcode":310000}` → **重试 4 次**（1 + 3 阶梯）→ 落 `t_dlq`（`attempts=4`、带 trace_id）→ 记 ERROR。**如实留白**：通知策略来自配置文件（`t_alarm_rule.notify` 未建）、通知组未展开成收件人（依赖 `t_user`/`t_role`）、未确认升级（30min/2h，依赖 svc-alarm 的一手数据）、统一出口代理、策略热加载、DLQ 重放工具与 7 天保留。**⚠️ 同款缺陷当时仍在另外 3 处**（`svc-pipeline` / `svc-quota` / `internal/cluster`），其中 `svc-quota` 会重复计数 —— **均已在本项目第 21 项修净** |
| 22 | **Stream 保留口径丢失（`MaxAge` 侧）· 全局统一** | **完成**。起因：各服务的 `EnsureStream` 都写「**不存在才创建**」，于是**先于口径代码建出来的流**永远停在旧配置。`/jsz` 实测铁证：`IOT_TELEMETRY max_age=0 msgs=57400`、`IOT_ODOO max_age=0`、`IOT_ALARM max_age=0`（代码里明明设了 7d / 24h）—— 这些流**建于该行代码存在之前**，"只建不校"让它们再也修不回来。后果：磁盘随写入无上限增长，且是「重启即重放」的放大器（保留窗口越长，一次误删消费者的代价越大）。**修法**：抽成 **`natsjs.EnsureStream` + `natsjs.StreamSpec`**（建或校），`internal/gateway` / `internal/connector` / `cmd/svc-alarm` 三处统一调用，不再各写一遍。要点： ① 漂移则 `UpdateStream` 并打 **WARN**（缩短 `MaxAge` 会立即删超期消息，破坏性动作必须留痕）—— 策略为**自动校正 + 告警**；② **零值 = 不约束**：`MaxAge<=0`/`MaxMsgsPerSubject<=0`/`Discard==0` 既不检查也不改，服务不拿默认值覆盖部署侧的显式配置；③ `StrictSubjects` 保留 `svc-alarm` 的既有主张（subjects 不覆盖就**报错**、不自动改订阅范围），而网关/连接器按需校正；④ subjects 用**集合**比较（NATS 返回顺序不保证一致）；⑤ 刻意不校副本数（属部署期按 RPO 决定）。用例（真 NATS）：`TestEnsureStreamReconcilesRetention`（`max_age=0` 旧流 → 24h/1000/DiscardNew，日志见 `max_age=0s →=24h0m0s`）、`TestEnsureStreamLeavesZeroFieldsAlone`（零值不动既存的 7d/DiscardOld）、`TestEnsureStreamStrictSubjects`、`TestSameStringSet`、`TestSubjectCovers`（钉住坑 40：`foo.>` 不覆盖 `foo` 自身）。**⚠️ 生效时机**：既存流的校正发生在**对应服务下次启动**时；当前开发栈的 `IOT_TELEMETRY` 等仍是 `max_age=0`，需重启网关/连接器/告警服务（或手工校正）才会落到 24h —— 且**缩短保留会删掉超期消息**，属破坏性，故未在本次代跑 |
| 21 | **「重启即重放」剩余 3 处（P1 · §5.1 原第 27 行）** | **完成**。`cmd/svc-pipeline` / `cmd/svc-quota` / `internal/cluster` 的路由消费者原先各自 `js.PullSubscribe(...)` 且 `defer sub.Unsubscribe()` —— 退出即**删掉消费者**，重启按 `DeliverAll` 重放整个保留窗口。三处统一改走 `internal/natsjs.Subscribe`：`svc-quota` 新增 `-consumer-inactive`（默认 24h）、`svc-pipeline` 新增同名列并透传 `MaxAckPending`（`natsjs.Options` 新增该字段）、`cluster.Options` 新增 `RouteRetention`（默认 1h，与路由流 `MaxAge` 用**同一个常量** `DefaultRouteRetention`，避免两处各自漂移）。⚠️ `internal/cluster.ReplayOffline` 的临时消费者（durable 为空 + `StartSequence` + `AckNone`）**刻意保留 `Unsubscribe`** —— 它现建现删、起点由 Redis 游标给出而非 `DeliverAll`，删掉才是正确清理；已就地加注释，防止后续被"统一"掉。**结构性防复发**：`natsjs.Subscribe` 的返回值由 `*nats.Subscription` 收窄为 `*natsjs.Subscription`（**只暴露 `Fetch`**）—— 调用方拿不到 `Unsubscribe`，这是坑 47「把规避方式做成可复用的东西」的落点；5 个调用点同步调整，`TestUnsubscribeDeletesDurableAndReplays` 改为**直接用原生 `js.PullSubscribe`** 演示坑本身（正因为收窄后就踩不到了）。**真 NATS 演练**（独立 stream/subject/durable/redis-key，不碰任何真实状态）：发布 5 条计量上报 → 计数器 `=5`；SIGTERM 停止后消费者**仍在**（`delivered=5 pending=0 ack_floor=5`，进度未丢）；重启后计数器**仍为 5**（未重放）。**对照**（复现缺陷）：停止后删掉消费者（等价旧的 `Unsubscribe`）→ 重启后计数器变成 **10**，重复计数被复现并由本修复消除。门禁 `gofmt` / `go build` / `go vet` / `go test` 全绿 |
| 23 | **B1 补充项（1）· 明细查询限行 + 自适应降采样（P0 · 02 §4.3.1）** | **完成**。`internal/tsdb` 新增契约：`MaxDetailRows=5000` 等保护常量、`SeriesQuery.Normalize`（project_id>0 / 设备 ≤50 / 指标白名单 / 回溯 ≤90d / Limit ∈[1,5000]）、纯函数 `AdaptiveBucket(span, devices, cap)`（在「设备数 × 桶数 ≤ cap」下取最小可读桶宽，阶梯 1s…24h）。适配层 `QuerySeries`：Normalize → **廉价探测** `SELECT 1 ... LIMIT cap+1`（只取常量、不解析指标、不排序，超限时尽早终止）→ 未超限返回原始明细；超限按桶宽降采样返回并标注 `Granularity`/`Bucket`/`CapHit`，**绝不静默截断**（聚合若仍超限则直接报错）。裸查询改名为 `SelectRangeUnprotected`/`SelectBucketsUnprotected`（仅压测/诊断），业务侧唯一读入口是 `QuerySeries`。**实测**（`make b1-bench` §5.2，2026-10-01）：Q3 形态（10 设备 × 1h ≈ 3.6 万行）返回 **35990 → 3610** 行，P95 **257.4 → 152.4ms（JSON）/ 224.3 → 106.7ms（宽表）**，双双落回 P95<200ms 预算内；探测成本从「取指标值 + ORDER BY」的 **95.7ms** 降到 **20.4ms**（1.12M 行表实测）。集成测试 `internal/tsdb/greptimedb/query_test.go`（`IOT_GREPTIMEDB_DSN` + `IOT_PERF_ASSERT` 门控，`make test-tsdb`）。结论与边界 `02` §4.3.1.1；原始报告 `docs/reports/b1-detail-limit.md`。**遗留**：并发上限（20）、慢查询降级（>3s）、多指标端点（≤4）、大窗口（如 50 设备 × 90d）延迟实测 |
| 24 | **B1 补充项（2）· 预聚合表 + 跨度路由（P0 · 02 §7）** | **完成**。**开工探针**（`internal/tsdb/greptimedb/capability_test.go`，临时表，6 项全过·无回退）：`INSERT…SELECT`+绑定参数可用、**非 append 表同键重写=覆盖**（幂等由表结构保证）、`json_get` 对 JSON 布尔返回 1（布尔可聚合）、rollup 表接受 TTL、`metric` 是保留字（须加引号）、全 NULL 分组写出 `sum=NULL,count=0` 行且 `NULLIF` 可用。**契约**：`Rollup`（1m/1h）、`Source`、纯函数 `RouteSource`（≤6h 原始 / 6h–30d 1m / >30d 1h，**只对 JSON 方案路由**），`SeriesResult.Source`。**表结构**：`telemetry_1m/1h` 长表 `(ts, project_id, device_id, device_type_id, "metric", "sum", "max", "count")`，**不设 append_mode**，物模型加指标零 DDL。**物化**：`Store.RollupWindow` 每指标一条 `INSERT…SELECT`；`cmd/svc-rollup` 增量调度（单飞 PG 咨询锁，失败不致命），水位落 PG `t_rollup_watermark`（迁移 `0005`，`GREATEST` 只进不退，**只在写入成功后推进**）。**路由**：`QuerySeries` 按源选表，agg 源再聚合 `SUM("sum")/NULLIF(SUM("count"),0)`（跨桶无损），输出桶宽不低于源粒度。**实测**（`make test-rollup`，2026-10-01）：10 设备 × 6h × 1Hz（21.6 万行）上 **守恒**（数值+布尔指标的 sum/max/count 与原始聚合逐项相等，1e-9）、**幂等**（重跑行数/取值不变）、**Q4 形态 P95 432.7 → 67.2ms**（同返回 730 行）；`make test-rollup-pg` 覆盖水位账本。结论与边界 `02` §4.3.2 / §7；原始报告 `docs/reports/b1-rollup.md`。**遗留**：1h（>30d）路径未单独压测、迟到数据超 `lag` 不回修、首次水位只回看 24h（更早需 `-backfill-since`）、多副本调度未压测、宽表方案未物化 |
| 25 | **读侧查询 API `svc-query` + 控制面主数据最小集（P1 · 02 §3 / §4.3）** | **完成**。**迁移 `0006`**：`t_project` / `t_device_type` / `t_device`（PK 修正为 **`(id, project_id)`** —— 原文 `id PRIMARY KEY` + `PARTITION BY HASH(project_id)` 在 PG 里是非法 DDL；16 个显式分区；复合外键 `(project_id, device_type_id)` 在 DB 层挡跨租户挂类型；**刻意不启用 RLS**，理由与替代方案见 `02 §3.5`）。**主数据**：`internal/catalog`（契约 + `PGStore` + `MemStore` + Argon2id 摘要编解码，复用 `internal/auth`）。**种子**：`cmd/iot-seed`（幂等；仅新建或 `-rotate` 时生成凭据；凭据落 `tmp/` 0600）。**认证**：`internal/apiauth` —— JWT 自写验签（ES256/RS256、`iss` 允许列表、`aud`、**强制 `tenant`**、`jti` 查 Redis 吊销且依赖故障 **fail-closed 503**）、JWKS（HTTP/本地文件）、开发静态令牌（常量时间比较 + **只允许绑回环**，启动与每次认证打 WARN）。**查询服务**：`internal/querysvc` + `cmd/svc-query`（默认 `127.0.0.1:18094`），`GET /api/v1/devices`、`GET /api/v1/series`、`GET /api/v1/latest`（每租户并发上限 20 + 有界排队、慢查询 >3s 记指标+WARN、稳定错误码）。**时序读路径增量**：`SeriesQuery.Bucket` 显式分桶 → `Granularity=aggregated`（零值=原行为，既有 tsdb 用例原样通过）。**实测**：`make test-catalog` 7/7（含外键挡跨租户、RLS 未启用断言）、`make test-query` 全绿（含 `-race`；JWT 各失败形态、限流器队列、handler 全状态码）、`make test-query-e2e` 真 PG + 真 GreptimeDB 端到端通过；真实二进制 curl 走查见报告。**遗留**：RLS、历史导出、多指标投影、Odoo 设备同步、按类型物模型校验、慢查询自动降级、每租户速率限制/读写池分离、**前端**（当前唯一"界面"是 GreptimeDB dashboard）。结论与契约 `02 §3.5` / §4.3.2、`05 §3.3`；报告 `docs/reports/query-svc.md` |

---

## 5. 未完成 / 下一步

### 5.1 待办清单（按优先级）

| 优先级 | 项 | 说明 |
|---|---|---|
| ~~P0~~ | ~~A2 · QoS1 PUBACK 时机~~ | ✅ **已完成**（见 §4 第 6 项） |
| ~~P0~~ | ~~B1 · GreptimeDB JSON vs 宽表~~ | ✅ **已完成**（见 §4 第 7 项） |
| ~~P0~~ | ~~C1 · `expr` 条件引擎~~ | ✅ **已完成**（见 §4 第 8 项） |
| ~~P1~~ | ~~A1 · 设备认证 Hook~~ | ✅ **已完成**（见 §4 第 9 项）。⚠️ 遗留：L2/L3 凭据来源（Redis/svc-auth）未接入，当前用本地凭据文件（仅开发/PoC） |
| P1 | A1 补充项 | A 档六项强制措施（IP 白名单/异常检测/轮换提醒/按键审计）未实现；mTLS 端到端未实测 |
| P1 | 规则索引与窗口算子 | C1 只覆盖了「表达式」；拓扑匹配过滤、`window.*` 的窗口算子、动作分发均未实现（`04` §1.2.1 ⑧）。**DAG 编排已于 §4 第 16 项落地**，与本项共同构成 `svc-rule` 的前置 |
| P2 | Phase 2/3 其余批次 | **无法一次做完**（`06 §7` Phase 2 含 10 条产品轨 + 11 条工程轨 DoD，Phase 3 更在其上）。依赖当前环境**不存在**的基础设施者：K8s 多副本与 PDB、Vault、可视化前端、OTA 固件与目标硬件、C/Python 端侧 SDK。按「可独立完成且可验证」切批推进，第一批（L2 DAG）已完成 ✅ |
| ~~P0~~ | ~~B1 补充项（1）~~ | ✅ **已完成**：明细查询限行 + 自适应降采样已落地（见 §4 第 23 项）。Q3 形态经 `QuerySeries` 后 P95 257→152ms（JSON）/ 224→107ms（宽表），双双达标 |
| ~~P0~~ | ~~B1 补充项（2）~~ | ✅ **已完成**：预聚合表 + 跨度路由已落地（见 §4 第 24 项）。Q4 形态 P95 432.7ms → 67.2ms，落回预算内 |
| P1 | B1 补充项（3） | 上生产前按 **3 副本集群 + NVMe** 重测写入吞吐；宽表开启前用**租户真实样本**重测存储占用 |
| ~~P1~~ | ~~读侧查询 API `svc-query` + 控制面主数据最小集~~ | ✅ **已完成**（见 §4 第 25 项）。**仍缺**：RLS、历史导出、多指标投影、Odoo 设备同步、按类型物模型校验、慢查询自动降级、每租户速率限制、**前端** |
| P1 | 控制面其余主数据表 | 迁移 `0006` 只建了 `t_project`/`t_device_type`/`t_device`。`t_user`/`t_role`/`t_api_token`/`t_alarm_rule`/`t_external_ref`/`t_idem_registry` 等仍缺，卡住「通知策略」「批量告警」「connector 对账第③项」 |
| P1 | **RLS（租户行级隔离）** | `02 §3.1` 要求所有租户表 `ENABLE ROW LEVEL SECURITY` + `SET LOCAL app.project_id`；当前**未启用**，隔离只在应用层。要落地需先建连接池侧的 `SET LOCAL` 管线 |
| ~~P1~~ | ~~A3 补充项~~ | ✅ **已完成**：Redis Locator/Cursor 跨节点端到端已实测（`redis_test.go`，`IOT_NATS_URL`+`IOT_REDIS_URL` 触发） |
| ~~P0~~ | ~~svc-pipeline（物模型解析 + GreptimeDB 写入 + 幂等）~~ | ✅ **已完成（MVP）**（见 §4 第 12 项）。**遗留**：`raw_parsers` 二进制解析沙箱、32 分片静态绑定消费、`normalized` 转发、DLQ 与毒消息落 `event(parse_error)` |
| P1 | A4 | **压测工具已完成三个阶段**（`cmd/mqtt-bench`：阶段 1 建连/保持/资源采样/泄漏趋势判定，阶段 2 QoS1 发布路径，阶段 3 背靠背吞吐 + 接入确认延迟 P50/P95/P99 + SLO 判定；引入 `eclipse/paho.mqtt.golang` v1.5.1）。**5 万连接 24h 正式实测待跑**：需先起网关，且压测客户端**须分机部署**（同机跑会把工具开销算进网关）。冒烟：10 连接背靠背 → 13585 msg/s、P99 = 2ms、SLO ✅；200 连接 + 1s 周期发布 → 2098 条全成功 |
| P1 | A4 补充项 | **机制性验证已完成**（`internal/gateway/a4_test.go`，不依赖真实 NATS）：PUBLISH 同步阻塞同连接的 PINGREQ，PINGRESP 推迟 ≈ `PubackTimeout`（400ms 用例实测 400ms）；阻塞**仅限该连接**。**5 万连接 24h 下的规模化影响**（连续上报 × 总线超时 → 设备侧误判重连）仍待压测观测（`03` §4.4.1 边界 1） |
| ~~P0~~ | ~~A5 · 计量埋点原型~~ | ✅ **已完成（原型）**（见 §4 第 13 项）。遗留：每分钟落 PG（按 `(metric, ts_minute)` 幂等）+ 每小时对账、配额限流与分级预警、其余三类指标（设备数/存储量/API 调用数） |
| ~~P1~~ | ~~D2 / D3~~ | ✅ **已完成（骨架 + 安装验证 + 已推送）**（见 §4 第 14 项）。**遗留**：D3 的完整实现（facade / 其余 IoT 路由 / cron 投递 / 视图 / `tests/` / `EXTENSIONS.md`）、S1/S3 集成场景端到端 |
| ~~P1~~ | ~~`odoo-connector` 补充项~~ | **已完成**：编排层、两条事件入口、Odoo cron、定时对账、主数据增量拉取、`t_external_ref` 外部引用、`t_integration_issue` 未绑定待办、结构化 DLQ、聚合告警与可选重放均已完成。DLQ 默认开启；重放默认关闭，需显式设置 `-dlq-replay-interval`；聚合告警默认每 5 分钟扫描最近 15 分钟，阈值为 1 条，可用 `-dlq-alert-*` 调整；配置 `IOT_DLQ_ALERT_WEBHOOK` + `IOT_DLQ_ALERT_EGRESS_ALLOW` 后可投递飞书/钉钉机器人。**遗留**：对象存储原文、告警去重/升级策略和生产轮换自动化。|
| P1 | 告警引擎补充项 | **五态 FSM + 去重/聚合/抑制 + PG `Store` + `svc-alarm` 服务（双通道 + 5s 扫描 + 至少一次发布）+ `svc-notify` 三通道通知均已完成**（见 §4 第 17、18、19、20 项）。**遗留**：未确认升级（30min 通知上级 / 2h P1 升级）、通知策略解析与模板渲染（需 `t_alarm_rule.notify`）、`dedup_key` 哈希分片、静默窗口的配置源、批量告警实体与通知合并路径 |
| ~~P1~~ | ~~**「重启即重放」缺陷的另外 3 处**~~ | ✅ **已完成**（见 §4 第 21 项）：`svc-pipeline` / `svc-quota` / `internal/cluster` 统一走 `internal/natsjs`，且 `natsjs.Subscribe` 的返回值已收窄为不暴露 `Unsubscribe`（结构性防复发）。真 NATS 演练：`svc-quota` 的重启不再重复计数（对照实验复现了旧的 5→10） |
| ~~P1~~ | ~~**流保留未设 `MaxAge`（`EnsureStream` 只建不校）**~~ | ✅ **已完成**（见 §4 第 22 项）：抽出 `natsjs.EnsureStream`（建或校 + WARN），`gateway` / `connector` / `svc-alarm` 三处统一；策略定为**自动校正 + 告警**，零值表示不约束，`svc-alarm` 的 subjects 仍保持严格。**遗留动作**：既存流要等**对应服务下次启动**才被校正，届时会删掉超期消息（`IOT_TELEMETRY` 当前 `max_age=0` / 57400 条） |
| P1 | **流保留的手工校正（开发栈）** | 需在网关/连接器/告警服务下次启动时自动完成；若想立刻生效，需接受**删除超期消息**这一破坏性后果。`IOT_TELEMETRY` 的 `MaxMsgsPerSubject=1000` 还会把每个 subject 裁到 1000 条 |
| P2 | 测试残留流未清理 | `/jsz` 里能看到 `TEST_NATSJS_*` / `DIAG_ROUTE_*` 等残留流 —— 用例被中断时 `t.Cleanup` 没跑到。数量会随中断次数累积 |
| P2 | 文档收尾 | 把「IoT 平台部署位置」的决策补进 `07` §11.2（**已定：宿主 Docker Compose**） |
| P2 | 日志口径统一 | 网关层用 `log/slog`（mochi-mqtt 的 Hook 签名即 slog），`cmd/iot-gateway` 仍用 zap，二者应合并为一条流水线（见 `main.go:newSlogLogger`） |

### 5.2 命令速查

```bash
# 进 devbox shell（日常最常用）
ssh xfusion-163
docker exec -it -u xfusion devbox bash

# 编译 / 静态检查 / 测试
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && go build ./... && go vet ./... && go test ./...'

# A2 真实总线验证（`go test ./...` 默认跳过，需显式给 NATS 地址）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && \
  IOT_NATS_URL=nats://100.64.0.3:28222 go test ./internal/gateway -run TestA2_RealNATS -count=1 -v'
# 等价：make test-nats

# B1 时序表模型压测（约 3 分钟；会 drop/重建 telemetry* 表）
#   建议先 docker restart iot-greptimedb：连续压测会留下 compaction backlog
make b1-bench

# B1 补充项（1）· 明细限行真实库验证（env 门控；⚠️ 会 drop/重建 telemetry）
#   断言 Q3 形态经 QuerySeries 降采样后 P95<200ms（报告见 docs/reports/b1-detail-limit.md）
make test-tsdb

# B1 补充项（2）· 预聚合真实库验证（env 门控；⚠️ 会 drop/重建 telemetry 与 telemetry_1m；约 2 分钟）
make test-rollup
# 水位账本的真实 PG 验证（建临时库）
make test-rollup-pg

# 预聚合服务（需先 iot-migrate 应用 0005）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && \
  go run ./cmd/svc-rollup -log-format text -interval 30s'
# 健康与指标：curl -s http://127.0.0.1:18093/readyz ; curl -s http://127.0.0.1:18093/metrics

# 控制面主数据 + 只读查询 API
make test-catalog      # 真 PG（临时库）：分区/约束/upsert 幂等/分页/归属
make test-query        # 单测：JWT 各失败形态、限流器、handler 状态码（不依赖真实库）
make test-query-e2e    # 端到端：真 PG + 真 GreptimeDB（⚠️ 会 drop/重建 telemetry）
make seed-dev          # 写开发种子（租户/设备类型/3 台设备，凭据落 tmp/iot-seed-creds.json）
make run-svc-query     # 前台起服务（dev 静态令牌，仅绑 127.0.0.1:18094）

# 手动走查（服务起来后）
#   注意：这些端口的服务绑在各有其 127.0.0.1，需在 devbox 内 curl
curl -s http://127.0.0.1:18094/readyz
curl -s -H 'Authorization: Bearer devtoken' 'http://127.0.0.1:18094/api/v1/devices?limit=2'
curl -s -H 'Authorization: Bearer devtoken' \
  'http://127.0.0.1:18094/api/v1/series?device_ids=1001&metric=temperature&since=2026-10-01T00:00:00Z&until=2026-10-01T01:00:00Z'
curl -s -H 'Authorization: Bearer devtoken' \
  'http://127.0.0.1:18094/api/v1/series?device_ids=1001&metric=temperature&bucket=5m&since=2026-10-01T00:00:00Z&until=2026-10-01T01:00:00Z'
curl -s -H 'Authorization: Bearer devtoken' 'http://127.0.0.1:18094/api/v1/devices?project_id=999'   # 400 TENANT_NOT_ALLOWED
curl -s -o /dev/null -w '%{http_code}\n' 'http://127.0.0.1:18094/api/v1/devices'                     # 401
# 生产模式（需要签发方与 JWKS；当前签发方 Odoo OIDC / svc-auth 都还没建）
#   -auth-mode jwt -jwt-issuer https://auth.example.com -jwt-jwks-url https://auth.example.com/.well-known/jwks.json \
#   -redis-url redis://100.64.0.3:28637/0

# 最新值（由 svc-pipeline 写入 Redis，查询服务读取；缺失时 available=false）
curl -s -H 'Authorization: Bearer devtoken' \
  'http://127.0.0.1:18094/api/v1/latest?device_ids=1001,1002'

# C1 规则条件引擎：P99 验收 + 求值基准
make c1-bench
# 等价于：IOT_PERF_ASSERT=1 go test ./internal/rules -run TestPerf -count=1 -v

# A1 认证容量实测（约 3 分钟）
make a1-capacity

# A4 网关容量压测（先起网关；⚠️ 5 万连接须分机部署压测客户端）
make a4-bench
# 等价：go run ./cmd/mqtt-bench -broker tcp://127.0.0.1:11883 -conn 50000 -rate 2000 -duration 24h
# 阶段 2 周期发布（QoS1）：追加 -publish-interval 1s
# 阶段 3 吞吐 + 接入确认延迟：-publish-interval 0（背靠背，尽可能快）
# 冒烟：go run ./cmd/mqtt-bench -conn 10 -rate 100 -duration 4s -interval 3s -publish-interval 0

# 消息管道 svc-pipeline（消费网关上报 → 物模型解析 → 攒批写 GreptimeDB；落库后 ACK）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && \
  go run ./cmd/svc-pipeline -nats-url nats://100.64.0.3:28222 \
  -dsn "postgres://greptime:greptime@100.64.0.3:28403/public"'

# 计量用量聚合 svc-quota（消费 iot.quota.usage → Redis 计数器 quota:{pid}:{metric}:{yyyymmdd}）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && \
  go run ./cmd/svc-quota -nats-url nats://100.64.0.3:28222 -redis-url redis://100.64.0.3:28637/0'

# 业务库迁移（项目栈 iot-postgres @ 100.64.0.3:28543，库 odoo20iot）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && go run ./cmd/iot-migrate -check'
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && go run ./cmd/iot-migrate'

# 打真 PG 的用例必须显式给 DSN，否则会 Skip（不是「通过」）
docker exec -u xfusion -e IOT_PG_DSN='postgres://iot:iot_dev_only_change_me@100.64.0.3:28543/odoo20iot' \
  devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && go test ./internal/pg/... ./internal/alarm -count=1'

# 通知服务（04 §2.3 三通道；出站白名单**必填**，为空则拒绝启动）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && \
  go run ./cmd/svc-notify -log-format text -egress-allow "*.dingtalk.com,hooks.example.com" \
  -default-webhook-to "https://oapi.dingtalk.com/robot/send?access_token=xxx"'
# 探活：curl -sS 127.0.0.1:18093/healthz ；就绪：/readyz ；指标：/metrics ；死信量：/metrics/dlq

# 告警服务（04 §2 双通道 + 5s 扫描；迁移必须先应用，否则启动即拒绝）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && go run ./cmd/svc-alarm -log-json false'
# 探活：curl -sS 127.0.0.1:18092/healthz ；就绪（真查 PG + 迁移）：/readyz ；指标：/metrics
# 发一条规则触发（无 nats CLI 时用原始协议）：PUB iot.rule.alarm {"project_id":"p1","device_id":"d1","rule_id":"r","level":"warn"}

# Odoo 连接器（07 §4.3 编排层 + §4.4 事件入口与对账；API Key 与令牌由 Vault/环境变量注入，切勿写进命令行历史）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && \
  ODOO_API_KEY=xxx ODOO_WEBHOOK_TOKEN=yyy IOT_DLQ_ALERT_WEBHOOK=xxx IOT_DLQ_ALERT_EGRESS_ALLOW=open.feishu.cn go run ./cmd/odoo-connector \
    -odoo-url http://127.0.0.1:8070 -odoo-db odoo20 \
    -log-format text -reconcile-models maintenance.equipment \
    -dlq-alert-interval 5m -dlq-alert-window 15m -dlq-alert-threshold 1'
# ⚠️ -log-format 是字符串 flag：不要用 -log-json false 这种写法 ——
#    Go 的 flag 包遇到第一个非 flag 参数会**静默丢弃其后所有参数**。
# 对账调试：-reconcile-interval 4s -reconcile-stale-after 2s（加速观察补投）
# 探活：curl -sS 127.0.0.1:18091/healthz ；就绪（真打 Odoo）：/readyz ；指标：/metrics
# DLQ 重放：追加 `-dlq-replay-interval 30s -dlq-replay-batch 20`；默认关闭。
# C-1 入口：Odoo 侧 cron XADD 到 Redis Stream odoo:outbox，连接器自动搬运到 NATS IOT_ODOO
# C-2 入口：curl -sS -X POST -H "Authorization: Bearer yyy" -H 'Content-Type: application/json' \
#   -d '{"model":"maintenance.equipment","id":7,"write_date":"2026-10-01 06:30:00","company_id":1,"data":{}}' \
#   http://127.0.0.1:18091/webhook/odoo
# 真实 Redis/NATS 下的集成用例（默认跳过）：
#   IOT_REDIS_URL=redis://100.64.0.3:28637/0 go test ./internal/connector -count=1

# 生成一份演示凭据并本地起网关（带认证）
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && \
  go run ./cmd/iot-gateway -gen-auth-file=tmp/dev-credentials.json'
make run-gateway        # 默认读取 tmp/dev-credentials.json

make run-gateway              # → MQTT 127.0.0.1:11883，HTTP 127.0.0.1:18080（/healthz /metrics）

# 开发栈
cd /home/xfusion/projects/odoo20iot/deploy/compose
docker compose up -d          # 启动
docker compose ps             # 状态
docker compose logs -f nats   # 跟随日志
docker compose down           # 停止（保留数据卷）；加 -v 会删数据卷

# Odoo 20
docker exec devbox systemctl status odoo@odoo20tbb
curl -fsS http://100.64.0.3:9070/web/login -o /dev/null -w '%{http_code}\n'

# Odoo 模块安装 / 升级（必须三步，禁止服务运行中执行 -i/-u）
#   1) sudo systemctl stop odoo@odoo20tbb
#   2) cd /home/xfusion/projects/odoo/odoo20tbb/odoo
#      /home/xfusion/venvs/odoo20/bin/python odoo-bin -c /home/xfusion/etc/odoo/odoo20tbb.conf -d odoo20 -u <模块> --stop-after-init
#   3) sudo systemctl start odoo@odoo20tbb
#   并核验：退出码 0 + 日志出现 "Module <模块> loaded" + systemctl is-active = active
```

---

## 6. 已踩过的坑（清单，避免重复）

| # | 坑 | 现象 | 结论 |
|---|---|---|---|
| 1 | 执行层吞 `$`、不支持 `\"` | 远程命令报 `unexpected EOF` / 变量为空 | 用「外层双引号 + 内层单引号」；复杂脚本走「本地写 → scp → 执行」 |
| 2 | `GOPROXY` 未设镜像 | `go get` 卡死/超时 | 官方 `proxy.golang.org` 不可达，必须 `https://goproxy.cn,direct` |
| 3 | `go env -w` 失败 | `open ~/.config/go/env: no such file or directory` | devbox 内 `~/.config` 是 root 所有；需 `chown` 后再配置 |
| 4 | **PostgreSQL 18 挂载点** | 容器**无限重启** | PG18 镜像 `PGDATA=/var/lib/postgresql/18/docker`，**挂载点必须是 `/var/lib/postgresql`**，挂到 `.../data` 会被判为「未使用的挂载」拒启 |
| 5 | **GreptimeDB 绑定地址** | 容器 `healthy`，但端口**连不上** | standalone 默认只绑 `127.0.0.1`；容器健康检查走 loopback 仍通过 → **极具迷惑性**。必须显式 `--http-addr 0.0.0.0:4000` 等 |
| 6 | remote 用了 `github.com` 别名 | push 报 `Repository not found` | 每仓一把 deploy key，必须用 `git@github-<标识>:...` |
| 7 | deploy key 一仓一把 | 无法跨仓复用（GitHub 限制） | 新仓生成新 key + 新 ssh 别名（见 `AGENTS.md`） |
| 8 | CRLF / LF | Linux 侧脚本、Makefile 异常 | `.gitattributes` 固定 LF |
| 9 | `max_cron_threads = 0` | `edge_outbox` 的 cron 投递**根本不执行** | 开发库故意关闭 cron；**集成测试前必须临时调起**，否则会把环境问题误判成代码 bug |
| 10 | 拿废弃快照当现状 | Odoo 配置审计 8 项里 **5 项误判** | Windows 的 `odoo20.conf` 属 2026-09-21 前的旧布局；**一切以服务器实际配置为准** |
| 11 | 归一化换行符后 `git status` 仍报 M | `cmp` 字节一致、`git hash-object` 与索引 SHA 相同、`git diff --summary` 为空，**但 status 显示 3 个文件被修改** | `git update-index --refresh` **只比 stat 不比内容**，清不掉；执行 `git add -A` 刷新索引即可（不产生暂存内容）。根因：bind mount 下 `sed -i` 的 mtime 与索引 stat 缓存不匹配 |
| 12 | **`OnPublish` 返回普通 error 无法阻止 PUBACK** | 以为「返回错误 = 拒绝」，实测 broker 照常回 PUBACK → A2 静默失效 | **只有 `packets.ErrRejectPacket` 会让 `processPublish` 直接 `return nil`**；其他 error 会继续走原生路径。见 `03` §4.4.1 |
| 13 | 手写 MQTT CONNECT 时漏填 `ProtocolName` | 服务端回 `CONNACK reason=130`（`BadUsernameOrPassword`），看起来像认证失败，实际是报文非法 | 用 `packets.Packet` 构造 CONNECT 时必须设 `Connect.ProtocolName = []byte("MQTT")` |
| 14 | `mqtt.Server.Close()` 不可重入 | 二次调用 panic：`close of closed channel` | 自己做 `sync.Once` 保护；**且它已经关闭了监听器**，不要再 `ln.Close()`（否则报 `use of closed network connection`） |
| 15 | 无匹配 Stream 的 subject | JetStream 返回 `nats: no response from stream`（**不是**超时，也**不是**连接错误） | 若误把该错误当「已确认」，配置失误就会静默丢数据。A2 实现按「未确认」处理（见 `03` §4.4.1 边界 3） |
| 16 | `js.SubscribeSync` 早于 Stream 创建 | `nats: no stream matches subject` | 测试里必须先建 Stream 再订阅 |
| 17 | `metrics` 是 GreptimeDB 保留关键字 | 建表报 `Cannot use keyword 'metrics' as column name` | DDL 里写成 `"metrics"`（`02` §4.2 已修正） |
| 18 | **JSON 列在 PG 协议下是 bytea** | 普通字符串参数报 `\x prefix expected for bytea`；`CAST($1 AS JSON)` 报 `Unsupported SQL type JSON` | 只有两条路：内联字面量（快 3.4×）或 `\x`+十六进制参数（体积 ×2） |
| 19 | pgx 的两个默认行为都不兼容 GreptimeDB | `Ping()` 发空语句 → `empty statements`；simple protocol → `standard_conforming_strings` 为 off 被 pgx 拒绝 | 探活用 `SELECT 1`；保持扩展协议并显式指定取值编码 |
| 20 | **漏检 `rows.Err()` = 静默空结果** | 本次探测中 `json_get` 作用于 TEXT 列，`Query()` 不报错、`Next()` 直接为 false，被误判成「静默返回空」 | pgx 的查询错误不一定从 `Query()` 返回；所有迭代后必须 `rows.Err()` |
| 21 | **压缩比必须先 `ADMIN FLUSH_TABLE`** | 写入刚结束数据还在 memtable，读 `disk_size` 得到的是残余，字节/行虚高 2~6 倍（实测 72.8 → 16.1） | 任何存储结论都要先强制落盘 |
| 22 | **小数据集压测会给出相反结论** | 4200 行的冒烟跑出「JSON 比宽表慢 3 倍」，112 万行时变成「JSON 快 4 倍」——预热开销主导 | 压测必须跑到稳态；本次全量 3 轮写入吞吐复现性 ±3% |
| 23 | **取值生成器会翻转存储结论** | 周期序列下 A/B 存储比 3.31×，随机/自相关下 0.96× | 压测必须声明取值模型（`-value-model`），存储维度不能只看一次 |
| 24 | 连续压测把 GreptimeDB 拖入 compaction backlog | 连跑 4 轮后单轮写入从 28s 涨到 5min+（CPU 560%） | 压测前 `docker restart iot-greptimedb`，并先 drop 无关表 |
| 25 | **`expr.Env(结构体)` 的字段名大小写敏感** | 物模型键是小写（`msg.temperature`），Go 字段必须导出（`Msg`）→ `unknown name msg`；`reflect.StructOf` 直接拒绝小写字段名 | 环境用 `map[string]any`（小写键），**字段校验自己做**（`04` §1.2.1） |
| 26 | **`map[string]any` 环境下 expr 不做任何校验** | `msg.typo > 60`、`msg.temperature > 'abc'`、`msg.running && msg.temperature` 全部编译通过 | 编译期校验必须是自建的 AST 层，不能寄托在库上 |
| 27 | **`expr.DisableAllBuiltins()` 挡不住谓词** | `all/filter/map/none/any/one` 在解析期就变成 `PredicateNode`，绕过 Builtins 表 → 关掉内置后仍编译通过 | 白名单必须在 AST 层再拦一次（`PredicateNode`/`PointerNode`） |
| 28 | expr 区分 `nil` 与 `null` | 文档里的 `prev == null` 编译不过（`null` 是未知标识符） | 统一写 `prev == nil` |
| 29 | **编译期模板里的 nil 会收窄类型** | 环境模板把 `prev` 写成 `nil` → 检查器把 prev 收窄成 nil 类型 → `prev.temperature` 报 `type nil has no field temperature` | 编译期模板一律给空映射；「prev 可能为空」只在运行时表达 |
| 30 | **`expr.Run` 的池化开销决定了 P99 是否达标** | 同样表达式：直接用 `expr.Run` P99 = 2.03 µs（超线），改用 `Runner` 复用 VM 后 0.98~1.52 µs | 热路径用 `rules.Runner`；这类预算里常数项就是结论 |
| 31 | **漏检 `rows.Err()` / 只看 P50 会给出错误结论** | 小规模与单点测量都会骗人（B1 的小数据集、C1 的 P50） | 结论必须看**分布**与**全量**，不看单点或均值 |
| 32 | **MQTT 3.1.1 的 SUBACK 返回码不是 0/1 表示成败** | 返回 `1` 被误读成失败，实际是「授予 QoS1」 | 判据是 `code < 0x80` 才算成功；`0x80` 是失败 |
| 33 | **Argon2 的并发上限不能按 CPU 核数定** | 槽位 4→32 时认证吞吐反而 51→19 次/秒；CPU 利用率封顶 25% | 瓶颈是**内存带宽**；按实测峰值附近取值（8C16G 取 8） |
| 34 | **「缓存命中率高」≠「认证容量够」** | L1 只省 157ns 的目录查询，省不掉 116ms 的 Argon2（占 0.00016%） | 容量必须按 KDF 成本算；别用命中率论证容量 |
| 35 | 限流与安全计数的混淆 | 把「网关过载」计入凭据失败窗口，重连风暴会把**全部正常设备**拉黑 | 过载/后端不可用必须单列：不计失败、不进黑名单、返回可重试语义 |
| 36 | **告警扫描（`Tick`）的遍历顺序不定** | 同一轮里父告警与子告警的求值顺序随机 —— 「父告警本轮才被关闭」时子告警仍被判为「被抑制」；**断言若依赖顺序就会偶发失败** | 判定要么做成顺序无关，要么扫两轮（本实现：`Tick` 遇乐观锁冲突**不重试**，测试合并两轮决策再断言） |
| 37 | **`pgxpool.Config.ConnString()` 原样返回最初传入的字符串** | 改完 `ConnConfig.Database` 之后再调它，拿到的还是**旧库名** —— 「换一个临时库跑测试」静默变成「连回基准库」，**而且用例还会通过** | 别用 `ConnString()` 取 DSN；用结构化的 `pg.OpenWithConfig` + 改好的 `ConnConfig`，并在连上后核对 `current_database()` |
| 38 | **测试基建失效时，用例会「假通过」而不是失败** | 上面那条让整套 PG 用例都打在基准库上，**第一次跑全绿**；第二次才因残留数据失败 —— 若只跑一次就提交，就会以为「这条路径测过了」 | 隔离与前置条件要**自检**（连上核对库名、断言前置状态），不能靠约定；真库用例未配置时**显式 Skip** 而不是静默通过 |
| 39 | **同一个 devbox 里有两个 PostgreSQL，很容易连错** | devbox 内的 `postgresql@18`（`127.0.0.1:5432`）是 **Odoo 的库实例**（`odoo20`/`erp_dev`）；业务库是项目栈里独立的 `iot-postgres` 容器（`100.64.0.3:28543` / 库 `odoo20iot`）。连错**不会报错** —— 建表、迁移、查询全都成功，只是表长在了 Odoo 的实例上 | 用 `pg.DefaultDSN`（已指向项目栈）；写死 DSN 时先 `SELECT current_database()` 确认 |
| 40 | **`foo.bar.>` 匹配不到 `foo.bar` 本身** | 用「去掉最后一段 + `>`」从 subject 派生流的 subjects 时，若 subject 只有两段（如 `iot.rule`），派生出的通配**不覆盖原 subject** —— 流里不含实际要发布的 subject，发布静默失败或报 `no response from stream` | 不足三段时原样返回；或直接对着 `jsz` 确认流的 subjects 含目标 subject |
| 41 | **`pkill -f "某个字符串"` 会杀掉执行它的 shell 自己** | 命令行走在 `bash -lc` 里，整个命令行文本包含那个字符串，于是 `pkill -f` 把自己也匹配上了 —— 表现为「命令执行到一半突然没了下文」 | 按**进程名**杀：`pkill -x svc-alarm`（`-f` 匹配全命令行，`-x` 匹配精确名字） |
| 42 | **对 JetStream 订阅调 `sub.Unsubscribe()` 会删掉消费者** | 服务每次退出都删、每次启动都新建，于是**重启即重放整个保留窗口** —— 流保留 24h，等于每次重启把全天事件重发一遍（通知服务＝通知风暴，`svc-quota`＝用量重复计数）。而且**完全不报错**，只在行为上体现 | 退出时只关连接，**不要** `Unsubscribe`（消费者是服务端状态）；统一用 `internal/natsjs.Subscribe`。**现已结构性封死**：该函数返回收窄的 `*natsjs.Subscription`（只暴露 `Fetch`），调用方**拿不到** `Unsubscribe`。唯一保留 `Unsubscribe` 的是 `cluster.ReplayOffline` 的**临时**消费者（durable 为空 + 起始序号，现建现删），已就地注释说明为何不能「统一」 |
| 43 | **NATS 2.10+ 对 durable 消费者有默认的空闲回收（5 分钟）** | 服务停机超过 5 分钟消费者被自动删除，于是「一个周末的发布」变成一次全量重放 —— 与坑 42 的表象完全一样，但成因不同，只看代码看不出来 | 显式设 `InactiveThreshold`（取流的保留时长）；比它更久的空闲本来也没东西可补 |
| 44 | **`::ffff:0:0/96` 会被 Go 规范化成 `0.0.0.0/0`** | 写进「禁止访问的网段」表里就等于**拦掉全部 IPv4**（所有 webhook 都发不出去），而错误信息只说「命中禁止访问的地址段」 | 网段表里不要列 IPv4 映射段；改为把地址 `To4()` 归一成 4 字节再判，`::ffff:10.0.0.1` 自然落到 `10/8` 规则上 |
| 45 | **IM 机器人与短信网关「HTTP 200 + 业务错误码」** | 钉钉/企微返回 `{"errcode":310000,"errmsg":"token is not exist"}`、飞书用 `code`、短信厂商同理。只判 HTTP 状态码会把 token 失效、机器人被移出群、模板未报备全部记成**投递成功** —— 通知静默丢失而指标一切正常 | 2xx 也要解析响应体里的码值；判成**可重试**（限流重试就能过，判错方向不对称） |
| 46 | **环境门控的用例会腐烂** | 需要 `IOT_NATS_URL` / `IOT_PG_DSN` 才跑的用例默认被跳过，于是断言过期了也没人发现 —— 本次补上环境变量后立刻暴露一条（`gateway` 的载荷断言在「遥测包进信封」那天就过期了） | 定期带环境变量跑一遍全量；断言写成「**结构包含**」而非「字节相等」，减少随实现演进过期 |
| 47 | **记进文档的坑，仍会在新代码里重犯** | `-log-json false` 的位置参数陷阱（§6 坑）在 `odoo-connector` 修过一次，本次 `svc-alarm` / `svc-notify` 又踩了一遍：其后所有参数（含出站白名单）被静默丢弃；坑 42 也一样 —— 已在 `svc-alarm`/`svc-notify` 修过，`svc-pipeline`/`svc-quota`/`cluster` 又各写了一遍 | 别只在「坑清单」里记，要把规避方式做成**可复用的东西**（`-log-format` 的约定、`internal/natsjs`），让下一个人不需要先读到那条坑。**判断「做够没有」的标准是：写错的代码还编不编得过** —— `internal/natsjs` 一开始只是「约定用我」，于是又被绕过 3 次；把返回值收窄成不暴露 `Unsubscribe` 之后，写错直接编译失败，才算真的封死 |

---

## 7. 文档索引

## 8. 2026-10-01 继续开发记录

- **第1项**：真实 MQTT → NATS → GreptimeDB → Redis 最新值 → `svc-query` 验收已完成；新增 Odoo 集成账本迁移 `0007`，并在真实 IoT PostgreSQL 验证外部引用唯一约束、集成日志月分区和幂等注册表。
- **第2项**：新增 `internal/extref` 的远端/本地查询、状态更新和租户事务写入基础；`odoo-connector` 已支持通过 `-masterdata-model maintenance.equipment` 开启主数据增量同步，使用 Redis `(write_date,id)` 游标，幂等 upsert `t_device_type`/`t_device` 并写入 `t_external_ref`。仍未完成 Odoo S1/S3 真实端到端验收、DLQ 重放编排和多模型字段映射。
- **2026-10-01 实际联调**：已安装 Odoo `maintenance`（`20.0.1.0`），`maintenance.equipment` JSON-2 返回 `HTTP 200`；IoT `t_project.id=1` 已绑定 Odoo `res.company.id=1`（圣宁咨询）。主数据同步首轮真实运行通过，`read=0/upserted=0/skipped=0`，原因是 Odoo 当前没有设备记录；不是链路失败。
- **2026-10-01 设备端到端验收**：通过 Odoo JSON-2 创建验收设备 `maintenance.equipment.id=1`，随后真实同步 `read=1/upserted=1`；修改 Odoo 设备后再次同步 `read=1/upserted=1`，重复运行 `read=0/upserted=0`。IoT 最终 `t_device.id=1`、`t_external_ref.local_id=1`、版本递增到 2，Redis 游标已推进。期间修复了 `t_device.id=0` 的序列生成缺陷。
- **2026-10-02 告警建单链路**：Odoo `sn_edge_integration` 已提供幂等 `POST /api/iot/v1/maintenance/request`；`odoo-connector` 新增持久 JetStream 告警消费者，按 `t_external_ref` 解析设备并在建单成功后 ACK。通过 `-alarm-to-odoo` 显式启用，未绑定设备记录为 `unbound_alarm` 集成问题。
- **DLQ 基础**：迁移 `0010_dlq_replay` 已应用；`internal/dlq` 新增实体类型、幂等键、待重放查询、重放标记和按实体聚合统计。
- **DLQ 执行层**：新增 `internal/dlq.Replayer`，按 `entity_type` 注册处理器，成功后标记解决，失败记录重放次数和原因，未知实体安全跳过，默认最多重放 3 次；已补幂等键透传和单元测试。当前仍需把 `odoo-connector`、`svc-notify` 的具体下游处理器注册到该执行层。
- **第3项**：新增 `internal/pg.WithProjectTx`，通过事务级 `set_config('app.project_id', ..., true)` 固定租户上下文；迁移 `0008` 已对 `t_external_ref`、`t_integration_log`、`t_idem_registry` 启用并强制 RLS。真实库已验证租户 101 与 202 只能看到各自行；`t_project`/`t_device_type`/`t_device` 等旧路径仍需逐个迁移后再开启 RLS。
- **第4项**：迁移 `0009` 新增 `t_alarm_rule` 与 `t_alarm_silence`；`svc-alarm` 启动时从 `t_alarm_silence` 加载有效静默窗口。未确认升级、通知策略热加载/模板渲染、批量告警实体和 dedup 分片仍是后续工作。
- **本轮验证**：`go test ./...`、`go vet ./...`、`go build ./...` 全部通过；真实库已应用 `0007`、`0008`、`0009`。

| 文件 | 内容 |
|---|---|
| `README.md`（docs） | 设计目标、NFR/SLO、对标矩阵、ADR 决策记录 |
| `01-architecture.md` | 分层架构、服务清单、技术选型、部署拓扑、数据流 |
| `02-domain-and-data.md` | 多租户、物模型、PG 表设计、GreptimeDB 时序、缓存与一致性 |
| `03-ingestion.md` | 网关集群、认证 ACL 限流、多协议、消息管道、SDK 契约 |
| `04-rules-alarm-automation.md` | `expr` + 自研 DAG + 受限 JS 逃生舱、告警 FSM、场景、OTA、影子 |
| `05-security.md` | 威胁模型、认证授权、加密、密钥、审计合规 |
| `06-operations.md` | 高可用、容灾、可观测、发布、容量、演进路线（双轨） |
| `07-odoo-integration.md` | **Odoo 对接**：现状审计、通道选型、幂等、扩展清单 |
| `08-odoo-perf-alignment.md` | 与 Odoo 性能/架构的对齐评审 |
| `odoo20iot-revision-checklist.md` | 四轮评审的全部缺陷与处置（含根因分析） |
| **`09-handoff.md`（本文）** | **交接与工作约定** |

## 9. 2026-10-02 安全与产品增强

- 查询服务新增每租户令牌桶速率限制：`-rate-per-second`、`-rate-burst`，超限返回 `429/RATE_LIMITED`。
- 查询服务内置最小控制台，访问 `/` 可查看设备和最新值；API 仍要求 Bearer Token。
- 迁移 `0012_control_plane_security` 已应用：新增 `t_role`、`t_user`、`t_api_token`，并为控制面、设备台账、告警配置启用 RLS 策略。
- 网关新增 `-auth-source pg`，可从业务库 `t_device.secret_hash` 读取设备认证摘要；默认 `file` 保持开发兼容。
- 通知服务新增 `-policy-source db|file`，默认从 `t_alarm_rule.notify` 读取策略，并按角色展开用户邮箱。
- 告警风暴升级现在发布 `iot.alarm.escalation.<project>` 事件，载荷含 `escalated=true`，不再只写日志。
- 规则层新增内存滑动窗口聚合器，支持 `avg/max/min/count/last/p50`，窗口缓存不作为事实数据持久化。
- 当前仍需外部配置/验收：真实飞书 Webhook、生产 Vault、mTLS、完整 Odoo 多模型字段映射和前端产品化页面。
