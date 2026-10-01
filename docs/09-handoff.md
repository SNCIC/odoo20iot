# 09 · 交接与工作约定

> **这份文档的作用**：让**新会话 / 新同事**读完这一份就能接着干，**不依赖任何对话上下文**。
> 每完成一个阶段、或改变工作方式时，**必须回来更新它**。

---

## 1. 一句话现状

**Phase 0（技术验证）进行中**：环境已就绪、骨架已跑通、开发栈已起来（四个中间件全部 healthy）。
**两个决策点已通过**：
- **A2（QoS1 PUBACK 时机）** —— ADR-001 的定制点成立，**不必回退 EMQX**（`03` §4.4.1）；
- **B1（GreptimeDB JSON vs 宽表）** —— JSON 方案写入达标 3.8×，**维持 JSON 为默认，不切宽表**（`02` §4.1.1）。

下一个要打的是 **C1（`expr` 条件引擎）**。

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
| **Go 1.27.1** | devbox `/usr/local/go`，软链 `/usr/local/bin/go` | **必须以 `xfusion` 用户运行**：`docker exec -u xfusion devbox ...` |
| `GOPROXY` | `https://goproxy.cn,direct` | **官方 `proxy.golang.org` 不可达**，必须走镜像 |
| `GOTOOLCHAIN` | `local` | 禁止自动下载其他工具链 |
| git | devbox 2.53.0 | 提交身份 `Gavin <963645882@qq.com>` |
| Docker / Compose | 宿主 29.8.1 / v5.5.1 | 镜像源已配国内镜像（daocloud / 1panel） |
| Odoo 20 | devbox `odoo@odoo20tbb` | 容器内 `127.0.0.1:8070`，Tailscale `100.64.0.3:9070` |
| `mochi-mqtt/server/v2` | **`v2.7.9`（必须锁版本）** | A2 的挂载点依赖其 `processPublish` 对 `packets.ErrRejectPacket` 的处理（见 `03` §4.4.1 边界 5） |
| `nats-io/nats.go` | `v1.54.0` | 纯 Go，无 cgo（ADR-010） |
| `jackc/pgx/v5` | `v5.11.0` | 访问 GreptimeDB 的 PostgreSQL wire 端点；纯 Go。**注意 `Ping()` 与 simple protocol 都不可用**，见 §6 坑 19 |
| GreptimeDB | **`1.2.1`**（compose 里是 `:latest`，**上生产前必须锁版本**） | JSON 能力边界按 1.2.1 实测，见 `02` §4.1.1 ④ |

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
| 2 | 仓库建立并推送 | commit `0a54067` → `8057ad5` → `2a0382a`，`git ls-remote origin main` = `2a0382a` |
| 3 | 开发栈四件套运行中 | 全部 `healthy`；**从 devbox 侧**逐项验证：GreptimeDB HTTP(28400) + PG-wire(28403) + 我们的 PG(28543) + NATS(28224) + Redis(28637) |
| 4 | 代码质量门禁通过 | `go build ./...`、`go vet ./...`、`go test ./...` 全绿 |
| 5 | Odoo 现状审计勘误 | `07-odoo-integration.md` §2.5 / §2.6 已按**服务器实际配置**重写（原审计基于已废弃的 Windows 旧布局快照，8 项里 5 项误判） |
| 7 | **B1 · 时序表模型（P0）** | **通过**。`cmd/tsdb-bench` 在 GreptimeDB 1.2.1 上对比 A（JSON）/ B（宽表）：写入 19.1 万 / 16.1 万 points/s（验收线 5 万，内联字面量路径 64.0 万）；查询单设备全部达标，多设备明细两方案**都**超线 → 结论「维持 JSON 默认」。含**前置一致性校验**（两方案 3599 点逐点相等）。结论与边界 `02` §4.1.1；原始报告 `docs/reports/b1-tsdb-bench.md` |
| 6 | **A2 · QoS1 PUBACK 时机（P0）** | **通过**。`mochi-mqtt` v2.7.9 可在「等待 NATS `PublishAck` 后再回 PUBACK」下工作：`OnPublish` 返回 `packets.ErrRejectPacket` 阻止自动 PUBACK，业务侧 `cl.WritePacket(ack)` 显式确认。实测往返 **0.52 ~ 1.12 ms**（5 次采样，中位 ≈ 0.92 ms）。结论与边界见 `03-ingestion.md` §4.4.1；代码 `internal/gateway`；用例 `a2_test.go`（含**崩溃注入**与**对照实验**）、`nats_test.go`（真实 JetStream，PUBACK 后消息可读回） |

---

## 5. 未完成 / 下一步

### 5.1 待办清单（按优先级）

| 优先级 | 项 | 说明 |
|---|---|---|
| ~~P0~~ | ~~A2 · QoS1 PUBACK 时机~~ | ✅ **已完成**（见 §4 第 6 项） |
| ~~P0~~ | ~~B1 · GreptimeDB JSON vs 宽表~~ | ✅ **已完成**（见 §4 第 7 项） |
| **P0** | **C1 · `expr` 条件引擎** | 编译期类型校验 + 求值 P99 ≤ 2 µs。**下一个打这个** |
| P0 | B1 补充项（1） | **明细查询限行**必须落地（`02` §4.3.1）：实测多设备 3.6 万行明细 P95 达 247/211 ms，两方案都超线 |
| P0 | B1 补充项（2） | **预聚合表从「可选优化」升为必做**：多设备 × 长跨度聚合在 JSON 下已骑在 SLO 线（198~213 ms），宽表 91 ms |
| P1 | B1 补充项（3） | 上生产前按 **3 副本集群 + NVMe** 重测写入吞吐；宽表开启前用**租户真实样本**重测存储占用 |
| P1 | A1 · 设备认证 Hook | **当前网关放行全部连接**（`gateway.New` 里显式标注），三档认证（ADR-008）未实现 |
| P1 | A3 / A4 / A5 | 集群路由、5 万连接 24h、计量埋点 |
| P1 | A4 补充项 | A2 引入了「收包协程同步阻塞 ≤ `PubackTimeout`」，需在 5 万连接 24h 压测中实测其对 `PINGREQ` 与掉线判定的影响（见 `03` §4.4.1 边界 1） |
| P1 | D2 / D3 | Odoo JSON-2 客户端骨架、`sn_edge_integration` 模块骨架 |
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

# 本地起网关（冒烟）
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

---

## 7. 文档索引

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
