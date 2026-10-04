# 01 · 总体架构设计

## 1. 设计原则

| 原则 | 落地要求 |
|---|---|
| **契约先行** | 设备↔平台的 Topic/Payload 契约、服务间 NATS Subject、开放 API 全部先定义并版本化（`v1`），再写实现。契约变更走评审 + 兼容性测试 |
| **控制面/数据面分离** | 数据面（网关、管道）追求极致吞吐与低延迟，可牺牲一致性；控制面（设备管理、配置、规则管理）追求强一致与可审计。两者独立部署、独立伸缩、独立故障域 |
| **无状态优先** | 除网关连接会话、时序库外，所有服务无状态，状态外置到 PG/Redis/NATS。有状态服务（调度器）通过 Leader 选举或分片实现 |
| **故障隔离** | 租户级隔离（限流、配额、并发池）；单设备异常不得放大到单租户；单租户不得影响集群 |
| **背压与降级** | 每一跳都有队列水位与丢弃策略；遥测可降级丢弃、事件与命令不可丢；核心链路不依赖非核心链路 |
| **可观测优先** | 任何新增服务必须同时上线 Metrics/Logs/Traces 与 SLO 面板，否则不予合并 |
| **一次一版本** | 物模型、规则、固件全部版本化，支持灰度与回滚 |
| **悲观运维** | 假设任何组件随时会挂：无常驻连接假设、无单副本假设、无本地状态假设 |

---

## 2. 逻辑架构

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  应用层 (Application)                                                        │
│  Web 控制台 · 可视化看板/大屏 · 移动端 App · 第三方系统(SCADA/ERP/MES)        │
└───────────────┬──────────────────────────────────────────────────────────────┘
                │ HTTPS / WSS
┌───────────────▼──────────────────────────────────────────────────────────────┐
│  开放层 (Open API)                                                           │
│  API Gateway: 鉴权 · 限流 · 路由 · 审计   |   App MQTT 订阅服务              │
└───────────────┬──────────────────────────────────────────────────────────────┘
                │ gRPC / REST (同步)         NATS (异步)
┌───────────────▼──────────────────────────────────────────────────────────────┐
│  控制面 (Control Plane) —— 强一致 · 可审计                                    │
│  svc-auth    租户/用户/令牌/RBAC          svc-device   设备/设备类型/物模型   │
│  svc-rule    规则 CRUD/发布                svc-alarm    告警 CRUD/状态查询    │
│  svc-scene   场景与任务 CRUD               svc-ota      固件与升级任务        │
│  svc-quota   配额与计量                    svc-audit    审计日志              │
└───────────────┬──────────────────────────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────────────────────────┐
│  数据面 (Data Plane) —— 高吞吐 · 低延迟 · 最终一致                            │
│                                                                              │
│  gw-mqtt (集群) ──┐                                                          │
│  gw-http         ──┼──►  NATS JetStream  ──►  svc-pipeline (解析/富化/分流)   │
│  gw-coap         ──┤      (事件总线)      │                                  │
│  gw-tcp / DTU    ──┘                      ├──► GreptimeDB (遥测时序)       │
│                                           ├──► Redis      (最新值/在线态)    │
│  svc-rule (执行)  ◄───────────────────────┤                                  │
│  svc-alarm (FSM)  ◄───────────────────────┤                                  │
│  svc-automation   ◄───────────────────────┘                                  │
│  svc-notify  (短信/邮件/语音/Webhook)                                        │
└──────────────────────────────────────────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────────────────────────┐
│  存储层 (Storage)                                                            │
│  PostgreSQL (元数据/告警/审计)   Redis Cluster (会话/缓存/配额/限流)          │
│  GreptimeDB (时序)               MinIO/S3 (固件/图片/归档)                    │
│  Vault (密钥)                    NATS JetStream Stream (离线消息/重试队列)     │
└──────────────────────────────────────────────────────────────────────────────┘
```

**关键边界**：`svc-pipeline`、`svc-rule`、`svc-alarm`、`svc-automation` 之间**只通过 NATS 通信**，不允许同步 RPC 调用，保证管道内任一环节重启不影响上游接入。

---

## 3. 服务清单与职责

| 服务 | 类型 | 默认副本 | 伸缩指标 | 职责 | 依赖 |
|---|---|---|---|---|---|
| `gw-mqtt` | 有状态（连接） | 3+ | 连接数 / CPU | MQTT 接入、认证、ACL、限流、会话保持、跨节点路由 | Redis, NATS, svc-auth(缓存) |
| `gw-http` | 无状态 | 2+ | QPS | HTTP 上报降级通道、DTU 透传 | NATS, Redis |
| `gw-coap` | 无状态 | 2+ | QPS | CoAP/LPWAN 接入 | NATS, Redis |
| `gw-tcp` | 有状态（长连接） | 2+ | 连接数 | 私有 TCP/DTU 透传协议 | NATS, Redis |
| `svc-auth` | 无状态 | 2+ | QPS | 租户/用户/角色/令牌签发与校验、设备凭据校验 | PG, Redis |
| `svc-device` | 无状态 | 2+ | QPS | 项目、设备、设备类型、物模型、配置模板、影子 | PG, Redis |
| `svc-pipeline` | 无状态（分区消费） | 4+ | 消费滞后 | 报文解析、物模型校验、单位换算、时间校正、分流写库 | NATS, GreptimeDB, Redis |
| `svc-rule` | 无状态（分区消费） | 3+ | 执行耗时 | 规则匹配（`expr` 条件求值）+ 自研 DAG 编排执行 + 受限 JS 逃生舱 | NATS, PG(规则缓存) |
| `svc-alarm` | 无状态（幂等） | 2+ | 事件速率 | 告警 FSM 状态机、去重聚合、通知触发 | NATS, PG, Redis |
| `svc-automation` | **有状态（分片/Leader）** | 3+ | 调度延迟 | 场景求值、cron 调度、重试队列、离线补发 | NATS, PG, Redis |
| `svc-notify` | 无状态 | 2+ | 队列深度 | 短信/邮件/语音/Webhook 多通道分发与降级 | NATS, PG, 三方 SDK |
| `svc-ota` | 无状态 | 2+ | — | 固件管理、升级任务编排、灰度、签名校验 | PG, MinIO, NATS |
| `svc-quota` | 无状态 | 2+ | — | 用量计量、配额校验、账单事件 | PG, Redis, NATS |
| `svc-audit` | 无状态 | 2+ | — | 审计事件落库与归档 | PG, MinIO |
| `api-gateway` | 无状态 | 2+ | QPS | 统一入口、鉴权、限流、路由、审计埋点 | 全部控制面服务 |
| `svc-openapi` | 无状态 | 2+ | QPS | 对外开放 API、App MQTT 订阅、SDK 支撑 | PG, Redis, NATS |
| `odoo-connector` | 无状态（按实体分片） | 2+ | 队列深度 | 与 Odoo 20 双向集成：主数据拉取、告警/产能回流、Outbox 与 webhook 接收。**独立部署，不嵌入网关与管道**；同时是 **NATS 与 Redis Streams 的唯一翻译层**；限流 + 熔断 + 幂等 + DLQ。详见 [07-odoo-integration.md](./07-odoo-integration.md) | NATS, Redis Streams, Redis, PG, Odoo HTTP API |

### 3.1 部署单元收敛（ADR 裁定）

上表是**代码模块边界**，不是部署单元。对齐《Odoo 20 高性能架构技术方案》手册 p.24 的反模式「服务爆炸：按负载而非按模型拆分」，一期只输出 **6 个可部署单元**：

| 部署单元 | 合并的代码模块 | 为什么可以合并 | 为什么不能并入其他单元 |
|---|---|---|---|
| `iot-gateway` | `gw-mqtt` / `gw-http` / `gw-coap` / `gw-tcp` | 同为接入适配，共享认证 / ACL / 限流 / 路由 | 连接数与消息并发特征与其他单元差一个数量级 |
| `iot-core` | `svc-device` / `svc-auth` / `svc-quota` / `svc-audit` / `api-gateway` / `svc-openapi` | 控制面，QPS 型，可水平复制 | 一二期合并，**三期按负载再拆**（代码边界已留好） |
| `iot-pipeline` | `svc-pipeline` / `svc-rule` | 同为分区消费，共享分区键与背压水位 | `svc-rule` 是 CPU 型；成为热点时按「先补 Go 原生动作、再评估 Rust」下沉（ADR-016） |
| `iot-alarm` | `svc-alarm` / `svc-automation` / `svc-notify` | 同为有状态 / 定时型，共享 leader 选举与分片 | 定时扫描与分区消费的负载特征不同 |
| `iot-ota` | `svc-ota` | 独立生命周期与流量模式 | — |
| `odoo-connector` | — | **必须独立** | 它是保护 Odoo 的限流边界，不可并入 gateway |

**原则**：**按负载特征拆，不按业务模型拆。** 代码层始终保持模块边界，为后续按实测瓶颈拆分留口。

**说明**：`iot-core` 内部的模块在代码上必须是独立 package，禁止跨模块直接访问对方的数据库表。

---

## 4. 技术选型

| 层次 | 选型 | 版本策略 | 理由 |
|---|---|---|---|
| 语言 | Go | 跟随最新稳定大版本 | 高并发、部署简单 |
| Web 框架 | Gin（对外 API）+ gRPC（内部） | | 生态成熟 / 强类型契约 |
| MQTT 库 | `mochi-mqtt/server` 派生 | 锁定 commit + fork 维护 | 可嵌入、Hook 体系完善、MQTT v5 完整 |
| 消息总线 | NATS Server + JetStream | 2.x | 单二进制、Raft、支持 KV/ObjectStore |
| 关系库 | PostgreSQL | 16+ | JSONB、RLS、分区、PITR |
| 时序库 | GreptimeDB | 最新稳定版 | **Apache-2.0**，无许可证传染风险；云原生、写入性能与压缩比满足 IoT；SQL + PromQL 双查询 |
| 缓存 | Redis | 7.x Cluster | 会话、最新值、限流、分布式锁 |
| 对象存储 | MinIO（私有化）/ S3（云） | | S3 兼容，可无缝切换 |
| 密钥管理 | HashiCorp Vault / 云 KMS | | 动态凭据、密钥轮换 |
| 规则条件 DSL | `expr-lang/expr`（MIT） | | 类型安全、编译期可静态分析、**无沙箱逃逸面**，覆盖 90% 规则场景 |
| 规则编排 | 自研 DAG 执行器 | | 编排语义简单（条件→动作链），自研可控；不引入重量级流处理引擎 |
| 规则逃生舱 | `goja`（受限，P2 默认关闭） | | 仅用于 `expr` 无法表达的复杂转换；强制超时/内存/指令数限制，无 IO 与网络 |
| ORM | `pgx` + `sqlc`（推荐）或 GORM | | 生成式、类型安全、可审计 SQL |
| 权限 | Casbin | | RBAC/ABAC 模型可配置 |
| 配置 | Viper + ConfigMap | | 分层覆盖 |
| 日志 | `zap` | | 结构化、高性能 |
| 追踪 | OpenTelemetry SDK | | 厂商中立 |
| 指标 | Prometheus client_golang | | |
| 指标/日志后端 | VictoriaMetrics + Loki + Grafana + Tempo | | 比 ELK 更省资源 |
| 前端 | Vue 3 + TypeScript + Vite | | 与生态一致 |
| 可视化 | 自研 + ECharts（可外采 DataRoom/GoView） | | 后期投入 |
| 容器编排 | Kubernetes + Helm + ArgoCD | | GitOps |
| 边缘 | K3s + 自研 edge-agent | | Phase 3 |
| CI/CD | GitHub Actions / GitLab CI + Trivy + cosign | | 含安全扫描与签名 |

### 4.1 依赖许可基线与接入约束

选型不仅看技术，还要看许可证能否支撑商业化。**基线规则（ADR-010）**：

| 规则 | 内容 |
|---|---|
| **AGPL/SSPL 组件需逐项法务评估，且必须满足四个条件** | ① **独立服务**形态（不同进程、不同容器）② **不修改其源码** ③ **不链接**（无 cgo/静态链接）④ **不与商业代码同进程**。四项缺一不可 |
| **一律通过网络协议接入外部组件** | 禁止 cgo/静态链接其客户端库。**这降低的是「链接型衍生」的争议，不等于完全规避** —— SaaS 提供、修改源码、私有化分发三种情形仍需逐项法务评估 |
| **每个依赖记录 `LICENSE` 快照到仓库** | CI 校验；许可证变更即阻断构建并触发评审 |
| **新引入中间件需走 RFC** | 连同许可证、运维成本、可替换性一并评估 |

> ⚠️ **口径修正（评审 R-21）**：早期写「**不引入** AGPL/SSPL 类依赖作为服务组件」，但同表下文就列了 MinIO（AGPL）—— 自相矛盾。且「网络协议访问即可规避衍生风险」的表述过于绝对。现按上表修正：**不是绝对禁止，而是必须满足四个条件并经法务评估**。

| 依赖 | 许可证 | 判定 | 约束 |
|---|---|---|---|
| GreptimeDB | Apache-2.0 | ✅ 直接可用 | 通过 gRPC/HTTP 接入 |
| PostgreSQL | PostgreSQL License | ✅ | — |
| NATS | Apache-2.0 | ✅ | — |
| Redis | 7.x 起 BSD-3 / 后续版本许可有变 | ✅ 需锁定版本并核对 | 锁定版本，升级前复核许可 |
| **MinIO** | **AGPL-3.0** | ⚠️ **需法务评估** | 满足四条件：独立服务 / 不修改源码 / 不链接 / 不同进程。**私有化分发时须随附源码** |
| mochi-mqtt | MIT | ✅ | fork 维护需保留版权声明 |
| `expr-lang/expr` | MIT | ✅ | — |
| `goja` | MIT | ✅ | 仅作逃生舱，默认关闭 |
| TDengine（若切换） | AGPL-3.0 | ⚠️ 需法务评估 | **必须走 taosAdapter REST/WS，禁止 cgo native 连接器**；修改源码则必然触发 AGPL §13 |
| TimescaleDB（若切换） | Apache-2.0 + Timescale License | ⚠️ | 社区版核心压缩 / 连续聚合功能受限，需实测确认够用 |

### 4.2 组件分层：一期必需 vs 规模化（评审 R-23）

早期约束写「中间件组件数量需可控」，但选型表列了 9 类组件 —— 两者张力明显。现按**交付阶段分层**，让「一期」保持精简：

| 组件 | 一期（最小可用） | 规模化（何时引入） | 引入触发条件 |
|---|---|---|---|
| PostgreSQL（**单实例 + 每日备份**） | ✅ 必需 | → Patroni + etcd 自动主从切换 | 可用性要求 > 99.9%，或客户明确要求多 AZ |
| NATS（单集群 3 节点） | ✅ 必需 | → 5 节点 + 跨 AZ | 吞吐 > 100 万 msg/s |
| Redis（单实例或 1 主 1 从） | ✅ 必需 | → Cluster 3 主 3 从 | 内存 > 32GB 或需故障自动切换 |
| GreptimeDB（单节点） | ✅ 必需 | → 3 副本集群 | 写入 > 20 万 points/s 或需高可用 |
| **MinIO** | ✅ 必需（可先用本地磁盘 + 定期归档） | → 4+ 节点纠删码 | 归档量 > 1 TB 或需多副本 |
| **Vault** | ❌ **可延后**（先用 K8s Secret + 加密 ConfigMap） | → Vault / 云 KMS | 有合规要求，或密钥轮换需自动化 |
| **完整可观测栈**（Tempo + Loki + VictoriaMetrics） | ⚠️ **精简起步**（Prometheus + 日志文件 + Grafana） | → 完整栈 | 服务数 > 6 或需要跨服务链路追踪 |
| **Kubernetes** | ⚠️ **见下**（Lite 交付可不用 K8s） | → K8s + ArgoCD | 见 06 文档的交付分层 |

> **原则**：**一期的目标组件数是 5 个**（PG / NATS / Redis / GreptimeDB / MinIO），Vault 与完整可观测栈推迟到规模化阶段。每个新增组件都必须能说出「不加会怎样」。

---

## 5. 部署拓扑

### 5.1 集群拓扑（多可用区）

```
Region
├── AZ-A                          ├── AZ-B                          ├── AZ-C
│   ├── node-pool: gateway        │   ├── node-pool: gateway        │   ├── node-pool: gateway
│   │   (hostNetwork, 大连接调优)  │   │                             │   │
│   ├── node-pool: app            │   ├── node-pool: app            │   ├── node-pool: app
│   │   (svc-*, api-gateway)      │   │                             │   │
│   └── node-pool: data           │   └── node-pool: data           │   └── node-pool: data
│       (PG standby / Redis /     │       (PG primary / Redis /     │     (GreptimeDB / NATS /
│      GreptimeDB / NATS)         │        NATS / MinIO)            │        MinIO)
├── 外部 LB（L4，跨 AZ，least_conn）
├── 对象存储跨区复制
└── 跨 Region 异步复制（主备）
```

**要点**：

- 网关使用**独立节点池 + hostNetwork 或 IPVS 直通**，规避 kube-proxy conntrack 表溢出。
- 数据节点池使用本地 NVMe SSD + 独立存储类；PG 主库与仲裁节点跨 AZ 分布。
- 网关与数据面的网络策略：仅允许必要端口，默认拒绝。
- 节点池打亲和/反亲和：同一服务的 Pod 必须跨 AZ 分散，`topologySpreadConstraints` 强制。

### 5.2 环境划分

| 环境 | 用途 | 数据 | 规模 |
|---|---|---|---|
| `dev` | 开发自测 | 合成数据 | 单副本，单机 K3s |
| `staging` | 集成测试 / 性能基线 | 脱敏仿真数据 | 生产 1/10 规模 |
| `prod` | 生产 | 真实数据 | 全量 |
| `sandbox` | 客户/生态验证 | 隔离 | 小规模共享 |
| `edge` | 边缘节点 | 本地缓存 | 按站点 |

环境间**禁止共用中间件实例**；配置通过 Kustomize overlay 分层，禁止在代码里写环境判断。

### 5.3 交付分层（ADR-007，评审 R-22）

早期只写「Kubernetes + Helm + ArgoCD」，对**中小私有化客户过重**（客户没有 K8s 运维能力，也不会为一套 IoT 平台建集群）。改为三层交付：

| 层 | 目标客户 | 编排 | 组件形态 | 高可用 | 何时用 |
|---|---|---|---|---|---|
| **Lite** | 中小私有化、单站点、≤ 5 万设备 | **Docker Compose 或单机 K3s** | 所有组件单实例（PG/NATS/Redis/GreptimeDB/MinIO 各 1） | ❌ 无（接受停机窗口，靠备份恢复） | **Phase 1 先用它跑通**；也用于客户 POC |
| **Standard** | 中型客户 / 公有云 | **K8s + Helm + ArgoCD** | 多副本无状态 + 中间件主从 | ✅ 单 AZ 内高可用 | 一期生产默认 |
| **Large** | 大型客户 / 多站点 | K8s 多节点池 + 多 AZ | 多可用区副本 + 灾备 | ✅ 跨 AZ，部分跨 Region | 有明确 RTO/RPO 要求时 |

**Lite 层的取舍（必须说清楚）**：

| 项 | Lite 的做法 | 明确放弃的 |
|---|---|---|
| 数据可靠性 | 每日全量备份 + WAL 归档到外部存储；**恢复 RTO ≤ 4h** | 不做自动故障切换 |
| 网关 | 单实例（可横向加到 2 个，但不做集群路由） | 不做跨节点消息路由（用共享 Redis 会话 + 单节点即可） |
| 可观测 | Prometheus + Grafana + 日志文件轮转 | 不做分布式追踪 |
| 升级 | 停机升级（窗口内完成） | 不做滚动更新与灰度 |
| **代码差异** | **零** —— 同一份代码，通过配置关闭集群特性（Lite 下单实例消费全部分片，见 03 §4.2.1） | — |

> **关键约束**：**Lite 与 Standard 必须是同一份代码 + 配置差异**，不允许分叉出「精简版」。集群特性（跨节点路由、分片消费）在单实例下必须能优雅退化为本地路径。

**组件分层**见 §4.2（一期必需 5 个组件）；`Vault` 与完整可观测栈属于规模化组件，Lite 层用 K8s Secret / 加密 ConfigMap + 精简监控替代。

---

## 6. 关键数据流

### 6.1 遥测入站（热路径）

```
Device
  │ 1. MQTTS CONNECT（B档默认：clientID = username = device_key，password = secret）TLS1.3
  ▼
gw-mqtt
  │ 2. 本地 LRU 校验凭据 → 未命中调 svc-auth（5s 超时 + 熔断）
  │ 3. ACL 校验（topic 是否属于该设备）
  │ 4. 令牌桶限流（本地二级桶 + Redis 批量补充配额）
  │ 5. 消息大小 / 格式预检，非法则丢弃并计数
  ▼
NATS  publish  iot.telemetry.{project}.shard.{fnv1a(device_key) % N}
  │ 6. 分片键 = device_id，保序（分片方案见 03 文档 §4.2）
  ▼
svc-pipeline（每分片一个 durable consumer，与实例静态绑定）
  │ 7. 幂等两阶段：done 命中→跳过；否则 SETNX processing
  │ 8. 查找物模型（本地缓存 + 版本号失效）
  │ 9. 原始报文解析（expr 纯函数，白名单见 04 §1.2）
  │ 10. 校验 / 单位换算 / 时间戳校正（偏差>5min 打标）
  ├─► GreptimeDB 批量写入（攒批 200ms 或 1000 条，任一先到）
  │      └─ **等待 flush 确认**，失败行入重试流
  ├─► Redis 更新最新值 + last_seen（可异步，失败不影响 ACK）
  └─► NATS publish iot.telemetry.normalized.{project}.shard.{i}
  │
  │ 11. **持久化确认之后**：写 done 标记 → 删 processing → NATS ACK
  ▼
svc-rule（expr 条件匹配 + 自研 DAG 编排）──► svc-alarm / svc-automation ──► 指令下发 / 通知
```

> **ACK 语义**：NATS ACK 发生在**持久化确认之后**，不是入队之后。这是 at-least-once 成立的前提（详见 03 文档 §4.3 的缺陷分析与两阶段标记）。
> `MaxAckPending` 需 ≥ `2 × batchSize`（默认 2000），以容纳「一个在途批次 + 一个待发批次」。

**延迟 SLI 拆分为三档（修正早期的口径打架）**：

早期把「消息到达网关 → 落库确认」写成一个 P99 < 100ms 的指标，但同一文档里 GreptimeDB 的攒批窗口是 200ms —— **物理上不可能**（R-06）。现拆为三个独立 SLI：

| SLI | 定义 | 目标 | 落点 |
|---|---|---|---|
| **接入确认延迟** | 网关收包 → 发布到 NATS | **P99 < 100 ms** | 本节 |
| 规则触发延迟 | 消息持久化 → 规则动作开始 | P99 < 200 ms | 04 文档 |
| **持久化延迟** | 消息持久化 → 可被查询 | **P99 < 500 ms** | 06 文档 §3.4 |

**延迟预算（接入确认口径，P99 < 100 ms）**：

| 环节 | 预算 |
|---|---|
| TLS 已建连情况下消息收包 | 1 ms |
| 认证缓存命中 + ACL | 2 ms |
| NATS 发布 + 投递 | 3 ms |
| svc-pipeline 接收并入批（不含 flush 等待） | 5 ms |
| 规则匹配（无重规则） | 5 ms |
| 余量 | 84 ms |
| **合计** | **≤ 100 ms** |

> **注意**：上表**不含 GreptimeDB 攒批等待**（那属于「持久化延迟」SLI）。ACK 虽在 flush 之后，但**并发吞吐由分片数与批次并发保障**（单分片 1000 条 / 200ms = 5000 msg/s），ACK 延迟不等于吞吐上限。

### 6.2 指令下发（跨节点路由）

```
调用方(svc-automation / odoo-connector / 场景)
  │ 1. 生成 idem_key，写 t_idem_registry(scope='command') + t_command(status=pending)
  │ 2. 通过 `cluster.Node.Route()` 查询 Redis `gw:client:{device_key}` → nodeID（会话定位）
  ▼
NATS  publish  iot.route.{nodeID}        ← 直接发路由 subject，不发设备级 subject
  │ 3. 目标 gw-mqtt 节点在本地连接表中定位连接并投递
  ▼
gw-mqtt → Device    topic: v1/devices/{key}/cmd/{cmdKey}
  │ 4. 等待设备回复 v1/devices/{key}/cmd/reply
  ▼
gw-mqtt 分流回复 → NATS publish iot.cmd.reply.{project}.{device_token}
  │ 5. 原调用方按 correlation_id 收结果，更新 t_command(status=acked)
  │ 6. 超时未回复 → 指数退避重试（默认 3 次）→ status=timeout → 记录并告警
```

> **为什么用 `iot.route.{nodeID}` 而不是设备级 subject（评审 R-13）**：若发布到 `iot.cmd.{project}.{deviceID}`，则每个 `gw-mqtt` 节点都要维护「本节点上所有设备的 subject 订阅集合」，订阅表随设备数线性膨胀（100 万设备 = 100 万订阅）。既然步骤 2 已经通过 Redis 拿到了 `nodeID`，就直接投递到该节点的路由 subject，**订阅表变成 O(节点数)**。

其中 `device_token` 是设备键的 Raw URL-safe Base64 编码，用于避免设备键中的点号破坏 NATS subject 层级；消费者收到后还原为原始 `device_key`。

**离线设备**：`gw:client:{device_key}` 无节点 → 不投递，只写 `t_command(status=queued)`，并写 NATS **分片离线流** `offline.shard.{hash(device_id)%N}`（TTL = 物模型 `offlineTTL`，默认 24h）；设备重连后由 `svc-device` 触发补发。

### 6.3 设备上下线（在线状态）

| 路径 | 触发 | 处理 |
|---|---|---|
| **异常断线** | Broker 发布 LWT | `gw-mqtt` Hook 捕获 LWT → 发布内部 `iot.device.offline` |
| **正常下线** | 设备发 DISCONNECT | `gw-mqtt` 会话回调 → 直接发布 `iot.device.offline`。**不能依赖 LWT**（LWT 只在非正常断线时触发） |
| **兜底** | 双路径都失效 | `svc-device` 每 30s 扫 Redis 中 TTL 过期但标记仍在线的设备 |

- 设备 CONNECT 成功 → 网关会话 Hook 发布 `iot.device.online`；正常与异常断开统一由 `OnDisconnect` 发布 `iot.device.offline`。事件进入 JetStream `IOT_DEVICE_EVENTS`，字段为 `event_id/event/device_key/occurred_at`。当前 Redis 在线状态仍由 A3 的 `gw:client:{device_key}` 位置键表达；不再使用旧的 `cache:*:online:v1:*` 键。
- `svc-device` 消费 `iot.device.*` 后更新 PG `last_seen_at`（**仅作审计与历史摘要**）。
- **实时在线判定以会话注册表（Redis `gw:client:{device_key}` / Broker 会话状态）为事实**，不用 PG 判定在线（评审 R-14）。LWT 的三个语义边界详见 03 文档 §5.2。

---

## 7. 服务间通信契约

### 7.1 NATS Subject 命名规范

```
iot.{domain}.{project_id}.{scope}

domain:  telemetry | telemetry.normalized | attr | event | cmd.reply | alarm
         | device | route | odoo | offline | dlq
scope:   shard.{0..N-1} | device_id | node_id | *
```

| Subject | 发布者 | 订阅者 | 顺序性要求 | 持久化 |
|---|---|---|---|---|
| `iot.telemetry.{project}.shard.{i}` | gw-* | svc-pipeline（分片级 durable） | **按 device 保序**（见 03 §4.2） | JetStream（24h） |
| `iot.telemetry.normalized.{project}.shard.{i}` | svc-pipeline | svc-rule, svc-quota | **按 device 保序** | JetStream |
| `iot.attr.{project}` | gw-* / api | svc-device | 弱 | 否 |
| `iot.event.{project}.shard.{i}` | gw-* / svc-* | svc-pipeline, svc-rule | **按 device 保序** | JetStream |
| `iot.cmd.reply.{project}.{device_token}` | gw-mqtt | 调用方 | 否 | JetStream（7d） |
| `iot.alarm.{project}` | svc-alarm | svc-notify, api-sub, odoo-connector | 否 | JetStream |
| `iot.device.online / offline` | gw-mqtt | svc-device, odoo-connector | 否 | 否 |
| `iot.route.{node_id}` | gw-mqtt / svc-* / odoo-connector | gw-mqtt（该节点） | 按 device 保序 | 否 |
| `iot.offline.shard.{i}.{device_id}` | svc-device / 调用方 | gw-mqtt | 按 device | JetStream（TTL 24h） |
| `iot.odoo.{project}.{event_type}` | **odoo-connector**（由 Redis Streams 翻译而来） | svc-device / svc-alarm | 否 | JetStream |
| `iot.dlq.{service}` | 各服务 | 人工 / 补偿任务 | 否 | JetStream |

**约定**：

- Subject 必须携带 `project_id`，杜绝跨租户串流。
- 所有消息体统一信封格式，见 §7.2。
- **保序域必须走分片 subject**（`shard.{i}`）；分片数与归属规则见 03 文档 §4.2。
- **不存在设备级命令 subject**：命令一律经 §6.2 的 `iot.route.{node_id}` 定向投递，避免订阅表随设备数膨胀（评审 R-13）。
- **Odoo 侧不直接往 NATS 写**：Odoo 的 `edge_outbox` 只投递到 **Redis Streams**，由 `odoo-connector` 翻译为 `iot.odoo.*` 后进入 NATS（评审 R-09，详见 07 §3.2）。
- `schema_version` 字段用于消息结构演进，消费者必须兼容向前一个小版本。
- 涉及 Odoo 的域（`device` / `alarm` / `cmd` / `odoo`）**必须携带 `company`**，由服务端映射得出（见 §7.2）。

### 7.2 统一消息信封

对齐《Odoo 20 高性能架构技术方案》手册 p.42 的「失效事件最小字段」，信封扩展为：

```json
{
  "msg_id": "01J8Z...",                    // 事件唯一标识，用于去重
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",  // W3C trace-id（32 位十六进制）
  "tenant": "proj_10231",                  // IoT 租户（project_id），由服务端确定
  "company": 7,                            // Odoo company_id，由服务端映射；设备不可指定
  "source": "gw-mqtt-3",
  "occurred_at": "2026-10-01T08:12:33.421Z",   // 业务发生时间（非接收时间）
  "ingested_at": "2026-10-01T08:12:33.512Z",   // 平台接收时间
  "entity": "device",                      // device | equipment | lot | workorder | alarm
  "entity_id": 100234,
  "fields": ["temperature", "humidity"],   // 受影响字段（变更类事件的必填项）
  "schema_version": "v1",                  // 信封结构版本
  "version": 12,                           // 实体版本号，拒绝旧事件覆盖新状态
  "payload": { }
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `msg_id` | ✅ | 消费者按此去重（至少一次投递） |
| `trace_id` | ✅ | **W3C `trace-id`（32 位十六进制）**，不是完整的 `traceparent`。缺失时由入口生成，**禁止中间件重新生成** |
| `tenant` | ✅ | `project_id`，由服务端映射，**不接受客户端传入** |
| `company` | 跨 Odoo 域必填 | `odoo_company_id`；由 `device_key → project_id → company` 映射链得出 |
| `occurred_at` | ✅ | **业务发生时间**，非接收时间（乱序判定与对账依赖它） |
| `entity` / `entity_id` | ✅ | 变更对象，用于缓存精确失效 |
| `fields` | 变更类必填 | 受影响字段列表，避免全量失效 |
| `version` | 变更类必填 | 实体版本号；**旧版本到达时丢弃，不按到达顺序盲目覆盖**（手册 p.12「乱序消息」） |

> **与 Odoo 侧信封的一致性**：Odoo `edge_outbox` 写出的事件字段（`event_id` / `tenant_id` / `company_id` / `aggregate_id` / `version` / `occurred_at`）与本信封**一一对应**，由 `odoo-connector` 做字段名转换（下划线 ↔ 驼峰），不改变语义。

### 7.3 同步接口

- 内部服务间**:gRPC**（Protobuf，携带 `trace_id` 与 `tenant` metadata）。
- 超时统一：查询 200ms，写入 1s，网关鉴权 5s（带熔断）。
- 所有同步调用**必须**配置超时 + 重试（最多 1 次，仅幂等接口）+ 熔断（`gobreaker`）。

---

## 8. 租户隔离与配额

| 隔离维度 | 默认 | 升级选项 |
|---|---|---|
| 数据库 | 共享库 + `project_id` + PG Row Level Security | 独立 schema → 独立库 |
| 时序库 | GreptimeDB 按 `project_id` 标签过滤（查询构建器强制注入） | 独立 database / 独立集群 |
| 缓存 | Redis key 前缀 `p:{project_id}:` | 独立 Redis DB / 实例 |
| 消息总线 | Subject 携带 `project_id` + 每租户独立流 | 独立 NATS Account（强隔离） |
| 计算 | 每租户并发配额 + 令牌桶 | 独立服务实例 / 独立命名空间 |
| 存储 | 对象存储前缀隔离 + 桶策略 | 独立桶 / 独立账号 |

**配额维度**（由 `svc-quota` 统一管理，Redis 计数 + PG 对账）：

- 设备数、消息数/日、存储量、规则数、场景数、API QPS、并发连接数、OTA 任务数。
- 超配额策略：`telemetry` 降速→丢弃（按租户配置），`cmd` 拒绝并返回明确错误码，控制面写操作拒绝。

---

## 9. 一期与二期的边界

| 项目 | 一期（MVP+生产可用） | 二期 |
|---|---|---|
| 协议 | MQTT / HTTP | CoAP / TCP-DTU / Modbus 网关 |
| 集群 | 单 Region，多 AZ | 跨 Region 主备 |
| 网关 | 内嵌 mochi-mqtt，多副本 + NATS 路由 | 会话迁移优化、边缘下沉 |
| 规则 | `expr` 条件 + 自研 DAG + 预处理/上报规则 | 受限 JS 逃生舱、独立进程沙箱、WASM |
| 告警 | FSM + Webhook + 短信/邮件 | 语音、升级策略、告警风暴抑制 |
| 影子 | 基础 desired/reported/delta | 离线可达、版本冲突策略 |
| 应用层 | 控制台 + 基础看板 | 零代码 App / 大屏 / 组态 |
