# 08 · 与《Odoo 20 高性能架构技术方案》的对齐分析

> 输入文档：
> - `docs/Odoo 20高性能⾏业数字化架构⽅案⼿册.pdf`（50 页，客户版）
> - `docs/Odoo 20高性能架构技术方案.pdf`（25 页，工程版）
> - 代码核验基准：Odoo Community 20.0 / commit `17ff827a1`（2026-09-26）
>
> 本文回答一个问题：**本项目与这套「Go + Redis + Rust + Odoo」方案是什么关系，冲突在哪，怎么收口。**

> **版本：rev.2（2026-10-01）**
> rev.1 → rev.2 的修订（评审「开工前必须关闭的清单」P1）：
> - §0 结论 3：模块名对齐为 `sn_edge_integration`（rev.1 仍写 `sn_iot_bridge`）
> - §2.1 对照表：缓存键更新为统一命名法 `cache:{tenant}:{company}:…`；「Redis 幂等」表述修正为「幂等占位（非账本）」
> - §9 待确认项：标注 5 项已采纳、1 项转执行
>
> **注**：§3 各冲突条目中的旧名称与旧键名（`sn_iot_bridge`、`p:{pid}:…`、`15 个服务`）是**问题描述的一部分**（记录"改之前是什么"），不是当前设计，请勿据此实施。

---

## 0. 结论摘要

| # | 结论 |
|---|---|
| **1** | 本项目**不是**这套方案的第五套系统，而是它在「设备厂商与工业物联网」垂类的**Go 层 + 时序层的实例化**。技术方案 p.35 已经为本项目预留了位置 |
| **2** | 两套 Go 服务**并存且职责正交**：`odoo-gateway`（面向人）与本项目（面向设备）。收敛规则：**人的流量只有一个入口，设备的流量只有一个入口，两者在 `odoo-connector` 处单向交汇** |
| **3** | 桥接模块**必须合并**：本文档 07 原规划的 `sn_iot_bridge` 并入 PDF 规划的 `edge_integration`，**最终定名 `sn_edge_integration`**（ADR-014），否则会出现两套幂等表、两套 outbox、两套审计 |
| **4** | 幂等模型**升级为 PDF 的五态状态机 + Odoo 唯一约束**；Redis 只做加速，不做账本 |
| **5** | **Rust 一期不引入**。PDF 自己规定「Rust 必须等 profiler / 基准 / 批任务数据证明其必要性」，本项目连 profiler 数据都还没有 |
| **6** | 服务数量从 **15 个收敛到 6 个可部署单元**（代码保持模块边界）。这是本项目 01 文档需要修正的地方 |
| **7** | PDF 的 Odoo 生产配置**直接解决**本文档 07 遗留的 C1/C2 待确认项，并暴露出当前 `odoo20tbb` 的 4 项安全配置问题 |

---

## 1. 两份 PDF 的关系与核心命题

| 文档 | 定位 | 读者 | 页数 |
|---|---|---|---|
| 行业数字化架构方案手册 | 业务价值与场景选型 | 老板/CIO/业务负责人 | 50 |
| 高性能架构技术方案 | 工程实现与接口契约 | 架构师/开发 | 25 |

**共同命题（一句话）**：不重写 Odoo，把「等待、重复读取、重计算」从 Odoo 单次请求中剥离出去；**Odoo 保持业务规则与最终写入的唯一真相源**。

四层分工（技术方案 p.2）：

| 层 | 做什么 | **明确不做** |
|---|---|---|
| Go 网关 | 公网连接、认证、限流、缓存编排、聚合、超时、熔断 | 价格、审批、税务、库存预留等业务裁决 |
| Redis | 可重建缓存、幂等状态、短期锁、Streams 队列 | 账务事实、长期订单主数据、权限事实 |
| Rust 计算 | CPU 密集且输入输出明确的批量计算 | 独立提交业务单据或绕过 Odoo ORM |
| Odoo 20 | ACL、记录规则、工作流、ORM 事务与 PG 真相 | 直接承受全部公网读流量和无界计算 |

三条不可越过的红线（手册 p.6）：**一条写入通道 / 一套权限事实 / 一个结果版本**。

---

## 2. 定位对齐：本项目在 PDF 架构中的位置

### 2.1 PDF 已为本项目预留了位置

技术方案 p.35「设备厂商与工业物联网」逐条写的就是本项目要做的事：

| PDF 为该场景设计的职责 | 本项目的对应实现 | 差异 |
|---|---|---|
| Go：云端设备注册与连接管理 | `gw-mqtt` 集群 + `svc-device` | ✅ 一致 |
| Go：命令分发、状态订阅和租户配额 | `svc-device` 命令通道 + `svc-quota` | ✅ 一致 |
| Go：OTA 任务编排与回执 | `svc-ota` | ✅ 一致 |
| Go：**设备到 Odoo 业务事件转换** | `odoo-connector`（本文档 07） | ✅ 一致，本项目补全了 PDF 未展开的部分 |
| Redis：缓存设备最新状态、配置版本与在线摘要 | `cache:{tenant}:{company}:device:last:v2:{did}` / `device:online` / `devicetype:tm` （已按 08 §C8 统一命名法） | ✅ 一致 |
| Redis：保存命令幂等和短任务状态 | **幂等占位**（Redis `idemp:` 仅加速，**账本按 `scope` 划分**：IoT 内部操作（命令 / 事件 / 告警）→ PG `t_idem_registry`；IoT→Odoo 写回 → Odoo `edge_idempotency`）+ **NATS 分片离线流** | ⚠️ **差异 C2**（队列选型）；幂等语义已对齐（账本归属见 07 §5.2.1） |
| **Rust：边缘协议解析、压缩与加密** | **未设计**（Go 侧解析） | ❌ **缺口 C3** |
| **Rust：本地数据清洗和异常检测** | **未设计**（`expr` + DAG） | ❌ **缺口 C3** |
| **Rust：断网批量聚合、资源受限运行时** | P2 规划 `edge-agent`（Go） | ❌ **缺口 C3** |
| Odoo：设备资产、客户、合同、备件、工单、服务历史 | `maintenance.equipment` + `maintenance.request` | ✅ 一致 |
| **「原始遥测留在时序平台，关键事件转为 Odoo 工单或业务记录」** | GreptimeDB + 告警→工单 | ✅ **完全一致**（这正是 07 文档的核心设计） |

p.28「能源与公用事业」同源表述：**「原始遥测进入时序数据库，对 Odoo 只提交摘要和业务事件」**。

### 2.2 结论

> **本项目 = PDF 架构在 IoT 垂类的实例化，不是并列的第五套系统。**

因此，两者的验收标准应当同源：PDF p.35 给的 IoT 垂类验收方向（连接在线与重连成功率、命令下发到回执时长、边缘 CPU/内存/掉电恢复、**有效告警到工单比例**）应当**直接成为本项目的验收项**，而 07 文档原来只定义了「告警→工单 P95 < 60s」，缺了「有效告警占比」这个业务口径。

---

## 3. 八项冲突与裁定

> **本节裁定已全部采纳并落地。** 各条的落地位置如下：
>
> | 冲突 | 裁定 | 落地位置 |
> |---|---|---|
> | C1 两个 Go 边界 | 人的入口 / 设备入口分离 | ADR-013、01 §3.1 |
> | C2 两套队列 | 并存，connector 为唯一翻译层 | 01 §3（服务清单）、01 §7.1 |
> | C3 Rust 位置 | 一期不引入 | ADR-016、06 Phase 3 |
> | C4 推送机制 | 高价值 Outbox / 低价值 webhook + 对账 | 07 §3.2、07 §4.4 |
> | C5 模块命名 | 合并为 `sn_edge_integration` | ADR-014、07 §7 |
> | C6 幂等模型 | 五态状态机 + Odoo 唯一约束 | ADR-015、07 §4.3.1 |
> | C7 服务数量 | 收敛到 6 个可部署单元 | 01 §3.1 |
> | C8 缓存键与事件字段 | 统一到 PDF 命名法 | 02 §5.1、01 §7.2 |

### C1 · 两个 Go 边界：`odoo-gateway` vs IoT 平台

**冲突**：PDF 的 Go 网关（技术方案 p.4）客户端列表里包含 **IoT**；本项目的 `gw-mqtt` 也是 Go 网关。会不会出现两个入口互相打架？

**裁定：人的流量只有一个入口，设备的流量只有一个入口。**

```
人（小程序 / App / POS / 合作方 / 运维控制台）
   │
   └──► odoo-gateway（PDF 的 Go 层）
            ├──► Odoo 20（业务读写）
            └──► IoT 平台 /internal/v1/*（需要设备摘要时中转）
                       ▲
                       │ 仅内部调用，mTLS + 服务令牌
设备（MQTT / HTTP / CoAP / DTU）
   │
   └──► IoT 平台（本项目）
            └──► odoo-connector ──► Odoo Facade 方法
```

| 规则 | 内容 |
|---|---|
| **禁止** | 设备流量（MQTT/长连接/二进制协议）经过 `odoo-gateway` |
| **禁止** | IoT 平台自建一套面向终端用户的鉴权/限流/缓存边缘 |
| **禁止** | `odoo-gateway` 直连 MQTT broker |
| **允许** | 用户查询设备状态 → 经 `odoo-gateway` 中转调 IoT 内部 API（复用同一套用户与租户身份） |
| **允许（例外）** | IoT 运维控制台（报文调试、设备详情）直连 IoT API，但**用户身份必须经 Odoo OIDC 联合**，不得有独立账号体系 |

**理由**：两个入口的流量特征差距达到两个数量级（PDF 网关是交互式 HTTP、千级 RPS；IoT 网关是百万长连接 + 50 万 msg/s）。合并会导致任一方的扩缩容决策被另一方绑架。

### C2 · 两套队列：Redis Streams vs NATS JetStream

**冲突**：PDF 选 **Redis Streams**（技术方案 p.12，理由是"不引入新组件"）；本项目选 **NATS JetStream**（本文档 ADR-006）。

**裁定：不强行统一，两者定位不同。**

| 维度 | Redis Streams（PDF） | NATS JetStream（本项目） |
|---|---|---|
| 定位 | **Odoo 出站 outbox 的投递载体** | **IoT 数据面总线** |
| 生产方 | Odoo ORM 事务内写 outbox 行 → cron 投递 | 网关 Hook 内直接发布 |
| 吞吐量级 | 低（业务事件，10²~10³ /s） | 高（峰值 5×10⁵ msg/s） |
| 关键需求 | 与 Odoo 的 cron/worker 集成、事务内原子写 | 扇出、按 `device_id` 分区保序、多消费组、请求-响应 |
| 选择理由 | Odoo 已依赖 Redis，不引入 Erlang 生态新组件 | Go 原生、单二进制、支持 KV/ObjectStore |
| 结论 | **保留** | **保留** |

**桥接规则（必须遵守）**：

1. `odoo-connector` 是**唯一的翻译层**，同时消费 Redis Streams（Odoo 侧）与 NATS（IoT 侧）。
2. **禁止双向互写**：不允许 IoT 平台直接往 Odoo 的 Stream 写，也不允许 Odoo 侧直接往 NATS 写。所有跨系统事件必须经 connector 单向流转，避免环形放大。
3. 两套队列的**故障域独立**：Redis Streams 积压不得影响 IoT 设备接入；NATS 积压不得阻塞 Odoo 出站。

### C3 · Rust 的位置：云端还是边缘？

**冲突**：PDF p.35 明确写「**Rust 在边缘守门**」（边缘协议解析、压缩加密、本地清洗、断网聚合）；本项目 01 文档 P2 规划的 `edge-agent` 是 **Go**。

**裁定：一期不引入 Rust，边缘先用 Go；把 Rust 留给"已被 profiler 证明的 CPU 热点"。**

| 依据 | 出处 |
|---|---|
| 「顺序很重要：Go 与 Redis 可以先交付可见收益；**Rust 必须等待 profiler、基准测试或批任务数据证明其必要性**」 | 手册 p.37 |
| 「准入门槛：先用 Odoo profiler/SQL 分析确认 CPU 占比；建立 Python 参考实现与黄金样例；Rust 在相同输入下通过性质测试与差分测试后才可上线」 | 技术方案 p.13 |
| 「团队无法承担 Redis、消息积压和跨服务追踪」→ 何时不该加这层 | 技术方案 p.3 |

**本项目当前的 Rust 候选评估（供 Phase 3 决策）**：

| 候选 | 是否 CPU 热点 | 判定 |
|---|---|---|
| 边缘二进制协议解析 | 可能是 | **待 profiler 数据** |
| 边缘压缩/加密（TLS 卸载） | 可能是 | 优先用 Go + 内核/硬件卸载 |
| 时序降采样与大规模规则匹配 | 否（IO/并发型） | 不建议过早上 Rust |
| 报警风暴期的聚合计算 | 否（突发、可抖动） | 用 Go + 队列削峰 |

**若引入，形态选择**：优先**独立 Rust 进程**（技术方案 p.14：「推荐默认。Rust 与 Odoo 解耦部署、独立扩缩容、故障隔离清楚」），**不用 PyO3 内嵌**——本项目连 Python 侧都不涉及，更不该把 Rust 塞进 Odoo worker 的崩溃半径。

**组织级建议**：如果公司已决定投资 Rust 能力，**本项目的边缘计算应共用组织级 Rust 基础设施，而不是自己另起一个 Rust 项目**（避免"服务爆炸"的变体：语言爆炸）。

### C4 · Odoo 推送机制：Outbox vs `ir.actions.server` webhook

**冲突**：PDF（技术方案 p.7、p.12）要求 **Outbox**：业务数据与 outbox 行在同一事务写入，提交后由 cron/worker 投递，**并且明确禁止在 `write()` 中同步 HTTP 调用**。本项目 07 文档用的是 `base.automation` + `ir.actions.server(state='webhook')`。

**核实`ir.actions.server`的实际行为**：`_run_action_webhook`（`odoo/addons/base/models/ir_actions.py:1058-1100`）用 `requests.post` 在 **postcommit 阶段异步**发送。

| 维度 | Outbox（PDF） | `ir.actions.server` webhook |
|---|---|---|
| 是否持有事务锁做 HTTP | ❌ 否 | ❌ 否（postcommit） |
| **持久性** | ✅ 与业务同事务，进程崩溃不丢 | ❌ **进程在 commit 后崩溃则事件丢失** |
| 重试 / DLQ | ✅ 指数退避 + 死信 | ❌ 无 |
| 可重放 / 可对账 | ✅ | ❌ |
| 实现成本 | 高（需建 outbox 表 + cron） | 低（配置化，零代码） |

**裁定：分级使用。**

| 事件类型 | 机制 | 理由 |
|---|---|---|
| **高价值事件**：维护工单创建/状态变更、库存移动、账务影响 | **Outbox**（PDF 的） | 需要持久化、可重放、可对账 |
| **低价值通知**：设备元数据变更、备注、非关键状态 | `base.automation` + webhook | postcommit 异步、零代码；**靠定期全量对账兜底** |

**必须配套（无论用哪种）**：**定时对账任务**（PDF p.12：「定时扫描 Odoo 非终态 job 与 Stream pending，修复遗漏」）。webhook 路径的兜底对账周期不超过 15 min。

→ **07 文档 §6 的 S1 / S3 需要按此修订**。

### C5 · 桥接模块命名与结构

**冲突**：PDF 规划 `edge_integration`（`edge_facade.py` / `edge_job.py` / `edge_idempotency.py` / `edge_outbox.py`）；本文档 07 规划 `sn_iot_bridge`。

**裁定：合并为一个模块，命名为 `sn_edge_integration`。**

- **模块名**采用 SNCIC 的 `sn_` 前缀 + `myaddons/README.md:8` 的 manifest 约定（`20.0.x.y.z` / `author: SNCIC` / `license: LGPL-3`）——因为它要进 `myaddons/` 且团队已有规范。
- **模型名**沿用 PDF 的 `edge_*` 前缀，保证与 PDF 的接口契约文档一致。

```
myaddons/sn_edge_integration/
├── __manifest__.py
├── models/
│   ├── edge_facade.py         # 业务原子方法（PDF）
│   ├── iot_facade.py          # IoT 专属原子方法（本项目新增）
│   ├── edge_job.py            # 计算任务状态机（PDF）
│   ├── edge_idempotency.py    # 幂等结果（PDF）
│   └── edge_outbox.py         # 事务内事件（PDF）
├── controllers/
│   └── iot_api.py             # /api/iot/v1/* 路由（本项目新增）
├── security/                  # ir.access.csv / rules
├── data/ir_cron.xml           # outbox dispatcher / reconciliation
└── tests/
    ├── test_facade.py
    ├── test_iot_api.py
    ├── test_idempotency.py
    └── test_outbox.py
```

**为什么必须合并**：两个模块会产生两套幂等表、两套 outbox、两套审计、两套服务账户映射。这正是 PDF 手册 p.46 列出的反模式「规则被多处复制」的模块级变体。

### C6 · 幂等模型：简化版 vs 五态状态机

**冲突**：07 文档的幂等是「`idem_key = sha256(...)` + 唯一索引兜底」；PDF（技术方案 p.11）要求完整状态机 + 结果存储。

**裁定：采用 PDF 的完整模型。**

```
状态机：ABSENT → PROCESSING → SUCCEEDED / FAILED_RETRYABLE / FAILED_FINAL

1. Go 校验 Idempotency-Key 格式，并计算规范化请求摘要
2. Redis 原子占位，用于快速拦截并发重复；相同键但摘要不同返回 409
3. Go 调用 Odoo Facade，同时传入幂等键与摘要
4. Odoo 在数据库唯一约束下创建/读取幂等记录；业务写与结果记录同事务提交
5. 响应丢失时，重试相同键；Odoo 返回已提交结果，而不是再次执行
```

**唯一约束与存储（PDF p.11）**：

```
UNIQUE(company_id, integration_name, idempotency_key)

stored result:
  - request_hash
  - state
  - model_name / record_id
  - response_json（必要字段）
  - created_at / expires_at
```

**Redis 只加速重复判断，不能成为唯一幂等账本** —— Redis 丢数据后仍应由 Odoo 唯一约束保证不重复创建。

**IoT 场景的补充（对 PDF 的实质性扩展）**：PDF 规定「幂等保留期覆盖客户端、平台和人工可能重试的最长窗口」（p.42）。在 IoT 场景下这个窗口被显著拉长：

| 场景 | 最长重试窗口 | 建议 `expires_at` |
|---|---|---|
| 交互式业务（PDF 原场景） | 分钟~小时 | 7 天 |
| **设备离线重传** | **可达 3~7 天**（物模型 `offline_ttl_ms`） | **30 天** |
| Odoo 侧人工补单 | 数天 | 90 天 |

→ `edge_idempotency.expires_at` 需按 `integration_name` 分档配置，**不能用一个全局值**。

### C7 · 服务数量：15 个 vs 「先保持一个 Go 服务」

**冲突**：PDF 手册 p.24 风险项明确写「**服务爆炸：先保持一个 Go 服务、一个 Rust worker 项目，按负载而非按模型拆分**」；本项目 01 文档列出 **15 个服务**。

**裁定：代码保持模块边界，部署收敛到 6 个可部署单元。**

| 部署单元 | 合并的代码模块 | 为什么可以合并 | 为什么不能并入其他单元 |
|---|---|---|---|
| `iot-gateway` | `gw-mqtt` / `gw-http` / `gw-coap` / `gw-tcp` | 同为接入适配，共享认证/ACL/限流/路由 | 连接数与消息并发特征与其他单元差一个数量级 |
| `iot-core` | `svc-device` / `svc-auth` / `svc-quota` / `svc-audit` / `api-gateway` / `svc-openapi` | 控制面，QPS 型，可水平复制 | 一二期合并，**三期按负载再拆**（代码边界已留好） |
| `iot-pipeline` | `svc-pipeline` / `svc-rule` | 同为分区消费，共享分区键与背压水位 | `svc-rule` 是 CPU 型；**成为热点时按 PDF 的规则下沉（优先 Go 原生动作，其次 Rust）** |
| `iot-alarm` | `svc-alarm` / `svc-automation` / `svc-notify` | 同为有状态/定时型，共享 leader 选举与分片 | 定时扫描与消费型负载特征不同 |
| `iot-ota` | `svc-ota` | 独立生命周期与流量模式 | — |
| `odoo-connector` | — | **必须独立** | 它是保护 Odoo 的限流边界，不可并入 gateway |

**原则**：**按负载特征拆，不按业务模型拆。** 这与 PDF 的「先保持一个 Go 服务」精神一致——IoT 场景的负载特征天然比 Odoo 边缘更分化，6 个单元是合理下限。

### C8 · 缓存键与失效事件字段

**冲突**：PDF（手册 p.42）规定命名法与事件字段；本项目 02/01 文档用的是另一套。

| 项 | PDF | 本项目现状 | 裁定 |
|---|---|---|---|
| 缓存键 | `cache:{tenant}:{company}:{domain}:v{schema}:{key}` | `p:{pid}:d:last:{did}` | **统一到 PDF 命名法**，IoT 侧前缀 `cache:{project}:{company}:{domain}:v{n}:{key}` |
| 幂等键 | `idemp:{tenant}:{operation}:{business_key}` | `{pid}:dedup:...` | **统一** |
| 任务键 | `job:{tenant}:{job_type}:{job_id}` | — | 引入 |
| 锁键 | `lock:{tenant}:{resource}:{resource_id}` | `lock:task:{task_id}` | **统一** |
| 失效事件字段 | `event_id` / `occurred_at` / `tenant/company` / `entity/id` / `fields` / `version` | 现有信封缺 `company` / `fields` / `version` | **补齐** |

**关于 `tenant` 与 `company` 的映射**（PDF 未定义，本项目必须补）：

```
tenant  = project_id（IoT 平台的租户）
company = odoo_company_id（Odoo 的公司）
映射关系：一个 project 可对应 1..N 个 Odoo company（多公司集团场景）
         映射维护在 odoo-connector 的配置中，不由客户端指定
```

PDF 的安全要求（技术方案 p.15）：「**tenant_id 由已验证身份映射，禁止客户端直接决定 allowed_company_ids**」。IoT 侧对应要求：**设备不得通过 Payload 指定 company，company 由 device → project → company 映射链决定**。

---

## 4. 统一约定层（跨系统必须一致）

跨两套 Go 系统与 Odoo，必须逐字节对齐的 6 件事：

| # | 约定 | 规范 | 落点 |
|---|---|---|---|
| 1 | **Trace 上下文** | W3C `traceparent`，贯穿 设备 → IoT 网关 → 管道 → connector → Odoo → outbox → 回调 | 01 文档 §7 信封 + 07 文档 §4.5 |
| 2 | **幂等键与状态机** | `idemp:{tenant}:{operation}:{business_key}`；五态状态机；**唯一约束为账本，按 `scope` 划分**：IoT→Odoo 写回 → Odoo `edge_idempotency`；IoT 内部操作 → PG `t_idem_registry` | 07 文档 §4.3.1 / **§5.2.1**、新增 `edge_idempotency` |
| 3 | **缓存键** | `cache:{tenant}:{company}:{domain}:v{schema}:{key}` | 02 文档 §5 |
| 4 | **失效事件 Schema** | `event_id` / `occurred_at` / `tenant` / `company` / `entity` / `id` / `fields` / `version` | 01 文档 §7.2 信封扩展 |
| 5 | **错误码映射** | 401/403 保留认证授权语义；Odoo 校验错误 → 422；冲突 → 409；下游超时 → 504；熔断 → 503；统一返回 `code` / `message` / `trace_id` | 07 文档 §4.3 |
| 6 | **契约版本** | URL 与消息均含版本；废弃有窗口；`contract_version` 字段 | 07 文档 §7.2、`/api/iot/v1` |

**补充（PDF 明确要求）**：

- 请求参数**全部为命名参数**（技术方案 p.6，JSON-2 特性）
- 多库必须带 `X-Odoo-Database` header（PDF 核验的 JSON-2 行为）—— **07 文档遗漏了这一项，需补**
- Odoo 侧「返回版本化 DTO，不把 ORM 记录结构直接泄露给网关」（技术方案 p.7）—— IoT 侧的 DTO 同样不得直接暴露 `t_external_ref` 等内部表结构

---

## 5. Odoo 生产化配置（PDF 直接解决 07 的待确认项）

PDF 技术方案 p.23 给出的 Odoo 生产配置，正对应本文档 07 §2.5 的 C1/C2 待确认项：

```ini
[options]
proxy_mode = True
workers = <压测确定>
gevent_workers = <按长连接确定>
max_cron_threads = <按任务确定>
db_maxconn = <连接预算确定>
limit_request = <容量策略>
dbfilter = <明确规则>
list_db = False
```

**当前 `D:\odoo\odoo20tbb\odoo20.conf` 的实际差距**（全文仅 16 行，以上 8 项**全部缺失**）：

| # | 当前值 | 风险 | 建议 |
|---|---|---|---|
| 1 | `list_db = True` | 暴露数据库列表，可被探测 | 改 `False` |
| 2 | `admin_passwd = admin` | **弱口令**，可被用于建库/删库 | 改为强随机值，或用环境变量注入 |
| 3 | 无 `proxy_mode` | 置于反向代理后会导致 URL 生成与协议判断错误 | 加 `True` |
| 4 | 无 `workers` | 默认多线程模式（非生产推荐），并发一高即阻塞 | 压测后设定 |
| 5 | 无 `dbfilter` | 多库实例可被库名探测 | 明确规则 |
| 6 | 无 `limit_request` | 无请求体大小限制 | 按容量策略设定 |
| 7 | 无 `db_maxconn` | 连接数不设上限，可能打满 PG | 按连接预算设定 |
| 8 | `db_password` 明文 | 凭据泄露（07 文档 C4） | Vault / 环境变量 |

**这 8 项应作为「Odoo 侧上线前置检查清单」纳入本文档 06 的 Go-Live Checklist。**

---

## 6. 对本项目既有文档的修订清单

| 文件 | 修订项 | 优先级 |
|---|---|---|
| `07-odoo-integration.md` | ① 模块名 `sn_iot_bridge` → `sn_edge_integration`（含 `iot_facade.py` / `iot_api.py`）② 幂等升级为五态状态机 + `UNIQUE(company_id, integration_name, idempotency_key)` + `expires_at` 分档 ③ S1/S3 推送机制改为「高价值走 Outbox / 低价值走 webhook + 定时对账」④ 补 `X-Odoo-Database` header ⑤ 补错误码映射表 ⑥ 新增 §7.7 Odoo 扩展清单（ADR-017） | P0 |
| `01-architecture.md` | ① 服务清单收敛为 **6 个可部署单元**（代码模块边界保留）② 消息信封补 `company` / `fields` / `version` ③ `odoo-connector` 补「双队列翻译层」职责 | P0 |
| `02-domain-and-data.md` | 缓存键命名统一为 `cache:{tenant}:{company}:{domain}:v{n}:{key}` | P1 |
| `06-operations.md` | ① Go-Live Checklist 增加「Odoo 侧 8 项生产配置」② 容量规划补 PDF 的排队论口径与「起始配置不是承诺值」③ 演进路线与 PDF 阶段 0-5 对齐 | P1 |
| `04-rules-alarm-automation.md` | 验收指标补「有效告警占比」（PDF p.35 的 IoT 业务口径） | P2 |
| `README.md` | 新增 ADR-013/014/015/016；索引加入本文档 | P0 |

---

## 7. 对 PDF 方案的三点补充（反向输出）

这三条是本项目场景下 PDF 未覆盖、但必须补上的：

### 7.1 设备身份如何映射到 Odoo 的租户/公司

PDF（技术方案 p.15）说「tenant_id 由已验证身份映射」，但**前提假设是调用方是人（有 Odoo 用户）**。IoT 场景的调用方是**设备**，没有 Odoo 用户。

**补充设计**：

```
设备 device_key
  → 认证得 project_id（IoT 租户）
    → 查 t_external_ref / 项目配置得 odoo_company_id
      → connector 以「该 company 对应的专用服务账号」调用 Odoo Facade
```

**红线**：设备**永远不能**通过 MQTT Payload 或 Header 决定 `company_id`；也不允许设备凭据直接换取 Odoo 用户身份。

### 7.2 幂等保留期需为 IoT 场景拉长

已在 §C6 展开。PDF 的「覆盖客户端、平台和人工可能重试的最长窗口」在 IoT 下最长可达 7 天（设备离线重传），`edge_idempotency.expires_at` 必须按 `integration_name` 分档，不能用全局值。

### 7.3 PDF 的容量公式缺 IoT 侧的一半

PDF 手册 p.40 给的公式适用于**读密集型**业务：

```
Odoo 回源请求 ≈ 总读请求 × (1 - 缓存命中率)
```

IoT 侧是**写密集型**，需要补：

```
Odoo 写入量 ≈ 有效告警数 × (1 - 告警合并率) + 产量回流频率 × 在线设备数
```

对应约束（PDF p.40 的「队列等待 ≈ 到达速度 - 可持续处理速度」）：

```
告警入队速率 < connector 的处理速率，否则积压无限增长
→ connector 必须对 Odoo 写入做「合并窗口 + 令牌桶」，并在积压超阈值时降级为「只记 IoT 侧，延后补建工单」
```

这与 07 文档 §4.3 的限流设计一致，但**需要显式写出容量不等式**，否则限流参数只能拍脑袋。

---

## 8. 分期对齐

| PDF 阶段 | 本项目 Phase | 对齐动作 |
|---|---|---|
| **0 诊断**（基线 2 周） | Phase 0 | 本项目的 Phase 0 增加「**Odoo 侧基线采集**」：Top 接口、慢 SQL、worker 饱和度、connector 目标写入速率 |
| **1 接入治理**（Go 透传，灰度 5%→25%→100%） | Phase 1 | `odoo-connector` **先不做缓存与批量**，只做认证、白名单、超时、trace、错误映射；按 5%→25%→100% 灰度 |
| **2 读路径**（Redis） | Phase 1 | 设备台账同步加缓存（Cache-aside + 短 TTL + singleflight + 随机抖动） |
| **3 写路径**（幂等） | Phase 2 | 告警→工单的幂等表上线；做「响应丢失、并发重试、故障恢复」三类测试 |
| **4 计算热点**（Rust） | Phase 3+ | **暂不引入**；先补 profiler 数据；若引入则用独立 Rust 进程，并优先复用组织级基础设施 |
| **5 规模化**（模板化） | Phase 4 | 集成模板化 + SLA + 容量交接 |

**关键**：本发明文 06 的 Phase 与 PDF 阶段**逐一对齐**后，两份路线图可以合并为一张表，避免团队执行时出现两套节奏。

---

## 9. 增量待确认项

在 07 文档的 8 项之外，原本新增 6 项。**其中 5 项已裁定并落地，1 项转为执行项**：

| # | 事项 | 状态 | 落地 |
|---|---|---|---|
| 9 | 桥接模块合并为 `sn_edge_integration` | ✅ **已采纳** | ADR-014、07 §7.1、07 §7.2 |
| 10 | 缓存键迁移到 PDF 命名法 | ✅ **已采纳** | ADR-015、02 §5.1、02 §5.2、01 §7.2 |
| 11 | 边缘 agent 语言 | ✅ **Go 先行**，Rust 等 profiler 数据 | ADR-016 |
| 12 | 两套队列并存（Redis Streams + NATS） | ✅ **已采纳**，connector 为唯一翻译层，禁止双向互写 | 01 §3、01 §7.1 |
| 13 | `project_id` ↔ `odoo_company_id` | ✅ **允许一对多**，映射维护在 connector，设备不可指定 | 07 §5.2.3、01 §7.2 |
| 14 | Odoo 侧 8 项生产配置整改 | ✅ **拆为 4 个批次**（§2.6）：本地开发只需批次 1 的 3 项（零窗口成本）；批次 2 的容量参数必须等 Phase 0 压测；批次 3/4 在加反代与进生产前。**是「进生产前」的前置条件，不是「开工前」** | 07 §2.6、06 §9 |

> 仍需外部输入的项收拢在 **07 文档 §11.2**（共 9 项，含网络可达方式、配置整改窗口、服务账号权限范围等）。

---

## 10. 一句话总结

> PDF 方案解决的是「**Odoo 被人的请求压垮**」，本项目解决的是「**Odoo 被设备的数据淹没**」。
> 两者共用同一套边界哲学（Odoo 是唯一真相源、不直写库、幂等贯穿、trace 贯穿），
> 但必须共用**同一个桥接模块**、**同一套幂等语义**、**同一个人的流量入口** ——
> 否则就会出现 PDF 手册 p.46 列出的第一个反模式：**规则被复制到两处，然后逐渐不一致**。
