# 02 · 领域模型与数据设计

## 1. 多租户模型

```
Platform
└── Project (项目 / 租户, 隔离与计费的基本单位)
    ├── Region / Timezone
    ├── Users (成员) ── Role ── Permission (Casbin)
    ├── DeviceTypes (设备类型 / 物模型模板, 可跨 Project 复用)
    │   └── ThingModel(version) · ConfigTemplate · PanelTemplate
    ├── Devices (设备实例)
    │   ├── Attributes (静态属性, 最新值)
    │   ├── Telemetry (遥测时序)
    │   ├── Events (事件)
    │   ├── Commands (命令记录)
    │   ├── Shadow (设备影子: desired/reported/delta + server/shared/client 三类属性)
    │   ├── Alarms (告警)
    │   └── Tags / Groups (分组, 支持层级)
    ├── Rules (规则) · Scenes (场景) · Tasks (定时任务)
    ├── OtaFirmwares · OtaTasks
    ├── ApiTokens
    └── Quota
```

**隔离模式（`t_project.isolation_mode`）**：

| 模式 | 值 | 说明 |
|---|---|---|
| 共享 | `shared` | 共享库 + RLS |
| 独立 Schema | `schema` | 独立 PG schema |
| 独立库 | `database` | 独立 PG database |
| 独立集群 | `cluster` | 独立连接串（多集群路由表） |

`svc-device` 通过 `TenantResolver` 根据 `project_id` 返回对应的 `*pgxpool.Pool`，业务代码不感知隔离模式。

---

## 2. 物模型（ThingModel）

### 2.1 设计要点

1. **版本化**：物模型有 `version`，设备绑定 `device_type_id + thing_model_version`；升级物模型时旧版本保留，设备按需迁移。
2. **可导入导出**：JSON 格式，支持跨项目复制与产品识别码（`product_key`）。
3. **驱动端侧契约**：物模型的 `properties[].report` 直接决定端侧 SDK 的采集与上报策略。
4. **驱动 ACL**：设备可发布的 topic/key 由物模型白名单推导，无需人工配置 ACL。
5. **驱动存储**：`properties[].storage` 决定该指标是否落时序库、是否只留最新值。

### 2.2 物模型 JSON Schema（v1）

```json
{
  "schema_version": "1",
  "thing_model_version": 3,
  "product_key": "pk_a1b2c3d4",
  "name": "温湿度采集器",
  "properties": [
    {
      "key": "temperature",
      "name": "温度",
      "data_type": "float",
      "unit": "℃",
      "precision": 1,
      "range": { "min": -40, "max": 125 },
      "access": "r",
      "report": { "mode": "periodic", "interval": 30, "deadband": 0.2, "qos": 1 },
      "storage": { "ts": true, "latest": true, "ttl_days": 365 }
    },
    {
      "key": "firmware_version",
      "name": "固件版本",
      "data_type": "string",
      "access": "rw",
      "report": { "mode": "on_change", "qos": 0 },
      "storage": { "ts": false, "latest": true }
    }
  ],
  "events": [
    {
      "key": "fault",
      "name": "故障事件",
      "level": "warn",
      "outputs": [ { "key": "code", "data_type": "int" } ]
    }
  ],
  "commands": [
    {
      "key": "set_mode",
      "name": "设置工作模式",
      "inputs": [
        { "key": "mode", "data_type": "enum", "specs": { "0": "自动", "1": "手动" }, "required": true }
      ],
      "timeout_ms": 5000,
      "retry": { "max": 3, "backoff": "exponential" },
      "offline_ttl_ms": 86400000
    }
  ],
  "raw_parsers": [
    {
      "format": "hex",
      "applies_to": "telemetry",
      "rules": [
        { "key": "temperature", "expr": "int16_be(payload, 1) / 10.0" },
        { "key": "humidity",    "expr": "uint16_be(payload, 3) / 10.0" }
      ]
    }
  ]
}
```

**字段设计约束**：

- `key` 全局唯一、`snake_case`、不可变更（变更即新 key）。
- `data_type` 枚举：`int / long / float / double / bool / string / enum / date / struct / array`。
- `report.mode`：`periodic`（周期）/ `on_change`（变化）/ `on_demand`（按需）。
- `storage.ts=false` 的属性不写时序库，仅更新最新值 —— **这是控制存储成本的关键开关**。
- `raw_parsers` 中的表达式在 `svc-pipeline` 的 **`expr` 受限环境**内执行，受 04 文档 §1.2 的同一套资源限制约束。

> ⚠️ **勘误**：早期示例用了 `parseInt(...)` / `payload.substring(...)`，**不属于 `expr` 的白名单**（那是 JS 写法）。现已改为 `expr` 纯函数白名单中的**二进制读取函数**：`int16_be` / `uint16_be` / `int32_be` / `uint32_be` / `int16_le` / `uint16_le` 等（签名 `fn(payload, byteOffset)`，大端/小端显式）。这些函数在 04 文档 §1.2 的白名单中定义。
>
> **为什么用 `offset` 而不是 `substring`**：字节序与偏移是二进制协议解析的核心语义，用专用函数表达比字符串切片更清晰、更少出错，也更容易做静态校验。

### 2.3 物模型变更流程

```
提交草稿 → Schema 校验 → 兼容性检查 → 灰度发布(指定设备分组) → 全量 → 旧版本标记 deprecated
```

**兼容性检查规则**（CI 中作为测试用例）：

| 变更 | 兼容性 |
|---|---|
| 新增 property（可选） | ✅ 向后兼容 |
| 新增 command | ✅ 向后兼容 |
| 删除 property | ❌ 阻断（需先标记 deprecated 并观察一个版本） |
| 修改 `data_type` | ❌ 阻断 |
| 修改 `key` | ❌ 阻断（视为删除+新增） |
| 收紧 `range` | ⚠️ 告警（可能导致存量数据校验失败） |

---

## 3. 关系型数据模型（PostgreSQL）

### 3.1 通用约定

- 主键：`BIGINT` 雪花 ID（`snowflake`，含时间戳，避免 UUID 索引膨胀）。
- 所有业务表含：`id, project_id, created_at, updated_at, version (乐观锁), deleted_at (软删除)`。
- 所有多租户表启用 **Row Level Security**，策略：`project_id = current_setting('app.project_id')::bigint`。
- 连接池在事务开始时 `SET LOCAL app.project_id = $1`，未设置则 RLS 拒绝所有行。
- 时间统一存 `TIMESTAMPTZ`（UTC 存储，展示层按项目时区转换）。
- JSONB 用于物模型、标签、扩展属性；**不用于需要索引过滤的高频字段**。

### 3.2 核心表清单

| 表 | 用途 | 分区/分片 | 预估量级 |
|---|---|---|---|
| `t_project` | 租户/项目 | 无 | 10³ |
| `t_user` / `t_role` / `t_user_project_role` | 用户与权限 | 无 | 10⁴ |
| `t_api_token` | 应用令牌 | 无 | 10⁴ |
| `t_device_type` | 设备类型 | 无 | 10³ |
| `t_thing_model` | 物模型版本 | 无 | 10³ |
| `t_device` | 设备台账 | 按 `project_id` 哈希分片 (16) | 10⁷ |
| `t_device_group` / `t_device_group_rel` | 设备分组 | 无 | 10⁵ |
| `t_device_shadow` | 设备影子 | 按 `device_id` 哈希 | 10⁷ |
| `t_device_attr_latest` | 属性最新值（冷备，热数据在 Redis） | 按 `device_id` | 10⁷ |
| `t_event` | 事件 | 按月分区 | 10⁸ |
| `t_command` | 命令记录 | 按月分区 | 10⁸ |
| `t_alarm` | 告警 | 按月分区 + 状态索引 | 10⁷ |
| `t_alarm_rule` | 告警规则 | 无 | 10⁴ |
| `t_rule` / `t_rule_version` | 规则与版本 | 无 | 10⁴ |
| `t_scene` | 场景 | 无 | 10⁴ |
| `t_task` / `t_task_exec_log` | 定时任务与执行日志 | 日志按月分区 | 10⁸ |
| `t_ota_firmware` / `t_ota_task` / `t_ota_task_device` | OTA | 明细按月分区 | 10⁷ |
| `t_quota` / `t_quota_usage` | 配额与用量 | 用量按月分区 | 10⁷ |
| `t_audit_log` | 审计 | 按月分区，append-only | 10⁸ |
| `t_dlq` | 死信 | 按月分区 | 10⁶ |
| `t_external_ref` | 外部系统实体映射（Odoo ↔ IoT） | 按 `project_id` 哈希 | 10⁷ |
| `t_integration_log` | 集成日志（幂等 + 审计） | 按月分区 | 10⁸ |
| `t_meter_reading` | 设备产量计量的**原始时序**（业务账在 Odoo 的 `iot.meter.reading`，见 07 §6 S4 三段式） | 按 `device_id` 哈希 + 按月分区 | 10⁹ |

### 3.3 关键表结构摘要

```sql
-- 设备台账
CREATE TABLE t_device (
  id              BIGINT PRIMARY KEY,
  project_id      BIGINT NOT NULL,
  device_type_id  BIGINT NOT NULL,
  device_key      TEXT   NOT NULL,          -- 端侧唯一标识, 全局唯一
  name            TEXT   NOT NULL,
  secret_hash     TEXT   NOT NULL,          -- Argon2id, 禁止明文
  secret_version  INT    NOT NULL DEFAULT 1,
  auth_mode       TEXT   NOT NULL DEFAULT 'project',    -- project | per_device | mtls
  status          TEXT   NOT NULL DEFAULT 'inactive',   -- inactive|active|disabled
  online          BOOLEAN NOT NULL DEFAULT false,       -- 冗余字段, 由影子服务维护
  last_seen_at    TIMESTAMPTZ,
  thing_model_version INT NOT NULL,
  config_template_id  BIGINT,
  tags            JSONB  NOT NULL DEFAULT '{}'::jsonb,
  version         BIGINT NOT NULL DEFAULT 1,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at      TIMESTAMPTZ,
  -- ⚠️ PG 分区表的唯一约束必须包含分区键：UNIQUE(device_key) 单独写会建表即报错
  CONSTRAINT uk_device_key UNIQUE (project_id, device_key)
) PARTITION BY HASH (project_id);
CREATE TABLE t_device_p0 PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 0);
-- ... p1..p15
CREATE INDEX idx_device_project_type ON t_device(project_id, device_type_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_device_tags ON t_device USING GIN (tags jsonb_path_ops);
ALTER TABLE t_device ENABLE ROW LEVEL SECURITY;

-- 命令记录（按月分区）—— 只存记录，不承担幂等约束
CREATE TABLE t_command (
  id             BIGINT NOT NULL,
  project_id     BIGINT NOT NULL,
  device_id      BIGINT NOT NULL,
  cmd_key        TEXT   NOT NULL,
  payload        JSONB  NOT NULL,
  status         TEXT   NOT NULL,     -- pending|queued|sent|acked|timeout|failed|canceled
  idem_key       TEXT   NOT NULL,
  retry_count    INT    NOT NULL DEFAULT 0,
  issued_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at        TIMESTAMPTZ,
  acked_at       TIMESTAMPTZ,
  expires_at     TIMESTAMPTZ,
  result         JSONB,
  PRIMARY KEY (id, issued_at)
) PARTITION BY RANGE (issued_at);
-- 注意：分区表上的唯一索引只保证「分区内唯一」，不是业务幂等键的全局唯一。
-- 幂等约束由下方 t_idem_registry 承担。
CREATE INDEX idx_command_idem ON t_command(project_id, idem_key, issued_at);

-- ★ 幂等注册表（非分区，用于跨分区的全局唯一幂等约束）
-- 之所以独立成表：PG 分区表的唯一约束无法跨分区生效（见 §3.4 说明）
CREATE TABLE t_idem_registry (
  project_id  BIGINT      NOT NULL,
  scope       TEXT        NOT NULL,   -- command | integration | event | alarm_notify
  idem_key    TEXT        NOT NULL,
  request_hash TEXT,                  -- 同键不同摘要 → 409
  state       TEXT        NOT NULL,   -- processing | done | failed
  ref_id      BIGINT,                 -- 关联的业务记录 id
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (project_id, scope, idem_key)
);
CREATE INDEX idx_idem_expire ON t_idem_registry(expires_at);
-- 由定时任务按 expires_at 清理（TTL 分档见 07 文档 §4.3.1）

-- 告警（状态机持久化）
CREATE TABLE t_alarm (
  id           BIGINT NOT NULL,
  project_id   BIGINT NOT NULL,
  device_id    BIGINT NOT NULL,
  rule_id      BIGINT NOT NULL,
  level        TEXT   NOT NULL,   -- info|warn|critical
  state        TEXT   NOT NULL,   -- idle|detected|confirmed|active|resolved
  trigger_value JSONB,
  first_ts     TIMESTAMPTZ NOT NULL,
  confirmed_ts TIMESTAMPTZ,
  active_ts    TIMESTAMPTZ,
  resolved_ts  TIMESTAMPTZ,
  last_ts      TIMESTAMPTZ NOT NULL,
  notify_count INT NOT NULL DEFAULT 0,
  dedup_key    TEXT NOT NULL,
  PRIMARY KEY (id, first_ts)
) PARTITION BY RANGE (first_ts);
CREATE INDEX idx_alarm_active ON t_alarm(project_id, state, level) WHERE state <> 'resolved';

-- 审计日志（append-only，无 UPDATE/DELETE 权限）
CREATE TABLE t_audit_log (
  id          BIGINT NOT NULL,
  project_id  BIGINT,
  actor_type  TEXT NOT NULL,   -- user|device|api|system
  actor_id    TEXT NOT NULL,
  action      TEXT NOT NULL,   -- device.delete / token.rotate / rule.publish ...
  resource    TEXT NOT NULL,
  resource_id TEXT,
  before      JSONB,
  after       JSONB,
  ip          INET,
  user_agent  TEXT,
  trace_id    TEXT,
  result      TEXT NOT NULL,   -- success|denied|error
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
```

### 3.4 分库分表与扩容策略

| 阶段 | 策略 |
|---|---|
| 一期（< 500 万设备） | 单库 + `t_device` 16 路 HASH 分区 + 大表按月 RANGE 分区 |
| 二期（500 万 ~ 5000 万） | 读写分离（1 主 2 从，读走从库，强一致读走主库）；分区数扩至 64 |
| 三期（> 5000 万） | 引入分片中间件（`pgcat` / 自研路由层），按 `project_id` 分库；`t_device` 路由规则外置到配置中心 |

**扩容注意**：`t_command` / `t_alarm` 等按月分区表，需提前 3 个月自动创建分区（定时任务 + 失败告警）。

**PG 分区表的唯一约束限制（必须遵守）**：

| 规则 | 说明 | 违反后果 |
|---|---|---|
| 分区表上的 `UNIQUE` **必须包含全部分区键列** | `t_device` 按 `project_id` 分区，则唯一键必须是 `(project_id, device_key)`；只写 `(device_key)` **建表即报错** | DDL 直接失败 |
| 分区表上的唯一索引**只保证分区内唯一** | `t_command` 按 `issued_at` 分区，`UNIQUE(project_id, idem_key, issued_at)` 只保证「同一时间分区内」唯一 —— **不是业务幂等键的全局唯一** | 跨月重放可产生重复 |
| **跨分区/跨时间的全局唯一必须用独立非分区表** | 统一由 `t_idem_registry`（非分区）承担幂等约束 | — |

> **设计后果**：`t_command` / `t_integration_log` 等分区表**只承担记录与查询职责**，不再承担幂等约束。所有幂等判定统一查 `t_idem_registry`（IoT 侧）与 `edge_idempotency`（Odoo 侧，见 07 §4.3.1）。

---

## 4. 时序数据模型（GreptimeDB）

### 4.0 许可证前置（重要）

本节基于 **GreptimeDB（Apache-2.0）**。这是相对 ThingsCloud 生态常见选型 TDengine（AGPL-3.0）的关键差异：

| 事项 | 约束 |
|---|---|
| 许可证 | Apache-2.0，**无 SaaS 源码开放义务**，商业闭源运营无歧义 |
| 接入方式 | 只允许**网络协议**：gRPC / HTTP API / MySQL 或 PostgreSQL wire protocol（`pgx`、`go-sql-driver/mysql` 均为纯 Go，无 cgo） |
| 禁止 | 禁止 cgo 链接数据库客户端库（ADR-010）；禁止修改 GreptimeDB 源码后就地构建并分发 |
| 可替换性 | 所有读写经 `TimeSeriesStore` 接口，**业务代码中不得出现 GreptimeDB 专有语法**；方言集中在 `pkg/tsdb/greptimedb` 适配层 |

> `TimeSeriesStore` 抽象在此有**双重身份**：既是架构解耦手段，也是**许可证对冲手段**。

### 4.1 表模型选型

GreptimeDB 使用「单表 + 时间索引 + 主键标签 + 字段列」模型，概念上等价于 TDengine 的超级表/子表：

| 方案 | 结构 | 优点 | 缺点 | 适用 |
|---|---|---|---|---|
| **A. 通用表 + JSON 字段**（默认） | `telemetry(ts TIME INDEX, project_id, device_id, device_type_id, metrics JSON, PRIMARY KEY(project_id, device_id))` | 灵活，物模型变更零成本，单表承载全租户 | JSON 字段的列式下推弱于原生列 | 默认全量 |
| **B. 按设备类型宽表**（可选优化） | `telemetry_{device_type}(ts TIME INDEX, project_id, device_id, temperature, humidity, ... PRIMARY KEY(project_id, device_id))` | 压缩比高、聚合下推快、可建专用索引 | 物模型变更需 DDL | 高频类型 / 大客户 |

**决策**：默认 A；允许租户在设备类型上开启 `storage.profile = "wide"` 切 B。宽表 DDL 由 `svc-device` 的物模型发布事件驱动，`svc-pipeline` 的执行器异步建表（幂等 + 失败重试 + 建表失败告警）。

> **Phase 0 待验证项**：A 方案依赖 GreptimeDB 的 `JSON` 类型。必须先实测 JSON 字段的写入吞吐与提取查询性能；**不达标则默认切 B**。这是 Phase 0 的决策点之一。
>
> ✅ **已于 2026-10-01 验证完成，结论见 §4.1.1：维持 A 为默认，不触发切换决策点。**

#### 4.1.1 验证结论（Phase 0 · B1，2026-10-01 · **维持 A 为默认**）

**一句话**：写入验收线（5 万 points/s）A 方案通过 **3.8×**；查询上 A 只在「多设备 + 长跨度」形态触及预算，而**该形态换宽表同样超线**（211 ms）—— 问题在查询形态，不在表模型。故**不触发「切宽表」的决策点**。

**复现方式**（工具与原始报告均已入库）：

```bash
make b1-bench        # → cmd/tsdb-bench，报告写到 tmp/b1-report.md
```

| 项 | 取值 |
|---|---|
| 被测实例 | GreptimeDB **1.2.1** standalone（devbox compose；共享宿主 64C/62G，**非**独立 NVMe，**非** 3 副本集群） |
| 数据集 | 300 台 × 30s × 24h ＋ 12 台 × 1s × 6h ＝ **1,123,200 行 / 5,616,000 points**（每行 5 指标，对齐 06 §5 口径） |
| 写入参数 | 8 并发连接；攒批 1000 行或 200 ms（§4.2 规定） |
| 查询参数 | 4 个模式各采样 30 次，串行 |
| **前置一致性校验** | A、B 在同一窗口返回 **3599 点，时间戳 / device_id / 取值逐点相等**（误差 < 1e-9）—— 保证比的是同一份数据，否则结论无意义 |

原始报告：[`docs/reports/b1-tsdb-bench.md`](./reports/b1-tsdb-bench.md)

**① 写入吞吐**（验收线 **5 万 points/s**，06 交付表）

| 组合 | points/s | 相对验收线 | 判定 |
|---|---:|---:|---|
| A · JSON（`\x` 十六进制参数） | 190,608 | 3.81× | **达标** |
| A · JSON（**内联字面量**） | **639,795** | **12.80×** | **达标** |
| B · 宽表 | 161,229 | 3.22× | 达标 |

- **A 方案的写入不是瓶颈，反而最快**（内联字面量是宽表的 3.97×）。
- 但**两条写入路径差 3.4×**：`\x` 参数把 JSON 线上体积翻倍。→ 结论：**默认用内联字面量**（见 §4.2 的传参规定与安全边界）。

**② 查询延迟**（验收线 **P95 < 200 ms**，06 §3.4）

| 查询 | 返回行数 | A · JSON P95 | B · 宽表 P95 | 宽表快 |
|---|---:|---:|---:|---:|
| Q1 单设备近 1h 曲线 | 3,599 | 48.0 ms ✅ | 36.9 ms ✅ | 1.30× |
| Q2 单设备 6h 分桶聚合 | 73 | 109.3 ms ✅ | 49.8 ms ✅ | 2.19× |
| Q3 多设备近 1h 曲线 | 35,990 | **246.7 ms ❌** | **210.9 ms ❌** | 1.17× |
| Q4 多设备 24h 分桶聚合 | 730 | **198.4 ms ⚠️** | 91.0 ms ✅ | 2.18× |

三点必须一起看：

1. **单设备场景 A 全部达标**（48 / 109 ms），余量充足。
2. **Q3 两个方案都超线** —— A 246.7 ms、B 210.9 ms。超线的根因是**返回 3.6 万行明细**，与存储布局无关。→ 这必须靠 §4.3.1 的**明细限行**解决，换表模型无效。
3. **只有 Q4（多设备 × 长跨度聚合）是宽表明确胜出**（91 ms vs A 的 198 ms，且 A 在跨轮次采样中落在 198~213 ms，**骑在预算线上**）。这是 A 方案唯一真实的短板，但它的解法是**预聚合表**（§5.3 的成本清单里本就有「降采样物化：查询成本降低 10–100 倍」），不需要换表模型。

**③ 存储占用**（落盘后统计；必须先 `ADMIN FLUSH_TABLE`，否则数据还在 memtable，字节/行会虚高 2~6 倍）

| 取值模型 | A 字节/行 | B 字节/行 | A ÷ B |
|---|---:|---:|---:|
| `linear`（周期序列，**不真实**） | 16.13 | 4.87 | 3.31× |
| `random`（纯随机） | 33.49 | 35.78 | 0.94× |
| `correlated`（自相关 + 噪声，**默认，最接近真实传感器**） | 35.72 | 37.25 | **0.96×** |

> **这三个数是本报告里最有价值的一组**：同一份代码、同一个数据集，只改取值的**时间相关性**，A/B 存储比从 3.31× 摆到 0.96×。
> 结论：**「宽表更省空间」并不成立** —— 它只在数据呈强周期性时显著。本项目的真实取值分布尚未采集，
> 因此**存储维度不具备选型决策力**，只能等某租户开启宽表前用其真实样本重测。
> 顺带修正 06 §5 的容量口径：实测 **3.3~7.5 字节/point**，优于该处「每点 16 B 压缩后」的假设（偏保守，方向安全）。

**④ GreptimeDB 1.2.1 的 JSON 能力边界（全部实测，勿凭文档推断）**

| 能力 | 结论 |
|---|---|
| `JSON` 列类型 | ✅ 可用 |
| 列名 `metrics` | ⚠️ **是保留关键字**，必须写 `"metrics"`，否则建表失败 |
| 取数值 | ✅ 只有 `json_get(col,'k',0.0)`（三参 → Float64）。两参形式返回 **String** |
| 三参的默认值 | ⚠️ **对缺失键不生效** —— 键不存在仍返回 NULL（不是第三个参数） |
| `->>` / `col['k']` / `json_path_query` / `json_keys` / `json_length` | ❌ 均不可用 |
| `json_get` 作用于 `TEXT` 列 | ❌ **报 `JSON_GET not supported argument type Utf8`**；若漏检 `rows.Err()` 会表现为「静默返回空结果集」 |
| `CAST($1 AS JSON)` | ❌ `Unsupported SQL type JSON` |
| 普通字符串参数写入 JSON 列 | ❌ `\x prefix expected for bytea`（服务端把 JSON 列声明为 bytea） |
| `\x` 十六进制参数写入 | ✅ 可用（线上体积 ×2） |
| 内联字面量写入 | ✅ 可用且最快 |
| JSON 上的倒排索引 | ❌ `INVERTED INDEX(col)` 不被识别；`WITH('inverted_index'=...)` 报未知选项 |
| `date_bin` / `date_trunc` / `= ANY(ARRAY[])` | ✅ 可用（`IN` 列表亦可） |
| PG `simple protocol` | ❌ `standard_conforming_strings` 为 off，pgx 直接拒绝 |
| pgx 的 `Ping()` | ❌ 发送空语句，报 `Invalid SQL, error: empty statements`；改用 `SELECT 1` 探活 |

**⑤ 未验证项（决定后续动作，不要当成「已验证」）**

| # | 未验证 | 影响与后续 |
|---|---|---|
| 1 | **3 副本集群**（06 §2 要求 `replication_factor=3`） | 本次是单机 standalone，写入吞吐会变。**上生产前必须按 3 副本补测** |
| 2 | 数据规模 | 本次 112 万行；生产 30 天热数据 ~2.4 TB（差 2 个数量级）。长跨度查询延迟会随数据量上升 |
| 3 | 并发查询 | 本次串行采样。§4.3 要求单租户并发上限 20，需补测并发下的相互影响 |
| 4 | 真实取值分布下的存储占用 | 见 ③，需按租户样本重测 |
| 5 | 预聚合表生效后的 Q4 | 本报告未建预聚合表；这是把 A 的 Q4 拉回预算内的**指定解法**，需在 Phase 1 落地后复测 |

**决策**

1. **默认维持 A（JSON 字段）**：写入验收线通过 3.8×，变更成本为零（物模型加指标不需要 DDL）。
2. **写入默认走内联字面量**（§4.2 已规定），并在 `TimeSeriesStore` 适配层内固化，业务代码不可见。
3. **必须落地 §4.3.1 的明细限行** —— 这是本次压测暴露的真实缺口，与表模型无关。
4. **把「多设备 × 长跨度聚合的预聚合表」从可选优化提升为 Phase 1 必做项**：实测该形态在 A 下已骑在 SLO 线上（198~213 ms）。
5. **B（宽表）保留为按设备类型可选开启**，开启条件写死在 §4.1 的「适用」列里，不再推迟到「以后再说」：
   - 该设备类型出现**分析型聚合负载为主**（实测聚合快 2.2×）；
   - 且开启前用**该租户真实遥测样本**重测存储占用（本报告已证明该维度跨模型摆动 3.4 倍）。

### 4.2 建表与写入

```sql
-- A 方案（默认）
-- ⚠️ 列名 metrics 必须加双引号：它是 GreptimeDB 的保留关键字，
--    不加引号建表直接失败（Cannot use keyword 'metrics' as column name）。
CREATE TABLE IF NOT EXISTS telemetry (
  ts              TIMESTAMP TIME INDEX,
  project_id      BIGINT,
  device_id       BIGINT,
  device_type_id  BIGINT,
  "metrics"       JSON,
  PRIMARY KEY (project_id, device_id)
)
WITH ('ttl' = '365d', 'append_mode' = 'true');

-- 写入（多值批量，必须攒批）
INSERT INTO telemetry (ts, project_id, device_id, device_type_id, "metrics")
VALUES
  ('2026-10-01T08:12:33.421Z', 10231, 100234, 55, '{"temperature":25.3,"humidity":62.1}'),
  ('2026-10-01T08:12:34.421Z', 10231, 100234, 55, '{"temperature":25.4,"humidity":62.0}');
```

> **JSON 列的传参约束（实测，见 §4.1.1）**：在 PostgreSQL wire protocol 下，GreptimeDB 把
> `JSON` 列声明为 `bytea`，因此**普通字符串参数会被服务端拒绝**（`\x prefix expected for bytea`），
> `CAST($1 AS JSON)` 同样不可用（`Unsupported SQL type JSON`）。可用路径只有两条：
> 内联字面量，或把文档编码成 bytea 文本表示（`\x` + 十六进制）。两者的代价与选择见 §4.1.1。

**写入规范**：

| 项 | 规定 |
|---|---|
| 批量 | 单次 `INSERT` 多行，攒批阈值 **`200ms` 或 `1000` 行（任一先到即 flush）**，由 `svc-pipeline` 的 batcher 控制；**禁止逐条写入**。⚠️ 不可写成 `max(200ms, 1000行)` —— 那是"两者都满足才 flush"，语义错误 |
| 表创建 | 幂等 `CREATE TABLE IF NOT EXISTS`；进程内 `sync.Map` 缓存已建表，避免每次写入都执行 DDL |
| 时间精度 | 使用默认毫秒精度（`TIMESTAMP`），与端侧 `ts` 精度一致；如需纳秒用 `TIMESTAMP(9)` |
| 时间戳来源 | 设备上报优先；偏差 > 5 min 以服务端时间为准，并在 `metrics` 中写 `_ts_corrected: true` |
| 乱序容忍 | GreptimeDB 允许乱序写入；`append_mode=true` 关闭库内去重（幂等统一在管道层做，见 §6） |
| 连接方式 | 优先 PostgreSQL wire protocol（复用 `pgx` 连接池 + 超时/熔断设施）；大批量导入用 HTTP/gRPC |
| **JSON 列传参** | 用**内联字面量**（吞吐为 hex 参数的 3.3×，见 §4.1.1）。仅当指标取值为数值/布尔时可用 —— 序列化后的文档是可打印 ASCII，转义只需处理 `'` 与 `\`；**一旦引入字符串型指标**（可能出现控制字符），必须改回 `\x` 十六进制参数或走 COPY，不能靠转义硬扛 |
| 失败处理 | 批量部分失败按行判定，失败行进重试流；**禁止整批重放**（幂等键会拦掉重复，但会浪费配额） |

### 4.3 查询模式与优化

| 查询场景 | 实现 |
|---|---|
| 单设备最新值 | **不走 GreptimeDB**，读 Redis `cache:{pid}:{co}:device:last:v2:{did}` |
| 单设备近 1h 曲线 | `SELECT ts, metrics FROM telemetry WHERE project_id=? AND device_id=? AND ts > now() - INTERVAL '1 hour' ORDER BY ts` |
| 单设备区间聚合 | SQL 聚合 `AVG/MAX/MIN/SUM/COUNT/STDDEV` + 时间分桶降采样 |
| 多设备对比 | `device_id IN (...)`，强制限制 ≤ 50 个 |
| 跨租户统计 | 禁止；统计类走离线导出（Parquet → 对象存储 → DuckDB/Spark） |
| 历史导出 | 异步任务 → 查询/导出 → Parquet → 对象存储 → 签名下载链接（有效期 24h） |
| 监控栈统一（可选） | GreptimeDB 提供 PromQL 兼容查询，可让 IoT 时序与平台监控共用同一存储 |

**查询保护**（全部在 `TimeSeriesStore` 适配层强制，业务代码不可绕过）：

- **强制注入 `project_id` 等值条件** —— 由查询构建器拼接，**不接受用户传入的原始 WHERE 片段**（防跨租户越权与注入）。
- **强制单次返回行数上限（明细 ≤ 5000 行）** —— 见下方专条，这是 Phase 0 压测暴露的缺口。
- 强制时间范围（默认回溯 24h，最大 90 天，超限引导走导出）。
- 强制设备数上限（50），聚合函数白名单。
- 单租户查询并发上限（默认 20），超出排队而非拒绝。
- 慢查询（> 3s）记录并上报指标；持续超阈自动降级到预聚合表。
- **读写连接池隔离**：查询走只读连接池，防止大查询挤占写入带宽。

#### 4.3.1 明细查询必须限行（Phase 0 压测暴露的缺口）

原规则只限制了**设备数**（≤ 50），没有限制**返回行数** —— 而 50 台 × 1h × 1Hz = 18 万行。
实测（§4.1.1）：**10 台设备 × 1h = 3.6 万行明细的 P95 已经是 250 ms（JSON）/ 206 ms（宽表），双双超出 §3.4 的 API 延迟预算（P95 < 200 ms）**。

| 规则 | 取值 | 依据 |
|---|---|---|
| 明细返回行数上限 | **默认 5000 行** | 实测 3.6 万行 ≈ 240 ms，按 200 ms 预算留 2× 余量反推 |
| 命中上限时的行为 | **自动降采样**（按时间分桶返回，桶宽自适应）或引导走异步导出 | 直接截断会让用户以为「只有 5000 个点」，是错误答案 |
| 投影字段数 | 明细查询单次最多 4 个指标 | 避免 `SELECT *` 式的 JSON 整列解析 |

> **关键判断**：这个问题**不是表模型能解决的** —— 换宽表同样超线（206 ms）。它是查询形态问题：
> 「多设备 × 长时间 × 逐点明细」本身就超出交互式查询的预算。
> 因此 §4.1 的 A/B 选型不受它影响，而这条限行规则是**必须**落地的。

#### 4.3.1.1 落地实现与实测（Phase 1，2026-10-01 · **已落地**）

**一句话**：明细限行 + 自适应降采样已实现。Q3 形态（10 设备 × 1h ≈ 3.6 万行）经受保护入口后
返回行数 **35990 → 3610**，P95 从 **257.4ms 降到 152.4ms（JSON）/ 224.3ms 降到 106.7ms（宽表）**，
两方案均落回 P95<200ms 预算内。

**实现（全部在适配层强制，业务代码不可绕过）**：

| 项 | 落地 |
|---|---|
| 入口 | `tsdb.Store.QuerySeries`（`internal/tsdb/greptimedb/store.go`）—— 业务唯一读入口 |
| 守卫 | `SeriesQuery.Normalize`：project_id>0、设备 1..50、指标白名单、Since<Until、回溯 ≤90d、Limit ∈[1,5000] |
| 上限 | `MaxDetailRows=5000`（硬上限，调用方不可抬高） |
| 超限行为 | **自动降采样**：`AdaptiveBucket(span, devices, cap)` 在「设备数 × 桶数 ≤ cap」下取最小可读桶宽（阶梯 1s…24h），返回聚合行并标注 `Granularity=downsampled` / `Bucket` / `CapHit`（后者供引导异步导出） |
| 防绕过 | 裸查询改名为 `SelectRangeUnprotected` / `SelectBucketsUnprotected`（仅压测/诊断），不再存在「默认不安全」的入口 |
| 不静默截断 | 聚合结果仍超限则直接报错；探测与取数之间若有并发写入导致取数超限，也落到降采样 |

**探测策略（实测选择）**：超限判定用 `SELECT 1 ... LIMIT cap+1`（只取常量、不解析指标、不排序），
而非「取指标值 + ORDER BY」。在 1.12M 行表上，前者 P95 **20.4ms**，后者 **95.7ms** ——
探测的职责只是判断「超没超」，取数与排序是下一步的事。

**实测**（2026-10-01，`make b1-bench` §5.2；原始报告 [`docs/reports/b1-detail-limit.md`](./reports/b1-detail-limit.md)）：

| 方案 | 入口 | 返回行数 | 粒度 | 桶宽 | P95 | 判定 |
|---|---|---:|---|---:|---:|---|
| A · JSON 字段 | 未保护 | 35990 | raw | — | 257.4ms | ❌ |
| A · JSON 字段 | 受保护 | 3610 | downsampled | 10s | **152.4ms** | ✅ |
| B · 宽表 | 未保护 | 35990 | raw | — | 224.3ms | ❌ |
| B · 宽表 | 受保护 | 3610 | downsampled | 10s | **106.7ms** | ✅ |

复现：`make test-tsdb`（env 门控真实库，会重建 `telemetry`）；压测走 `make b1-bench`（报告含 §5.2）。

**未验证项（不要当成「已验证」）**：

| # | 未验证 | 影响与后续 |
|---|---|---|
| 1 | 并发查询 | 本节为串行采样，§4.3 要求单租户并发上限 20，需补测并发相互影响 |
| 2 | 多指标投影（≤4） | 当前 `SeriesQuery` 为单指标；`MaxProjectedMetrics=4` 仅为契约常量，多指标端点属后续批次 |
| 3 | 大窗口实际延迟（如 50 设备 × 90d） | 目前只有桶数学上界（返回 ≤5000 行），未实测 |
| 4 | 预聚合表路由 | 与本节正交：限行治「明细形态」，预聚合治「多设备 × 长跨度聚合」 |

---

## 5. 缓存设计（Redis Cluster）

#### 5.1 键命名规范（对齐 `odoo-gateway`）

缓存键统一采用《Odoo 20 高性能架构技术方案》手册 p.42 的命名法，**保证与 `odoo-gateway` 的键空间可互认**（否则两个 Go 系统的缓存排障需要两套心智模型）：

```
cache:{tenant}:{company}:{domain}:v{schema}:{key}   缓存
idemp:{tenant}:{operation}:{business_key}           幂等占位
job:{tenant}:{job_type}:{job_id}                    任务状态
lock:{tenant}:{resource}:{resource_id}              分布式锁
```

#### 5.2 键清单

| Key 模式 | 类型 | TTL | 用途 |
|---|---|---|---|
| `cache:{pid}:{co}:device:online:v1:{did}` | string | keepalive×2.5 | 在线状态 |
| `cache:{pid}:{co}:device:conn:v1:{did}` | string | conn TTL | 会话所在网关节点 |
| `cache:{pid}:{co}:device:last:v2:{did}` | hash | 7d | 最新属性值（字段=property key） |
| `cache:{pid}:{co}:device:shadow:v1:{did}` | hash | 永久（有 PG 兜底） | 影子 desired/reported/delta + 版本号 |
| `cache:{pid}:{co}:devicetype:tm:v{ver}:{dtid}` | string(JSON) | 1h | 物模型缓存 |
| `cache:{pid}:{co}:auth:devtok:v1:{dkey}` | hash | 10m | 设备凭据校验缓存 |
| `cache:{pid}:{co}:equipment:meta:v1:{eq_id}` | string(JSON) | 5m + 抖动 | Odoo 设备台账元数据（来自 connector） |
| `rate:{pid}:{scope}:{id}` | string | 窗口 | 令牌桶计数（无 `company`，纯平台侧） |
| `quota:{pid}:{metric}:{yyyymmdd}` | string | 7d | 配额计数 |
| `idemp:{pid}:{operation}:{business_key}` | string | 业务窗口（见 07 §4.3.1） | 幂等占位（**非账本**） |
| `lock:{pid}:{resource}:{resource_id}` | string | 30s | 分布式锁（`SET NX PX` + 续期） |
| `gw:sub:{node}` | set | 连接周期 | 节点订阅的 topic filter 集合 |
| `gw:route:{filter_hash}` | set | 连接周期 | topic filter → 节点集合（反向索引） |

**命名要点**：

| 变量 | 取值 | 说明 |
|---|---|---|
| `tenant` | `project_id` | IoT 平台租户 |
| `company` | `odoo_company_id` | 由 `t_external_ref` 映射得出；**未绑定 Odoo 时用 `0`** |
| `v{schema}` | 缓存对象结构版本 | **结构变更必须递增**，使旧值自然失效 |
| 平台内部键 | `rate:` / `quota:` / `gw:` 前缀 | 不涉及 Odoo 的键不加 `company`，避免无意义维度 |

**缓存一致性策略**：

- 物模型/设备台账：**Cache-Aside + 版本号失效**。写操作递增 `version` 并通过 NATS 广播 `iot.device.config_changed`，各节点收到后失效本地缓存。TTL 作为兜底。
- 最新值：**Write-Through**（pipeline 写 Redis 与 GreptimeDB 并行），Redis 丢失可从 GreptimeDB 重建。
- 在线状态：**以会话注册表（Redis `d:conn` / Broker 会话状态）为实时事实**；PG 仅作审计与历史摘要（`last_seen_at`）。**不要用 PG 判定设备是否在线**。
- 禁止使用 `KEYS`、`FLUSHALL`；批量失效用 `SCAN` + `UNLINK`。

#### 5.3 Redis 故障策略（按数据类型拆分，不共用一句话）

早期文档写「Redis 不可用 → 读走 PG + 降级」，与 05 文档的「JWT 吊销 fail-closed」直接冲突。**正确做法是按数据类型分别定义，因为它们的风险性质完全不同**：

| 数据 / 用途 | Redis 不可用时的策略 | 依据 | 落点 |
|---|---|---|---|
| **用户 JWT 吊销列表** | **fail-closed：拒绝**（无吊销信息即视为可能已吊销） | 安全优先，宁可拒绝不可放行 | 05 文档 §2.2 |
| **设备认证凭据缓存** | **fail-closed：拒绝新认证**；仅放行 L1 本地 LRU 中已认证过的设备 | 防止凭据吊销后仍被放行 | 03 文档 §2.1 |
| **设备在线 / 会话定位** | **fail-open 于业务、fail-closed 于路由**：设备接入不受影响（网关本地会话表可判断本节点连接）；但跨节点命令下发**改为同步寻址失败 → 入离线队列**，不做错误投递 | 在线状态是"快路径"，PG/Broker 有兜底；但投递到错误节点会造成丢消息 | 01 文档 §6.2 |
| **物模型 / 设备台账缓存** | **降级到 PG 直读**（有 TTL 兜底，PG 是权威）；网关限流阈值降低 50% 以保护 PG | 纯缓存，PG 有真相 | 本节 |
| **最新值缓存** | 降级到 GreptimeDB 查询（`last` 可从时序重建） | 可从时序重建 | 02 §4.3 |
| **配额 / 限流计数** | 降级为**基于本地计数的保守阈值（×0.5）**，接受短暂不精确 | 计数可容忍误差 | 01 §8 |
| **幂等占位** | 降级后仍以 **PG `t_idem_registry` / Odoo `edge_idempotency` 唯一约束**为准 | Redis 从不是幂等账本 | 07 §4.3.1 |

> **原则**：**Redis 是加速层，不是事实层。** 凡是"Redis 挂了就放行"的地方都必须能说出底层的事实来源在哪里；说不出来的，一律 fail-closed。

---

## 6. 一致性策略总表

| 数据域 | 一致性模型 | 写路径 | 幂等 | 顺序 | 冲突处理 |
|---|---|---|---|---|---|
| 租户/用户/权限 | 强一致 | PG 事务 | 天然 | 无需 | 乐观锁 `version` |
| 设备台账/物模型 | 强一致 | PG 事务 + 事件广播失效缓存 | 天然 | 无需 | 乐观锁，冲突返回 409 |
| 遥测时序 | 最终一致（at-least-once） | NATS → pipeline → GreptimeDB | `device_id+ts+seq` 去重 | 按 device 分区保序 | 同 key 后写覆盖（时间戳为准） |
| 属性最新值 | 最终一致（秒级） | pipeline → Redis | 同上 | 按 device 保序 | 后到覆盖 |
| 设备影子 | 强一致（单设备级） | PG 事务 + Redis 同步 | `version` 校验 | 单设备串行 | **乐观锁 + 版本比较**；客户端可带 `expected_version` |
| 命令 | at-least-once + 幂等 | PG 状态机 + NATS | `idem_key` 唯一索引 | 单设备按序 | 重复执行返回已有结果 |
| 告警 | effectively-once | PG 状态机 + Redis 去重 | `dedup_key` | 单设备单规则串行 | 行级锁，状态只允许向前迁移 |
| 审计 | 强一致（append-only） | PG 直写（可异步化） | `msg_id` | 无需 | 无更新 |
| 用量计量 | 最终一致（分钟级） | Redis 计数 → PG 对账 | 幂等聚合 | 无需 | 定时对账修正 |

**告警状态迁移合法性矩阵**（非法迁移一律拒绝并记录）：

```
idle       → detected
detected   → confirmed | idle(恢复)
confirmed  → active | idle(恢复)
active     → resolved | active(更新 last_ts, 抑制期内不重复通知)
resolved   → idle
```

---

## 7. 数据生命周期与成本控制

| 数据 | 热（在线查询） | 温 | 冷（归档） | 删除 |
|---|---|---|---|---|
| 遥测时序 | 30 天（NVMe） | 30–365 天（压缩，降采样 1m/1h） | > 365 天 → 对象存储 Parquet | 按租户配置（默认 3 年） |
| 事件 | 30 天 | 30–90 天 | > 90 天归档 | 180 天后删除 |
| 命令记录 | 30 天 | 30–180 天 | > 180 天归档 | 1 年 |
| 告警 | 90 天 | 90–365 天 | > 365 天归档 | 3 年 |
| 审计日志 | 180 天 | 180 天–3 年 | > 3 年归档（WORM） | 不删除 |
| 执行日志 | 15 天 | — | — | 15 天 |
| 死信 | 7 天 | — | — | 处理后清理 |

**成本控制手段**：

1. 物模型 `storage.ts=false` 的属性不落时序库 —— 从源头控制写入量。
2. 死区（`deadband`）过滤无效上报 —— 从源头控制消息量。
3. GreptimeDB 列式压缩 + 表级 `ttl` 自动过期（避免 `DELETE` 造成写放大）。
4. 降采样物化（1m/1h 预聚合表），查询按时间范围自动路由到最粗粒度表。
5. 对象存储生命周期策略自动转低频/归档存储。

**降采样实现（分期）**：

| 阶段 | 方案 | 说明 |
|---|---|---|
| 一期 | **定时聚合任务**：`svc-pipeline` 每 1 min / 1 h 执行 `INSERT INTO telemetry_1m SELECT ... date_bin(...) GROUP BY`，幂等写入预聚合表 | 实现简单、可控、可回放；任务失败可补跑 |
| 二期 | 评估 GreptimeDB 的流式/持续聚合能力（Flow 引擎） | 需先验证其稳定性与运维复杂度，**不作为一期依赖** |

**查询路由**：查询规划器按时间跨度选择数据源 —— `≤ 6h` 走原始表，`6h–30d` 走 1m 表，`> 30d` 走 1h 表；三张表 schema 对齐，业务侧无感。
