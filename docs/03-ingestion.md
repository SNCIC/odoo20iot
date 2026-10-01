# 03 · 接入层与消息管道

## 1. 接入网关总体设计

### 1.1 目标

- 单集群支撑 **100 万并发 MQTT 连接**，可线性扩展至 1000 万。
- 单集群支撑 **50 万 msg/s** 入站峰值，持续 1 小时不劣化。
- 网关节点**无单点**，任意节点宕机仅影响该节点上的连接（秒级重连）。
- 网关节点可**在 3 分钟内**完成 ±50% 的水平伸缩。

### 1.2 为什么自研网关（ADR-001 展开）

| 需求 | EMQX 方案 | 内嵌 mochi-mqtt 方案 |
|---|---|---|
| 物模型驱动 ACL（按 property key 粒度） | 需写 Erlang 插件，团队无栈 | 直接 Go Hook 实现 |
| 租户级配额与限流 | 插件实现，跨租户视角受限 | 直接控制 |
| 消息直接进内部管道（NATS） | 需桥接/规则引擎转发，多一跳 | Hook 内直接发布，少一跳 |
| 单位设备成本 | 商业版功能受限 / 社区版集群能力有限 | 可控 |
| 风险 | 低（成熟） | 中（需自建集群与稳定性投入） |

**结论**：采用内嵌方案，但必须满足以下前置条件，否则回退到 EMQX：

1. 完成 100 万连接压测并稳定运行 ≥ 2 周无异常。
2. 会话迁移、节点扩缩容、故障切换三类场景通过混沌测试。
3. 具备 MQTT 协议一致性测试（`paho` 互操作套件 + 自研用例）。

> **回退开关**：网关对外只暴露标准 MQTT 协议，内部路由通过 NATS。若切换 EMQX，只需把 `gw-mqtt` 替换为 EMQX 集群 + 一个 NATS 桥接器，端侧无感。

### 1.3 集群架构

```
                    ┌─────────────────────────────┐
   设备 ──TLS──►    │  L4 LB (least_conn, 跨AZ)   │
                    └──────────────┬──────────────┘
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
   gw-mqtt-1                  gw-mqtt-2                  gw-mqtt-3
   (内嵌 broker)              (内嵌 broker)              (内嵌 broker)
        │                          │                          │
        │  ┌───── 订阅注册表 (Redis) ─────┐                    │
        └──┤  gw:sub:{node} / gw:route:{filter_hash}          │
           └────────────────────┬─────────┘                   │
                                │                           │
        ┌───────────────────────┴───────────────────────────┴──┐
        │              NATS JetStream (3~5 节点 Raft)          │
        │   iot.route.{node}  /  iot.telemetry.*  /  iot.cmd.* │
        └──────────────────────────────────────────────────────┘
```

### 1.4 跨节点投递机制

**问题**：设备 A 连在节点 1，设备 B 连在节点 2；场景触发需向 A 下发命令；A 订阅的 topic 也可能被节点 3 的发布者命中。

**方案：订阅注册表 + 定向路由**

1. 连接建立时，`gw-mqtt` 将 `(node_id, topic_filter)` 写入 Redis：
   - `SADD gw:sub:{node_id} {filter_hash}`
   - `SADD gw:route:{filter_hash} {node_id}`（反向索引）
   - 本地同时维护 `subscribeStore`（Trie）用于本节点内投递。
2. 发布消息时，查找匹配的 `topic_filter` 集合 → 求节点并集。
3. 若仅本节点命中 → 本地直接投递（零网络开销，覆盖 90%+ 场景）。
4. 若有其他节点命中 → 发布到 `iot.route.{node_id}`，携带原始 topic 与 payload。
5. 目标节点消费后，用本地 Trie 精确匹配订阅者并投递。

**优化**：

- `gw:route` 查询结果本地缓存 1s（订阅关系变更不频繁），降低 Redis QPS。
- 高频 topic filter（如 `v1/devices/+/telemetry`，平台侧订阅）单独维护，排除在广播路径外。
- **平台侧订阅不走 MQTT**：`svc-pipeline` 直接消费 NATS，网关无需关心。
- 节点数 ≤ 8 时可用广播模式（`iot.route.broadcast`）简化实现，超过则切换定向模式。

**投递语义**：

| 场景 | QoS | 保证 |
|---|---|---|
| 本节点内投递 | 0/1 | 与客户端 QoS 一致 |
| 跨节点投递 | 1 | 至少一次，接收端按 `msg_id` 去重 |
| 节点故障 | — | NATS 消息未 ACK 则重投到其他节点（该节点已无连接，消息进离线队列） |

### 1.5 会话管理

| 场景 | 策略 |
|---|---|
| `CleanSession=true` | 会话仅存于内存，节点故障即丢弃（设备需自行重连上报） |
| `CleanSession=false` | 会话元数据持久化到 Redis：订阅列表、QoS1 未确认消息、inflight 窗口 |
| 离线消息 | 落 **分片 Stream** `offline.shard.{0..N-1}`（分片键 `hash(device_id) % N`，N 可配置，建议 N=16~64），TTL = 物模型配置（默认 24h），单设备最大条数 1000 |
| 会话恢复 | 设备重连（可能落到不同节点）时从 Redis + JetStream 恢复，重发未确认消息（DUP=1） |
| 会话过期 | `session_expiry_interval` 到期后清理 Redis 与 Stream |
| 扩容 | 新节点加入后 LB 自动分流新连接；存量连接通过 **max_conn 软限制 + 长连接自然淘汰**（建议连接 TTL 上限 7 天，配合 `ServerKeepAlive` 强制重连）实现再平衡 |

> **设计取舍**：不做主动连接迁移（复杂且易丢消息）。采用**自然再平衡 + 定期强制重连**，配合客户端重连策略，工程复杂度大幅降低。

> **为什么离线 Stream 必须分片（勘误记录）**：早期设计是**每设备一个 Stream**（`offline.{device_id}`）。百万设备即百万个 Stream/Consumer 元数据，NATS 的元数据与 Raft 同步压力不可接受。已改为**分片 Stream**：`offline.shard.{0..N-1}`，每设备一个 **subject**（`offline.shard.{i}.{device_id}`）承载于分片 Stream 内，Stream 数量固定为 N。
>
> **Phase 0 验证项**：压测 Stream/Consumer 元数据规模，给出 N 的选型依据（N 越大隔离性越好，元数据越多）。

---

## 2. 认证、授权与限流

### 2.1 设备认证（三档，默认设备级）

| 档位 | `auth_mode` | 凭据 | 泄露影响面 | 适用 |
|---|---|---|---|---|
| A. 项目级（**显式开启**） | `project` | `clientId=device_key`、`username=AccessToken`、`password=ProjectKey` | **整个租户** | 试用 / PoC / 极小规模；**不作为生产默认** |
| **B. 设备级（默认）** | `per_device` | `clientId=device_key`、`username=device_key`、`password=secret`（32B 高熵随机串） | 单设备 | 生产默认 |
| C. 双向证书 | `mtls` | X.509，SNI 路由，CN = `device_key` | 单设备 | 高安全行业（电力、轨交） |

> **决策反转记录：默认档从 A 改为 B。**
>
> 早期为对齐 ThingsCloud 的「10 分钟接入」体验，把项目级设为默认，并对其施加 6 项强制约束（含 IP 白名单）。评审发现：**该约束在蜂窝网络 / 移动设备 / 动态出口 IP 场景下不可行**，而这类设备在 IoT 场景中占绝大多数 —— 既然最强的补偿措施无法落地，项目级就不能作为生产默认。
>
> 权衡结果：**易用性让位于安全性**。A 档保留为显式开启的「快速接入」模式，仅用于试用与 PoC。相应地 ADR-008 已同步修订。

#### A 档的控制措施（可选开启，非强制）

| 措施 | 要求 |
|---|---|
| IP / 地域白名单 | **可选**：仅当租户有**固定出口 IP** 时建议启用（厂区内网网关、专线）；蜂窝 / 移动设备跳过 |
| 异常检测 | **建议启用**（不依赖 IP）：同一 AccessToken 出现异常地理分布或异常连接速率 → 告警 |
| 强制轮换 | ProjectKey 有效期默认 **90 天**，到期前 30 天提醒；支持一键轮换 + 旧值 24h 并存 |
| 按键审计 | 每次校验失败、轮换写 `t_audit_log` |
| 更严限流 | 单 ProjectKey 失败 20 次/分即锁 10 分钟（B 档为 10 次/分） |
| 显式风险确认 | 开启 A 档需控制台二次确认并接受风险声明；**设备数 > 1000 时禁止开启** |

> **风险声明**：A 档的爆炸半径是**整个租户**。它只应出现在试用、PoC，或设备数极少且出口 IP 固定的场景。

#### 认证流程（Hook: `OnConnectAuthenticate`）

```
1. 解析 clientID / username / password
2. 由 clientID 查设备 → 得到 project_id 与 auth_mode
     （L1 本地 LRU 1 万条 / TTL 60s；未命中查 svc-device）
3. 按 auth_mode 分派校验：
   ├─ A(project)   : clientID 校验设备归属；username 比对 project.access_token_hash；
   │                 password 比对 project.project_key_hash；
   │                 可选 IP 白名单 + 凭据版本 + 失败计数
   ├─ B(per_device): clientID == username == device_key（防串号）；
   │                 password = 设备 secret 原文 → 服务端 Argon2id 校验
   └─ C(mtls)      : 由 TLS 层完成，CN 即 device_key
4. 缓存分级：
     L1 本地 LRU（1 万条，TTL 60s）
     L2 Redis  cache:{pid}:{co}:auth:devtok:v1:{cred_key}（TTL 10m）
     L3 回源 svc-auth /internal/v1/device/authenticate（gRPC，超时 5s，熔断）
5. 通过 → 返回 AuthResult{project_id, device_type_id, thing_model_version,
     auth_mode, allowed_pub[], allowed_sub[], quota}
6. 失败 → 计数（按 IP / device_key / project 三个维度）→ 超阈值进黑名单
```

**降级**：`svc-auth` 不可用时，网关只能用 L1/L2 缓存放行**已认证过**的设备，**拒绝新设备认证**（fail-closed）。

> **为什么 B 档不用 HMAC（勘误记录）**：早期版本写 `password = HMAC-SHA256(device_key, secret)`，但服务端只存 `Argon2id(secret)` —— **Argon2 是单向的，服务端无法重算 HMAC，该流程在数学上不可实现**。
>
> 修正：B 档改为**客户端直接提交 secret 原文**，服务端用 Argon2id 校验。安全性依据：① secret 是 32 字节高熵随机串，不是人类密码；② 传输由 TLS 1.3 保护；③ `clientID == username == device_key` 的强校验阻止跨设备冒用。
>
> 若客户要求抗重放，则走 C 档（mTLS），或启用 nonce challenge-response 变体（需服务端以**可解密**方式保存 secret，见下表的可选行）。

#### 凭据存储规则

| 凭据 | 存储方式 | 说明 |
|---|---|---|
| 设备 secret（B 档） | `Argon2id(secret, salt, t, m, p)` | 32 字节随机，Base64，**仅创建时展示一次**；参数由 Phase 0 压测确定（见下） |
| ProjectKey（A 档） | `Argon2id(...)` → `t_project.project_key_hash` | 租户级，带 `key_version` 支持轮换 |
| AccessToken（A 档） | `SHA-256` → `t_project.access_token_hash` | 高熵随机串，无需慢哈希；**不存明文** |
| Challenge 密钥（可选变体） | Vault / KMS **AES-256-GCM 可解密存储** | 仅在启用 challenge-response 时使用（05 文档 §4 已有该机制） |

#### Argon2 参数与认证容量（Phase 0 必须验证）

原参数 `t=3, m=64MB, p=4` 未经验证。风险场景：**设备重连风暴**（网关重启、网络恢复、批量断电恢复）时大量设备同时冷认证。

| 验证项 | 通过标准 |
|---|---|
| 单次 Argon2 校验的耗时与内存占用 | 在目标机型上实测 |
| L1 / L2 缓存命中率 | 稳态 ≥ 95%；**另需给出重连风暴下的最坏值** |
| 重连风暴压测 | N 台设备 60s 内并发重连，网关 CPU 不饱和、认证 P99 可接受 |
| 参数选型结论 | 按实测给出 `t/m/p` 与网关副本数；**若 64MB 成为瓶颈，可降低 `m` 并提高 `p` 或增加副本** |

> **降级语义的一致性**：`svc-auth` 不可用时只能放行 L1/L2 已认证设备、拒绝新认证（fail-closed）—— 该语义在重连风暴下会更严格，必须与上表一并压测验证。

#### 吊销与轮换

- **吊销**：`secret_version` / `key_version` 递增 + Redis 写 `cache:{pid}:0:auth:revoked:v1:{cred_key}`（TTL = 缓存 TTL），网关缓存命中后强制回源。
- **轮换**：双凭据并存窗口（旧值 24h 内可用），由 `svc-auth` 维护 `*_prev` 字段与 `*_prev_expires_at`。
- **紧急封禁**：单设备 / 单租户（含 A 档 ProjectKey）/ 单 IP 三级黑名单，Redis 全局生效，网关每秒同步一次。
- **一键全量吊销**：租户级操作，用于凭据泄露应急，需二次确认并写审计。

### 2.2 ACL（物模型驱动）

ACL 规则**自动生成**，不手工配置：

```
允许发布：
  v1/devices/{device_key}/telemetry
  v1/devices/{device_key}/attributes
  v1/devices/{device_key}/events
  v1/devices/{device_key}/cmd/reply
  v1/devices/{device_key}/shadow/reported

允许订阅：
  v1/devices/{device_key}/cmd/+
  v1/devices/{device_key}/shadow/desired
  v1/devices/{device_key}/cfg/+
  v1/devices/{device_key}/ota/+

拒绝：其余一切（含通配符订阅 $SYS/#、#、+/+ 等）
```

- 设备**禁止**订阅其他设备 topic（防横向越权），网关在 `OnACLCheck` 中强制校验。
- 透传/网关型设备（`device_type.is_gateway=true`）：允许 `v1/gateways/{key}/devices/{sub_key}/...` 的子设备命名空间，子设备需在平台登记。
- 应用端（App/第三方）**不直连设备 topic**，统一走 `openapi` 与 `iot.cmd.*`，由服务端鉴权。

### 2.3 限流与保护

| 层级 | 机制 | 默认阈值 | 超限动作 |
|---|---|---|---|
| LB / 边界 | 连接速率限制（per IP） | 100 conn/s | 拒绝（TCP RST） |
| 网关 | 未认证连接超时 | 5s | 断开 |
| 网关 | 单 IP 连接数 | 500 | 拒绝 |
| 网关 | 单租户连接数 | 按套餐 | 拒绝（返回 CONNACK 0x97 配额超限） |
| 网关 | 单设备消息速率 | 10 msg/s（可配） | 令牌桶丢弃 + 计数（不拒绝连接） |
| 网关 | 单租户消息速率 | 按套餐 | 同上 |
| 网关 | 单条消息大小 | 32 KB（可配） | 丢弃 + 计数 |
| 网关 | inflight 窗口 | 32 | 阻塞发送（背压） |
| 网关 | 发送队列水位 | 1000 条 / 8 MB | 超水位：遥测丢弃（按优先级） |
| 管道 | 消费滞后 | 10 万条 | 网关侧降速 + 告警 |
| 存储 | 写入失败率 | 5% | 降级：只写 Redis + 落重试流 |

**令牌桶实现**：Redis Lua 脚本原子执行（`EVALSHA`），按 `(scope, id, window)` 计数。网关本地做**二级桶**（本地令牌桶 + 定期从 Redis 补充配额），避免每条消息都打 Redis：

```
每设备本地桶容量 = 阈值 × 1s，每 200ms 向 Redis 申请一次配额批量补充。
Redis 不可用时降级为本地桶（阈值 × 0.5），并上报降级指标。
```

**分级丢弃策略（队列压力时）**：

```
优先级 1（永不丢弃）：cmd.reply, shadow.reported, events(level=critical)
优先级 2（限速）：attributes, events(level=info)
优先级 3（可丢弃）：telemetry
```

### 2.4 报文校验（进入管道前）

1. 长度校验（≤ 32 KB）。
2. JSON 合法性（RFC 8259，拒绝尾随逗号/单引号/NaN/Infinity）。
3. 时间戳格式：**ISO 8601 必须**；同时兼容 Unix 秒/毫秒（自动识别并打标，默认关闭以强制规范）。
4. 属性 key 白名单（来自物模型缓存）。
5. 值类型与范围校验（超出范围 → 打标 `_invalid` 入库但不阻断，避免丢失证据）。
6. 通过后包装统一信封，发布到 NATS。

---

## 3. 多协议适配

### 3.1 协议矩阵

| 协议 | 服务 | 传输 | 认证 | 典型设备 | 一期 |
|---|---|---|---|---|---|
| MQTT 3.1.1 / 5.0 | `gw-mqtt` | TLS 8883 / WS 8084 | Token / mTLS | 全品类 | ✅ |
| HTTP/HTTPS | `gw-http` | TLS 443 | Header Token | NB-IoT / 无 MQTT 栈 MCU | ✅ |
| CoAP | `gw-coap` | UDP / DTLS | Token | LPWAN | 二期 |
| TCP 私有协议 | `gw-tcp` | TLS | 注册包携带凭据 | DTU、老设备 | 二期 |
| Modbus RTU/TCP | `svc-modbus-gw`（云网关轮询） | — | 平台侧配置 | PLC / 仪表 | 二期 |
| 网关子设备 | `gw-mqtt`（命名空间） | — | 网关身份 | Zigbee/LoRa 网关 | 二期 |

### 3.2 HTTP 降级通道

```
POST /api/v1/telemetry
Headers:
  Content-Type: application/json
  X-Device-Key: <device_key>
  X-Auth-Token: <hmac or token>
  X-Ts: <ISO8601>
Body:
  { "temperature": 25.3, "humidity": 62.1 }
```

- 复用同一套认证与 ACL 逻辑（提取到 `pkg/authz`，MQTT/HTTP 共享）。
- 限流更严格（HTTP 建连成本高）：单设备 1 req/s。
- 批量上报：`POST /api/v1/telemetry/batch`，最多 100 条，单次 ≤ 256 KB。
- 响应：`202 Accepted`（异步）；可选 `?sync=1` 返回 `200`（仅用于调试，生产禁用）。

### 3.3 DTU / 透传设备

- 设备以 TCP 长连接接入 `gw-tcp`，首包为注册包（`device_key` + 签名）。
- 注册成功后进入透传模式：设备发送的字节流由**该设备类型的 `raw_parsers`** 解析。
- 下行指令编码为设备约定的字节格式（物模型 `commands[].codec` 定义）。
- 心跳：`gw-tcp` 维护 idle 检测（默认 5 分钟无数据则断开）。
- 该通道的消息与 MQTT 通道**共用同一条管道**，仅接入适配层不同。

---

## 4. 消息管道（svc-pipeline）

### 4.1 职责

1. 消费网关发布的原始消息。
2. 物模型查找与缓存。
3. 原始报文解析（`raw_parsers`，受限沙箱，见 04 文档 §1）。
4. 数据校验、单位换算、时间校正、打标。
5. 幂等去重。
6. 分流写出：GreptimeDB（时序）、Redis（最新值）、NATS（下游规则/配额）。
7. 失败重试与死信。

### 4.2 分区与并行

**先纠正一个早期错误**：曾写「消费端按 `device_id` 做本地 sharding（worker = `hash(device_id) % N`）」。在 **queue group 多副本**下这不成立 —— 同一设备的消息会被随机投给不同消费者实例，实例内的 `hash % N` **保不住跨实例顺序**。本节改为**发布端分片 + 静态分片分配**。

| 项 | 设计 |
|---|---|
| 分区键 | `device_id` |
| 分片方式 | **按 subject 分片**：`iot.telemetry.{project}.shard.{0..N-1}`，发布端计算 `shard = fnv1a(device_id) % N` |
| 消费者 | **每个分片一个 durable consumer**（不是一个大 queue group）；分片与消费者实例**静态绑定** |
| 分片分配 | 由 `StatefulSet` 序号或 NATS KV 协调，维护 `shard-owner` 映射；每实例负责 `N / replicas` 个分片 |
| 实例故障 | 该实例的分片由其他实例接管，**接管期间该分片暂停消费**（不并行消费），保证顺序不破 |
| 扩缩容 | 变更分片归属前先 drain 当前分片（处理完在途消息）再切换；切换期间该分片短暂阻塞 |
| **分片数 N** | **全局固定为常量 `N = 32`**（编译期常量，**不允许按环境/规格配置**）。取值依据：32 可同时满足 Lite（单实例消费全部 32 分片）与 Standard（`实例数 × 4`，最多 8 实例）的安全余量，且是 2 的幂便于取模 |
| 单分片吞吐目标 | ≥ 5000 msg/s（含解析） |
| Stream 配置 | `MaxAge=24h`、`MaxMsgsPerSubject=1000`（防洪）、`Discard=New` |

**哪一层真正需要顺序**（这决定了分片必须覆盖到哪一段）：

| 层 | 是否需要严格顺序 | 说明 |
|---|---|---|
| 时序存储（GreptimeDB） | ❌ 不需要 | 按时间戳存储、范围查询，乱序到达不影响查询结果 |
| 幂等去重 | ❌ 不需要 | 按 `device_id + ts + seq` 去重，与到达顺序无关 |
| **规则引擎的 `prev` 上下文** | ✅ **需要** | `prev` 语义依赖「上一次消息」，乱序会导致差分计算错误 |
| **影子的 reported 合并** | ✅ **需要** | 逐字段合并依赖顺序与版本号 |

> 即：**顺序是为规则与影子服务的，不是为存储服务的**。因此分片方案必须保证**同一 `device_id` 在 `pipeline → rule` 全链路落到同一分片**。

#### 4.2.1 分片分配与 Lite → Standard 迁移

分片是**逻辑概念**，与实例数是解耦的。这一点决定了交付层可以平滑演进：

| 部署形态 | 实例数 | 分片分配 | subject 命名 |
|---|---|---|---|
| **Lite**（单机 / Compose） | 1 | **单实例消费全部 32 个分片** | `…shard.{0..31}` |
| **Standard**（K8s，2~8 实例） | 2~8 | 每实例 32/replicas 个分片，静态绑定 | **完全相同**（不变） |
| **Large**（多节点池） | > 8 | 需扩 N → **不扩**，改为水平拆 project（见下） | 不变 |

**关键设计：`N` 是编译期常量，不是配置项。**

早期写「`N = max(实例数 × 4, …)`，且 N 一经确定不再变更」—— 这两个条件放在一起是**实现不了**的：`实例数` 是运行时变量，而「不再变更」要求编译期固定。若真的按实例数取 N，则 **Lite（1 实例 → N 可能算成 4）迁移到 Standard（4 实例 → N=16）时，subject 名与分片归属全变，历史消息无法续读，必须停机重平衡**。

修正后的规则：

| 规则 | 内容 |
|---|---|
| N 取值 | **`N = 32`，编译期常量** |
| 禁止 | 不允许通过环境变量 / ConfigMap 配置 N；**不同环境必须使用相同的 N** |
| Lite → Standard 迁移 | **无需迁移**：分片归属从「1 实例持有 32 个」变为「instance 持有 8 个」，subject 名与消息都不变。迁移时按 4.2 的「扩缩容」规则 drain 再切换即可 |
| 何时才扩 N | **不扩 N**。当单实例需要持有的分片数导致吞吐不足时（> 8 实例），改为**按 `project_id` 水平拆分到多套集群**（每套集群 N=32），而非扩大 N |
| 为什么 | 扩大 N 需要全量重平衡 + 历史消息重新分片，代价高且不可逆；按 project 拆集群可以逐租户迁移，可回滚 |

> **Lite 单实例消费全部分片的注意点**：单实例下 32 个 durable consumer 会带来 32 个消费协程。若单机资源紧张，可**在 Lite 配置下用一个协程顺序消费 32 个分片**（仍保证分片内串行），代价是跨分片的并行度降低 —— 但顺序语义不变，且这是配置开关而非代码分叉（对齐 01 §5.3「同一份代码 + 配置差异」）。

### 4.3 处理流程（含失败处理）

```
consume(msg)          // NATS JetStream durable consumer, AckWait = 30s
 ├─ 1. 解信封 → 校验 tenant / trace_id / schema_version
 ├─ 2. 幂等检查（两阶段，见下方「幂等与 ACK 的时序」）
 │      ① 查 done 标记：命中 → 重复交付，直接 ACK 并跳过（唯一允许提前 ACK 的情况）
 │      ② SETNX processing 标记（TTL=60s）作并发互斥
 │           占用失败 → **NAK 延迟重试，绝不 ACK**
 ├─ 3. 取物模型（本地 LRU → Redis → PG），未找到 → NAK 延迟重试（最多 3 次）→ DLQ
 ├─ 4. 解析（raw_parsers 沙箱，超时 50ms）
 │      └─ 解析失败 → 计数 + 原样落 event(level=parse_error) → 写 done 标记 → ACK（毒消息不重试）
 ├─ 5. 校验 / 换算 / 时间校正
 ├─ 6. 分流：
 │      ├─ batcher.AddTimeSeries(did, ts, metrics)   // 攒批写入，**必须等待 flush 确认**
 │      ├─ Redis 最新值（可异步；失败不影响 ACK —— 可从 GreptimeDB 重建）
 │      └─ publish iot.telemetry.normalized.{pid}.{dtid}
 └─ 7. **持久化确认之后**：写 done 标记 → 删除 processing 标记 → ACK
```

**幂等与 ACK 的时序（修正两个数据丢失缺陷）**：

早期流程是「先 SETNX 占位 → 处理 → 异步攒批 → 立即 ACK」，存在两条**永久丢数据**路径：

| 缺陷 | 场景 | 后果 |
|---|---|---|
| **ACK 早于持久化** | 步骤 6 入 batcher 后即 ACK，进程在 flush 前崩溃 | 消息已 ACK、数据未落库 → **永久丢失** |
| **幂等标记早于处理完成** | SETNX 成功后崩溃（步骤 3–6 未完成），消息重投 | 被判为重复并直接 ACK → **永久丢失**，且无任何告警 |

修正后的两阶段语义：

| 标记 | 值 | 写入时机 | TTL | 含义 |
|---|---|---|---|---|
| `idemp:{pid}:telemetry:{did}-{ts}-{seq}` | `processing` | 步骤 2② | 60s（≥ AckWait × 2） | **仅作并发互斥**，不代表已完成 |
| 同上 | `done` | **步骤 7，持久化确认后** | 5 min（覆盖 NATS 最大重投窗口） | **代表已落库**，重投可直接跳过 |

规则：

1. **只有 `done` 命中**才允许提前 ACK；`processing` 命中必须 **NAK 延迟重试**。
2. `processing` 标记 TTL 到期后自动释放，崩溃后的重投可重新进入处理。
3. **ACK 语义从「入队后」变为「落库后」**，ACK 延迟 = 攒批窗口（≤ 200ms）。这直接影响延迟 SLO 口径 —— 见 01 文档 §6.1 的三档 SLI 拆分。
4. **`MaxAckPending` 必须 ≥ `2 × batchSize`（默认 2000）** —— 需容纳「一个在途批次（等待 flush）+ 一个待发批次」。若 `MaxAckPending` 小于该值，消费者会在等待 flush 时无法接收新消息，**吞吐被自身的 ACK 窗口卡死**。

**毒消息（Poison Message）处理**：

- 同一消息重试 3 次仍失败 → 写入 `t_dlq` + 对象存储保存原始报文 → **写 `done` 标记 → ACK 释放**（否则会无限重投）。
- DLQ 每日巡检，按 `device_type` 聚合，重复出现则自动生成工单并告警。

**攒批器（Batcher）设计**：

```go
type Batcher[T any] struct {
    buf       []T
    maxSize   int           // 1000
    maxWait   time.Duration // 200ms
    flushCh   chan struct{}
    onFlush   func([]T) error   // 必须返回持久化结果：nil 才算成功
    onFailure func([]T, error)  // 部分失败按行进重试流
}
```

**flush 触发条件**：`len(buf) >= maxSize` **或** `距上次 flush ≥ maxWait`，**任一先到即 flush**。

> ⚠️ 常见笔误：写成 `max(200ms, 1000 行)` 是**错的**（那是取最大值 = 两个条件都满足才 flush，最坏延迟 200ms 且批更大）。正确语义是「或」。

- **按分区独立 instance**，避免跨分区锁竞争。
- 部分失败：GreptimeDB 批量写入支持逐行结果判定，失败行单独进重试流，成功行不重放。
- 内存上限：单实例缓冲区 ≤ 4 MB，超限强制 flush 并触发背压。
- 优雅退出：`SIGTERM` → 停止消费 → flush 剩余数据（最长 10s）→ 退出。

### 4.4 QoS1 PUBACK 时机（端到端 at-least-once 的最后一环）

前几节讲的是「NATS → 消费者 → 存储」这一段。但端到端 at-least-once 还有**第一段**：`设备 → 网关`。若这一段提前确认，崩溃窗口内数据同样会丢。

**规则：网关对设备 PUBLISH 的 PUBACK，必须在 NATS publish 确认之后发出。**

```
设备 ──PUBLISH(QoS1)──► gw-mqtt
                          │ ① 认证 / ACL / 限流 / 预检
                          │ ② publish 到 iot.telemetry.{project}.shard.{i}
                          │ ③ **等待 NATS JetStream PublishAck**
                          ▼ ④ PublishAck 到达后，才向设备回 PUBACK
设备 ◄──PUBACK────────────┘
```

| 项 | 规定 | 理由 |
|---|---|---|
| PUBACK 时机 | **NATS `PublishAck` 到达之后** | 若先 PUBACK 再 publish，网关在两步之间崩溃 → 设备认为已送达、不重传 → **该消息永久丢失，且无任何检测手段** |
| 等待超时 | 默认 **5 s**。超时未收到 PublishAck → **不回 PUBACK**（设备将重传），并计入 `gw_puback_timeout_total` | 让设备侧的重传机制成为兜底 |
| Stream 副本 | 遥测 Stream 建议 `Replicas ≥ 1`；**要求 RPO=0 的租户用 `Replicas=3`** | 单副本下 PublishAck 只代表写入 leader 内存+磁盘，leader 宕机仍可能丢 |
| 吞吐影响 | 每设备消息在网关多一次 RTT（本地 NATS，通常 < 1 ms） | 可接受；若 NATS 与网关跨机，需评估网络 RTT 对 `gw_msg_latency` 的影响 |
| 批量优化 | **禁止**把多个设备的 PUBLISH 合并等待同一个 PublishAck | 会破坏「一设备一确认」的语义，无法定位是哪条消息失败 |
| 与 §4.3 的关系 | §4.3 保证「NATS → 存储」不丢；本节保证「设备 → NATS」不丢。**两者缺一，端到端 at-least-once 都不成立** | — |

> **实现位置**：`gw-mqtt` 的 `OnPublished` Hook 为异步，无法直接满足「先确认再 PUBACK」。需改为在消息处理链中**同步等待**：`mochi-mqtt` 的 PUBLISH 处理路径中，用 `OnPublish` 返回错误来阻止自动 PUBACK，业务代码在 PublishAck 到达后显式触发 PUBACK。**这是 ADR-001「内嵌 broker」相对 EMQX 的一个明确定制点**，Phase 0 必须验证该 Hook 语义可行。

#### 4.4.1 验证结论（Phase 0 · A2，2026-10-01 · **通过**）

**结论：ADR-001 的定制点成立** —— `mochi-mqtt` v2.7.9 可以在「等待 NATS `PublishAck` 后再回 PUBACK」的语义下工作，无需自建 PUBLISH 处理路径，也无需回退 EMQX。

| 事实 | 内容 |
|---|---|
| **挂载点** | `mqtt.Hook` 的 `OnPublish(cl, pk) (packets.Packet, error)`，**同步**执行于该连接的收包协程内 |
| **阻止自动 PUBACK 的精确机制** | 返回 `packets.ErrRejectPacket` 时，`server.go:processPublish` 立即 `return nil` —— 既不回 PUBACK，也不投递给本地订阅者。**必须用这个哨兵错误**：返回其他 error 会继续走原生路径并照常回 PUBACK（静默失效，极难发现） |
| **显式确认** | 业务侧构造 `packets.Packet{Type: Puback, PacketID, ReasonCode: packets.QosCodes[1].Code}` 并调用 `cl.WritePacket(ack)`；字段构造对齐库内部 `buildAck`，避免 MQTT 5 的 Properties 语义分叉 |
| **超时** | 复用同一返回路径：不回 PUBACK 即可，设备按 QoS1 重传。实测「未收到 PUBACK」与「收到 PUBACK」两种行为可被客户端区分 |
| **实测延迟** | 设备 PUBLISH → PUBACK 往返 **0.52 ~ 1.12 ms**（5 次采样，中位 ≈ 0.92 ms；含 JetStream 落盘 + 一次本地 RTT）。与 §4.4「本地 NATS 通常 < 1 ms」的假设吻合，且远低于 §3 的 100 ms P99 接入确认预算 |

**验证用例**（`internal/gateway`）：总线未确认时无 PUBACK；超时后无 PUBACK 且计入 `gw_puback_timeout_total`；**注入网关崩溃**（收包后、确认前关闭进程）后设备重传、消息不丢；对照实验证明原生 broker 会立即回 PUBACK（排除断言假阳性）；真实 JetStream 上 PUBACK 之后消息可被读回。

**代价与已知边界**（实现时按此验收，不要当成缺陷）：

| # | 边界 | 影响与处置 |
|---|---|---|
| 1 | **阻塞该连接的收包协程**（最长 = `PubackTimeout`，默认 5 s） | 这是「一设备一确认」的直接后果（本节已禁止合并等待）。对 `KeepAlive ≥ 60s` 的设备无协议影响，但 `PINGREQ` 响应同样被推迟 —— **需在 A4（5 万连接 24h）实测其对重传与掉线判定的影响** |
| 2 | **QoS2 未被覆盖** | 端侧契约只使用 QoS0/QoS1；实现选择**显式拒绝 QoS2**并计入 `gw_unsupported_qos_total`，而不是静默降级（后者会让消息绕过「先持久化再确认」） |
| 3 | **配置失误会暴露为重传而非丢数据** | 若业务 subject 未被任何 JetStream Stream 捕获，总线返回 `no response from stream`；网关按「未确认」处理、拒绝 PUBACK。**这是期望行为**（故障可见，而不是静默丢数据） |
| 4 | **优雅关闭会取消在途等待** | 关闭时未确认的消息不回 PUBACK，由设备重传兜底。否则关闭会被拖长到超时上限 |
| 5 | **依赖库内部行为** | 该机制依赖 `processPublish` 对 `ErrRejectPacket` 的处理，属库内部实现。**必须锁版本**（当前 `v2.7.9`），且 `TestA2_PubackOnlyAfterPersist` 即为回归哨兵 —— 库若改变该行为，该用例会立刻失败 |

> 验证代码：`internal/gateway`（`hook.go` 为上表机制的实现，`a2_test.go` / `nats_test.go` 为判据）。运行方式见 `09-handoff.md` §5.2。

### 4.5 背压与过载保护

| 水位 | 动作 |
|---|---|
| 消费滞后 < 1 万 | 正常 |
| 1 万 ~ 10 万 | 上报 `pipeline_lag_high` 指标，触发 HPA 扩容（按 lag 指标） |
| 10 万 ~ 50 万 | 网关侧对 `telemetry` 降速（通过 NATS 广播 `iot.ctrl.throttle`，网关订阅后降低令牌桶速率） |
| > 50 万 | 网关按优先级丢弃 `telemetry`，保留 `cmd.reply` / `events(critical)`；触发 P1 告警 |
| 存储写入连续失败 1 min | 只写 Redis + 落重试流；`telemetry` 仍 ACK（数据进重试流不丢） |

---

## 5. 端侧 SDK 契约

### 5.1 Topic 命名（固定，`v1` 不可变）

```
上行（设备→平台）
  v1/devices/{device_key}/telemetry          遥测
  v1/devices/{device_key}/attributes         属性（静态/配置）
  v1/devices/{device_key}/events             事件
  v1/devices/{device_key}/cmd/reply          命令应答
  v1/devices/{device_key}/shadow/reported    影子上报
  v1/devices/{device_key}/ota/progress       OTA 进度

下行（平台→设备）
  v1/devices/{device_key}/cmd/{cmd_key}      命令
  v1/devices/{device_key}/shadow/desired     影子期望
  v1/devices/{device_key}/cfg/{cfg_key}      配置下发
  v1/devices/{device_key}/ota/{action}       OTA 动作
```

### 5.2 Payload 规范

```json
// 遥测
{ "ts": "2026-10-01T08:12:33.421Z", "seq": 1042,
  "data": { "temperature": 25.3, "humidity": 62.1 } }

// 属性
{ "ts": "...", "data": { "firmware_version": "v2.1.3" } }

// 事件
{ "ts": "...", "event": "fault", "level": "warn", "data": { "code": 12 } }

// 命令应答
{ "ts": "...", "id": "<correlation_id>", "code": 0, "msg": "ok", "data": {} }
```

**端侧强制约定**：

| 项 | 要求 | 理由 |
|---|---|---|
| `ts` | ISO 8601 UTC，带毫秒 | 平台统一时间轴，拒绝 Unix 时间戳 |
| `seq` | 设备侧单调递增（重启后从 0 开始可接受，平台按 `ts+seq` 去重） | 幂等去重 |
| QoS | 遥测 QoS1、属性 QoS0、事件 QoS1、命令应答 QoS1 | 成本与可靠平衡 |
| CleanSession | **false**（需离线消息时），否则 true | 离线可达 |
| KeepAlive | 60~300s，低功耗设备用 300s | 在线判定精度 vs 功耗 |
| LWT | `v1/devices/{key}/events` 发 `{"event":"offline"}`，retain=true | 掉线检测（**语义见下方说明**） |
| 离线缓存 | 设备侧环形缓冲，最多 N 条，重连后补发（带原始 `ts`） | 弱网数据完整性 |
| 重连 | 指数退避 1s→2s→4s…最大 120s，带 ±20% 抖动 | 防重连风暴 |
| 时间同步 | 首次连接后调用 `cmd/time_sync` 校准 | 保证 `ts` 精度 |

**LWT 的三个必须说明的语义边界**：

| # | 事实 | 影响 |
|---|---|---|
| 1 | **LWT 只在非正常断线时由 Broker 发布**（TCP 异常断开、KeepAlive 超时、Broker 检测到故障）；**客户端正常发 DISCONNECT 不会触发 LWT** | 正常下线不会产生 `offline` 事件 → 必须有「正常 DISCONNECT 处理路径」 |
| 2 | **Broker 不会把 LWT 自动转成内部事件** —— LWT 只是往某个 topic 发一条消息，需要**网关 Hook / 桥接显式实现**：捕获 LWT topic → 发布内部 `iot.device.offline` | 机制上等同于 §1.4 的跨节点路由；实现位置在 `gw-mqtt` 的 Hook 层（`OnPublish` 或会话状态回调） |
| 3 | **TTL 扫描只能作兜底**，不能作为主路径 | 主路径 = LWT（异常断线）+ 显式 DISCONNECT（正常下线）；兜底 = `svc-device` 每 30s 扫 Redis 中 TTL 过期但在线标记未清的设备（对应 01 文档 §6.3） |

**实现要点**：

```
设备异常断线 → Broker 发布 LWT 到 v1/devices/{key}/events
                → gw-mqtt Hook 捕获 → 发布内部 iot.device.offline
                → svc-device 消费 → 更新 Redis 在线态 + PG last_seen_at

设备正常 DISCONNECT → gw-mqtt 会话回调 → 直接发布 iot.device.offline
                       （不能依赖 LWT）

双路径都未生效（Hook 丢失 / Broker 崩溃）→ 30s TTL 扫描兜底
```

### 5.3 SDK 交付物

| 端 | 形态 | 说明 |
|---|---|---|
| C（MCU） | 源码 + 静态库 | 不依赖 OS，事件循环式；支持 ESP-IDF / STM32 HAL / FreeRTOS |
| ESP-IDF 组件 | `idf_component.yml` | 基于内置 MQTT 库的薄封装 |
| Python | `pip install` | 网关/边缘设备 |
| Go | module | 边缘网关 |
| Linux C | 源码 | 带 TLS、断点缓存、多设备聚合 |
| 微信小程序 / Web | `npm` | 应用端订阅（走 WSS） |

**SDK 质量门禁**：每个 SDK 必须有「弱网 + 断线 + 时间偏移」三类场景的自动化测试。

---

## 6. 网关性能调优

### 6.1 主机内核参数

```bash
# /etc/sysctl.d/99-iot.conf
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_synack_retries = 2
net.ipv4.tcp_max_tw_buckets = 2000000
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_keepalive_time = 300
net.ipv4.ip_local_port_range = 10240 65535
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.core.netdev_max_backlog = 65535
net.core.netdev_budget = 600
net.ipv4.tcp_fastopen = 3
fs.file-max = 2097152
fs.nr_open = 2097152
```

```bash
# 容器/进程限制
ulimit -n 1048576
```

**启动时必须校验**上述参数，未生效则拒绝启动并明确报错（避免带病运行）。

### 6.2 网络与调度

| 项 | 做法 |
|---|---|
| 网络模式 | `hostNetwork: true` + 独立节点池（或用 Cilium 的 eBPF host-routing 替代 kube-proxy） |
| conntrack | 提高 `nf_conntrack_max` 或对网关流量使用 `NOTRACK`（LB 直连） |
| 中断亲和 | 启用 `irqbalance` 或手动绑核，网卡多队列与 CPU 亲和 |
| CPU | 网关节点独占（不与其他服务混部）；`GOMAXPROCS` = 物理核数 |
| Go 调优 | `GOGC=200`（连接类服务，降低 GC 频率）、`GOMEMLIMIT` = 容器内存 × 0.8 |
| TLS | 会话票据复用（`SessionTicketsDisabled=false`）；启用 `TLS 1.3`；必要时引入 BoringSSL/多线程握手（可选） |
| BPF | 可选：`SO_REUSEPORT` 多监听器 + XDP 做连接级限速 |

### 6.3 容量参考（单节点，8C16G，TLS 1.3）

| 场景 | 连接数 | 消息速率 | 备注 |
|---|---|---|---|
| 长连接保持（心跳 60s） | 8 万 ~ 12 万 | 低 | 内存约 12 KB/连接 |
| 中等负载（每连接 1 msg/s） | 5 万 | 5 万 msg/s | CPU 瓶颈在 TLS + 序列化 |
| 高吞吐（每连接 20 msg/s） | 1 万 | 20 万 msg/s | 需多节点 |

> 实际以压测为准。**容量规划按实测值的 60% 作为安全水位**。

### 6.4 压测方案

| 项 | 内容 |
|---|---|
| 工具 | `emqtt-bench`（连接与吞吐）、`k6`（API）、自研 Go 压测器（可控 Payload 与断连行为） |
| 场景 1 | 50 万连接建立 + 保持 30 min，观察内存/GC/句柄 |
| 场景 2 | 50 万 msg/s 持续 1 h，观察 P99 延迟与丢弃率 |
| 场景 3 | 锯齿波负载（10 万 ↔ 50 万连接）连续 3 次，验证伸缩与再平衡 |
| 场景 4 | 混沌注入：随机 kill 网关 Pod、网络分区、Redis 不可用、GreptimeDB 写失败 |
| 验收 | P99 < 100ms；内存无单调增长（连续 6h RSS 波动 < 5%）；无消息重复入库（幂等验证）；无连接泄漏 |
| 报告 | 每次发布前跑基准场景，结果归档对比（防性能回归） |
