# 04 · 规则引擎、告警、自动化与 OTA

## 1. 规则引擎

### 1.1 分层架构与选型理由

规则能力拆成三层，各管一件事。**这是本设计与 ThingsCloud「统一云函数」形态最大的分歧点**（ADR-003）：

| 层 | 承担什么 | 技术 | 覆盖比例 | 为何不合并 |
|---|---|---|---|---|
| **L1 条件 DSL** | 「什么时候触发」：阈值、区间、逻辑组合、时间窗、标签匹配 | `expr-lang/expr`（MIT） | ~90% | 类型安全、**编译期即可校验**、无运行时逃逸面、求值 1–2 µs |
| **L2 DAG 编排** | 「触发之后做什么」：条件 → 动作链 → 分支 / 延时 / 并行 / 重试 | 自研 DAG 执行器 | 全部编排 | 编排语义简单且稳定；执行体必须是受控的 Go 函数，引入重量级流处理引擎不划算 |
| **L3 转换脚本** | 「数据怎么变」：非标私有协议编解码、复杂坐标/单位换算 | 受限 JS 沙箱（`goja`） | ~5% | **仅作为逃生舱**；若把它当地基，安全面与性能双输 |

**关键原则**：**能用 L1 表达的，绝不允许用 L3**。L3 需租户白名单开启 + 单独审批 + 更严资源限制，**默认全局关闭**。

**被否决的方案（记录以备复审）**：单一 JS 引擎（对齐 ThingsCloud 的「云函数」形态）。

| 否决理由 | 说明 |
|---|---|
| 安全面 | 任意 JS 意味着沙箱逃逸、原型链污染、资源耗尽三类风险常驻 |
| 可分析性 | 脚本无法可靠静态分析；而条件判断这种高频逻辑本可编译期校验 |
| 性能 | goja 求值 ~200 µs/次，`expr` 为 ~2 µs/次，差两个数量级 |
| 收益 | 90% 的规则形如 `temperature > 60 && tags.site == 'sh'`，用 JS 表达毫无必要 |

**触发点分类**（三层共用同一套触发点定义）：

| 类型 | 触发点 | 典型用途 | 主要用哪层 |
|---|---|---|---|
| 上报预处理（`preprocess`） | 解析后、写库前 | 单位换算、去噪、字段补齐、异常值标记 | L1 + L3 |
| 上报规则（`on_report`） | 写库后 | 派生指标、跨设备聚合、触发告警/场景、转发第三方 | L1 + L2 |
| 下发预处理（`pre_command`） | 命令下发前 | 参数转换、合法性拦截、审批 | L1 + L2 |
| 下发规则（`on_command_result`） | 命令应答后 | 结果校验、联动 | L1 + L2 |

### 1.2 L1：条件 DSL（expr）

**两段式过滤**：先按元数据（设备类型 / 分组 / 标签）做**拓扑匹配**，命中的规则才进入 `expr` 求值。这让 95% 的 (消息 × 规则) 组合在 ≤ 1 µs 内被排除，是性能的第一道闸门。

```go
// 编译期：规则保存时执行一次，编译失败直接拒绝保存（这就是最好的质检）
//
// ⚠️ 校验**不是** expr 做的（详见 §1.2.1）：物模型字段是小写动态键，
// expr 的类型化环境覆盖不到它们，因此字段/类型/白名单校验由我们自己在
// AST 上完成，expr 只负责解析与生成字节码。
tree, _ := parser.Parse(rule.Match.Expr)
if cerr := rules.Check(rule.Match.Expr, &tree.Node, schema); cerr != nil {
    return cerr // 含定位、字段建议，直接回显控制台
}
prog, err := expr.Compile(rule.Match.Expr,
    expr.Env(rules.NewEnv()),      // 只声明「五个命名空间存在」
    expr.AsBool(),                 // 强制布尔结果，防「真值判断」歧义
    expr.DisableAllBuiltins(),     // 纵深防御：内置函数整体关闭
    expr.MaxNodes(rules.MaxNodes), // 规模上界，取代运行时超时（见 §1.7）
    // + 白名单函数逐个 expr.Function 注册
)

// 热路径求值：每个消费 goroutine 持有一个 Runner 复用 VM
ok, err := runner.Eval(prog, env)  // P50 ≈ 0.6 µs，P99 ≈ 1.3 µs（实测）
```

| 项 | 规定 |
|---|---|
| 可用语法 | 比较、逻辑运算、算术、`in` / `not in`、`matches`（RE2）、字符串函数、时间比较 |
| 可用变量 | `msg.*`、`meta.*`、`prev.*`、`window.*`、`state.*`（`state` 是映射：`state.alarm` 或 `state['alarm']`；**`state.get(key)` 不可用**，见 §1.2.1）。**`prev` 的取值语义见下方专节**，不可自行假设 |
| **内置函数白名单** | 见下方专表。**只有该表内的函数可调用**，其余一律编译期报错 |
| 禁用 | 白名单外的任何函数调用、赋值、循环、成员写入；JS 风格方法（`parseInt` / `substring` / `payload.toString`）**不可用** |
| 类型校验 | 编译期完成，由**自建的 AST 校验器**基于物模型类型表完成（不是 expr 的 Env 检查，见 §1.2.1）；错误信息带定位、源码片段与「是否想用 X」建议 |
| 缓存 | `rule_id + version + expr_hash` → `*vm.Program`，LRU 10000 条，命中率 ≥ 99.99% |
| 正则安全 | 使用 Go 原生 RE2，**天然无回溯爆炸**；额外限制正则长度 ≤ 512 字符 |
| 规模上界 | **编译期节点数 ≤ 512**（`expr.MaxNodes`）。原定的「20 ms 运行时超时」已废弃 —— 它每次求值要多付一次 goroutine/channel 开销，直接吃掉 P99 预算；见 §1.7 |

#### `prev` 的取值语义（必须明确定义，规则引擎据此编码）

`prev` 用于差分与趋势判断（如「温度较上次上升超过 5℃」）。**早期文档只在变量表里列了 `prev.*`，却从未定义取值口径** —— 这在实现时无法编码。现定义如下：

**定义：`prev` = 该设备该指标在 `ts` 序（而非到达序）上的「上一条已落库记录」。**

| 维度 | 规定 | 理由 |
|---|---|---|
| **排序依据** | **按 `ts`（设备上报时间）排序**，不按到达序 | 分片消费下同一设备仍可能因重投而乱序到达（见 03 §4.3 的 NAK 重试）；按到达序取值会让「上一次」随投递时序漂移，规则结果不可复现 |
| 取值来源 | **已落库的最后一条**（从 GreptimeDB 或最新值缓存读取），**不含当前消息** | 只有落库的数据才是「已确认的历史」；用未落库的缓冲值会让回滚后的重算结果不一致 |
| 同 `ts` 重复 | 取 `seq` 最大者；`seq` 相同则取 `ingested_at` 最晚者 | 设备时钟回拨时可能出现同 `ts` |
| `seq` 回绕 | **不作为跨 `ts` 的排序依据**，仅用于两处：① 幂等去重（见 03 §4.3）；② **同一 `ts` 内的并列裁决**（见上方「同 `ts` 重复」行）。`prev` 的**主序只看 `ts`** | `seq` 是设备本地单调计数，重启后从 0 开始，不能作为全局序；但**同一设备、同一 `ts`** 的两条消息用 `seq` 区分是安全的 |
| **乱序到达**（新消息 `ts` < 已落库最新 `ts`） | **`prev` 取「按 `ts` 排序时位于当前消息之前的那条」**，即历史插入位置的前一条，**不是最新一条** | 这是 `prev` 语义的关键：迟到数据必须插入到正确的时间位置，而不是当成"最新的下一条" |
| 首条消息 | `prev` = `null`；表达式必须能处理 `null`（`prev == null ? 0 : ...`） | — |
| 跨重启 / 跨分片迁移 | `prev` 从**持久化数据**重建，不依赖内存 | 分片接管后语义不变 |

**表达式写法约束**：

```
✅ 允许：  prev.temperature != null && msg.temperature - prev.temperature > 5
✅ 允许：  prev == null || msg.temperature > prev.temperature
❌ 禁止：  prev.temperature > 5        // 未处理 null，首条消息会求值失败
```

`prev` 是**只读快照**（当前消息的部分字段副本），不是可变对象；写入 `prev.*` 编译期报错。

**读取路径与查询语义**：

| 路径 | 触发条件 | 实现 |
|---|---|---|
| **快路径（非迟到，占绝大多数）** | 当前 `msg.ts` **≥** 缓存中的 `_ts`（新消息在时间上就是最新一条） | 直接读 Redis 最新值缓存 `cache:{tenant}:{company}:device:last:v2:{did}`（含 `_ts` / `_seq` 元字段），**零额外查询** |
| **慢路径（迟到消息 / 缓存未命中）** | `msg.ts` **<** 缓存 `_ts`，或 Redis 未命中 | 回落 GreptimeDB 单点查询 |

**慢路径必须取「时间上的前一条」，不能取「最新一条」**：

```sql
-- ❌ 错误：取到的是最新一条，对迟到消息而言是「后一条」，与维度 5 的语义相反
SELECT * FROM telemetry
 WHERE project_id = $1 AND device_id = $2
 ORDER BY ts DESC LIMIT 1;

-- ✅ 正确：取 ts 序上严格位于当前消息之前的那条
SELECT * FROM telemetry
 WHERE project_id = $1 AND device_id = $2
   AND (ts, seq) < ($3, $4)              -- ($3,$4) = 当前消息的 (ts, seq)
 ORDER BY ts DESC, seq DESC
 LIMIT 1;
```

> **这正是维度 5（乱序到达）与早期实现互斥的地方**：早期只写了 `ORDER BY ts DESC LIMIT 1`，对迟到消息查出的其实是「最新的那条」（即时间上的**后一条**），规则引擎按文档**写不出**维度 5 要求的行为。上面的**行值比较 `(ts, seq) <` + 复合降序**才是维度 5 的可执行形式；`seq` 在此仅作**同一 `ts` 内的并列裁决**，与上表「`seq` 回绕」行一致，**没有**把 `seq` 升格为跨 `ts` 的序。

> **`seq` 缺失的兼容处理**：若物模型未声明 `seq`、设备也未上报，退化为 `ts < $3 ORDER BY ts DESC LIMIT 1`，并计数指标 `prev_tiebreak_degraded_total`（用于评估是否值得在 SDK 契约里强制上报 `seq`）。

**Redis 只加速快路径**：迟到消息**不更新** `cache:...:last`（最新值不应被历史数据覆盖），因此**迟到消息必然走慢路径是预期行为，不是缺陷**。两条路径都取不到时 `prev = null`，**不得阻塞或重试**（规则必须容忍 `null`）。

**内置函数白名单**（`expr` 环境注入的纯函数，全部为 Go 实现）：

| 类别 | 函数 |
|---|---|
| **二进制读取**（`raw_parsers` 主力） | `int8` / `uint8` / `int16_be` / `uint16_be` / `int16_le` / `uint16_le` / `int32_be` / `uint32_be` / `int32_le` / `uint32_le` / `float32_be` / `float64_be`、`hex_bytes(payload, offset, len)`、`bcd_to_int(payload, offset, len)` |
| 数值 | `abs` / `ceil` / `floor` / `round` / `clamp` / `min` / `max` / `pow` |
| 单位换算 | `convert(v, from, to)`（kPa/bar/℃/℉/m³/kWh 等） |
| 字符串 | `len` / `contains` / `has_prefix` / `has_suffix` / `upper` / `lower` / `trim` |
| 时间 | `time_now` / `time_format` / `time_add` / `time_diff_s` / `time_trunc` |
| 地理（可选） | `wgs84_to_gcj02` / `gcj02_to_bd09` / `distance_m` |
| 校验 | `crc16` / `crc32` / `luhn_ok` |

所有函数签名统一为 `fn(value...) → scalar`，**无副作用、无 IO、无全局状态** —— 这也是 `expr` 层无需沙箱的原因。

> ⚠️ **勘误**：早期在 `02-domain-and-data.md` 的 `raw_parsers` 示例中写了 `parseInt(payload.substring(2,6),16)/10` —— 那是 JS 写法，**不在白名单内**。现已改为 `int16_be(payload, 1) / 10.0`，并补齐上表的二进制读取函数。

> **核心收益**：规则错误**前置到编写阶段**。用户在控制台写错字段名，立刻得到「`msg.temp` 不存在，是否想用 `msg.temperature`？」这类反馈，而不是上线后静默失效。

#### 1.2.1 验证结论（Phase 0 · C1，2026-10-01 · **通过，但实现方式与原文不同**）

**一句话**：编译期校验与求值性能都达标，但**校验不能靠 `expr.Env` 的类型化环境** —— 物模型字段是小写动态键，Go 结构体字段必须导出，两者根本对不上。实现改为**自建 AST 校验器 + 物模型类型表**，expr 只负责解析与生成字节码。过程中还发现 3 处文档写法不可用、1 个安全缺口、1 处未定义的运行时语义。

**① 为什么 `expr.Env(RuleEnv{})` 走不通（实测）**

| 环境形态 | `msg.temperature` | `msg.typo` | `msg.temperature > 'abc'` | `msg.running && msg.temperature` |
|---|---|---|---|---|
| `expr.Env(Go 结构体)` | ❌ `has no field temperature` | ❌ 能报错 | ❌ 能报错 | ❌ 能报错 |
| `expr.Env(map[string]any)` | ✅ | **✅ 编译通过（漏检）** | **✅ 编译通过（漏检）** | **✅ 编译通过（漏检）** |

- expr 的成员访问**大小写敏感**：结构体字段 `Temperature` 匹配不到 `msg.temperature`；
- 而 `reflect.StructOf` **拒绝创建小写字段名**（`field "running" is unexported but missing PkgPath`），
  所以「按物模型动态造一个类型化结构体」这条路是死的；
- 换成 `map[string]any` 后 expr **完全不做字段与类型校验** —— 三条错误写法全部通过。

**结论：物模型的字段校验必须自己做。** 实现见 `internal/rules/gate.go`：`parser.Parse` 出 AST，
用物模型类型表逐节点推导类型，一次性返回全部问题。

**② 校验器覆盖的规则**（`internal/rules`，18 个用例）

| 规则 | 行为 |
|---|---|
| 字段名 | `msg.*` / `prev.*` 必须在物模型里；`meta.*` 限静态字段表；`window.*` 必须是**本规则已声明**的聚合；`meta.tags.*` 按设备类型标签表校验 |
| 字段建议 | 未知字段给出最近候选（前缀缩写 + 编辑距离），如 `msg.temp` → `temperature` |
| 类型 | 比较/算术/逻辑/`in`/`matches`/三元的操作数类型逐一校验 |
| 函数白名单 | 41 个函数（全表实现），含参数个数与类型；非白名单函数编译期报错 |
| 禁用语法 | `let` 声明、成员方法调用、谓词语法（`all`/`filter`/`map`/…） |
| 正则安全 | `matches` 右侧必须是字面量且长度 ≤ 512 |
| `prev` 判空 | 引用 `prev.*` 前必须有 `prev != nil` 短路保护，否则编译期报错 |
| 可选指标兜底 | 标记为「可能缺失」的指标必须写成 `(msg.x ?? 0)` |
| 结果类型 | 根表达式必须是布尔 |

**③ 文档写法勘误（全部实测，勿再照抄）**

| 原写法 | 实测 | 正确写法 |
|---|---|---|
| `prev == null` | expr 只有 `nil`，`null` 是未知标识符（编译期报错） | `prev == nil` |
| `state.get(key)` | 解析不到方法（Go 方法名必须导出，`get` 不存在） | `state.key` 或 `state['key']` |
| `matches(a, b)`（若读作函数） | `matches` 是**中缀运算符**，函数形式不存在 | `a matches '^x'` |
| `msg.at > time_now() - 60` | `time.Time` 不能与 `int` 相减 | `time_diff_s(time_now(), msg.at) > 60` |
| `msg.temperature ?? 0 > 60` | `??` 与比较运算符不能混用 | `(msg.temperature ?? 0) > 60` |

**④ 安全缺口：`expr.DisableAllBuiltins()` 挡不住谓词语法**

实测 `all` / `none` / `any` / `one` / `filter` / `map` 在**解析阶段**就变成 `PredicateNode`，
绕过了 expr 的 `Builtins` 表 —— 关掉内置后它们**依然编译通过**。
因此「白名单外的函数一律编译期报错」这条规定**必须**在 AST 层再拦一次（`scanBanned`），
不能只依赖库的选项。另外 `let` 在 expr 里是支持的，我们**主动禁用**以保持可静态分析。

**⑤ 求值性能（实测，单条表达式轮流混合）**

| 场景 | 耗时 | 分配 |
|---|---:|---:|
| `expr.Run` 成本地板（`1 > 0`） | 213 ns | 1 alloc / 32 B |
| 单条件 `msg.temperature > 60` | 382 ~ 658 ns | 2 allocs / 48 B |
| 典型 `msg.temperature > 60 && meta.tags.site == 'sh'` | ~1.35 µs | 4 allocs / 80 B |
| 同上，改用 `Runner`（复用 VM） | ~1.24 µs | 3 allocs / 48 B |
| 编译缓存命中 | 121 ns（0 分配） | — |
| 冷编译（保存规则时一次） | 142 µs | — |

**验收线（§1.7：求值 P99 ≤ 2 µs）**：`Runner` 路径 6 轮实测 ——
**P50 0.60~0.81 µs、P90 0.77~1.54 µs、P99 0.98~1.74 µs → 达标，余量 1.15~2.04×**。

> ⚠️ **余量偏薄，且随宿主负载波动**（同一份代码 6 轮里 P99 最高 1.74 µs，已用掉 87% 预算）。
> 本次是共享宿主；**上生产前必须在目标机型上复测**，并把 P99 纳入 §3.4 的常规监控。
>
> 首版直接用 `expr.Run` 时 P99 = 2.03 µs，**骑线未过**；差异只来自每次求值多一次 VM 池化进出与一次分配
>（~1.24 µs → ~1.35 µs 均值）。**在这类预算里，常数项就是结论本身** ——
> 因此 `Runner` 不是可选优化，而是达标的前提。

**单实例吞吐**（§1.7 要求 ≥ 50 万次/秒纯条件）：单核 ~80 万次/秒，达标。

**⑥ 由此修订的两条规定**

| 原规定 | 修订 | 理由 |
|---|---|---|
| 20 ms 运行时超时兜底 | 改为**编译期 `expr.MaxNodes(512)`** | `expr` 无循环无 IO，耗时上界由 AST 规模决定；goroutine+select 做超时每次要多花一次调度，直接吃掉 P99 预算 |
| 求值用 `expr.Run` | 热路径用 `Runner`（每 goroutine 一个 VM） | 见 ⑤：池化开销让 P99 从 ~1.3 µs 涨到 2.03 µs |

**⑦ 新规定的运行时语义：缺失指标**

设备未上报某指标时 `msg.x` 求值为 `nil`，`nil > 60` 会让**整条规则求值失败**（而不是 false）。
两条后果必须一起处理：

- **调用方禁止把求值失败当成 false** —— 两者在「是否触发动作」上后果相反；
  失败按 `error_policy` 分派并计数（§1.5）。
- **编译期强制兜底**：物模型里标记为「可能缺失」的指标必须写 `(msg.x ?? 0)`，否则拒绝保存。

**⑧ 未验证项（不要当成已验证）**

| # | 未覆盖 | 说明 |
|---|---|---|
| 1 | 拓扑匹配过滤 ≤ 1 µs | 属于规则索引/命中筛选，本次只验证了表达式本身 |
| 2 | DAG 节点推进 ≤ 5 µs、含动作分发 ≤ 300 µs | L2 编排尚未实现 |
| 3 | `window.*` 的值从哪来 | 本次只定义了窗口字段的类型与「必须声明」规则，**窗口算子未实现** |
| 4 | 并发形态 | `Runner` 非并发安全，每 goroutine 一个；未做多核吞吐实测 |
| 5 | 分配压力 | 每次求值 3~4 次分配（48~80 B），按 §1.7 的 50 万次/秒约 **40~80 MB/s** 垃圾。生产需实测 GC 影响并调 `GOGC`/`GOMEMLIMIT` |

> 验证代码：`internal/rules`（`gate.go` 校验器、`compile.go` 编译与缓存、`funcs.go` 白名单、
> `compile_test.go` 判据、`bench_test.go` 性能）。运行方式见 `09-handoff.md` §5.2。

### 1.3 L2：DAG 编排（自研）

节点类型（全部为 Go 侧受控实现，**平台不执行用户提交的编排代码**）：

| 节点 | 语义 | 关键参数 |
|---|---|---|
| `condition` | 条件分支，复用 L1 的 `expr` | `expr`、`on_true`、`on_false` |
| `action` | 调用注册表中的动作 | `action`、`params`、`retry`、`timeout_ms` |
| `delay` | 延时后继续 | `ms`（上限 5 min；更长延时改走定时任务） |
| `parallel` | 并发执行多个分支 | `branches[]`、`limit`（默认 5） |
| `branch` | 多路分支（switch） | `cases[]`、`default` |
| `end` | 终止 | — |

**动作注册表（Action Registry）** —— 执行体全部是编译进二进制的 Go 函数，不接受动态代码：

| action | 说明 | 幂等 |
|---|---|---|
| `alarm.raise` / `alarm.clear` | 触发 / 清除告警 | 是（按 `dedup_key`） |
| `command.send` | 下发设备命令 | 是（按 `idem_key`） |
| `notify.send` | 通知（通道由通知策略决定） | 否（有去重窗口） |
| `webhook.call` | 出站 HTTP（强制白名单 + 禁内网） | 否 |
| `timeseries.write` | 写派生指标 | 是（按 `device_id+ts+key`） |
| `shadow.update` | 更新设备影子 desired | 是（按 `version`） |
| `state.set` / `state.get` | 规则级持久状态（Redis，带 TTL） | 是 |
| `script.run` | **L3 逃生舱**，需 `capability` 声明 | 否 |

> **新增动作 = 写 Go 代码 + 注册 + 上线**，而不是让用户在平台上写脚本。这是安全边界的根本所在。

**DAG 约束（构建时强制校验）**：

| 约束 | 值 | 违反后果 |
|---|---|---|
| 节点数上限 | 32 | 拒绝保存 |
| 深度上限 | 8 | 拒绝保存 |
| 环检测 | 构建时 DFS 检测 | 拒绝保存 |
| 单节点超时 | 5 s | 节点失败，按 `error_policy` 处理 |
| 整体超时 | 30 s | 中断 DAG，写执行日志 |
| 并行上限 | 5 | 拒绝保存 |
| 变量引用 | `$msg.temperature` 只读取值，**禁止表达式拼接** | 拒绝保存 |

> **与场景（§3）的关系**：场景 = 预置的 DAG 模板 + 定时/事件触发源。**两者共用同一个执行器**，避免维护两套编排引擎。这是 ADR-003 选「自研 DAG」而非「引入外部编排框架」的核心原因。

### 1.4 L3：受限 JS 逃生舱（P2，默认关闭）

**开启条件（须全部满足）**：租户在 `project.settings.sandbox_script = true`；规则显式声明 `capabilities`；通过人工审批；资源限制比 L1 更严。

| 维度 | 限制 | 实现 |
|---|---|---|
| 执行超时 | 50 ms（硬中断） | `vm.Interrupt("timeout")` + `context` |
| 指令数 | 100 万条 | goja `SetMaxCallStackSize` + 自定义 `Interrupt` 计数 |
| 内存 | 单 VM 16 MB | 运行时堆监控 + 超限 `Interrupt` |
| 并发 | 每租户独立 worker pool（默认 8，小于 L2 的池） | 令牌控制，防单租户耗尽 |
| 网络 | 禁止 | 不注入 `fetch`/`http`，`utils` 中也不提供 |
| 文件 / 进程 | 禁止 | 不注入 `os`/`exec` |
| 反射逃逸 | 禁止 | 不通过 `vm.Set` 暴露 `reflect`；关闭 `__proto__` 相关污染面 |
| 全局状态 | 禁止跨消息共享 | **不复用 Runtime，只复用编译产物**（见下） |

```go
// 只缓存编译产物，不池化 Runtime —— 规避全局变量污染与状态残留
type scriptCache struct {
    prog *lru.Cache // script_hash -> *goja.Program
}

func (c *scriptCache) Run(prog *goja.Program, in *SandboxCtx) (Out, error) {
    vm := goja.New()                  // 每次新建（~10 µs，可接受）
    vm.SetMaxCallStackSize(10_000)
    // 按 capabilities 白名单注入只读输入与受控输出
    ...
}
```

> **不要池化 `goja.Runtime`**：VM 复用存在全局变量污染风险，且清理成本高于新建成本。缓存编译产物才是真正的性能收益点。

> ⚠️ **上表的限制有三项「待 PoC 证明」，不能当作已实现能力**（评审 R-17）：
>
> | 声明 | 现状 | Phase 0 必须验证 | 若不成立则降级为 |
> |---|---|---|---|
> | 指令数 100 万条 | 靠 `SetMaxCallStackSize` + 自定义 `Interrupt` 计数 —— goja 不提供精确指令计数原语，实现难度高 | 写 PoC 验证能否精确计数，误差容忍度多少 | **仅保留执行超时 + VM 并发上限的近似限制**，并在文档与 UI 中如实标注 |
> | 内存单 VM 16 MB | 靠"运行时堆监控 + 超限 `Interrupt`" —— goja 无公开的堆内存硬限制 API | 验证能否在超限时可靠中断，以及中断后 VM 是否可安全丢弃 | 降级为「VM 不复用 + 进程级内存上限 + OOM 即重启 worker」 |
> | 并发池 8 / 租户 | 逻辑清晰，但未压测 | 压测单 worker 的实际 CPU/内存占用 | 按实测调整池大小与 `GOMEMLIMIT` |
>
> **原则**：**宁可在文档里写"近似限制"，也不要在文档里写一个实现不了的精确承诺。** 这三项在 Phase 0 的闭环是 L3 逃生舱启用的前置条件。

**沙箱上下文契约**（L3 专用，字段与 L1 变量空间**完全一致**，避免用户心智分裂）：

```javascript
// 只读输入
ctx = { msg, meta, prev, window, state }

// 输出能力：必须先在规则元数据声明 capabilities，未声明的调用直接抛错
emit("timeseries", { energy_kwh: 12.5 })
emit("alarm", { rule: "high_temp", value: 88 })
emit("command", { device_id: 123, cmd: "set_mode", data: { mode: 1 } })
state.set("last", 42); state.get("last")

// 工具库（白名单，部分为 Go 实现以避免浮点精度问题）
utils.parseHex(s), utils.crc16(bytes), utils.wgs84ToBd09(lng, lat),
utils.time.format(ts, "YYYY-MM-DD"), utils.unit.convert(v, "kPa", "bar"),
utils.math.clamp(v, min, max), utils.json.parse/stringify
```

**输出能力必须显式声明**（`capabilities: ["emit:alarm","state"]`），沙箱按声明注入 `emit`，未声明的能力调用即抛错。这防止误用与越权。

### 1.5 规则定义

```json
{
  "rule_id": "r_10231_001",
  "name": "车间A 温度超限联动",
  "type": "on_report",
  "enabled": true,
  "priority": 100,
  "match": {
    "device_type_id": 55,
    "groups": ["车间A"],
    "expr": "msg.temperature > 60 && meta.tags.site == 'sh'"
  },
  "window": { "type": "sliding", "size_s": 300, "agg": "avg", "key": "temperature" },
  "dag": {
    "entry": "n1",
    "nodes": [
      { "id": "n1", "type": "condition", "expr": "window.avg > 60",
        "on_true": "n2", "on_false": "end" },
      { "id": "n2", "type": "action", "action": "alarm.raise",
        "params": { "rule": "high_temp_5m", "level": "warn", "value": "$window.avg" } },
      { "id": "n3", "type": "action", "action": "command.send",
        "params": { "device_id": "$meta.device_id", "cmd": "set_fan", "data": { "speed": 3 } },
        "retry": { "max": 3, "backoff": "exponential", "base_ms": 500 },
        "timeout_ms": 5000 },
      { "id": "n4", "type": "delay", "ms": 60000, "next": "n5" },
      { "id": "n5", "type": "action", "action": "notify.send", "params": { "group": "oncall_a" } }
    ]
  },
  "timeout_ms": 30000,
  "error_policy": "dlq",
  "version": 4,
  "effective_from": "2026-10-01T00:00:00Z"
}
```

| 字段 | 说明 |
|---|---|
| `priority` | 数字越小越先执行；同一消息的多条规则**串行**执行，累计超时 200 ms 后中断并记录 |
| `window` | 滑动/滚动窗口，由 `svc-rule` 的窗口算子维护（内存 + 定期快照到 Redis） |
| `error_policy` | `drop`（跳过继续）/ `retry`（按节点 `retry` 配置，上限 3 次）/ `dlq`（立即进 DLQ） |
| `version` | 版本化，支持灰度与回滚 |
| `capabilities` | **仅 L3 规则需要**；声明 `emit:alarm` / `emit:command` / `state` 等能力 |

### 1.6 发布与灰度

```
草稿 → 编译校验（expr 编译 + 类型检查 + DAG 环检测 + 动作白名单 + 变量引用语法）
   → 样例试跑（3 组输入，校验输出与耗时）
   → 灰度（指定设备分组，观察 30 min）→ 全量发布 → 旧版本 deprecated（保留 7 天可回滚）
```

**发布必须在 `svc-rule` 中原子生效**：通过 NATS 广播 `iot.ctrl.rule_reload.{project}` + 版本号，各实例收到后从 PG 重载并按版本号防御（避免乱序覆盖）。

**质量门禁（绝大部分在「保存」阶段即阻断，而非等到发布）**：

| 检查 | 阶段 | 规则 |
|---|---|---|
| 表达式编译 | 保存 | `expr.Compile` 失败即拒绝，错误信息回显给用户 |
| 变量与类型 | 保存 | 编译期类型检查，未知字段直接报错 |
| DAG 环检测 / 深度 / 节点数 / 并行上限 | 保存 | 见 §1.3 约束表 |
| 动作白名单 | 保存 | 非注册表内的 `action` 拒绝 |
| 变量引用语法 | 保存 | 仅允许 `$path.to.value`，**禁止表达式拼接** |
| 样例试跑 | 发布 | 3 组样例输入，验证输出正确且耗时 < 5 ms |
| L3 脚本巡检 | 发布 | AST 扫描：禁止 `while(true)`、未声明的能力调用、脚本 ≤ 16 KB |

### 1.7 性能模型

| 场景 | 目标 | 实测（Phase 0 · C1） |
|---|---|---|
| 拓扑匹配过滤（纯元数据比对，不解析表达式） | ≤ 1 µs / 消息 / 规则 | — 未验证（属规则索引，本次未覆盖） |
| **L1 表达式求值（已编译）** | ≤ 2 µs / 次 | ✅ **P50 0.60~0.81 µs、P90 0.77~1.54 µs、P99 0.98~1.74 µs**（6 轮，5 条表达式混合）。**必须走 `Runner`（复用 VM）**：直接用 `expr.Run` 时 P99 = 2.03 µs，骑线未过。⚠️ 余量 1.15~2.04×，**生产机型需复测** |
| DAG 节点推进 | ≤ 5 µs / 节点 | — 未验证（L2 未实现） |
| 含动作分发（如 `command.send`，含 NATS 发布） | ≤ 300 µs / 次 | — 未验证 |
| 单实例吞吐 | ≥ 50 万 次/秒（纯条件）/ ≥ 3 万 次/秒（含动作分发） | 单核 ~80 万 次/秒（纯条件）✅；含动作分发未验证 |
| 编译缓存命中率 | ≥ 99.99% | ✅ 命中路径 121 ns / 0 分配；键口径 `rule_id + version + sha256(expr)[:8]`，LRU 10000 |
| 单条表达式规模 | —（原为 20 ms 运行时超时） | 改为**编译期 `MaxNodes = 512`**（§1.2.1 ⑥） |
| 求值分配 | —（原文未规定） | 3~4 次分配 / 48~80 B。按 50 万次/秒约 **40~80 MB/s** 垃圾 —— 生产需实测 GC 并调 `GOGC`/`GOMEMLIMIT` |

> **一句提醒**：P99 从 1.3 µs 涨到 2.03 µs，只差一次「VM 池化进出 + 一次分配」。
> 这类预算里，**微小的常数项就是结论本身**，不能靠「反正很快」糊过去。

**降级**：`svc-rule` 消费滞后 > 30 万时，自动跳过 `priority > 500` 的低优先级规则，并上报降级指标。

---

## 2. 告警引擎

### 2.1 状态机（五态 FSM）

```
                    ┌────────────────────────────────────────┐
                    │                                        │
   [idle] ──────► [detected] ──────► [confirmed] ──────► [active] ──────► [resolved]
      ▲               │                    │                 │                 │
      │               │ 恢复               │ 恢复            │ 恢复            │
      └───────────────┴────────────────────┴─────────────────┘                 │
      ▲                                                                       │
      └───────────────────────────────────────────────────────────────────────┘
```

| 状态 | 含义 | 默认参数 |
|---|---|---|
| `idle` | 无告警 | — |
| `detected` | 条件首次满足，进入**观察期**（防抖） | 观察期 60 s（可配） |
| `confirmed` | 观察期内持续满足，准备通知 | — |
| `active` | 通知已发出，进入**抑制期** | 抑制期 10 min（可配） |
| `resolved` | 恢复条件满足，等待关闭 | 自动关闭延迟 5 min（避免抖动反复） |

**推进机制**：由 `svc-alarm` 的**定时扫描（每 5s）+ 事件驱动**双通道推进。定时扫描兜底（防事件丢失），事件驱动保证时效。

### 2.2 去重、聚合与抑制

| 机制 | 规则 |
|---|---|
| 去重键 | `dedup_key = sha1(project_id + device_id + rule_id)` —— **同一设备同一规则只允许一个活跃告警** |
| 聚合 | 同一 `device_type` 下 ≥ 5 台设备在 60s 内触发同一规则 → 合并为一条"批量告警"，`notify_count` 累加 |
| 抑制 | `active` 状态下重复触发只更新 `last_ts`，不重复通知 |
| 静默窗口 | 支持维护窗口（定时/临时），窗口内只记录不通知 |
| 风暴抑制 | 单租户 1 分钟内新增告警 > 500 条 → 熔断，只发一条"告警风暴"聚合通知，并 P1 升级 |
| 根因抑制 | 支持配置 `parent_alarm`，父告警活跃时子告警自动抑制（如"设备离线"抑制其下所有指标告警） |

#### 2.2.1 告警质量指标（业务口径）

对齐《Odoo 20 高性能架构技术方案》手册 p.35「设备厂商与工业物联网」的验收方向，告警域必须暴露以下业务口径指标，而不是只有技术指标：

| 指标 | 定义 | 目标 | 用途 |
|---|---|---|---|
| **有效告警占比** | `有效告警数 / 总告警数`，其中「有效」= 人工确认或转化为 Odoo 工单 | **≥ 60%**（首期基线，需按现场校准） | 衡量规则质量，防止"误报淹没运维" |
| 告警到工单转化率 | `生成 Odoo 工单的告警数 / 有效告警数` | ≥ 90% | 衡量集成链路完整性 |
| 告警合并率 | `1 - (生成工单数 / 原始告警数)` | 记录基线 | 直接决定 Odoo 写入量（见 07 §6 S3 的容量不等式） |
| 告警风暴次数 | 单租户 1 min 内新增 > 500 条的次数 | 0 | 规则或设备异常信号 |
| 平均确认时长 | `confirmed_ts - first_ts` 的中位数 | 记录基线 | 反映通知链路与值班响应 |

> **为什么必须定义这个**：手册 p.35 明确把「**有效告警到工单比例**」列为该垂类的验收方向。只统计 P99 延迟而不统计误报率，会出现「系统很快但没人看告警」的失败形态。

### 2.3 通知与升级

```
告警进入 active
  ├─ 1. 解析通知策略（rule.notify）：通知组 / 通道 / 模板 / 静默规则
  ├─ 2. 模板渲染（Go text/template，支持过滤器）
  ├─ 3. 通道分发（svc-notify，按优先级 + 降级）：
  │       Webhook（钉钉/飞书/企微）→ 邮件 → 短信 → 语音
  │       失败降级：短信拥塞（失败率 > 30%）自动切换邮件
  ├─ 4. 记录 t_alarm.notify_count、通知结果
  └─ 5. 未确认升级：30 min 未确认 → 通知上级；2 h → P1 升级
```

**Webhook 安全**：

- URL 必须命中租户白名单（域名/IP 段），出站经统一出口 + DNS 固定解析（防 SSRF）。
- 禁止访问内网地址段（`10/8`, `172.16/12`, `192.168/16`, `169.254/16`, `127/8`）与云元数据端点（`169.254.169.254`）。
- 重试：3 次指数退避，失败进 DLQ。
- 超时：5 s；响应体截断至 1 KB 记录。

### 2.4 告警规则定义

```json
{
  "rule_id": "ar_001",
  "name": "温度过高",
  "level": "critical",
  "scope": { "device_type_id": 55, "groups": ["车间A"] },
  "condition": "{ \"temperature\": { \"gt\": 85, \"for_s\": 60 } }",
  "recover": "{ \"temperature\": { \"lt\": 70, \"for_s\": 120 } }",
  "timing": { "detect_window_s": 60, "suppress_s": 600, "auto_close_s": 300 },
  "notify": {
    "groups": ["oncall_a"],
    "channels": ["webhook", "sms"],
    "template": "device_alarm_v2",
    "escalation": [{ "after_s": 1800, "groups": ["leader"] }]
  },
  "enabled": true
}
```

条件表达式使用**受限 DSL（JSON 结构）而非任意 JS**，保证可静态分析与索引优化；复杂逻辑通过引用 `svc-rule` 的规则输出（`emit("alarm")`）实现。

### 2.5 可靠性

- 状态持久化在 PG（`t_alarm`），内存中维护活跃告警索引（`map[dedup_key]→state`），启动时从 PG 重建。
- 状态迁移使用**行级锁 + 乐观锁**：`UPDATE ... WHERE id=? AND state=?`，失败则重读重试（最多 3 次）。
- `svc-alarm` 多副本通过 **dedup_key 哈希分片**（Redis 分布式锁 + 分片路由）保证同一 `dedup_key` 只被一个实例处理。
- 定时扫描任务使用分布式锁，避免多实例重复扫描全量数据。

---

## 3. 场景联动与定时任务

> **执行器复用**：本节的场景 = **预置 DAG 模板 + 触发源**，与 §1.3 的自研 DAG 执行器是**同一套代码**。下方 JSON 是面向控制台用户的友好视图，落库前会编译为标准 DAG（`trigger` → `condition` 链 → `action` 链）。**禁止出现第二套编排引擎。**

### 3.1 场景（条件 + 动作）

```json
{
  "scene_id": "s_001",
  "name": "高温自动开风机",
  "enabled": true,
  "trigger": { "type": "device_property", "device_id": 100, "key": "temperature", "op": "gt", "value": 80 },
  "conditions": [
    { "type": "device_online", "device_id": 200 },
    { "type": "time_range", "from": "08:00", "to": "20:00", "tz": "Asia/Shanghai" },
    { "type": "scene_not_active", "scene_id": "s_002" }
  ],
  "actions": [
    { "type": "command", "device_id": 200, "cmd": "set_speed", "data": { "speed": 3 }, "timeout_ms": 5000 },
    { "type": "notify", "group": "oncall_a", "template": "fan_on" },
    { "type": "delay", "ms": 60000 },
    { "type": "command", "device_id": 200, "cmd": "check_status" }
  ],
  "execution": { "mode": "sequential", "parallel_limit": 5, "timeout_ms": 30000,
                 "retry": { "max": 3, "backoff": "exponential", "base_ms": 500 },
                 "cooldown_s": 300, "max_concurrent": 1 }
}
```

**执行语义**：

| 项 | 规定 |
|---|---|
| 条件求值 | 读取**最新值快照**（Redis 缓存，TTL 10s）；同时取 `prev` 值判断变化趋势 |
| 动作顺序 | `sequential`（默认，含 `delay`）或 `parallel`（并发上限 5） |
| 超时 | 单动作 5 s，整体 30 s |
| 重试 | 指数退避 500ms/1s/2s，最多 3 次；**仅对幂等命令重试** |
| 冷却 | `cooldown_s` 内同一场景不重复触发。**必须两阶段**：`SETNX ... = processing`（TTL = 整体超时 + 缓冲）→ 执行成功后才写 `done`（TTL = `cooldown_s`）。**只写 processing 就返回会在崩溃后永久抑制该场景**（与 03 文档 §4.3 的流水线幂等同一类缺陷） |
| 并发 | `max_concurrent=1` 保证同一场景串行，避免竞态 |
| 离线设备 | 命令入离线队列（物模型 `offline_ttl_ms`）；超过 TTL → 标记失败 |
| 执行日志 | 每次执行写 `t_task_exec_log`（触发条件、动作明细、耗时、结果） |
| 循环依赖 | 场景触发链深度限制 5 层，超限中断并告警（防死循环） |

**原子性**：场景执行是"尽力而为 + 可观测"，**不做分布式事务**。要求：每个动作必须幂等；失败时记录明确的部分成功状态（`partial_success`），并触发补偿告警。

### 3.2 定时任务

| 项 | 设计 |
|---|---|
| 调度表达式 | 标准 Cron（含秒级，`robfig/cron`） |
| 分片 | `shard = hash(task_id) % scheduler_replicas`；每个 scheduler 实例只处理自己的分片 |
| 实例变更 | 副本数变化时重新分片；任务迁移需**幂等 + 断点续跑**（记录 `last_cursor`） |
| 触发检测 | 每 1 s 扫描一次到期任务（内存索引 + PG 持久化） |
| 大批量 | 群发类任务（百万设备）拆分为分批子任务（每批 1000），限速下发（默认 500/s），进度可查、可暂停、可取消 |
| 幂等 | `(task_id, scheduled_ts)` 唯一索引，防止重复触发 |
| 错过补偿 | 调度器宕机期间错过的触发：配置 `misfire_policy` = `skip` / `run_once` / `catch_up` |
| 时区 | 任务绑定 `project.timezone`，DST 处理由 cron 库负责 |

### 3.3 重试与死信

```
失败 → 重试队列（NATS JetStream Stream `RETRY`，延迟通过 `NakWithDelay` 或延迟 subject）
  ├─ 重试 1: +500ms
  ├─ 重试 2: +2s
  ├─ 重试 3: +8s
  └─ 超限 → t_dlq + 对象存储保存原文 → 按 service 聚合告警
```

- DLQ 条目含：原始消息、失败原因、重试历史、trace_id。
- 提供 **DLQ 重放工具**（按时间范围 / device_id / 错误类型筛选后重放），重放需二次确认并记录审计。
- DLQ 保留 7 天，超期自动清理（清理前归档到对象存储）。

---

## 4. OTA 固件升级

### 4.1 流程

```
1. 上传固件 → 服务端计算 SHA-256 + 生成签名（Ed25519，私钥在 Vault）
2. 创建升级任务：目标范围（设备类型/分组/设备列表）+ 灰度策略 + 升级策略
3. 分批次下发：
   ┌─ 批次 1（1%）→ 观察 30 min → 成功率 ≥ 98% 才继续，否则自动暂停并告警
   ├─ 批次 2（10%）→ 观察 1 h → ...
   ├─ 批次 3（50%）
   └─ 批次 4（100%）
4. 设备通过 MQTT 收到 ota/notify（含 URL、大小、哈希、签名、版本）
5. 设备 HTTP(S) 下载（支持 Range 断点续传），上报 ota/progress
6. 设备校验哈希与签名 → 安装 → 重启 → 上报新 firmware_version
7. 平台比对版本，标记成功/失败；连续失败率超阈值自动全局暂停
```

### 4.2 关键机制

| 项 | 设计 |
|---|---|
| 下载加速 | 对象存储直出（预签名 URL，TTL 1h）+ CDN；避免流量经平台服务 |
| 断点续传 | 支持 HTTP `Range`；设备侧记录下载偏移 |
| 幂等 | `(task_id, device_id)` 唯一；重复收到 notify 不重复升级 |
| 回滚 | 保留上一可用版本；设备侧 A/B 分区或恢复标记；支持"一键回滚"任务 |
| 限速 | 全局带宽/并发下载限制 + 分批下发速率控制（防存储被打爆） |
| 安全 | 固件签名强制校验；未签名固件拒绝安装；下载 URL 短时效 |
| 离线设备 | 入离线队列，上线后 `offline_ttl` 内补发；超期标记"已过期" |
| 进度可观测 | `t_ota_task_device` 实时状态 + 大屏进度条；卡住（30 min 无进度）设备单独告警 |

---

## 5. 设备影子（差异化能力）

### 5.1 模型

```
shadow = {
  desired:  { ... },   // 期望状态（应用/规则写入）
  reported: { ... },   // 设备上报状态
  delta:    { ... },   // desired 与 reported 的差集，设备需执行
  version:  12,        // 每次变更递增
  ts:       "2026-10-01T08:12:33Z"
}
```

### 5.2 交互流程

```
应用/规则 → UpdateDesired(patch, expected_version)
   │  写入 PG（事务, 乐观锁）+ Redis 同步
   │  version++ → 计算 delta
   ▼
   delta 非空 → ① 查 Redis cache:{pid}:{co}:device:conn:v1:{did} → nodeID
                ② publish iot.route.{nodeID}（携带目标 device_key + shadow/desired 载荷）
   ▼
gw-mqtt（该节点）在本地连接表定位连接 → 投递 MQTT 主题 v1/devices/{key}/shadow/desired
   ▼
设备执行 → publish v1/devices/{key}/shadow/reported
   ▼
svc-device 更新 reported → 重算 delta → 若为空，desired 与 reported 收敛
```

> ⚠️ **链路修正（评审 P1）**：早期此处写 `publish iot.cmd.{pid}.{did}`，与 01 文档 §6.2「**不存在设备级命令 subject，统一走 `iot.route.{nodeID}`**」冲突 —— 影子下发链路实际是断的。现统一为「查会话 → 发 `iot.route.{nodeID}`」，与命令下发同一条路径。
>
> 设备离线时（`d:conn` 无 nodeID）：**不投递**，desired 已持久化在 PG；设备上线后由 `svc-device` 检测 delta 非空并补推（对应 §5.3 的「离线设备」策略）。

### 5.3 冲突与离线

| 项 | 策略 |
|---|---|
| 并发写冲突 | 乐观锁；调用方带 `expected_version` 则严格校验（不匹配返回 409），不带则 last-write-wins |
| 离线设备 | desired 持久化；设备上线时由 `svc-device` 检测并推送全量 desired |
| 部分上报 | reported 按属性 key 逐字段合并，不做整体覆盖 |
| 大小限制 | 单设备影子 ≤ 8 KB；超限拒绝并提示改用 attributes |
| 收敛保证 | `delta` 由服务端计算，设备无需理解全量状态 —— **这是比"平台直接下发完整配置"更可靠的地方** |

**与 ThingsCloud 的差异（结论已修正）**：ThingsCloud 的「**属性获取 / 属性推送**」在语义上**就是** shadow 的 reported / desired —— 它并非「没有影子」。真正的差异在于：ThingsCloud 缺少 **`version` 版本化收敛机制**（无 delta 计算、无冲突检测、无收敛判定）。

因此本项目的差异化表述应为：**「有版本化收敛的影子」**，而不是「有影子」。前者更难被对方一次产品更新抹平。

### 5.4 属性三分类（借鉴 ThingsBoard 概念）

影子的字段并非只有 desired / reported 两分。参考 ThingsBoard 的**属性三分类**，本项目在物模型层显式区分，避免语义混淆：

| 类别 | 谁能写 | 谁能读 | 生命周期 | 存储 |
|---|---|---|---|---|
| **Server-side（服务端属性）** | 仅平台（规则 / API / 场景） | 平台可读；按需下发设备 | 长期，随设备存活 | PG + Redis |
| **Client-side（设备端属性）** | 仅设备上报 | 平台可读 | 随设备存活 | PG（最新值）+ Redis |
| **Shared（共享属性）** | 平台与设备**双向** | 双方可见 | 长期 | PG + Redis + 影子 |

映射关系：

```
Shared      ⇄ shadow.desired + shadow.reported（双向、需收敛）
Server-side  → shadow.desired（单向，只由平台写）
Client-side  → shadow.reported（单向，只由设备写）
```

**设计要求**：物模型的每个 property 必须显式声明 `attr_class: server | client | shared`。缺省为 `client`（兼容既有设备）。未声明时的写权限按最小权限原则收敛 —— **禁止任何属性的默认双向写入**。

---

## 6. 计量与配额（svc-quota）

| 指标 | 采集方式 | 精度 | 用途 |
|---|---|---|---|
| 设备数 | 定时快照（5 min） | 精确 | 计费、配额 |
| 消息数 | `svc-pipeline` 批量上报（1 min 聚合） | ±0.1% | 计费 |
| 存储量 | 每日统计（GreptimeDB + PG + 对象存储） | ±1% | 计费 |
| API 调用数 | api-gateway 采样 + 批量 | ±1% | 计费、限流 |
| 连接峰值 | 网关每 10s 上报 | 近似 | 套餐校验 |

**实现**：网关/管道本地累加 → 每 10s 通过 NATS 批量上报 `iot.quota.usage` → `svc-quota` 写 Redis 计数器（`INCRBY`）→ 每分钟落 PG（幂等：按 `(metric, ts_minute)` 去重）→ 每小时与原始数据抽样对账 → 差异 > 1% 告警。

**超配额处理**：分级预警（80% / 90% / 100%），100% 后按租户策略（`throttle` / `reject`）执行；`cmd` 与状态查询永不被配额拒绝（保证可运维性）。

---

## 7. 服务的降级与自愈清单

| 故障 | 降级行为 | 恢复 |
|---|---|---|
| Redis 不可用 | 读走 PG；限流阈值 ×0.5；网关凭据校验用本地 LRU（只读） | Redis 恢复后自动回切，缓存逐步预热 |
| NATS 不可用 | 网关暂停上报（连接保持），本地缓冲至水位上限后丢弃 `telemetry` | 恢复后补发缓冲 |
| GreptimeDB 不可用 | 只写 Redis + 落重试流；API 查询返回"数据暂不可用"并给出降级提示 | 恢复后重试流回放 |
| PG 不可用 | 控制面只读；数据面继续（遥测不依赖 PG） | 主从切换后恢复 |
| svc-rule 不可用 | 消息照常入库，跳过规则；积压消息在恢复后按时间顺序补算（标记 `late=true`） | 自动追平 |
| svc-alarm 不可用 | 告警事件积压于 NATS；恢复后按 `first_ts` 回放，抑制重复通知 | 自动追平 |
| 单网关节点故障 | 该节点连接断开（设备秒级重连），NATS 未投递消息重路由到离线流 | 自动 |
| 证书过期 | 提前 30 天告警；cert-manager 自动轮换 + 热加载（无需重启） | 自动 |
