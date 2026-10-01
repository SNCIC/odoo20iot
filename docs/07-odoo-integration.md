# 07 · 与 Odoo 20 的集成设计

> 目标系统：`D:\odoo\odoo20tbb`（Odoo 20.0 Community，Windows，PostgreSQL 17）
> 本文所有结论均附源码路径与行号，未经核实的内容单独标注为「待确认」。

> **本文已按 [08-odoo-perf-alignment.md](./08-odoo-perf-alignment.md) 的对齐裁定完成修订。**
> 08 文档对照《Odoo 20 高性能架构技术方案》裁定了 8 项冲突，本文已落实以下 6 项（其余 2 项属架构层，见 01/06）：
>
> | 本文位置 | 已修订内容 | 依据 |
> |---|---|---|
> | §7 模块 | 桥接模块定名为 **`sn_edge_integration`**（含 `edge_facade.py` / `iot_facade.py` / `iot_api.py`），与 `odoo-gateway` 侧共用同一模块 | ADR-014、ADR-017 |
> | §5.2 幂等 | 简化 `idem_key` → **五态状态机 + `UNIQUE(company_id, integration_name, idempotency_key)` + `expires_at` 分档** | ADR-015 |
> | §6 S1 / S3 | 纯 webhook → **高价值走 Outbox，低价值走 webhook + 15 min 定时对账** | 08 §C4 |
> | §4.2 认证 | 补 **`X-Odoo-Database`** 多库 header | 08 §4 |
> | §5.2 / §5.3 | 缓存键统一为 **`cache:{tenant}:{company}:{domain}:v{n}:{key}`** | ADR-015 / 08 §C8 |
> | §4.3 | 补**错误码映射表**（401/403/422/409/504/503） | 08 §4 |
>
> 涉及架构层的两项裁定不在本文范围：[ADR-013](./README.md)（人的入口与设备入口分离）见 01 文档 §3.1；[ADR-016](./README.md)（Rust 一期不引入）见 08 §C3。

---

## 1. 集成定位与原则

### 1.1 职责边界：谁是真相来源（Source of Record）

| 数据域 | 真相来源 | 理由 |
|---|---|---|
| 设备接入、连接会话、遥测时序 | **IoT 平台** | Odoo 不做时序，且无可用 IoT 栈（见 §2.1） |
| 设备台账（型号、序列号、厂商、保修） | **Odoo** `maintenance.equipment` | 已与企业资产/采购/维保流程绑定 |
| 物料、批次/序列号 | **Odoo** `stock.lot` / `product.*` | Odoo 是 ERP 主数据 |
| 生产工单、工序、工时 | **Odoo** `mrp.production` / `mrp.workorder` | — |
| 生产现场的实时计量（产量、节拍、能耗） | **IoT 平台**（回流写 Odoo 摘要字段） | 高频数据不进 Odoo |
| 告警、事件 | **IoT 平台**（派生为 Odoo 维护工单） | — |
| 工单/指令的下发意图 | **Odoo**（由 IoT 平台执行） | 人的决策在 ERP |

**一句话**：**Odoo 管业务与主数据，IoT 平台管设备与数据流；两者之间做「事件级」同步，不做「记录级」复制。**

### 1.2 五条铁律

| # | 铁律 | 原因（基于实测证据） |
|---|---|---|
| **R1** | **绝不把 Odoo 放进遥测热路径** | Odoo 单实例、无 `workers` 配置、仅绑回环（§2.5）；每设备每秒写一次会直接打垮实例 |
| **R2** | **禁止用 Go 直连 PostgreSQL 做写操作** | 绕过 ORM、ACL、记录规则、审计与 `mail.thread` 消息；写入必须走 API（§3.4） |
| **R3** | **从第一天只用 `/json/2`，禁止 `/jsonrpc` `/xmlrpc`** | `/jsonrpc` `/xmlrpc` 已在 Odoo 19 弃用、计划 22 移除（§2.2） |
| **R4** | **允许在自有模块内扩展 Odoo（新增 `iot_*` 字段与 `iot.*` / `edge.*` 模型），禁止修改核心语义** | 早期「零改动」约束造成了多处设计扭曲（位置无处存、产量漂在 Odoo 之外、`working_state` 不可写导致状态无处落、Community 无质检可落）。放开后风险用「单一模块 + 扩展登记表 + 逐项升级影响评估」管理（§7.7） |
| **R5** | **所有对接逻辑集中在单一 bridge 模块 + Go 侧适配层** | 禁止业务代码散落调用 Odoo；升级时只需改一处（§7） |
| **R6** | **扩展字段只做「承载与展示」，不做「业务裁决」** | 新增字段可以被读写，但状态判定、数量确认、审批仍由 Odoo 核心逻辑或显式的人工动作完成。**不允许用自定义字段绕过核心业务流**（如直接写 `iot_qty` 冒充产量） |

### 1.3 事实基线（已核实）

| 项 | 结论 | 证据 |
|---|---|---|
| 版本 | Odoo 20.0 Community | `odoo/release.py:15` `version_info = (20, 0, 0, FINAL, 0, '')` |
| 版本性质 | **非 Enterprise** | 全树无 `web_enterprise` / `account_accountant` / `quality*` / `web_studio` / `mrp_workorder` |
| IoT 栈 | **不可用** | `addons/iot_drivers/__manifest__.py:24` `'installable': False`；`addons/iot_webserial` 仅浏览器 Web Serial（无服务端 MQTT） |
| 质量模块 | **不存在**（Community 特性） | 全树搜 `quality.point` / `quality.check` / `quality.alert` 零命中 |
| 推荐 API | JSON-2 REST + API Key | `addons/rpc/controllers/json2.py:48` |
| 绑定地址 | **`http_interface = 0.0.0.0`，`http_port = 8070`（devbox 容器内）** | 服务器 `/home/xfusion/etc/odoo/odoo20tbb.conf`（五轮更正，原写「仅 127.0.0.1:8105」是基于已废弃的 Windows 旧布局副本） |
| PG | **18.6**，容器内 `127.0.0.1:5432`，**Unix socket + peer 认证（无口令）** | `postgresql@18-main`；`db_host = /var/run/postgresql` |
| 运行形态 | **devbox 容器 + systemd 单元 `odoo@odoo20tbb`**，非 Windows 裸进程 | `systemctl list-units`；`~/projects/AGENTS.md` |
| 源码/模块位置 | `/home/xfusion/projects/odoo/odoo20tbb/{odoo,addons}`（宿主与容器同一份，bind mount）；`addons` 是独立 git 仓 `SNCIC/odoo20tbb` | `~/projects/AGENTS.md` |

---

## 2. 目标环境画像（已核实）

### 2.1 IoT 相关模块：**没有可用的**

| 模块 | 状态 | 判定 |
|---|---|---|
| `addons/iot_drivers` | `installable: False`，`name: 'Hardware Proxy'`，**无 `depends`** | 上游框架占位，不可安装 |
| `addons/iot_webserial` | `depends: ['web']`，Web Serial API | 仅浏览器直连串口，**无服务端网关、无 MQTT** |
| `iot` / `pos_iot` / `stock_iot` / `maintenance_iot` / `mrp_iot` | 均不存在 | — |

> **结论**：无法复用 Odoo 的 IoT Box 能力。设备接入必须由本项目承担 —— 这也正是本项目的存在意义。

### 2.2 对外接口能力

| 通道 | 端点 | 鉴权 | 状态 | 证据 |
|---|---|---|---|---|
| **JSON-2（推荐）** | `POST /json/2/<model>/<method>` | `auth='bearer'`，`bearer_scope='rpc'` | ✅ 官方推荐 | `addons/rpc/controllers/json2.py:48-56` |
| 派发器 | — | — | ✅ | `odoo/http/dispatcher.py:414` `class Json2Dispatcher` |
| JSON-RPC | `/jsonrpc` | 会话 / `auth='none'` | ⚠️ **已弃用** | `addons/rpc/controllers/jsonrpc.py:12` |
| XML-RPC | `/xmlrpc/2` | 账号密码 | ⚠️ **已弃用** | `addons/rpc/controllers/xmlrpc.py:132` |
| 弃用声明 | — | — | — | `addons/rpc/controllers/__init__.py:6-11`「自 Odoo 19 起弃用，计划 Odoo 22 移除」 |
| 版本探测 | `/web/version`、`/json/version` | 无 | ✅ | `addons/rpc/controllers/__init__.py:24` |
| **API 文档（自描述契约）** | `/doc-bearer/*.json` | Bearer | ✅ | `addons/api_doc/controllers/api_doc.py:49,167` |

**API Key 机制**（`odoo/addons/base/models/res_users.py`）：

- 模型 `res.users.apikeys`（`:1589-1599`，`_auto = False`，自建物理表 `:1601-1614`）
- 字段：`name` / `user_id` / `scope` / `create_date` / `expiration_date`；密钥本体列为 `key`
- 校验 `_check_credentials`（`:1644`）、生成 `_generate`（`:1661`）
- 反向关系 `res.users.api_key_ids`（`:213`）
- 生成/吊销可通过 REST：`/json/2/res.users.apikeys/generate|revoke`

> **重要收益**：`api_doc` 模块提供 `/doc-bearer/*.json`，**Go 侧可以据此自动生成客户端契约**，而不是手写模型字段。这应当作为对接的第一手资料。

**请求约定（JSON-2 特性，必须遵守）**：

| 约定 | 内容 | 依据 |
|---|---|---|
| 参数形式 | **全部为命名参数**，不支持位置参数 | 技术方案 p.6 |
| 多库路由 | **必须携带 `X-Odoo-Database: <db>`**（多库部署时缺省会路由到错误库） | `odoo/addons/test_http/tests/test_webjson2.py` |
| Content-Type | `application/json` | — |
| 鉴权头 | `Authorization: Bearer <API_KEY>`（scope 必须含 `rpc`，或本项目的自定义 scope） | `json2.py:48-56` |
| 返回 | 模型对象返回时**转为 ids**；本项目要求 Facade 返回**版本化 DTO**，不泄露 ORM 记录结构 | 技术方案 p.7 |

### 2.3 可用业务模型（IoT 要挂靠的实体）

| 模型 | 位置 | IoT 关心的字段 |
|---|---|---|
| `maintenance.equipment` | `addons/maintenance/models/maintenance.py:113` | `name:133`、`serial_no:142`（唯一 `:203-206`）、`partner_ref:140`、`category_id:137`、`partner_id:139`、`model:141`、`equipment_properties:151`；来自 mixin `effective_date:76`、`maintenance_team_id:77`、`technician_user_id:78`、`company_id:74` |
| `stock.lot` | `addons/stock/models/stock_lot.py:25` | `name:42`、`ref:43`、`product_id:44`、`location_id:61` |
| `stock.quant` | `addons/stock/models/stock_quant.py:21` | `product_id:47`、`location_id:59`、`lot_id:66`、`quantity:80`、`in_date:93` |
| `stock.move.line` | `addons/stock/models/stock_move_line.py:16` | `product_id:31`、`quantity:38`、`lot_id:49`、`location_id:67`、`location_dest_id:71`、`state:82` |
| `mrp.production` | `addons/mrp/models/mrp_production.py:40` | `name:81`、`product_id:90`、`product_qty:116`、`qty_producing:127`、`state:182-190`、`qty_produced:244`、`date_start:153`、`date_finished:158`、`duration_expected:162`、`duration:163` |
| `mrp.workcenter` | `addons/mrp/models/mrp_workcenter.py:22` | `name:34`、**`barcode:35`**、`costs_hour:47`、`working_state:60`、`oee:70`、`performance:72`、`capacity_ids:84` |
| `mrp.workorder` | `addons/mrp/models/mrp_workorder.py:16` | `workcenter_id:33`、`production_id:42`、`qty_produced:58`、`state:67-74`、`duration_expected:89`、`duration:92`、`date_start:79`、`date_finished:84`、`time_ids:115`、`progress:101` |
| 工时明细 | `mrp.workcenter.productivity`（`mrp_workorder.py:115` 引用） | 工时回流目标 |
| `product.template` | `addons/product/models/product_template.py` | `barcode:192`、`default_code:198` |
| `product.product` | `addons/product/models/product_product.py` | `default_code:44`、`barcode:54` |
| 序列号追踪开关 | `addons/stock/models/product.py:835-841` | `tracking`（`lot` / `serial`）、`lot_sequence_id:850` |

**两个原生缺口 —— 已通过 ADR-017 的扩展方案解决，不再需要「扭曲设计」**：

| 缺口 | 影响 | 原应对（R4 零改动） | **新应对（ADR-017）** |
|---|---|---|---|
| `maintenance.equipment` **无 `location_id`、无 `employee_id`**（已对 `addons/maintenance` 全量搜 `location_id\|employee_id` 零命中） | 设备无法绑定物理位置与现场责任人 | 位置只落 IoT 侧 `tags.site`；责任人借用 mixin 的 `technician_user_id:78`（指向 `res.users`，语义是维护账号） | 新增 `iot_site_id`（→ `iot.site`）与 `iot_employee_id`（→ `hr.employee`，该模块存在）；`technician_user_id` 保留原语义，两者并存 |
| **无 `quality.*` 模型**（Community 特性） | 质检闭环落不了 | 降级为 `maintenance.request`（异常工单） | 自建 `iot.quality.point`（规格）+ `iot.quality.check`（记录）独立闭环，见 §7.7.2 |

### 2.4 事件与钩子（双向都有，且都是官方能力）

**Odoo → 外部（出站 Webhook）**：

- `base.automation`（`addons/base_automation/models/base_automation.py:130`，mixin `mail.thread`）
- 触发器 `trigger:161-186`，**支持 `on_create:170` / `on_create_or_write:171` / `on_write:172` / `on_unlink:174` / `on_webhook:184`**，另含 `on_time_*`、`on_message_*`
- 过滤 `filter_domain:238`（"Apply on"）、`filter_pre_domain:231`、`trigger_field_ids:253`
- 动作 `action_server_ids:141` → `ir.actions.server`
- **`ir.actions.server` 支持 `state='webhook'`（`odoo/addons/base/models/ir_actions.py:596`）**，字段 `webhook_url:687`、`webhook_field_ids:688`，实现 `_run_action_webhook:1058-1100` 用 `requests.post` 在 postcommit 异步发送
- 另有 `state='code':595` 可执行 Python（**不推荐用于集成**，绕过所有治理）

**外部 → Odoo（入站 Webhook）**：

- `base.automation.url:148` + `webhook_uuid:149` + `record_getter:150` + 触发器 `on_webhook:184`
- 即：外部系统 POST 到该 URL 即可触发 Odoo 侧规则

**变更审计流**：

- `maintenance.equipment`、`stock.lot`、`mrp.production`、`mrp.workorder`、`mrp.workcenter` **全部 mixin 了 `mail.thread`** → 变更都会写 `mail.message`。可作为「Odoo 侧变更」的统一旁路观测源（但仍以 `base.automation` 为主）。

### 2.5 部署现实约束

> ⚠️ **重大勘误（五轮）**：本节 C1–C6 原先审计的是 **Windows 上的 `d:/odoo/odoo20tbb/odoo20.conf`**。经核实，该副本是 **2026-09-21 之前的旧布局**（带 `myaddons/`、`http_port=8105`），而服务器上的 `~/projects/AGENTS.md` 明确记载：旧布局 `/home/xfusion/projects/odoo/{19.0,20.0}` 与 `myaddons` 已于 **2026-09-21 删除**。
>
> **权威来源改为服务器实际运行配置**：`devbox` 容器内 `/home/xfusion/etc/odoo/odoo20tbb.conf`
>（Odoo 20.0，systemd 单元 `odoo@odoo20tbb`，库 `odoo20`，http **8070** / gevent **8073**）。
>
> **结论变化很大：原 8 项里有 5 项服务器上早已做对**。此前把它们列为「待整改」，照旧结论去改会把**已经正确的配置改坏**。

| # | 项 | 服务器实际值 | 判定 |
|---|---|---|---|
| C1 | HTTP 监听 | `http_interface = 0.0.0.0`，`http_port = 8070` | ✅ **不存在「仅回环」约束** —— 旧结论作废 |
| C2 | 多进程 | `workers = 0` | ⚠️ **开发模式**（多线程）；进生产前必须按压测设定 |
| C3 | 进程守护 | systemd 单元 `odoo@odoo20tbb`（`Restart=on-failure`） | ✅ 已有守护，非「无守护、单点、重启即断」 |
| C4 | 数据库凭据 | `db_host = /var/run/postgresql`（**Unix socket + peer 认证**），配置中**没有任何口令** | ✅ **不存在明文口令问题**，且比「迁 Vault」更彻底 —— 旧结论作废 |
| C5 | 主口令 | `admin_passwd = 198d…`（24 位随机十六进制） | ✅ **已是强口令** —— 旧结论作废 |
| C6 | 库过滤 | `dbfilter = ^odoo20$` | ✅ 已设置 |
| C7 | 库列表暴露 | `list_db = True` | ⚠️ **少数仍需处理的项**（开发期可接受） |
| C8 | 代理模式 | `proxy_mode = True`，但**当前没有反向代理** | ⚠️ **可疑**：该参数让 Odoo 信任 `X-Forwarded-*` 头。直连却开着，等于信任可伪造的转发头。应二选一：加反代，或改回 `False` |
| C9 | 定时任务 | `max_cron_threads = 0` | ✅ 开发库**故意关闭**（避免自动对外发通知），符合预期 |

**环境事实（实现时必须按这些写，不要按 Windows 副本推测）**：

| 项 | 事实 |
|---|---|
| 运行环境 | `devbox` 容器（Ubuntu 26.04.1 LTS，限 16C / 16G），**容器内不跑 Docker**，服务由 systemd 管理 |
| Odoo 源码与模块 | `/home/xfusion/projects/odoo/odoo20tbb/{odoo,addons}`，且该路径在宿主与容器内**同一份**（bind mount） |
| `addons_path` | 项目内两个目录：`<项目>/odoo/addons` 与 `<项目>/addons` |
| venv / 日志 | `/home/xfusion/venvs/odoo20`；`/home/xfusion/logs/odoo20tbb.log` |
| 数据库 | **PostgreSQL 18.6**（`postgresql@18-main`），**仅监听容器内 `127.0.0.1:5432`** |
| 自定义模块仓库 | `odoo20tbb/addons` 是**独立 git 仓库**，远端 `git@github-odoo20tbb:SNCIC/odoo20tbb.git`，直接提交推 `main` |
| 模块升级流程 | **必须**「`systemctl stop` → `odoo-bin -u` → `systemctl start`」三步，**禁止服务运行中执行 `-i/-u`**，禁止 `kill -9` |
| manifest 约定 | `version = 20.0.x.y.z`、`author = SNCIC`、`license = LGPL-3` |
| 提交风格 | `type(scope): 中文摘要——补充说明`，正文写背景 / 实现要点 / 验证结论 |

### 2.6 Odoo 生产配置基线（采纳技术方案 p.23）

> **第二轮前提修正（五轮）**：审计基线已从 Windows 的 `odoo20.conf` 换成**服务器实际配置**（见 §2.5 勘误）。**原 8 项里有 5 项服务器上早已做对，不再是整改项。**
>
> 「窗口」的含义：这些项都改 `/home/xfusion/etc/odoo/odoo20tbb.conf`，**改完必须按 `AGENTS.md` 的三步流程重启 Odoo**（`systemctl stop` → 改配置 → `systemctl start`），重启期间服务不可用。因此「窗口」= **允许 Odoo 开发实例中断的时间**。
>
> ⚠️ **注意**：devbox 内的 Odoo 是**共享开发实例**（`odoo19wsd` 与 `odoo20tbb` 同宿主），改配置前先确认没有人在用。

```ini
# 服务器实际值（节选）—— 标注的是本轮判定，不是待办
http_interface = 0.0.0.0        # ✅ 已放开，无需处理
http_port      = 8070
dbfilter       = ^odoo20$       # ✅ 已设置
admin_passwd   = <24 位随机>     # ✅ 已合规（旧文档误判为弱口令）
db_host        = /var/run/postgresql   # ✅ socket + peer 认证，无口令（旧文档误判为明文口令）

proxy_mode     = True           # ⚠️ 待定：当前无反代，应二选一
workers        = 0              # ⚠️ 开发模式，进生产需按压测设定
list_db        = True           # ⚠️ 待定
max_cron_threads = 0            # ⚠️ 开发故意关闭；集成测试需临时调起
```

#### 批次划分（按服务器实际配置重定）

| 批次 | 项 | 何时做 | 阻塞什么 |
|---|---|---|---|
| **批次 0 · 已完成，无需动作** | `admin_passwd` 强口令、数据库凭据（Unix socket 无口令）、`dbfilter = ^odoo20$`、systemd 守护 | — | 无。**不要再改** |
| **批次 1 · 生产前** | `list_db = False` | 进生产前（开发期保留 `True` 便于建库） | 不阻塞开发 |
| **批次 2 · Phase 0 压测后** | `workers`（当前 `0`）、`db_maxconn`、`limit_request`、`gevent_workers` | Phase 0 基线采集完成 | **阻塞 connector 限流阈值标定**（§4.3） |
| **批次 3 · 二选一** | `proxy_mode`：加反代则保持 `True`；不加则改 `False` | 进生产前 | 不阻塞开发，**但必须定** —— 否则等于信任可伪造的 `X-Forwarded-*` |
| **批次 4 · 集成测试前** | `max_cron_threads`：`0` → 测试值 | **做 Outbox 集成测试之前** | **直接阻塞 `edge_outbox` 投递链路的可测性** |

#### 逐项说明

| # | 服务器当前值 | 判定 | 批次 | 备注 |
|---|---|---|---|---|
| 1 | `admin_passwd = 198d…` | ✅ 已是强口令 | **0** | 旧文档「改成强口令」属误判 |
| 2 | `db_host = /var/run/postgresql`，无口令 | ✅ Unix socket + peer 认证 | **0** | 旧文档「迁 Vault」属误判，现状更彻底 |
| 3 | `dbfilter = ^odoo20$` | ✅ 已设置 | **0** | — |
| 4 | systemd `odoo@odoo20tbb` | ✅ 已有守护与自动重启 | **0** | — |
| 5 | `list_db = True` | ⚠️ 库列表可探测 | **1** | 副作用：登录页库选择器消失；开发期保留 `True` 更顺手 |
| 6 | `workers = 0` | ⚠️ 开发模式（多线程） | **2** | 决定 connector 限流上限，**必须压测确定**，不能拍脑袋 |
| 7 | 无 `db_maxconn` | ⚠️ 无连接上限 | **2** | 按「所有 Odoo 进程 + 运维连接」核算 |
| 8 | 无 `limit_request` | ⚠️ 无请求体大小限制 | **2** | 按容量策略设定 |
| 9 | `proxy_mode = True` 但无反代 | ⚠️ **可疑配置** | **3** | 二选一：加反代，或改 `False` |
| 10 | `max_cron_threads = 0` | ⚠️ 开发库故意关闭 | **4** | **会导致 `edge_outbox` 的 cron 投递根本不执行** —— 集成测试前必须先调起，否则「Outbox 投递」链路测不出来 |
| 11 | `gevent_workers` 未设 | — | — | 与本次集成无关：connector 只用 JSON-2 短请求，不走长连接 / WebSocket |

#### 对 `odoo-connector` 的实际影响

| 已完成的批次 | connector 可用配置 |
|---|---|
| **批次 0（现状）** | ✅ **可正常开发**：限流取 §4.3 默认值（20 req/s），功能可跑、可测；但**禁止全量数据回填**（`workers = 0` 的多线程模式扛不住批量） |
| 批次 0+2 | 可按压测结果放开限流与批量回写 |
| 批次 0+2+3 | 可跨机 / 经反代部署 |
| 全部 | 可进生产 |

> **结论：现状（批次 0）已足够开始编写 `odoo-connector`，无需任何配置整改。**
>
> 唯一例外是第 10 项：`max_cron_threads = 0` 会让 **Outbox 投递链路在开发环境根本跑不起来**。这暴露一个真实冲突 —— **「开发库故意关 cron」与「集成需要 cron」直接对立**，必须在测试脚本里显式处理（临时调起 → 测完恢复），否则会误判为「代码有问题」。06 文档的 Go-Live Checklist 保留为「进生产前」检查清单。

---

## 3. 集成通道选型（ADR-011）

### 3.1 三条通道对比

| 通道 | 方向 | 实时性 | 对 Odoo 的压力 | 实现成本 | 判定 |
|---|---|---|---|---|---|
| **A. JSON-2 API + API Key** | 双向 | 轮询秒级 / 调用即时 | 中（受连接器限流控制） | 低（零 Odoo 侧开发） | ✅ **主力通道** |
| **B. 自建 bridge 模块 + 路由** | 双向 | 即时 | 中 | 中（需维护 Odoo 模块） | ✅ **写回首选**（业务规则收敛在 Odoo 侧） |
| **C. 出站 Webhook（`base.automation` → `ir.actions.server`）** | Odoo → IoT | **准实时（postcommit）** | 低 | 低（配置化，无代码） | ✅ **变更推送首选** |
| **D. PostgreSQL 直连** | 只读 | 即时 | **高（绕过所有保护）** | 低 | ⚠️ **仅限只读离线分析**（ADR-012） |
| ~~E. `/jsonrpc` `/xmlrpc`~~ | — | — | — | — | ❌ 已弃用，Odoo 22 移除 |

### 3.2 推荐组合

```
┌─────────────────────────────────────────────────────────────────────┐
│                        IoT 平台（Go）                                │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │              odoo-connector（独立服务）                         │ │
│  │  · 认证（API Key 轮换）  · 限流（令牌桶）  · 熔断（gobreaker）   │ │
│  │  · 批量合并  · 幂等（idem_key）  · 重试（指数退避）  · DLQ       │ │
│  └───┬──────────────────────┬──────────────────────┬──────────────┘ │
│      │ ①JSON-2 拉取          │ ②自建模块写回          │ ③接收 webhook │
└──────┼──────────────────────┼──────────────────────┼─────────────────┘
       │ POST /json/2/<m>/<f> │ POST /api/iot/<...>  │ HTTPS 回调
       │ Bearer: API Key      │ Bearer: API Key      │ HMAC 签名校验
       ▼                      ▼                      ▲
┌─────────────────────────────────────────────────────────────────────┐
│                        Odoo 20 Community                             │
│  res.users.apikeys ── 鉴权                                           │
│  sn_edge_integration（自建模块，ADR-014）                             │
│    · edge_facade / iot_facade ── 业务原子方法 + ORM 调用              │
│    · edge_idempotency ── 幂等账本（唯一约束）                        │
│    · edge_outbox ── 事务内事件 + cron 投递                           │
│  base.automation + ir.actions.server(state=webhook) ── 低价值通知    │
│  maintenance / stock / mrp ── 业务数据                               │
└─────────────────────────────────────────────────────────────────────┘
```

| 方向 | 通道 | 用途 |
|---|---|---|
| **Odoo → IoT** | B（bridge 路由） | 主数据全量/增量拉取（设备台账、批次、工单） |
| **Odoo → IoT** | C-1（**Outbox** + cron 投递到 **Redis Streams**） | **高价值事件**：工单状态、库存移动、账务影响 —— 持久化、可重放、可对账。**Odoo 侧不直接写 NATS**，由 connector 翻译（见 §3.2 说明） |
| **Odoo → IoT** | C-2（`base.automation` webhook） | **低价值通知**：设备元数据变更、备注 —— postcommit 异步、零代码，**靠 15 min 定时对账兜底** |
| **IoT → Odoo** | B（bridge 路由） | 遥测摘要、告警工单、生产数据回流（业务规则在 Odoo 侧） |
| **IoT → Odoo** | A（JSON-2，仅标准方法） | 兜底 / 一次性修复 / 脚本化运维 |
| **分析** | D（只读 PG） | 离线聚合、对账（**禁止写**） |

**C-1 与 C-2 的取舍依据**（08 §C4）：`_run_action_webhook`（`ir_actions.py:1058-1100`）确实是 postcommit 异步、不持事务锁，但**不持久** —— 进程在 commit 后崩溃即丢事件，且无重试与 DLQ。因此高价值事件必须走 Outbox。

### 3.3 为什么写回收敛到自建 bridge 模块（通道 B）

| 理由 | 说明 |
|---|---|
| 业务规则归属 | 「收到设备告警 → 要不要开工单、开给谁、什么优先级」是 Odoo 侧业务规则，放在 Go 里会把 ERP 规则散落到外部系统 |
| 幂等与去重 | Odoo 侧可基于 `idem_key` 做唯一约束查询，比 Go 侧先查后写更可靠（避免竞态） |
| 审计 | 通过 ORM 写入自然带 `mail.thread` 消息与字段变更记录 |
| 升级安全 | 业务规则随 Odoo 一起升级；Go 侧只依赖稳定的 HTTP 契约 |
| **已有先例** | 本项目已有 `sn_miniapp_order` 走通了「自建模块 + `auth='public'` + 手工 Bearer」的模式（见 §7.3），风格可复用 |

### 3.4 为什么禁止 Go 直连 PostgreSQL 写（ADR-012）

| 风险 | 说明 |
|---|---|
| 绕过 ACL / 记录规则 | Odoo 的多公司、多仓库权限在 ORM 层，直连 SQL 全部失效 |
| 绕过业务约束 | 必填、onchange、compute、sequence、`_check_*` 全部跳过 → 产生脏数据 |
| 绕过审计 | 无 `mail.message`、无字段追踪，事后无法追溯 |
| Schema 耦合 | Odoo 升级改表即 break，且无编译期保护 |
| 事务与锁 | 与 Odoo 的 ORM 事务竞态，可能造成死锁或丢失更新 |

**只读例外**：离线分析允许只读直连，但必须：① 使用**独立的只读数据库账号**；② 只读视图/物化视图，不直接读业务表；③ 接受「schema 变更即失效」的风险；④ 绝不参与在线链路。

---

## 4. 连接器设计（`odoo-connector`）

### 4.1 组件与部署

| 项 | 设计 |
|---|---|
| 形态 | 独立 Go 服务（`cmd/odoo-connector`），**不嵌入网关/管道** |
| 部署位置 | 与 IoT 平台同集群；通过反向代理访问 Odoo（避开 C1/C5 约束） |
| 副本 | 2+（无状态）；**Odoo 侧写入需按 `entity_type` 分片加锁**，避免并发写同一记录 |
| 依赖 | NATS（收发集成事件）、Redis（限流/幂等/游标）、PG（映射表 + 集成日志） |
| 配置 | Odoo base URL、API Key（Vault 注入）、限流参数、实体白名单 |

### 4.2 认证与凭据

| 项 | 设计 |
|---|---|
| 凭据类型 | `res.users.apikeys` + `Bearer`（scope=`rpc`） |
| 专用账号 | **不使用 `admin`**；创建专用服务账号（如 `svc_iot`），只授予必要模型的 ACL |
| **多库路由** | **每次请求必须携带 `X-Odoo-Database: <db>`**。多库部署下缺省会路由到错误库，属于静默错误，必须在客户端强制注入（配置化，不允许业务代码省略） |
| 存储 | Vault 注入，禁止明文写配置文件 |
| 轮换 | 支持双密钥并存；轮换由运维流程触发，连接器收到 NATS 通知后热加载 |
| 过期 | API Key 设置 `expiration_date`；到期前 30 天告警 |
| 失败处理 | 401/403 → 立即熔断 + P1 告警（**不重试**，避免账号被锁） |
| **上线前置条件** | Odoo 侧 §2.6 的 8 项生产配置**全部整改完成**；未完成时连接器必须以极保守限流运行（≤ 10 req/s）且禁止开启批量回写 |

### 4.3 限流 / 熔断 / 重试 / 幂等

| 机制 | 配置 |
|---|---|
| 限流 | 令牌桶：默认 20 req/s（可配）。**这是保护 Odoo 的第一道闸门**（对应 C2：Odoo 未配 `workers`） |
| 并发 | 最大在途请求 8；超出排队（队列上限 1000，超出拒绝并告警） |
| 超时 | 连接 3s / 读 15s（Odoo 有慢查询风险，读超时给足但不超过 15s） |
| 熔断 | `gobreaker`：连续 10 次失败或失败率 > 50% 且样本 ≥ 20 → 打开 60s |
| 重试 | 仅对 **5xx / 网络错误 / 429** 重试；指数退避 1s/3s/9s，最多 3 次；**4xx 不重试** |
| 幂等 | 见 §4.3.1（五态状态机 + **数据库唯一约束为账本，按 `scope` 划分**：写回路径 → Odoo `edge_idempotency`；IoT 内部操作 → PG `t_idem_registry`，详见 §5.2.1） |
| 死信 | 超过重试上限 → `t_dlq`（`service='odoo-connector'`）+ 对象存储原文 + 按 `entity_type` 聚合告警 |
| 游标 | 拉取使用 `write_date` 水位 + `id` 断点，存 Redis（TTL 7d）+ PG 兜底，支持重放 |
| 时钟 | 依赖 Odoo 的 `write_date`（服务端时间），不信任本地时钟 |

#### 4.3.1 幂等模型（ADR-015）

采用《技术方案》p.11 的完整模型，替换早期简化版。

**状态机**：

```
ABSENT → PROCESSING → SUCCEEDED
                    → FAILED_RETRYABLE   （技术失败，允许自动重试）
                    → FAILED_FINAL       （业务拒绝，禁止重试）
```

**流程**：

1. 调用方（IoT 平台内部）生成幂等键，格式 `idemp:{tenant}:{operation}:{business_key}`，并计算**规范化请求摘要** `request_hash`。
2. `odoo-connector` 在 Redis 原子占位，用于快速拦截并发重复；**相同键但 `request_hash` 不同 → 返回 409**。
3. 连接器调用 Odoo Facade，同时传入幂等键与摘要。
4. Odoo 在数据库唯一约束下创建/读取幂等记录；**业务写与结果记录在同一事务提交**。
5. 响应丢失时，重试相同键；Odoo 返回已提交结果，而**不是再次执行**。

**Odoo 侧账本（`edge_idempotency`）**：

```
UNIQUE(company_id, integration_name, idempotency_key)

字段：request_hash / state / model_name / record_id
      response_json（仅必要字段）/ created_at / expires_at
```

> **Redis 只加速重复判断，不能成为唯一幂等账本。** Redis 丢数据后仍应由 Odoo 唯一约束保证不重复创建。

**`expires_at` 必须按 `integration_name` 分档** —— IoT 场景的最长重试窗口远大于交互式场景：

| 场景 | 最长重试窗口 | `expires_at` |
|---|---|---|
| 交互式业务（工单状态查询等） | 分钟~小时 | 7 天 |
| **设备离线重传**（物模型 `offline_ttl_ms`） | **3~7 天** | **30 天** |
| Odoo 侧人工补单 | 数天 | 90 天 |

#### 4.3.2 错误码映射（统一约定）

| 上游情况 | HTTP | 返回 code | 是否重试 |
|---|---|---|---|
| 未认证 / 令牌无效 | 401 | `AUTH_REQUIRED` | ❌ 立即熔断 + P1 告警 |
| 已认证但被 ACL / 记录规则拒绝 | 403 | `FORBIDDEN` | ❌ 不重试，记审计 |
| Odoo 业务校验失败（必填、约束、业务规则） | 422 | `BUSINESS_REJECTED` | ❌ 不重试 |
| 幂等冲突（同键不同请求摘要） | 409 | `IDEMPOTENCY_CONFLICT` | ❌ 不重试，返回已有结果并告警 |
| 下游（Odoo）超时 | 504 | `UPSTREAM_TIMEOUT` | ✅ **仅在携带幂等键时**重试 |
| 熔断打开 | 503 | `CIRCUIT_OPEN` | ❌ 快速失败，告知调用方退避 |
| 限流拒绝 | 429 | `RATE_LIMITED` | ✅ 退避后重试 |
| Odoo 5xx | 502 | `UPSTREAM_ERROR` | ✅ 退避后重试 |

统一响应体含 `code` / `message` / `trace_id`。**禁止把 Python 堆栈或 ORM 异常原文暴露给外部**（技术方案 p.7）。**技术错误与业务拒绝必须分开**——前者可自动重试，后者重试只会放大问题。

### 4.4 削峰与批量

| 场景 | 策略 |
|---|---|
| 主数据全量拉取 | 分页（`limit=200`）+ 限速（10 页/s）+ 断点续跑 |
| 主数据增量 | 每 60s 按 `write_date > 水位` 拉取；单次上限 2000 条，超限则缩短窗口 |
| 遥测摘要回写 | **聚合后写**（每设备每 5 min 一条），绝不逐条写 |
| 告警工单 | 事件驱动 + 去重（同设备同规则 10 min 内合并为一单） |
| 变更推送接收（C-1 Outbox） | Odoo cron 投递到 **Redis Streams** → **连接器消费后翻译为 `iot.odoo.*` 发布到 NATS** → 下游异步消费；**至少一次投递，幂等按 `event_id` 去重**；XACK 只在业务处理成功后 |
| 变更推送接收（C-2 webhook） | Odoo postcommit → 连接器 HTTP 入口 → 进 NATS 异步消费；**幂等按 `(ext_model, ext_id, write_date)` 去重** |
| **定时对账（必做）** | 每 **15 min** 扫描一次：① Odoo `edge_outbox` 非终态行；② C-2 路径的 `write_date` 水位差；③ 未绑定告警待办。发现遗漏即补投，并对连续两次遗漏告警 |

### 4.5 可观测性

| 类型 | 内容 |
|---|---|
| Metrics | `odoo_connector_request_duration_seconds{op,model}`、`odoo_connector_errors_total{code}`、`odoo_connector_breaker_state`、`odoo_connector_ratelimit_wait_seconds`、`odoo_connector_cursor_lag_seconds`、`odoo_connector_dlq_total` |
| Logs | 结构化 JSON，含 `trace_id` / `entity_type` / `entity_id` / `idem_key`；**禁止打印 API Key** |
| Traces | span：`odoo.pull` / `odoo.push` / `odoo.webhook.recv` |
| 核心 SLO | 主数据同步延迟 P95 < 5 min；告警→工单端到端 P95 < 60 s；连接器自身可用性 99.9% |

---

## 5. 数据映射模型

### 5.1 实体映射总表

| IoT 平台概念 | Odoo 模型 | 方向 | 同步方式 | 备注 |
|---|---|---|---|---|
| 设备（物理资产） | `maintenance.equipment` | Odoo → IoT | 增量轮询 + webhook | `serial_no` 为天然关联键 |
| 设备类型 | `maintenance.equipment.category` | Odoo → IoT | 低频轮询 | 映射到 IoT 的 `device_type` 或标签 |
| 物料 / 序列号 | `stock.lot` | Odoo → IoT | 增量轮询 + webhook | 用于「批次绑定设备」 |
| 生产工单 | `mrp.production` | Odoo → IoT | 增量轮询 | 只读上下文，用于关联产量 |
| 工序 | `mrp.workorder` | 双向 | 轮询 + 回流 | 产量/工时回流 |
| 工作中心 | `mrp.workcenter` | Odoo → IoT | 低频轮询 | `barcode:35` 可作扫码绑定键 |
| 告警 | `maintenance.request` | IoT → Odoo | 事件驱动 | 携带 `iot_*` 结构化字段（§7.7.3） |
| 工序工时 | `mrp.workcenter.productivity` | IoT → Odoo | 工序结束时新建日志 | 唯一正当的**直接写入**点（见 §6 S4） |
| 产量计量 | `iot.meter.reading`（staging） | IoT → Odoo | 聚合（5 min）写 `pending` | 由 Odoo 侧应用到 `mrp.workorder`，见 §6 S4 三段式 |
| 计量点配置 | `iot.meter.point` | Odoo → IoT | 低频轮询 | 换算规则（`scale` / `rollover` / `counter_type`） |
| 质检规格 | `iot.quality.point` | Odoo → IoT | 低频轮询 | 供端侧判断初筛 |
| 质检记录 | `iot.quality.check` | IoT → Odoo | 事件驱动 | 设备测量值 → `pending` → 人工/规则确认 |
| 物理站点 | `iot.site` | Odoo → IoT | 低频轮询 | **位置维度的业务真相** |
| 现场责任人 | `hr.employee`（经 `iot_employee_id`） | Odoo → IoT | 低频轮询 | 与 `technician_user_id` 并存 |

### 5.2 外部引用表（IoT 平台侧，新增）

**核心设计**：映射关系的**权威副本存放在 IoT 平台**（`t_external_ref`）。Odoo 侧允许按 ADR-017 做受控扩展（`iot_device_key` 等字段作为**搜索与展示入口**），但**映射的权威判定仍在 IoT 侧** —— 避免两处都可写造成不一致（铁律 R4 已修订）。

#### 5.2.1 幂等账本归属（按 `scope` 划分，**不是「只有一本」**）

> **勘误**：本节早期写「幂等账本在 Odoo，**IoT 侧不建第二本账**」。该表述**只在「IoT → Odoo 写回」这一条路径上成立**，被当成全局结论后，就与 02 §3.3（`t_idem_registry`）、02 §5.3、08 §2.1 直接矛盾 —— **IoT 侧确实有自己的账本**。正确表述是按 `scope` 划分：

| 幂等范围 | `scope` | **权威账本** | PG / Redis 的角色 |
|---|---|---|---|
| 命令下发、内部事件、告警通知 | `command` / `event` / `alarm_notify` | **PG `t_idem_registry`**（02 §3.3） | PG **即账本** |
| **IoT → Odoo 写回** | `integration` | **Odoo `edge_idempotency`**（§4.3.1） | PG 存**本地回执**（可重建，非权威）；Redis `idemp:` 仅加速 |

**判定顺序（写回路径）**：Redis 占位（快速拦截）→ 命中则查 PG 本地回执 → 未命中或需权威结果时调用 Odoo，由 `UNIQUE(company_id, integration_name, idempotency_key)` 做最终裁决。

> **为什么 Odoo 侧必须是权威**：业务写与幂等记录在 Odoo 的**同一事务**内提交（§4.3.1）。IoT 侧无法独立判断「业务是否已生效」，只能作为回执缓存。
>
> 反过来，命令下发、内部事件这类**根本不落 Odoo** 的操作，Odoo 无从裁决，**只能由 PG 承担** —— 这也是 02 §3.3 把 `t_idem_registry` 独立成非分区表的直接原因（分区表唯一约束无法跨分区生效）。
>
> **两处共同的红线不变**：**Redis 从不是账本**（02 §5.3）。「IoT 侧不建第二本账」的**原意**（不为 Odoo 写回路径再建一本平行账）仍然成立，只是不能外推为「IoT 侧没有账本」。

#### 5.2.2 外部引用表与集成日志（IoT 侧）

```sql
-- 外部系统引用映射：IoT 实体 ↔ 外部系统（Odoo）实体
CREATE TABLE t_external_ref (
  id              BIGINT PRIMARY KEY,
  project_id      BIGINT NOT NULL,
  odoo_company_id BIGINT,                 -- 用于构造缓存键 cache:{tenant}:{company}:...
  ext_system      TEXT   NOT NULL,        -- 'odoo20tbb'
  ext_model       TEXT   NOT NULL,        -- 'maintenance.equipment'
  ext_id          BIGINT NOT NULL,        -- Odoo 记录 id
  ext_version     TEXT,                   -- Odoo write_date / 版本水位
  local_entity    TEXT   NOT NULL,        -- 'device' | 'device_type' | 'group'
  local_id        BIGINT NOT NULL,
  bind_source     TEXT   NOT NULL,        -- 'serial_no' | 'manual' | 'barcode' | 'import'
  bind_confidence SMALLINT NOT NULL DEFAULT 100,  -- 0-100，自动绑定的置信度
  status          TEXT   NOT NULL DEFAULT 'active', -- active | conflict | orphan
  synced_at       TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uk_ext_ref UNIQUE (project_id, ext_system, ext_model, ext_id)
);
CREATE INDEX idx_ext_ref_local ON t_external_ref(project_id, local_entity, local_id);
CREATE INDEX idx_ext_ref_status ON t_external_ref(project_id, status) WHERE status <> 'active';
ALTER TABLE t_external_ref ENABLE ROW LEVEL SECURITY;

-- 集成日志（排查与审计用，**不是幂等账本**）
CREATE TABLE t_integration_log (
  id             BIGINT NOT NULL,
  project_id     BIGINT NOT NULL,
  direction      TEXT   NOT NULL,   -- odoo_to_iot | iot_to_odoo
  channel        TEXT   NOT NULL,   -- json2 | bridge | outbox | webhook | pg_readonly
  entity_type    TEXT   NOT NULL,
  entity_id      BIGINT,
  ext_model      TEXT,
  ext_id         BIGINT,
  idem_key       TEXT   NOT NULL,   -- idemp:{tenant}:{operation}:{business_key}
  request_hash   TEXT   NOT NULL,   -- 规范化请求摘要，与 idem_key 一同校验
  state          TEXT   NOT NULL,   -- ABSENT|PROCESSING|SUCCEEDED|FAILED_RETRYABLE|FAILED_FINAL
  request        JSONB,
  response       JSONB,             -- 仅必要字段，禁止落敏感数据
  error          TEXT,
  trace_id       TEXT,
  duration_ms    INT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
-- 注意：分区表的唯一索引只保证「分区内唯一」。幂等约束不在此表，
-- 而是统一由非分区的 t_idem_registry(project_id, scope, idem_key) 承担（见 02 文档 §3.3）。
CREATE INDEX idx_integration_idem ON t_integration_log(idem_key, created_at);
CREATE INDEX idx_integration_entity ON t_integration_log(project_id, entity_type, entity_id, created_at DESC);
ALTER TABLE t_integration_log ENABLE ROW LEVEL SECURITY;
```

> ⚠️ **勘误（评审 R-04）**：早期在本表上写了 `CREATE UNIQUE INDEX uk_integration_idem ON t_integration_log(idem_key, created_at)`，作为幂等约束 —— **这是错的**：该表按 `created_at` RANGE 分区，唯一索引只在单个分区内生效，**跨月重放不会被拦截**。现降级为普通索引，幂等约束上移到 `t_idem_registry`。

#### 5.2.3 缓存键规范（统一到《技术方案》命名法）

IoT 侧缓存键**迁移到与 `odoo-gateway` 一致的命名法**，避免两个 Go 系统的键空间互不相识、排障困难（ADR-015 / 08 §C8）：

```
cache:{tenant}:{company}:{domain}:v{schema}:{key}
idemp:{tenant}:{operation}:{business_key}
job:{tenant}:{job_type}:{job_id}
lock:{tenant}:{resource}:{resource_id}
```

| 变量 | 取值 |
|---|---|
| `tenant` | `project_id`（IoT 平台的租户） |
| `company` | `odoo_company_id`（来自 `t_external_ref.odoo_company_id`） |
| `domain` | `device` / `device_type` / `equipment` / `lot` / `workorder` |
| `v{schema}` | 缓存对象的结构版本，**结构变更必须递增**（保证旧值自然失效） |

**示例**：

```
cache:10231:7:device:last:v2:100234          # 设备最新值
cache:10231:7:equipment:meta:v1:481          # equipment 元数据
idemp:10231:warranty_order:eq-481-20261001   # 幂等键
lock:10231:workorder:8812                    # 工单级锁
```

> **`tenant` 与 `company` 的映射不变量**：`company` 由 IoT 平台的映射链决定（`device_key → project_id → odoo_company_id`），**设备永远不能通过 Payload 或 Header 指定 company**（对齐 08 §C1 与《技术方案》p.15「禁止客户端直接决定 allowed_company_ids」）。

### 5.3 设备 ↔ Odoo 实体的绑定策略

**绑定键优先级**（自动绑定，按置信度降序）：

| 优先级 | 绑定键 | 置信度 | 说明 |
|---|---|---|---|
| 1 | `maintenance.equipment.serial_no` ↔ IoT `device.serial_no` | 100 | 最可靠；`serial_no` 在 Odoo 侧有唯一约束（`maintenance.py:203-206`） |
| 2 | `mrp.workcenter.barcode` ↔ IoT 设备扫码绑定 | 100 | `mrp_workcenter.py:35`，现场扫码录入 |
| 3 | `stock.lot.name` / `ref` ↔ IoT 设备批次 | 90 | 批次级绑定 |
| 4 | 人工在 IoT 控制台绑定 | 100（人工确认） | `bind_source='manual'` |
| 5 | 命名规则推断（如 `device_key` 含设备编号） | 60 | **仅建议，不自动生效**，需人工确认 |

**冲突与孤儿处理**：

| 情况 | 处理 |
|---|---|
| Odoo 侧 `serial_no` 重复/修改 | 标记 `status='conflict'`，控制台待办 + 告警；**不自动重绑** |
| Odoo 记录被删除 | 标记 `status='orphan'`，保留映射用于历史数据关联；设备继续工作（解绑不打断数据采集） |
| 一个 Odoo 实体绑多个 IoT 设备 | 允许（如一个工作中心挂多个传感器），但需在 IoT 侧显式配置「主设备」 |
| 一个 IoT 设备绑多个 Odoo 实体 | 禁止（一对多会造成告警工单重复），标记 `conflict` |

### 5.4 字段级映射（示例：设备台账）

| Odoo 字段 | 位置 | IoT 平台字段 | 方向 | 说明 |
|---|---|---|---|---|
| `id` | `maintenance.py:113` | `t_external_ref.ext_id` | ↔ | 主键映射 |
| `name` | `:133` | `device.name` | Odoo → IoT | 以 Odoo 为准 |
| `serial_no` | `:142` | `device.serial_no` | Odoo → IoT | 绑定键，**IoT 侧只读** |
| `category_id` | `:137` | `device_type` 或 `device.tags.odoo_category` | Odoo → IoT | 映射到设备类型需人工配置对照表 |
| `partner_id` | `:139` | `device.tags.vendor` | Odoo → IoT | 只读展示 |
| `partner_ref` | `:140` | `device.tags.vendor_ref` | Odoo → IoT | 厂商参考号 |
| `model` | `:141` | `device.tags.model` | Odoo → IoT | — |
| `effective_date` | `:76` | `device.tags.in_service_date` | Odoo → IoT | 投用日期 |
| `technician_user_id` | `:78` | `device.tags.responsible` | Odoo → IoT | **替代缺失的 `employee_id`** |
| `maintenance_team_id` | `:77` | `device.tags.maintenance_team` | Odoo → IoT | 用于告警路由 |
| `equipment_properties` | `:151` | — | — | 视内容决定是否同步 |
| `write_date` | 通用 | `t_external_ref.ext_version` | Odoo → IoT | 增量水位 |
| （IoT 独有） | — | `device.device_key` | IoT 内部 | **不回写 Odoo**（避免改表，铁律 R4） |
| （IoT 独有） | — | `device.tags.site` | IoT 内部 | **不回写**（`equipment` 无 `location_id`） |

> **`location_id` 缺失的应对（已按 ADR-017 修订）**：新增 `iot.site` 模型 + `maintenance.equipment.iot_site_id`，**Odoo 侧成为位置维度的业务真相**；IoT 侧 `device.tags.site` 保留为端侧配置，由 connector 双向校验。现场责任人用新增的 `iot_employee_id`（→ `hr.employee`），与 `technician_user_id`（维护账号，原语义）并存 —— 两者语义不同，不可互相替代。

---

## 6. 集成场景（按优先级）

### S1 · 主数据同步：设备台账（P0）

```
Odoo maintenance.equipment
  │ ① 全量：连接器调 POST /json/2/maintenance.equipment/search_read
  │         （分页 limit=200，带 X-Odoo-Database）
  │ ② 增量：双通道
  │     C-1 高价值（归档 / 报废 / 责任团队变更）
  │          → edge_outbox（与业务同事务写入）
  │          → cron 投递到 Redis Streams
  │          → odoo-connector 消费并翻译 → NATS `iot.odoo.*`
  │     C-2 低价值（名称 / 标签 / 备注）
  │          → base.automation(on_create/on_write)
  │             → ir.actions.server(state=webhook)
  │             → POST IoT /internal/v1/odoo/events（HMAC 签名 + 时间戳窗口 ±5 min）
  ▼
odoo-connector
  ├─ 校验签名 + 幂等（C-1 按 event_id；C-2 按 (ext_model, ext_id, write_date)）
  ├─ 解析 → 按 §5.3 规则做绑定（serial_no 优先）
  ├─ 写入/更新 IoT device 元数据（tags）+ t_external_ref（含 odoo_company_id）
  ├─ 精确失效缓存 cache:{tenant}:{company}:equipment:*（禁止全量 flush）
  └─ 冲突/孤儿 → 待办 + 告警
       ▲
       │ 每 15 min 对账：扫 edge_outbox 非终态行 + C-2 的 write_date 水位差，补投遗漏
```

**要点**：

- IoT 侧对 Odoo 来的字段**只读**；IoT 自己新增的字段（`device_key` / `site`）**不回写**。
- **C-2 路径必须配对账兜底** —— webhook 是 postcommit 异步但**非持久**，进程崩溃即丢事件（§3.2）。

### S2 · 设备实时状态在 Odoo 侧可见（P1）

**不采用**「IoT 把遥测写进 Odoo」的做法（违反铁律 R1）。两种可选实现：

| 方案 | 实现 | 评价 |
|---|---|---|
| **推荐**：反向查询 | `sn_edge_integration` 加一个 `compute` 字段或按钮，前端点击时**经 `odoo-gateway` 中转**调 IoT API `GET /v1/devices/{key}/latest` 返回最新值 | 零存储、零同步延迟、Odoo 无压力；符合 ADR-013「人的入口只有一个」 |
| 备选：摘要落库 | 每 5 min 把「最新值 + 在线状态」写到 Odoo 的自建轻量表（定义在 `sn_edge_integration` 内，**不放业务表**） | 可在 Odoo 列表视图直接看，但引入写压力 |

### S3 · 告警 → Odoo 维护工单（P0）

```
IoT 告警进入 active
  ▼ NATS: iot.alarm.{project}
odoo-connector
  ├─ 查 t_external_ref 解析 device_id → maintenance.equipment.id
  │    └─ 未绑定 → 落「未绑定告警」待办，不建工单（避免脏数据）
  ├─ 去重：同 device + 同 rule 在 10 min 内合并为一单
  ├─ 幂等键：idem:{tenant}:maintenance_req:eq-{eq_id}-{alarm_dedup_key}
  │          request_hash = sha256(规范化 payload)
  ▼ POST /api/iot/v1/maintenance/request   （走 sn_edge_integration 的 iot_api 路由）
sn_edge_integration（Odoo 侧）
  ├─ 幂等：查 edge_idempotency 的 UNIQUE(company_id, integration_name, idempotency_key)
  │    ├─ 命中且 request_hash 一致 → 直接返回已提交的 record_id（不重复创建）
  │    └─ 命中但 request_hash 不一致 → 409 IDEMPOTENCY_CONFLICT
  ├─ 校验 equipment 存在且未被归档
  ├─ 创建 maintenance.request，携带结构化 IoT 字段（§7.7.3）：
  │     iot_alarm_id / iot_severity / iot_device_key
  │     iot_metric_snapshot(Json) / iot_alarm_ts
  │     + 描述含告警详情与曲线链接
  │    与 edge_idempotency 结果记录**在同一事务提交**（§4.3.1）
  ├─ 写 edge_outbox（工单已创建事件，供 C-1 投递）
  └─ 返回 odoo_id → 回写 t_integration_log + t_external_ref(reverse)
```

**注意**：

- `expires_at` 按 `integration_name='maintenance_req'` 取 **90 天**（覆盖人工补单窗口）。
- Community 版**无 `quality.alert`**，异常一律走 `maintenance.request`；若未来需要质检闭环，二期自建轻量模型。
- **Odoo 写入量约束**（对齐 08 §7.3）：
  ```
  Odoo 写入量 ≈ 有效告警数 × (1 - 告警合并率) + 产量回流频率 × 在线设备数
  约束：告警入队速率 < connector 的处理速率，否则积压无限增长
  → 超载时降级为「只记 IoT 侧，延后补建工单」，不允许无限堆积
  ```

### S4 · 生产数据回流（P1）

**第一步是核实字段性质** —— 已对 Odoo 20.0 源码逐字段确认，这决定了哪些能写、哪些只能读：

| 目标字段 | 位置 | 字段性质 | 能否外部写入 |
|---|---|---|---|
| `mrp.production.qty_produced` | `mrp_production.py:244` | `compute="_get_produced_qty"`，**非 store** | ❌ **不可写**（只读汇总，来自 done moves） |
| `mrp.workcenter.working_state` | `mrp_workcenter.py:60` | `compute="_compute_working_state", store=True` | ❌ **不可写** |
| `mrp.workcenter.oee` / `performance` / `blocked_time` / `productive_time` | `mrp_workcenter.py:64-72` | 全部 compute，非 store | ❌ 不可写 |
| `mrp.workorder.state` | `mrp_workorder.py:67-74` | `compute` + `store=True` | ❌ 不可写 |
| `mrp.workorder.qty_produced` | `mrp_workorder.py:58` | 普通 Float，**可写**，`tracking=True` | ⚠️ 技术上可写，但**不建议** |
| `mrp.workorder.duration` / `date_start` / `date_finished` | `mrp_workorder.py:79-94` | compute + **inverse** | ⚠️ 可经 inverse 写，但仍走 Odoo 方法更安全 |
| `mrp.workcenter.productivity` | `mrp_workcenter.py:515` | 普通模型（工时日志） | ✅ **唯一正当的外部写入点** |

**修订后的回流设计（一期）**：

| 回流项 | IoT 来源 | Odoo 目标 | 频率 | 说明 |
|---|---|---|---|---|
| 工序工时 | 设备运行时长 | **`mrp.workcenter.productivity`**（新建日志行） | 工序结束时 | `workcenter_id:536` + `loss_id:544` 必填；`date_start:550` / `date_end:551` 控制区间；`duration:552` 由 compute 得出，**不可直接写** |
| 停机原因 | 设备告警码 | 经 **`iot.loss.mapping`** 解析为 `mrp.workcenter.productivity.loss_id` | 事件驱动 | **不直接映射**：先查 `iot.loss.mapping`（决策 #23）。`loss_id` 为 `required` + `ondelete='restrict'`；原生 loss 字典的 `loss_type` 仅 `quality` / `availability` |
| **工序产量** | 设备计数（光电/编码器） | **`iot.meter.reading`（staging）→ Odoo 业务方法应用** | 5 min 聚合 | 见下方「产量三段式」 |
| 工作中心状态 | 设备在线/运行/停机 | **`iot_state`（新增字段，§7.7.3）** | 1 min | **不写 `working_state`**（compute 不可写）；如需联动，用 `base.automation` 在 `iot_state` 变化时触发动作 |

**产量三段式（ADR-017 后重做，替代原「完全不写 Odoo」）**：

```
① IoT 侧    设备原始计数 → t_meter_reading（原始时序，不承载业务语义）
② staging   connector 每 5 min 聚合 → 写 iot.meter.reading(state='pending')
              · delta_qty 由 iot.meter.point 的 scale / rollover / counter_type 换算
③ 应用      Odoo 侧（cron 或操作员按钮）调**业务方法**把 pending 行应用到
              mrp.workorder.qty_produced → 标记 state='applied' + applied_by/at
              拒绝时 state='rejected' + reject_reason
```

**这样做的三个好处**：

1. **产量数据在 Odoo 里有账**，不再漂在 Odoo 之外；
2. **不绕过业务方法** —— 应用动作由 Odoo 侧发起，`finished move` 等副作用正常产生；
3. **可对账可拒绝** —— `pending / applied / rejected` 三态让「设备算的」与「Odoo 认的」可逐条比对，差异进 `reject_reason` 统计。

**红线（均据源码核实）**：

1. **绝不写 compute 字段** —— `mrp.production.qty_produced`（`mrp_production.py:244`，非 store）、`mrp.workcenter.working_state`（`mrp_workcenter.py:60`）、`oee` / `performance`（`:64-72`）、`mrp.workorder.state`（`:67-74`）全部是 compute。
2. **绝不绕过业务方法写数量** —— `mrp.workorder.qty_produced`（`mrp_workorder.py:58`）虽是可写的普通 Float，但直接写**不会创建 finished move**，会造成产量与库存脱节。**必须走「staging + Odoo 应用」路径**。
3. **换算规则不写在 IoT 侧** —— `scale` / `rollover` / `counter_type` 属于业务规则，放在 `iot.meter.point`，变更走 Odoo 侧配置与审计。

> ⚠️ **两次勘误记录**：① 本文早期版本建议「只写 `working_state`」，源码核实该字段是 `compute="_compute_working_state", store=True`，**根本不可写**；② 早期版本因 R4 零改动约束改为「产量一期完全不写 Odoo」，现按 ADR-017 改为 staging 三段式。两处均已修订。

### S5 · 批次/库存事件联动（P2）

```
Odoo stock.lot 创建/变更 → webhook → IoT
  └─ 用途：① 批次与设备绑定；② 出货前自动校验收货设备的质检数据
```

### S6 · Odoo 下发指令到设备（P1）

**原则：Odoo 只表达「意图」，IoT 平台负责「执行」与「结果回传」。**

```
Odoo 操作员在 maintenance.equipment 上点击「启动设备」按钮
  ▼ sn_edge_integration 路由 /api/iot/v1/device/command
  └─ body: { equipment_id, cmd: "start", params: {...}, idem_key }
odoo-connector
  ├─ 解析 equipment_id → device_id（查 t_external_ref）
  ├─ 转调 IoT 平台内部 API（复用既有 svc-device 命令通道）
  └─ 返回 command_id（异步）
设备执行 → IoT 回传结果 → 连接器 → POST 回 sn_edge_integration → 写 Odoo 消息/备注
```

**要点**：Odoo 侧**同步等待仅到「已受理」**，不等待设备应答（设备离线可能数十秒），结果通过消息异步回填。

---

## 7. 需要在 Odoo 侧开发的模块：`sn_edge_integration`

### 7.1 模块定位与命名（ADR-014）

**本模块是 `odoo-gateway` 与本项目共用的唯一桥接模块。** 《技术方案》p.7 已规划其结构（`edge_facade` / `edge_job` / `edge_idempotency` / `edge_outbox`），本项目只在该结构上新增 IoT 专属部分，**不另开模块**。

| 命名维度 | 取值 | 依据 |
|---|---|---|
| 模块目录名 | `sn_edge_integration` | 前缀 `sn_` 沿用 `myaddons/README.md:8` 的团队规范 |
| 模型名前缀 | `edge_*` | 沿用《技术方案》p.7，保证与其接口契约文档一致 |
| manifest 版本 | `20.0.x.y.z` | `myaddons/README.md:8` |
| `author` / `license` | `SNCIC` / `LGPL-3` | 同上 |

> **为什么必须合并**：两个模块会产生**两套幂等表、两套 outbox、两套审计、两套服务账户映射**。这是《行业手册》p.46 反模式「规则被多处复制 → 逐渐不一致」的模块级变体。

### 7.2 模块结构（合并后）

```
myaddons/sn_edge_integration/
├── __manifest__.py
├── models/
│   ├── edge_facade.py            # 业务原子方法（《技术方案》p.7）
│   ├── edge_job.py               # 计算任务状态机（《技术方案》）
│   ├── edge_idempotency.py       # 幂等结果账本（《技术方案》，§4.3.1）
│   ├── edge_outbox.py            # 事务内事件（《技术方案》）
│   ├── iot_facade.py             # IoT 专属原子方法（本项目）
│   ├── iot_site.py               # 物理站点（替代缺失的 location 维度）
│   ├── iot_meter.py              # 计量点 + 产量读数 staging
│   ├── iot_quality.py            # 质检规格 + 检查记录
│   └── iot_ext.py                # 核心模型扩展字段（_inherit，§7.7.3）
├── controllers/
│   ├── api.py                    # /api/v1/* 业务路由（《技术方案》）
│   └── iot_api.py                # /api/iot/v1/* IoT 路由（本项目）
├── security/
│   ├── iot_security.xml          # 权限组定义
│   └── ir.model.access.csv       # ACL + 记录规则
├── data/
│   └── ir_cron.xml               # outbox dispatcher / 对账 / 产量应用
├── views/
│   ├── equipment_views.xml       # 继承式：加 iot_ 字段 + 智能按钮
│   ├── workcenter_views.xml      # 继承式：加 iot_state
│   ├── maintenance_request_views.xml
│   ├── iot_site_views.xml
│   ├── iot_meter_views.xml
│   └── iot_quality_views.xml
├── EXTENSIONS.md                 # ★ 扩展登记表（§7.7.4），CI 强制核对
└── tests/
    ├── test_facade.py
    ├── test_iot_api.py
    ├── test_idempotency.py
    ├── test_outbox.py
    ├── test_meter_apply.py        # 产量 staging → 应用 → 拒绝
    └── test_extension_registry.py # 校验 EXTENSIONS.md 与实际字段一致
```

```python
# myaddons/sn_edge_integration/__manifest__.py
{
    'name': 'SNCIC 边缘集成桥接（业务 + IoT）',
    'version': '20.0.1.0.0',
    'author': 'SNCIC',
    'license': 'LGPL-3',
    'depends': [
        'maintenance', 'stock', 'mrp', 'base_automation', 'mail',
        'hr',            # 现场责任人：iot_employee_id → hr.employee
    ],
    'data': [
        'security/iot_security.xml',
        'security/ir.model.access.csv',
        'data/ir_cron.xml',
        'views/equipment_views.xml',
        'views/workcenter_views.xml',
        'views/maintenance_request_views.xml',
        'views/iot_site_views.xml',
        'views/iot_meter_views.xml',
        'views/iot_quality_views.xml',
    ],
}
```

**Facade 规则**（沿用《技术方案》p.7）：

| 规则 | 内容 |
|---|---|
| 一个公共方法对应一个**业务意图**，而不是一个 CRUD 动作 | 例如 `edge.iot.create_maintenance_request`，而非 `maintenance.request.create` |
| 先校验调用用户、公司、记录规则，再读取或写入 | 不使用 `sudo` 绕过隔离 |
| 所有副作用在同一事务中；失败整体回滚 | 含 `edge_idempotency` 结果记录 |
| 返回**版本化 DTO**，不把 ORM 记录结构泄露给网关 | — |

**Outbox 规则**（沿用《技术方案》p.7）：

| 规则 | 内容 |
|---|---|
| 业务数据与 outbox 行**在同一事务**写入 | — |
| 提交后由 cron/worker 投递，**不在 `write()` 中同步 HTTP 调用** | — |
| 事件含 `event_id` / `tenant_id` / `company_id` / `aggregate_id` / `version` / `occurred_at` | 对齐 01 文档 §7.2 信封 |
| 投递失败指数退避；超过阈值进入 dead-letter 并告警 | — |

### 7.2 路由清单

| 路由 | 方法 | 用途 | 幂等 | `expires_at` |
|---|---|---|---|---|
| `/api/iot/v1/equipment/search` | POST | 设备台账分页拉取（增量按 `write_date`） | 只读，无需 | — |
| `/api/iot/v1/equipment/detail` | POST | 单条详情 | 只读，无需 | — |
| `/api/iot/v1/lot/search` | POST | 批次拉取 | 只读，无需 | — |
| `/api/iot/v1/workorder/search` | POST | 工序拉取 | 只读，无需 | — |
| `/api/iot/v1/maintenance/request` | POST | 创建维护工单 | ✅ 必需 | **90 天** |
| `/api/iot/v1/workorder/report` | POST | 产量/工时回流 | ✅ 必需 | 30 天 |
| `/api/iot/v1/device/command` | POST | 下发指令（转 IoT） | ✅ 必需 | 7 天 |
| `/api/iot/v1/health` | GET | 健康检查 | 无 | — |

所有路由统一：`auth='public'` + **自定义 Bearer 鉴权** + `save_session=False`（对齐 `sn_miniapp_order` 的既有模式，见 §7.4）。**必须携带 `X-Odoo-Database`**。

**统一响应信封**（与 IoT 平台及《技术方案》错误码表一致，便于 Go 侧复用解析）：

```json
{ "ok": true,  "code": 0,     "msg": "",                "data": {...}, "trace_id": "..." }
{ "ok": false, "code": 40901, "msg": "idempotency conflict", "data": {"id": 123}, "trace_id": "..." }
```

`code` 取值遵循本文 §4.3.2 的映射表（`AUTH_REQUIRED` / `FORBIDDEN` / `BUSINESS_REJECTED` / `IDEMPOTENCY_CONFLICT` / …），**禁止把 Python 堆栈或 ORM 异常原文返回**。

### 7.4 与 `sn_miniapp_order` 的风格一致性

`sn_miniapp_order` 已跑通的模式，`sn_edge_integration` 应保持一致：

| 维度 | `sn_miniapp_order` 做法 | `sn_edge_integration` 采用 |
|---|---|---|
| 路由装饰器 | `type='jsonrpc', auth='public', methods=['POST'], save_session=False` | 同（`auth='public'` + 自定义鉴权，避免依赖 session） |
| 鉴权 | 自定义 Bearer（`controllers/common.py:28-37`），令牌只存 sha256（`models/miniapp_session.py:21`） | 同思路；凭据表独立，不复用小程序会话表 |
| 服务层分层 | `miniapp.core.service` / `miniapp.order.service`（`models/miniapp_order_service.py:34`） | `sn_edge_integration.service` + 按域拆分 |
| 审计 | 独立 `miniapp.audit.log`（`models/miniapp_audit_log.py:9`） | 复用 `t_integration_log`（IoT 侧）+ Odoo 侧 `mail.message` |
| 错误处理 | `models/miniapp_utils.py` 错误类 | 复用同一套错误码风格 |

> **建议**：直接把 `sn_miniapp_order/controllers/common.py` 的 Bearer 解析与 `request.update_env(user=...)` 模式抽成公共工具，两个模块共用。

### 7.5 安全要求

| 项 | 要求 |
|---|---|
| 专用账号 | `svc_iot` 账号，仅授予上述模型的读 + 必要写的 ACL，**不给 admin、不用 sudo** |
| 路由鉴权 | 复用 `res.users.apikeys`（scope 自定义，如 `iot_bridge`）或独立令牌表 |
| 服务身份拆分 | **按集成拆分 API 用户**（业务 Facade / IoT 各一个），不共享管理员密钥（《技术方案》p.15） |
| 入站校验 | HMAC 签名（`X-Signature`）+ 时间戳窗口 ±5 min + `idem_key` + `request_hash` 去重 |
| 字段白名单 | 每个 Facade 方法显式声明可读字段清单，**不接受调用方传入任意 `fields` 列表** |
| **模型方法白名单** | **不提供任意 `model`/`method` 透传代理**；外部路由只能映射到固定 Facade 方法（《技术方案》p.15） |
| 公司边界 | `company_id` 由服务端映射确定，**禁止调用方指定 `allowed_company_ids`** |
| 出站 webhook（C-2） | `base.automation` → `ir.actions.server(state='webhook')`，URL 为 IoT 平台**内网地址**；禁止公网暴露 |
| **出站 Outbox（C-1）** | 事件含 `event_id` / `tenant_id` / `company_id` / `aggregate_id` / `version` / `occurred_at`；投递失败指数退避 + DLQ 告警 |
| 数据最小化 | 只返回必要字段，不外泄成本、供应商价格等敏感字段 |
| 审计 | 所有写操作写 `mail.message`（ORM 自动）+ 记 `trace_id` / `idem_key` / 来源账号 |
| 日志脱敏 | 日志、trace、错误体中**不得出现 API Key / Authorization / 完整个人信息 / 支付信息**（《技术方案》p.15） |
| 禁止 | **禁止使用 `ir.actions.server` 的 `state='code'`（`ir_actions.py:595`）做集成** —— 绕过所有治理与审计 |

### 7.6 服务账号与 ACL 清单（决策 #15）

**按集成拆分两个账号**（《技术方案》p.15：「服务身份：按集成/租户拆分 Odoo API 用户，不共享管理员密钥」）：

| 账号 | `login` | 用途 | API Key scope |
|---|---|---|---|
| 业务 Facade 账号 | `svc_iot_facade` | 主数据拉取（设备台账 / 批次 / 工单）、创建维护工单 | `iot_facade` |
| 设备域账号 | `svc_iot_device` | 工时日志写入、指令受理、健康检查 | `iot_device` |

**自定义权限组**（定义在 `sn_edge_integration/security/`）：

| 组 | 模型 | 读 | 写 | 建 | 删 |
|---|---|---|---|---|---|
| `group_iot_facade` | `maintenance.equipment` | ✅ | ❌ | ❌ | ❌ |
| | `maintenance.request` | ✅ | ✅ | ✅ | ❌ |
| | `maintenance.equipment.category` | ✅ | ❌ | ❌ | ❌ |
| | `stock.lot` | ✅ | ❌ | ❌ | ❌ |
| | `mrp.production` | ✅ | ❌ | ❌ | ❌ |
| | `mrp.workorder` | ✅ | ❌ | ❌ | ❌ |
| | `hr.employee` | ✅ | ❌ | ❌ | ❌ |
| | `res.partner` / `res.company` | ✅ | ❌ | ❌ | ❌ |
| `group_iot_device` | `mrp.workcenter` | ✅ | ❌ | ❌ | ❌ |
| | `mrp.workcenter.productivity` | ✅ | ✅ | ✅ | ❌ |
| | `mrp.workcenter.productivity.loss` | ✅ | ❌ | ❌ | ❌ |
| | `maintenance.equipment` | ✅ | ❌ | ❌ | ❌ |
| | `iot.quality.check` | ✅ | ✅ | ✅ | ❌ |
| 两者共有 | `edge.idempotency` | ✅ | ✅ | ✅ | ❌ |
| | `edge.outbox` | ✅ | ❌ | ✅ | ❌ |
| | `iot.site` / `iot.meter.point` / `iot.quality.point` / `iot.loss.mapping` | ✅ | ❌ | ❌ | ❌ |
| | `iot.meter.reading` | ✅ | ✅（仅 `state='pending'`） | ✅ | ❌ |
| 人工角色（`group_iot_manager`） | `iot.site` / `iot.meter.point` / `iot.quality.point` | ✅ | ✅ | ✅ | ✅ |
| | `iot.meter.reading` | ✅ | ✅ | ❌ | ❌ |

**字段级权限**：

- `iot_*` 扩展字段对服务账号**只读**（除 `iot.meter.reading` / `iot.quality.check` 这两个由 connector 创建的 staging 模型）。
- `iot_site_id` / `iot_employee_id` 由**人工角色**（`group_iot_manager`）维护，服务账号不可写 —— 这两个字段是业务配置，不是设备数据。
- 通过 `ir.model.fields` 的 `groups` 属性限制敏感字段（如后续引入的成本类 IoT 指标）。

**硬性要求**：

| # | 要求 | 原因 |
|---|---|---|
| 1 | **不授予任何模型的 `unlink` 权限** | 外部系统不得删除 Odoo 记录 |
| 2 | **不授予 `res.users` / `ir.model` / `ir.config_parameter` / `ir.rule` 的访问权** | 防提权 |
| 3 | **不使用 `sudo()`**；如确有跨公司访问需求，必须是显式、有记录、有评审的例外 | 越权风险 |
| 4 | 记录规则必须限制到本公司范围：`[('company_id', 'in', company_ids)]` | 公司边界 |
| 5 | API Key 的 `scope` 与 `/json/2` 的 `rpc` scope **分开** | 防止服务账号被用于通用 RPC 透传 |
| 6 | `expiration_date` 必填，到期前 30 天告警 | 凭据生命周期 |

### 7.7 Odoo 扩展清单与升级风险控制（ADR-017）

> 前提变更：**允许在自有模块内扩展 Odoo**。早期「零改动」约束造成了多处设计扭曲（位置无处存、产量漂在 Odoo 之外、`working_state` 不可写导致状态无处落、Community 无质检可落）。本节给出放开后的完整清单与配套的风险控制。

#### 7.7.1 扩展策略：什么能做，什么不能做

| 允许 | 禁止 |
|---|---|
| `_inherit` 新增字段（统一前缀 `iot_`） | 修改核心字段的类型 / 必填 / `compute` 定义 |
| 新增自有模型（`iot.*` / `edge.*`） | 覆盖核心方法的核心行为（`create` / `write` / `_compute_*` / `button_*`） |
| 新增权限组、记录规则 | 使用 `sudo()` 绕过权限 |
| 继承式视图（`xpath` 插入按钮 / 智能按钮） | 替换（`position="replace"`）核心视图节点 |
| 新增 `ir.cron`、`base.automation`、报表 | 修改上游源码树（`odoo/`、`addons/`）中的任何文件 |
| 新增自有 `Selection` 字段 | 向核心 `Selection` 字段追加选项（升级必坏） |

**若确需 override 核心方法**，必须同时满足：

1. 调用 `super()` 且**不改变返回值语义**；
2. 有对应的回归测试；
3. 在 §7.7.4 的扩展登记表中登记并附升级影响评估；
4. 经架构评审（≥ 2 人）。

#### 7.7.2 新增模型清单

| 模型 | 用途 | 关键字段 | 写入方 |
|---|---|---|---|
| `edge.idempotency` | 幂等账本（§4.3.1） | `company_id` / `integration_name` / `idempotency_key` / `request_hash` / `state` / `model_name` / `record_id` / `response_json` / `expires_at` | connector |
| `edge.outbox` | 事务内事件 | `event_id` / `tenant_id` / `company_id` / `aggregate_id` / `event_type` / `version` / `occurred_at` / `state` / `attempts` | Odoo 业务事务 |
| `edge.job` | 计算任务状态机（《技术方案》） | `job_type` / `contract_version` / `algorithm_version` / `state` | Odoo / 顾问 |
| **`iot.site`** | 物理站点（**替代缺失的位置维度**） | `name` / `code` / `parent_id` / `geo_lat` / `geo_lng` / `timezone` | 人工 / 导入 |
| **`iot.meter.point`** | 计量点：**设备计数 → 产量单位的换算规则** | `name` / `device_key` / `workcenter_id` / `equipment_id` / `unit` / `counter_type`(`absolute`/`incremental`) / `rollover` / `scale` / `active` | 人工配置 |
| **`iot.meter.reading`** | 产量读数暂存（staging） | `meter_point_id` / `workorder_id` / `workcenter_id` / `raw_value` / `delta_qty` / `ts` / `state`(`pending`/`applied`/`rejected`) / `applied_by` / `applied_at` / `reject_reason` | connector（聚合后写） |
| **`iot.loss.mapping`** | **设备告警码 → 原生停机原因**的映射（决策 #23） | `project_id` / `company_id` / `workcenter_id`（可空=租户级默认）/ `alarm_code` / `loss_id`(→ `mrp.workcenter.productivity.loss`) / `auto_create_productivity` / `min_duration_s` / `active` | 人工配置 |
| **`iot.quality.point`** | 质检规格定义 | `name` / `product_id` / `workcenter_id` / `check_type` / `spec_min` / `spec_max` / `unit` / `active` | 人工配置 |
| **`iot.quality.check`** | 质检记录 | `point_id` / `device_key` / `equipment_id` / `workorder_id` / `measured_value` / `result`(`pass`/`fail`/`warn`) / `state`(`pending`/`confirmed`/`rejected`) / `source`(`device`/`manual`) / `checked_by` / `checked_at` | connector / 人工 |

#### 7.7.3 新增字段清单

| 目标模型 | 字段 | 类型 | 写入方 | 用途 |
|---|---|---|---|---|
| `maintenance.equipment` | `iot_device_key` | Char, index | connector | 设备绑定键，Odoo 侧可直接搜索筛选 |
| | `iot_project_id` | Integer, readonly | connector | IoT 租户（跳转用） |
| | `iot_site_id` | Many2one `iot.site` | 人工 | **替代缺失的 `location_id`** |
| | `iot_employee_id` | Many2one `hr.employee` | 人工 | **现场责任人**（区别于 `technician_user_id` 的系统账号语义，两者并存） |
| | `iot_online` | Boolean, readonly | connector | 在线状态（展示用） |
| | `iot_last_seen` | Datetime, readonly | connector | 最后通信时间 |
| | `iot_firmware_version` | Char, readonly | connector | OTA 定向与合规审计 |
| `stock.lot` | `iot_device_key` | Char, index | connector | 批次 ↔ 设备绑定 |
| `mrp.workcenter` | `iot_device_key` | Char, index | connector | 与已有 `barcode:35` 互补（扫码用 barcode，集成用 device_key） |
| | `iot_state` | Selection(`running`/`idle`/`fault`/`offline`), readonly | connector | **替代不可写的 `working_state`**（见 §6 S4） |
| | `iot_state_ts` | Datetime, readonly | connector | 状态时间戳 |
| `maintenance.request` | `iot_alarm_id` | Char, index | connector | 告警去重与回链 |
| | `iot_severity` | Selection(`info`/`warn`/`critical`) | connector | **结构化严重级别**（替代在描述里贴文本） |
| | `iot_device_key` | Char, index | connector | 设备回链 |
| | `iot_metric_snapshot` | Json | connector | 触发告警的指标快照 |
| | `iot_alarm_ts` | Datetime | connector | 告警发生时间 |

> **字段前缀统一 `iot_`**：便于识别扩展字段、便于升级时批量检查、便于未来清理。

#### 7.7.4 扩展登记表（CI 强制）

**每新增一个字段、模型或 override 一个方法，都必须在本表登记**，否则阻断合并。

| # | 目标模型 | 类型 | 名称 | 引入版本 | 升级影响评估 | 回归用例 |
|---|---|---|---|---|---|---|
| E1 | `maintenance.equipment` | 字段 | `iot_device_key` | 20.0.1.0.0 | 无（独立字段，不参与核心 compute） | `test_iot_api.py::test_equipment_binding` |
| E2 | `mrp.workcenter` | 字段 | `iot_state` | 20.0.1.0.0 | 无（**不 override `_compute_working_state`**） | `test_iot_api.py::test_workcenter_state` |
| E3 | `maintenance.request` | 字段 | `iot_alarm_id` 等 5 项 | 20.0.1.0.0 | 无 | `test_iot_api.py::test_maintenance_request` |
| — | … | … | … | … | … | … |

**升级检查方法（Odoo 21 升级前必做）**：

1. 逐行核对本表，确认目标模型与字段在 21 中**未被重命名 / 移除 / 改变类型**；
2. 对被引用的核心字段（`state`、`qty_produced`、`barcode` 等），`grep` 21 源码确认定义未变；
3. 跑全部 `tests/`；
4. 在 staging 用**真实数据副本**演练升级。

#### 7.7.5 数据归属的迁移（放开扩展后的调整）

| 数据 | 原方案（R4 零改动） | **新方案（ADR-017）** |
|---|---|---|
| 设备物理位置 | 只存 IoT 侧 `device.tags.site` | **Odoo `iot.site` 为业务真相**；IoT 侧保留 `tags.site` 作为端侧配置，connector 双向校验 |
| 现场责任人 | 借用 `technician_user_id` | **`iot_employee_id`（`hr.employee`）**；`technician_user_id` 保留原语义 |
| 产量换算规则 | 在 IoT 侧硬编码 | **迁到 `iot.meter.point`**（`scale` / `rollover` / `counter_type`）—— 换算规则属于业务，属于 Odoo |
| 产量数据 | 只存 IoT 侧 `t_meter_reading` | **Odoo `iot.meter.reading` 为业务账**（`pending` → `applied`）；IoT 侧 `t_meter_reading` 只保留原始时序 |
| 质检 | 降级为 `maintenance.request` | **`iot.quality.point` + `iot.quality.check`** 独立闭环 |
| 工作中心状态 | 无处可写（`working_state` 是 compute） | **`iot_state` 独立字段**，不碰核心 compute |
| 设备绑定 | 仅 IoT 侧 `t_external_ref` | `t_external_ref` 仍为权威；**Odoo 侧 `iot_device_key` 提供搜索与筛选入口** |

**边界不变的部分**：告警 FSM、时序数据、规则引擎、设备接入仍全部归 IoT 平台。扩展只是让 Odoo 侧能**结构化承载业务结果**，不是把 IoT 的能力搬进 Odoo（守住铁律 R6）。

#### 7.7.6 `iot.loss.mapping`：为什么不复用原生 loss 字典加字段（决策 #23）

**问题**：IoT 收到设备告警码（如 `E01`）时，要把它翻译成 Odoo 的停机原因（`mrp.workcenter.productivity.loss`）才能开工时日志。映射放哪？

**候选方案对比**：

| 方案 | 做法 | 否决理由 |
|---|---|---|
| A. 在原生 loss 上加 `iot_alarm_code` 字段 | `_inherit` 给 `mrp.workcenter.productivity.loss` 加一列 | ❌ **原生 loss 字典是全局的**（`_name = 'mrp.workcenter.productivity.loss'`，`_order = "sequence, id"`，**无 `company_id`**）。两个租户都想把 `E01` 映到不同原因时会直接冲突 |
| B. 并入 `iot.meter.point` | 复用计量点模型 | ❌ **职责混淆**：`iot.meter.point` 是「设备计数 → 产量单位」的换算规则；停机原因是另一件事。一个是连续性数值换算，一个是离散枚举映射 |
| **C. 单开 `iot.loss.mapping`（采纳）** | 新建租户级映射模型，`loss_id` 指向**原生 loss** | ✅ **复用原生的原因字典**（业务人员在 Odoo 正常维护原因名称，平台不重复维护一套）；映射本身是租户/工作中心级的 |

**这就是「复用数据，不复用记录」**：`mrp.workcenter.productivity.loss` 作为**原因字典**被复用（不新增、不改名），而**映射关系**是新增的租户级数据。

**字段设计**：

| 字段 | 类型 | 说明 |
|---|---|---|
| `project_id` | Integer, required, index | IoT 租户（告警码是租户级的） |
| `company_id` | Many2one `res.company`, required | 用于记录规则与 RLS |
| `workcenter_id` | Many2one `mrp.workcenter`, **可空** | 空 = 租户级默认映射；非空 = 该工作中心的专用映射 |
| `alarm_code` | Char, required, index | 设备告警码（如 `E01`） |
| `loss_id` | Many2one `mrp.workcenter.productivity.loss`, required, `ondelete='restrict'` | 映射到的**原生**停机原因 |
| `auto_create_productivity` | Boolean, **默认 `false`** | 是否自动创建工时日志（见下方红线） |
| `min_duration_s` | Integer, 默认 60 | 停机时长短于此不记录（防抖） |
| `active` | Boolean | — |

**唯一约束（利用 PG17 特性）**：

```sql
-- PostgreSQL 15+ 支持 NULLS NOT DISTINCT，正好解决「workcenter_id 可空」的唯一性问题
-- （否则 NULL 互不相等，会允许重复的租户级默认映射）
CONSTRAINT uk_loss_mapping
  UNIQUE NULLS NOT DISTINCT (project_id, workcenter_id, alarm_code);
```

**解析顺序**（在 `edge_iot` 的 Facade 方法内）：

```
1. 查 (project_id, workcenter_id, alarm_code)   —— 工作中心级专用映射
2. 未命中 → 查 (project_id, NULL, alarm_code)   —— 租户级默认映射
3. 都未命中 → **不创建 productivity 记录**，落「未映射告警」待办 + 告警
   （与「未绑定告警」同一条待办链路，见 §6 S3）
```

**红线：`auto_create_productivity` 默认关闭。**

`mrp.workcenter.productivity` 是**工时原始日志**，`mrp.workcenter` 的 `oee`（`:70`）与 `performance`（`:72`）都由它计算得出。IoT 自动建记录 = **直接影响 OEE 报表**。因此：

| 阶段 | 行为 |
|---|---|
| **一期（默认）** | `auto_create_productivity = false`：IoT 只把停机事件转成 `maintenance.request`（已有链路，见 §6 S3），**不碰 productivity** |
| 二期（业务确认后） | 在确认「IoT 自动记录与人工报工的去重规则」后，**按 mapping 逐条开启** |

> 这与产量走「staging + Odoo 应用」是**同一个理由**（铁律 R6）：扩展只做承载，不做业务裁决。让外部系统静默写进 OEE 的计算输入，属于典型的「用自定义字段绕过核心业务流」。

---

## 8. 数据库与升级风险

| 风险 | 影响 | 对策 |
|---|---|---|
| Odoo 20 → 21/22 升级 | 自定义模块与 JSON-2 契约可能变更 | 对接逻辑集中在 `sn_edge_integration` + Go 侧 `pkg/odoo` 适配层；升级前跑集成回归用例；`/json/2` 是官方推荐通道，弃用风险低于 `/jsonrpc` |
| **新增 `iot_*` 字段与模型被升级破坏**（ADR-017 引入） | 扩展字段引用的核心模型/字段在 21 中被重命名或移除（如 `mrp.workorder.state`、`mrp.workcenter.barcode`） | **扩展登记表（§7.7.4）逐行核对**为升级前置项；核心字段定义用 `grep` 验证未变；禁止 override 核心方法语义（降低耦合面）|
| `/jsonrpc` `/xmlrpc` 在 22 移除 | 若误用会直接失效 | 铁律 R3：从第一天只用 `/json/2`；CI 中禁止出现 `jsonrpc`/`xmlrpc` 字样 |
| `api_doc` 的 `/doc-bearer/*.json` 演进 | 契约漂移 | 定期拉取并与本地 `pkg/odoo/client_gen` 对比，差异告警 |
| Community 无 `quality.*` | 质检场景无法闭环 | 一期降级到 `maintenance.request`；二期自建轻量模型，或评估升级 Enterprise |
| `maintenance.equipment` 无 `location_id` | 位置信息无处落 | 位置只存 IoT 侧；Odoo 侧用 `maintenance_team_id` + `technician_user_id` |
| 直连 PG 只读的 schema 耦合 | Odoo 升级即 break | 限定为离线分析；用视图隔离；失败不进入在线链路 |
| **`odoo20.conf` 明文口令** | 凭据泄露（C4） | **待确认是否已入 Git**；确认后立即轮换并改用环境变量 / Vault。**任何文档、日志、工单中不得复述该口令** |
| **Odoo 生产配置 8 项未整改**（§2.6） | 多线程非生产模式、库列表可探测、无请求体与连接上限；connector 的限流阈值无法安全设定 | 作为 `odoo-connector` 上线**前置条件**；未整改前限流压到 ≤ 10 req/s 且禁止批量回写 |
| **webhook 路径丢事件**（C-2 非持久） | 高价值事件若误走 webhook，进程崩溃即永久丢失 | 高价值事件强制走 Outbox；webhook 路径配 15 min 对账兜底 |
| **两套队列（Streams + NATS）并存** | 排障需跨两套工具；可能出现环形放大 | `odoo-connector` 是唯一翻译层；**禁止双向互写**（08 §C2） |

---

## 9. 可靠性与降级

| 故障 | IoT 平台行为 | 恢复 |
|---|---|---|
| Odoo 不可达 | 设备接入/遥测/规则**全部不受影响**；集成事件积压于 NATS（Outbox 继续在 Odoo 侧累积） | 恢复后按游标回放；告警工单批量补建并合并去重 |
| Odoo 响应慢（> 15s） | 熔断打开；事件继续积压 | 熔断半开探测成功后自动恢复 |
| API Key 失效（401） | **立即停止重试** + P1 告警（避免账号锁定） | 运维轮换后热加载 |
| 桥接模块被卸载 / 升级中 | 契约探测失败 → 降级到通道 A（JSON-2 标准方法） | 模块恢复后回到通道 B |
| **Outbox 投递积压 / DLQ 增长** | Odoo 侧事务不受影响（outbox 行已提交）；告警提示投递链路故障 | 修复后由 cron 重投；DLQ 人工重放 |
| **对账任务发现遗漏** | 自动补投；连续两次遗漏 → P2 告警 | 排查 webhook / Outbox 中断原因 |
| 映射缺失（设备未绑定） | **不写 Odoo**；落「未绑定」待办队列 | 人工绑定后批量补投 |
| Odoo 数据被人工修改 | 以 Odoo 为真相（主数据域）；冲突标记待办 | §5.3 冲突处理 |
| Redis 不可用（占位丢失） | 幂等快速拦截失效，但 **Odoo 唯一约束仍保证不重复** | Redis 恢复后占位自然重建 |

**核心保证**：**Odoo 的可用性不影响 IoT 平台的核心能力**（接入/时序/规则/告警）。集成是「最终一致」的旁路，不是同步依赖。

---

## 10. 分期落地

| 阶段 | 集成交付 | 验收标准 |
|---|---|---|
| **P0**（与 IoT Phase 1 同期） | ① **Odoo 侧 §2.6 的 8 项生产配置整改完成**（前置条件）② `t_external_ref` / `t_integration_log` 建表（含 `odoo_company_id`）③ `odoo-connector` 骨架：**先只做透传** —— 认证、白名单、超时、trace、错误码映射，**不上缓存与批量** ④ S1 主数据同步（C-2 webhook + 对账） | Odoo 侧配置检查单全绿；设备台账双向一致（冲突清单为空）；`X-Odoo-Database` 生效于所有请求；Odoo 重启不影响 IoT；灰度 5%→25%→100% 完成 |
| **P1**（与 IoT Phase 2 同期） | ① `sn_edge_integration` 模块落地（`edge_idempotency` / `edge_outbox` / `edge_facade` / `iot_facade` / `iot_api`）② **C-1 Outbox 接通** ③ S1 增量切到 C-1（高价值事件）④ S3 告警→工单 ④ S4 生产数据回流（三段式）⑤ S6 指令下发 ⑥ **ADR-017 扩展落地**（`iot.site` / `iot.meter.*` / `iot.quality.*` + `iot_*` 字段 + `EXTENSIONS.md`） | **幂等用例全绿**：同键同摘要只产生一笔记录、同键异摘要返回 409；响应丢失后重试不重复创建；重复投递不产生重复工单；产量 `pending → applied / rejected` 流转正确且拒绝原因可统计；Outbox 投递至少一次且可重放；扩展登记表与实际字段一致 |
| **P2**（与 IoT Phase 3 同期） | S2 实时状态在 Odoo 可见（经 `odoo-gateway` 中转）；S5 批次联动；**缓存键迁移到 `cache:{tenant}:{company}:...` 完成** | 反向查询 P95 < 300 ms；Odoo 侧无新增写压力；缓存键命名与 `odoo-gateway` 完全一致 |
| **P3** | 只读 PG 离线对账；集成 SLO 看板；升级回归用例集；容量不等式监控 | 对账差异 < 0.1%；Odoo 21 升级演练通过；告警入队速率恒小于 connector 处理速率 |

> **阶段划分依据**：《技术方案》p.20 的实施路线（0 诊断 → 1 接入治理 → 2 读路径 → 3 写路径 → 4 计算热点 → 5 规模化）已逐项映射到上述 P0–P3（详见 08 文档 §8）。

---

## 11. 决策记录

### 11.1 已裁定（全部采纳并落地）

**架构层（来自 08 文档）**

| # | 事项 | 裁定 | 落点 |
|---|---|---|---|
| 9 | 桥接模块是否合并 | ✅ **合并为 `sn_edge_integration`**，模型名沿用 `edge_*` | ADR-014、本文 §7 |
| 10 | 缓存键是否迁移 | ✅ **迁移到 `cache:{tenant}:{company}:{domain}:v{n}:{key}`** | ADR-015、本文 §5.2.3 |
| 11 | 边缘 agent 语言 | ✅ **Go 先行**；Rust 等 profiler 数据 | ADR-016、08 §C3 |
| 12 | 两套队列是否并存 | ✅ **并存**；connector 为唯一翻译层，禁止双向互写 | 08 §C2 |
| 13 | `project_id` ↔ `odoo_company_id` | ✅ **允许一对多**；映射维护在 connector，设备不可指定 | 本文 §5.2.3 |
| 14 | Odoo 8 项配置整改 | ✅ **拆为 4 个批次**；本地开发只需批次 1 的 3 项（零窗口成本）。**是「进生产前」的前置条件，不是「开工前」**（见 §2.6 与 §11.2 的澄清） | 本文 §2.6、06 §9 |

**集成层（本轮裁定）**

| # | 事项 | 裁定 |
|---|---|---|
| 15 | **服务账号与权限范围** | ✅ **按集成拆分两个账号**：`svc_iot_facade`（业务 Facade，只读 + 工单创建）与 `svc_iot_device`（设备/IoT 域，只读 + 工时日志 + 指令受理）。**均非 admin、均不用 sudo**，ACL 清单见 §7.6 |
| 16 | **生产数据回流粒度** | ✅ **产量走三段式**：IoT 原始时序 → `iot.meter.reading`(staging, `pending`) → Odoo 侧业务方法应用到 `mrp.workorder`（`applied` / `rejected`）。**不写任何 compute 字段、不绕过业务方法**。工时仍写 `mrp.workcenter.productivity`。详见 §6 S4 |
| 17 | **位置维度** | ✅ **Odoo 侧新增 `iot.site` 模型 + `maintenance.equipment.iot_site_id`**，作为位置维度的业务真相；IoT 侧 `device.tags.site` 保留作端侧配置，由 connector 双向校验。同时新增 `iot_employee_id`（→ `hr.employee`）承载现场责任人 |
| 18 | **质量场景** | ✅ **自建轻量质检闭环**：`iot.quality.point`（规格）+ `iot.quality.check`（记录），不改用 `maintenance.request` 降级，也不评估升级 Enterprise |
| 21 | **工作中心 IoT 状态** | ✅ 新增 `mrp.workcenter.iot_state`（`running`/`idle`/`fault`/`offline`）独立字段承载；**不 override `_compute_working_state`**；如需联动（如自动开关工时日志），用 `base.automation` 挂 `iot_state` 变化 |
| 22 | **Odoo 扩展的治理方式** | ✅ 全部扩展集中在 `sn_edge_integration`；字段统一 `iot_` 前缀；**扩展登记表（§7.7.4）为 CI 强制项**；升级前逐行核对；禁止改上游源码树 |
| **23** | **设备告警码 → 停机原因的映射归属** | ✅ **单开 `iot.loss.mapping`**（§7.7.6）。理由：① 原生 `mrp.workcenter.productivity.loss` 是**全局字典无 `company_id`**，直接加字段会在多租户下冲突；② 并入 `iot.meter.point` 属职责混淆（连续换算 vs 离散枚举映射）。**复用原生原因字典、新增租户级映射**；唯一约束用 PG15+ 的 `UNIQUE NULLS NOT DISTINCT` 支持「`workcenter_id` 可空」；`auto_create_productivity` 默认 `false`（不静默影响 OEE） |
| **24** | **Odoo 一期网络与开发环境** | ⚠️ **已作废，待重定（五轮）**。原结论「`odoo-connector` 与 Odoo 同机、走 `127.0.0.1:8105`、不放开 `http_interface`」**建立在错误前提上** —— 它依据的是 Windows 旧布局副本。服务器实际：Odoo 20 运行在 **devbox 容器内，`http_interface = 0.0.0.0` / `http_port = 8070`**，且开发环境已迁到服务器。新拓扑见 §2.5 勘误；**待定：IoT 平台与 connector 部署在 devbox 内还是宿主 Docker** |
| 19 | **指令下发审批** | ✅ **高危指令需二次确认**：按指令在物模型中标 `risk_level`；`high` 级必须① 有 `device_operator` 以上角色的 Odoo 用户发起，② 弹窗二次确认并填理由，③ 全量写审计。**低危指令免确认但记审计**。详见 05 文档 §3.3 |
| 20 | **Redis 实例** | ✅ **IoT 侧独立实例**，不与 `odoo-gateway` 共用。理由：故障域隔离（IoT 的限流与会话状态直接影响设备接入，不能被 Odoo 缓存淘汰挤占）；且 07 §4.3 的幂等占位本就是「可丢」语义，与 Odoo 侧 `cache` 的语义不同 |

### 11.2 环境整改（已确认，分批执行）

原先挂起的 3 项已全部确认，**不阻塞一期开发**：

| # | 事项 | 确认结果 | 执行批次 |
|---|---|---|---|
| 1 | **Odoo 网络可达方式**（C1） | ⚠️ **已作废（五轮）**：原「仅 127.0.0.1:8105」的约束**不存在** —— 服务器实际已 `http_interface = 0.0.0.0` / `http_port = 8070`。开发环境已迁至服务器（devbox 容器）。**待定：IoT 平台部署位置**（devbox 内 → loopback 8070；宿主 Docker → `100.64.0.3:9070`） | 开工前定 |
| 2 | **`odoo20.conf` 是否已提交 Git** | ✅ **未入 Git**，无泄露事实。仍需在 `.gitignore` 显式加入 `odoo20.conf` | 立即（零成本） |
| 3 | **Odoo 8 项配置整改** | 拆为 4 个批次（§2.6）。**本地开发只需批次 1 的 3 项**（`list_db` / `admin_passwd` / `dbfilter`，零窗口成本） | 见 §2.6 |

**关键澄清：这些是「进生产前」的前置条件，不是「开工前」的前置条件。**

早期把它们列为 P0 阻塞项是过严的 —— 本地开发重启 Odoo 零成本，且其中 5 项（`workers` / `db_maxconn` / `limit_request` / `max_cron_threads` / `proxy_mode`）**要么必须等压测数据、要么本地根本不需要**，现在无法也不该确定。

| 阶段 | 需要完成的批次 | 对 connector 的影响 |
|---|---|---|
| **本地开发（现在）** | 批次 1 | ✅ 可正常开发：限流取默认 20 req/s，功能可跑可测；**但禁止全量数据回填** |
| Phase 0 压测后 | 批次 2 | 可按压测结果放开限流与批量回写 |
| 进生产 | 批次 3 + 4 | 可跨机部署、可上线 |

> **结论：设计侧已全部就绪，无剩余阻塞项。** 唯一的即时动作是给 `.gitignore` 加一行。
