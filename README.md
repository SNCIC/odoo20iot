# odoo20iot

面向 Odoo 20 的 IoT 平台（Go 实现）。完整设计文档见 [`docs/`](./docs/)。

## 当前阶段

**Phase 0 · 技术验证**。本仓库目前只包含：

- 可编译、可运行的最小 Go 骨架（证明构建链路可用）
- 本地开发栈的 Compose 定义
- 既有设计文档（10 份）

Phase 0 的目标**不是做出产品**，而是回答 ADR-001 / 002 / 003 三个决策点是否成立。

## 环境

| 角色 | 位置 |
|---|---|
| 代码与运行 | 宿主 `xfusion-163`（Ubuntu 24.04，64C / 62G），全部服务跑宿主 Docker Compose |
| Go 工具链 | `devbox` 容器 `/usr/local/go`（**1.27.1**），仅供编译与测试 |
| Odoo 20 | `devbox` 容器，systemd `odoo@odoo20tbb`；容器内 `127.0.0.1:8070`，Tailscale `100.64.0.3:9070` |
| 代码路径 | `/home/xfusion/projects/odoo20iot`（宿主与 devbox 共享同一份，bind mount） |

> **模块代理必须用镜像**：官方 `proxy.golang.org` 在本网络不可达，已固定
> `GOPROXY=https://goproxy.cn,direct` 与 `GOTOOLCHAIN=local`（见 `go env`）。

## 本地开发栈

```bash
cd deploy/compose
docker compose up -d
docker compose ps
```

| 服务 | 容器内 | Tailscale 发布 | 用途 |
|---|---|---|---|
| PostgreSQL 18 | 5432 | `100.64.0.3:28543` | 控制面台账、幂等注册表 |
| Redis 7 | 6379 | `100.64.0.3:28637` | 缓存、限流、最新值 |
| NATS 2（JetStream） | 4222 / 8222 | `100.64.0.3:28222` / `28224` | 事件总线、离线队列 |
| GreptimeDB | 4000 / 4001 / 4003 | `100.64.0.3:28400` / `28401` / `28403` | 遥测时序 |

**端口全部落在 28xxx 段**，避开宿主已占用：`80/443`（dify-nginx）、`3001`（grafana）、
`3002`（uptime-kuma）、`5678`（n8n）、`8060`、`9090/9093`、`9069/9070/9072/9073`（devbox Odoo）、
`9910/9920`（cc-connect）。

服务和端口统一绑定 Tailscale IP `100.64.0.3`，局域网 `192.168.127.163` 上不暴露
（沿用 `devbox` 的安全基线）。**副作用**：Tailscale 未启动时本栈无法启动。

## 构建与测试

Go 命令在 `devbox` 容器内执行：

```bash
docker exec -u xfusion devbox bash -lc 'cd /home/xfusion/projects/odoo20iot && go build ./... && go test ./...'
```

`make` 目标见 `Makefile`（`make help`）。

## 目录结构

```
cmd/          每个服务一个 main，只做装配
internal/     私有实现，按域分包
pkg/          可被外部复用的库，禁止依赖 internal
api/          OpenAPI / protobuf 契约
deploy/       部署定义（compose = 开发栈；helm = 交付）
migrations/   PostgreSQL 迁移
test/         压测与混沌脚本
docs/         设计文档
```

## 与 Odoo 的边界（铁律）

1. **Odoo 是唯一真相源**：业务数据不直写 Odoo 库，一律走 JSON-2 API 或自建模块的 Facade 方法。
2. **Odoo 不进遥测热路径**：该实例 `workers = 0`、`max_cron_threads = 0`。集成一律「事件级、异步、可积压、最终一致」。
3. **模块升级必须走三步**：`systemctl stop` → `odoo-bin -u` → `systemctl start`，禁止服务运行中执行 `-i/-u`。

> 注意：`max_cron_threads = 0` 会让 `edge_outbox` 的 cron 投递在开发环境**不执行**，
> 集成测试前需临时调起（见 `docs/07-odoo-integration.md` §2.6 第 10 项）。
