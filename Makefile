# odoo20iot —— 开发命令入口
#
# Go 命令在 devbox 容器内执行（宿主不装 Go）；Compose 在宿主执行。
# 若在 devbox 内直接运行 make，GO_RUN 可置空：make test GO_RUN=

DEVBOX      ?= devbox
DEVBOX_USER ?= xfusion
PROJECT_DIR ?= /home/xfusion/projects/odoo20iot
GO_RUN      ?= docker exec -u $(DEVBOX_USER) $(DEVBOX) bash -lc
NATS_URL    ?= nats://100.64.0.3:28222
GREPTIME_DSN ?= postgres://greptime:greptime@100.64.0.3:28403/public
IOT_PG_DSN   ?= postgres://iot:iot_dev_only_change_me@100.64.0.3:28543/odoo20iot

COMPOSE_DIR := deploy/compose
WEB_DIR     := $(PROJECT_DIR)/web
WEB_EMBED   := $(PROJECT_DIR)/internal/querysvc/web/dist
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -X github.com/SNCIC/odoo20iot/internal/buildinfo.Version=$(VERSION) \
               -X github.com/SNCIC/odoo20iot/internal/buildinfo.Commit=$(COMMIT) \
               -X github.com/SNCIC/odoo20iot/internal/buildinfo.BuildTime=$(BUILD_TIME)

.PHONY: help
help: ## 显示可用目标
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## ---------- Go ----------

.PHONY: web-build
web-build: ## 构建 Vue 控制台并同步到 Go embed 目录
	cd $(WEB_DIR) && npm ci && npm run build
	find $(WEB_EMBED) -mindepth 1 -delete 2>/dev/null || true
	mkdir -p $(WEB_EMBED)
	cp -R $(WEB_DIR)/dist/. $(WEB_EMBED)/

.PHONY: web-typecheck
web-typecheck: ## 检查 Vue 控制台 TypeScript
	cd $(WEB_DIR) && npm ci && npm run typecheck

.PHONY: fmt
fmt: ## 格式化
	$(GO_RUN) 'cd $(PROJECT_DIR) && gofmt -l -w .'

.PHONY: vet
vet: ## 静态检查
	$(GO_RUN) 'cd $(PROJECT_DIR) && go vet ./...'

.PHONY: test
test: ## 单元测试
	$(GO_RUN) 'cd $(PROJECT_DIR) && go test ./...'

.PHONY: test-race
test-race: ## 单元测试（含竞态检测）
	$(GO_RUN) 'cd $(PROJECT_DIR) && go test -race ./...'

.PHONY: tidy
tidy: ## 整理依赖
	$(GO_RUN) 'cd $(PROJECT_DIR) && go mod tidy'

.PHONY: build
build: web-build ## 编译全部服务到 bin/
	$(GO_RUN) 'cd $(PROJECT_DIR) && mkdir -p bin && \
	  go build -ldflags "$(LDFLAGS)" -o bin/iot-gateway ./cmd/iot-gateway && \
	  go build -ldflags "$(LDFLAGS)" -o bin/odoo-connector ./cmd/odoo-connector && \
	  go build -ldflags "$(LDFLAGS)" -o bin/svc-query ./cmd/svc-query'

.PHONY: test-nats
test-nats: ## A2 真实总线验证（要求 NATS 可达；`go test ./...` 默认跳过）
	$(GO_RUN) 'cd $(PROJECT_DIR) && IOT_NATS_URL=$(NATS_URL) \
	  go test ./internal/gateway -run TestA2_RealNATS -count=1 -v'

.PHONY: test-tsdb
test-tsdb: ## B1 补充项（1）· 明细限行真实库验证（要求 GreptimeDB 可达；⚠️ 会 drop/重建 telemetry）
	$(GO_RUN) 'cd $(PROJECT_DIR) && IOT_GREPTIMEDB_DSN=$(GREPTIME_DSN) IOT_PERF_ASSERT=1 \
	  go test ./internal/tsdb/greptimedb -run TestQuerySeries -count=1 -v'

.PHONY: test-rollup
test-rollup: ## B1 补充项（2）· 预聚合真实库验证（⚠️ 会 drop/重建 telemetry 与 telemetry_1m）
	$(GO_RUN) 'cd $(PROJECT_DIR) && IOT_GREPTIMEDB_DSN=$(GREPTIME_DSN) IOT_PERF_ASSERT=1 \
	  go test ./internal/tsdb/greptimedb -run TestRollup -count=1 -v'

.PHONY: test-rollup-pg
test-rollup-pg: ## 预聚合水位账本的真实 PG 验证（会建临时库）
	$(GO_RUN) 'cd $(PROJECT_DIR) && IOT_PG_DSN=$(IOT_PG_DSN) \
	  go test ./internal/rollup -run TestPGStore -count=1 -v'

.PHONY: test-catalog
test-catalog: ## 控制面主数据（catalog）真实 PG 验证（会建临时库）
	$(GO_RUN) 'cd $(PROJECT_DIR) && IOT_PG_DSN=$(IOT_PG_DSN) \
	  go test ./internal/catalog -run TestPGStore -count=1 -v'

.PHONY: test-query
test-query: ## 查询服务单元测试（不依赖真实库）
	$(GO_RUN) 'cd $(PROJECT_DIR) && go test ./internal/querysvc ./internal/apiauth -count=1'

.PHONY: test-query-e2e
test-query-e2e: ## 查询服务端到端（真实 PG + 真实 GreptimeDB；⚠️ 会 drop/重建 telemetry）
	$(GO_RUN) 'cd $(PROJECT_DIR) && IOT_PG_DSN=$(IOT_PG_DSN) IOT_GREPTIMEDB_DSN=$(GREPTIME_DSN) \
	  go test ./internal/querysvc -run TestE2E -count=1 -v'

.PHONY: test-latest
test-latest: ## 最新值缓存与管道 Write-Through 单测
	$(GO_RUN) 'cd $(PROJECT_DIR) && go test ./internal/latest ./internal/pipeline ./internal/querysvc -count=1'

.PHONY: seed-dev
seed-dev: ## 写入开发种子（租户 / 设备类型 / 设备）
	$(GO_RUN) 'cd $(PROJECT_DIR) && go run ./cmd/iot-seed -pg-dsn $(IOT_PG_DSN) \
	  -devices 3 -out tmp/iot-seed-creds.json'

.PHONY: run-svc-query
run-svc-query: ## 前台运行查询服务（dev 静态令牌；仅绑本机）
	$(GO_RUN) 'cd $(PROJECT_DIR) && go run ./cmd/svc-query -pg-dsn $(IOT_PG_DSN) \
	  -auth-mode dev -dev-token devtoken -dev-project-id 1 -log-format text'

.PHONY: b1-bench
b1-bench: ## Phase 0 · B1 时序表模型压测（需要 GreptimeDB 可达，约 3 分钟）
	$(GO_RUN) 'cd $(PROJECT_DIR) && go run ./cmd/tsdb-bench -plan all \
	  -fleet-devices 300 -span 24h -hot-devices 12 -hot-span 6h \
	  -writers 8 -query-iters 30 -value-model correlated \
	  -report tmp/b1-report.md'

.PHONY: c1-bench
c1-bench: ## Phase 0 · C1 规则条件引擎：P99 验收 + 求值基准
	$(GO_RUN) 'cd $(PROJECT_DIR) && IOT_PERF_ASSERT=1 go test ./internal/rules -run TestPerf -count=1 -v'
	$(GO_RUN) 'cd $(PROJECT_DIR) && go test ./internal/rules -run XXX -bench . -benchtime 300000x'

.PHONY: a1-capacity
a1-capacity: ## Phase 1 · A1 认证容量实测（Argon2 参数 + 重连风暴，约 3 分钟）
	$(GO_RUN) 'cd $(PROJECT_DIR) && IOT_CAPACITY=1 go test ./internal/auth -run TestCapacity -count=1 -v'

.PHONY: a4-bench
a4-bench: ## Phase 0 · A4 网关连接容量压测（⚠️ 5 万连接须分机部署压测客户端；先起网关）
	$(GO_RUN) 'cd $(PROJECT_DIR) && go run ./cmd/mqtt-bench \
	  -broker tcp://127.0.0.1:11883 -conn 50000 -rate 2000 -duration 24h'

.PHONY: run-gateway
run-gateway: ## 前台运行网关（连开发栈 NATS）
	$(GO_RUN) 'cd $(PROJECT_DIR) && go run ./cmd/iot-gateway \
	  -mqtt-addr 127.0.0.1:11883 -http-addr 127.0.0.1:18080 -nats-url $(NATS_URL)'

.PHONY: ci
ci: fmt vet test ## 提交前门禁

## ---------- 开发栈 ----------

.PHONY: up
up: ## 启动开发栈
	cd $(COMPOSE_DIR) && docker compose up -d

.PHONY: down
down: ## 停止开发栈（保留数据卷）
	cd $(COMPOSE_DIR) && docker compose down

.PHONY: ps
ps: ## 查看开发栈状态
	cd $(COMPOSE_DIR) && docker compose ps

.PHONY: logs
logs: ## 跟随开发栈日志
	cd $(COMPOSE_DIR) && docker compose logs -f --tail 100
