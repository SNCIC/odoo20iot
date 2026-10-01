# odoo20iot —— 开发命令入口
#
# Go 命令在 devbox 容器内执行（宿主不装 Go）；Compose 在宿主执行。
# 若在 devbox 内直接运行 make，GO_RUN 可置空：make test GO_RUN=

DEVBOX      ?= devbox
DEVBOX_USER ?= xfusion
PROJECT_DIR ?= /home/xfusion/projects/odoo20iot
GO_RUN      ?= docker exec -u $(DEVBOX_USER) $(DEVBOX) bash -lc

COMPOSE_DIR := deploy/compose
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
build: ## 编译全部服务到 bin/
	$(GO_RUN) 'cd $(PROJECT_DIR) && mkdir -p bin && \
	  go build -ldflags "$(LDFLAGS)" -o bin/iot-gateway ./cmd/iot-gateway'

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
