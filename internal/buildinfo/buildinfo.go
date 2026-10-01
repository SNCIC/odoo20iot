// Package buildinfo 提供构建期注入的版本信息，供启动日志与 /healthz 使用。
//
// 三个变量都由链接器注入，例如：
//
//	go build -ldflags "-X github.com/SNCIC/odoo20iot/internal/buildinfo.Version=v0.1.0 \
//	                   -X github.com/SNCIC/odoo20iot/internal/buildinfo.Commit=$(git rev-parse --short HEAD) \
//	                   -X github.com/SNCIC/odoo20iot/internal/buildinfo.BuildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
package buildinfo

import "runtime"

var (
	// Version 语义化版本号，未注入时为 dev。
	Version = "dev"
	// Commit 构建时的 git 短哈希。
	Commit = "unknown"
	// BuildTime 构建时间（RFC3339，UTC）。
	BuildTime = "unknown"
)

// Runtime 返回 Go 版本与目标平台，用于确认二进制是否为预期工具链产出。
func Runtime() string {
	return runtime.Version() + " " + runtime.GOOS + "/" + runtime.GOARCH
}
