package protocolruntime

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/SNCIC/odoo20iot/internal/auth"
	"github.com/SNCIC/odoo20iot/internal/gateway"
)

// FileRuntime 是 CoAP/TCP 独立进程的最小运行时。它只用于开发/边缘单机，
// 生产多租户仍应把认证目录和配额运行时从 iot-gateway 主进程抽出后共用。
type FileRuntime struct {
	Authenticator *auth.Authenticator
	Publisher     *gateway.NATSPublisher
}

func NewFileRuntime(ctx context.Context, authFile, natsURL, stream, project string, shards int, logger *slog.Logger) (*FileRuntime, error) {
	if logger == nil {
		logger = slog.Default()
	}
	dir, err := auth.LoadFile(authFile)
	if err != nil {
		return nil, err
	}
	authenticator := auth.NewAuthenticator(auth.NewCachedDirectory(dir), auth.DefaultPolicy(), nil)
	pub, err := gateway.NewNATSPublisher(natsURL, stream)
	if err != nil {
		return nil, fmt.Errorf("连接 NATS: %w", err)
	}
	if err := pub.EnsureStream(gateway.StreamSpec{Subjects: []string{"iot.telemetry.>", "iot.attributes.>", "iot.events.>"}, Replicas: 1}); err != nil {
		_ = pub.Close()
		return nil, err
	}
	logger.Info("协议运行时已初始化", "auth_file", authFile, "nats_url", natsURL, "stream", stream, "project", project, "shards", shards)
	return &FileRuntime{Authenticator: authenticator, Publisher: pub}, nil
}
