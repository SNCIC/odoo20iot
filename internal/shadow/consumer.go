package shadow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/SNCIC/odoo20iot/internal/cluster"
	"github.com/SNCIC/odoo20iot/internal/natsjs"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
)

const ReportedStream = "IOT_SHADOW_REPORTED"

func EnsureReportedStream(js nats.JetStreamContext) error {
	return natsjs.EnsureStream(js, natsjs.StreamSpec{
		Name: ReportedStream, Subjects: []string{"iot.shadow.reported.>"},
		Replicas: 1, MaxAge: 7 * 24 * time.Hour,
	})
}

func ConsumeReported(ctx context.Context, js nats.JetStreamContext, store Store, durable string, logger *slog.Logger) error {
	if store == nil || logger == nil {
		return fmt.Errorf("shadow: reported consumer 依赖未配置")
	}
	if durable == "" {
		durable = "svc-shadow-reported"
	}
	sub, err := natsjs.Subscribe(js, natsjs.Options{
		Subject: "iot.shadow.reported.>", Durable: durable, Stream: ReportedStream,
		AckWait: 30 * time.Second, Inactive: 24 * time.Hour, MaxDeliver: 5,
	})
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		messages, err := sub.Fetch(64, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for _, message := range messages {
			projectID, deviceKey, err := cluster.ParseShadowReportedSubject(message.Subject)
			if err != nil {
				logger.Warn("影子上报 subject 非法，已 ACK", "subject", message.Subject, "error", err)
				_ = message.Ack()
				continue
			}
			var patch map[string]any
			if err := json.Unmarshal(message.Data, &patch); err != nil || patch == nil {
				logger.Warn("影子 reported 载荷非法，已 ACK", "subject", message.Subject, "error", err)
				_ = message.Ack()
				continue
			}
			if _, err := store.ApplyReported(ctx, projectID, deviceKey, patch); err != nil {
				if errors.Is(err, ErrNotFound) || errors.Is(err, pgx.ErrNoRows) {
					logger.Warn("影子上报设备不存在，已 ACK", "project_id", projectID, "device_key", deviceKey)
					_ = message.Ack()
					continue
				}
				logger.Warn("合并影子 reported 失败，延迟重投", "project_id", projectID, "device_key", deviceKey, "error", err)
				_ = message.NakWithDelay(time.Second)
				continue
			}
			_ = message.Ack()
		}
	}
	return nil
}
