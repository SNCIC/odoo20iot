package ota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/SNCIC/odoo20iot/internal/cluster"
	"github.com/SNCIC/odoo20iot/internal/natsjs"
	"github.com/nats-io/nats.go"
)

const ProgressStream = "IOT_OTA_PROGRESS"

type ProgressStore interface {
	ReportProgress(context.Context, int64, string, Progress) error
}

func EnsureProgressStream(js nats.JetStreamContext) error {
	return natsjs.EnsureStream(js, natsjs.StreamSpec{Name: ProgressStream, Subjects: []string{"iot.ota.progress.>"}, Replicas: 1, MaxAge: 7 * 24 * time.Hour})
}

func ConsumeProgress(ctx context.Context, js nats.JetStreamContext, store ProgressStore, durable string, logger *slog.Logger) error {
	if store == nil || logger == nil {
		return fmt.Errorf("ota: 进度消费者依赖未配置")
	}
	if durable == "" {
		durable = "svc-ota-progress"
	}
	sub, err := natsjs.Subscribe(js, natsjs.Options{Subject: "iot.ota.progress.>", Durable: durable, Stream: ProgressStream, AckWait: 30 * time.Second, Inactive: 24 * time.Hour, MaxDeliver: 5})
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
			projectID, deviceKey, err := cluster.ParseOTAProgressSubject(message.Subject)
			if err != nil {
				logger.Warn("OTA 进度 subject 非法，已 ACK", "subject", message.Subject, "error", err)
				_ = message.Ack()
				continue
			}
			var progress Progress
			if err := json.Unmarshal(message.Data, &progress); err != nil || progress.Validate() != nil {
				logger.Warn("OTA 进度载荷非法，已 ACK", "project_id", projectID, "device_key", deviceKey, "error", err)
				_ = message.Ack()
				continue
			}
			if err := store.ReportProgress(ctx, projectID, deviceKey, progress); err != nil {
				logger.Warn("写入 OTA 进度失败，延迟重投", "project_id", projectID, "device_key", deviceKey, "error", err)
				_ = message.NakWithDelay(time.Second)
				continue
			}
			_ = message.Ack()
		}
	}
	return nil
}
