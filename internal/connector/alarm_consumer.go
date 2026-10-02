package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/extref"
	"github.com/SNCIC/odoo20iot/internal/natsjs"
)

const (
	AlarmStream       = "IOT_ALARM"
	AlarmConsumerName = "odoo-connector-maintenance-request"
	AlarmSubject      = "iot.alarm.>"
	AlarmAckWait      = 30 * time.Second
	AlarmInactive     = 7 * 24 * time.Hour
	AlarmRetryDelay   = 10 * time.Second
)

type AlarmOdooClient interface {
	PostJSON(context.Context, string, any, any) error
}

type AlarmRefStore interface {
	FindByLocal(context.Context, int64, string, int64) (extref.Ref, error)
	RecordIssue(context.Context, extref.IntegrationIssue) error
}

type AlarmConsumerOptions struct {
	JS      nats.JetStreamContext
	Odoo    AlarmOdooClient
	Refs    AlarmRefStore
	Stream  string
	Durable string
	Batch   int
	Logger  *slog.Logger
}

type AlarmConsumer struct {
	sub    *natsjs.Subscription
	odoo   AlarmOdooClient
	refs   AlarmRefStore
	batch  int
	logger *slog.Logger
}

func NewAlarmConsumer(opts AlarmConsumerOptions) (*AlarmConsumer, error) {
	if opts.JS == nil || opts.Odoo == nil || opts.Refs == nil {
		return nil, fmt.Errorf("alarm consumer: JS、Odoo、Refs 都不能为空")
	}
	if opts.Stream == "" {
		opts.Stream = AlarmStream
	}
	if opts.Durable == "" {
		opts.Durable = AlarmConsumerName
	}
	if opts.Batch <= 0 {
		opts.Batch = 32
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	sub, err := natsjs.Subscribe(opts.JS, natsjs.Options{
		Subject: AlarmSubject, Durable: opts.Durable, Stream: opts.Stream,
		AckWait: AlarmAckWait, Inactive: AlarmInactive, MaxAckPending: opts.Batch * 2,
	})
	if err != nil {
		return nil, err
	}
	return &AlarmConsumer{sub: sub, odoo: opts.Odoo, refs: opts.Refs, batch: opts.Batch, logger: opts.Logger}, nil
}

func (c *AlarmConsumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		messages, err := c.sub.Fetch(c.batch, nats.MaxWait(2*time.Second))
		if err != nil {
			if ctx.Err() != nil || err == nats.ErrTimeout {
				continue
			}
			return err
		}
		for _, msg := range messages {
			if err := c.handle(ctx, msg); err != nil {
				c.logger.Error("IoT 告警建单失败，保留消息等待重投", "error", err)
				if nakErr := msg.NakWithDelay(AlarmRetryDelay); nakErr != nil {
					c.logger.Warn("安排 IoT 告警延迟重投失败", "error", nakErr)
				}
				continue
			}
			if err := msg.Ack(); err != nil {
				c.logger.Warn("ACK IoT 告警失败", "error", err)
			}
		}
	}
	return nil
}

func (c *AlarmConsumer) handle(ctx context.Context, msg *nats.Msg) error {
	var event alarm.Event
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return fmt.Errorf("解析告警事件: %w", err)
	}
	if event.State != alarm.StateActive {
		return nil
	}
	if strings.TrimSpace(event.AlarmID) == "" || strings.TrimSpace(event.DedupKey) == "" {
		return fmt.Errorf("告警事件缺少 alarm_id 或 dedup_key")
	}
	projectID, err := strconv.ParseInt(event.ProjectID, 10, 64)
	if err != nil || projectID <= 0 {
		return fmt.Errorf("告警 project_id 非法: %q", event.ProjectID)
	}
	deviceID, err := strconv.ParseInt(event.DeviceID, 10, 64)
	if err != nil || deviceID <= 0 {
		return fmt.Errorf("告警 device_id 非法: %q", event.DeviceID)
	}
	ref, err := c.refs.FindByLocal(ctx, projectID, "device", deviceID)
	if err != nil {
		if err == pgx.ErrNoRows {
			if recordErr := c.refs.RecordIssue(ctx, extref.IntegrationIssue{
				ProjectID: projectID, System: "odoo20tbb", Model: "maintenance.equipment", ExternalID: deviceID,
				Type: "unbound_alarm", Details: map[string]any{"alarm_id": event.AlarmID, "device_id": event.DeviceID},
			}); recordErr != nil {
				return fmt.Errorf("记录未绑定告警: %w", recordErr)
			}
			return nil
		}
		return fmt.Errorf("查询设备外部引用: %w", err)
	}
	if ref.Status != "active" || ref.ExtID <= 0 {
		return nil
	}
	payload := AlarmMaintenancePayload(event, ref.ExtID)
	var result map[string]any
	if err := c.odoo.PostJSON(ctx, "/api/iot/v1/maintenance/request", payload, &result); err != nil {
		return err
	}
	return nil
}

func AlarmMaintenancePayload(event alarm.Event, equipmentID int64) map[string]any {
	severity := strings.ToLower(event.Level)
	if severity == "warning" {
		severity = "warn"
	}
	if severity != "info" && severity != "warn" && severity != "critical" {
		severity = "warn"
	}
	alarmTime := event.FirstTS
	if alarmTime.IsZero() {
		alarmTime = event.LastTS
	}
	payload := map[string]any{
		"alarm_id":        event.AlarmID,
		"equipment_id":    equipmentID,
		"severity":        severity,
		"title":           event.RuleName,
		"description":     event.Reason,
		"metric_snapshot": json.RawMessage(event.MetricSnapshot),
		"alarm_ts":        alarmTime.UTC().Format("2006-01-02 15:04:05"),
		"idempotency_key": fmt.Sprintf("idem:%s:maintenance_req:eq-%d-%s", event.ProjectID, equipmentID, event.DedupKey),
	}
	return payload
}
