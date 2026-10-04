package command

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/natsjs"
	"github.com/nats-io/nats.go"
)

const ReplyStream = "IOT_COMMAND_REPLY"

func EnsureReplyStream(js nats.JetStreamContext) error {
	return natsjs.EnsureStream(js, natsjs.StreamSpec{Name: ReplyStream, Subjects: []string{"iot.cmd.reply.>"}, Replicas: 1, MaxAge: 7 * 24 * time.Hour})
}

func ConsumeReplies(ctx context.Context, js nats.JetStreamContext, store *Store, durable string, logger *slog.Logger) error {
	if store == nil || logger == nil {
		return fmt.Errorf("command: 回执消费者依赖未配置")
	}
	if durable == "" {
		durable = "svc-command-replies"
	}
	sub, err := natsjs.Subscribe(js, natsjs.Options{Subject: "iot.cmd.reply.>", Durable: durable, Stream: ReplyStream, AckWait: 30 * time.Second, Inactive: 24 * time.Hour, MaxDeliver: 5})
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		msgs, err := sub.Fetch(64, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			return err
		}
		for _, msg := range msgs {
			projectID, deviceKey, err := projectFromReplySubject(msg.Subject)
			if err != nil {
				logger.Warn("命令回执 subject 非法，已 ACK", "subject", msg.Subject, "error", err)
				_ = msg.Ack()
				continue
			}
			reply, err := DecodeReply(msg.Data)
			if err != nil {
				logger.Warn("命令回执非法，已 ACK", "subject", msg.Subject, "error", err)
				_ = msg.Ack()
				continue
			}
			if reply.DeviceKey != "" && reply.DeviceKey != deviceKey {
				logger.Warn("命令回执设备不匹配，已 ACK", "subject", msg.Subject, "device_key", reply.DeviceKey)
				_ = msg.Ack()
				continue
			}
			reply.DeviceKey = deviceKey
			if err := store.ApplyReply(ctx, projectID, reply); err != nil {
				logger.Warn("更新命令回执失败，重投", "project_id", projectID, "error", err)
				_ = msg.NakWithDelay(time.Second)
				continue
			}
			_ = msg.Ack()
		}
	}
	return nil
}

func projectFromReplySubject(subject string) (int64, string, error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 5 || parts[0] != "iot" || parts[1] != "cmd" || parts[2] != "reply" || parts[3] == "" || parts[4] == "" {
		return 0, "", fmt.Errorf("期望 iot.cmd.reply.<project>.<device>，得到 %q", subject)
	}
	id, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || id <= 0 {
		return 0, "", fmt.Errorf("project_id 非法")
	}
	return id, parts[4], nil
}
