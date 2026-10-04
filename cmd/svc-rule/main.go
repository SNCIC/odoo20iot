// Command svc-rule consumes telemetry envelopes and evaluates enabled tenant rules.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/cluster"
	"github.com/SNCIC/odoo20iot/internal/command"
	"github.com/SNCIC/odoo20iot/internal/dlq"
	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/gateway"
	"github.com/SNCIC/odoo20iot/internal/latest"
	"github.com/SNCIC/odoo20iot/internal/natsjs"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/ruleconfig"
	"github.com/SNCIC/odoo20iot/internal/ruleengine"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
	"github.com/SNCIC/odoo20iot/internal/tsdb/greptimedb"
)

type config struct {
	pgDSN, natsURL, redisURL, historyDSN, stream, subject, durable string
	fetch                                                          int
	ackWait, inactive, reload                                      time.Duration
	logFormat                                                      string
	scriptEnabled                                                  bool
	commandOriginID                                                string
}

func main() {
	if err := run(parseFlags()); err != nil {
		fmt.Fprintln(os.Stderr, "svc-rule 退出:", err)
		os.Exit(1)
	}
}
func parseFlags() config {
	c := config{}
	flag.StringVar(&c.pgDSN, "pg-dsn", pg.DefaultDSN, "IoT PostgreSQL DSN")
	flag.StringVar(&c.natsURL, "nats-url", "nats://100.64.0.3:28222", "NATS 地址")
	flag.StringVar(&c.redisURL, "redis-url", "redis://100.64.0.3:28637/0", "最新值 Redis 地址")
	flag.StringVar(&c.historyDSN, "history-dsn", "postgres://greptime:greptime@100.64.0.3:28403/public", "GreptimeDB DSN")
	flag.StringVar(&c.stream, "stream", "IOT_TELEMETRY", "遥测流")
	flag.StringVar(&c.subject, "subject", "iot.telemetry.>", "遥测 subject")
	flag.StringVar(&c.durable, "durable", "svc-rule", "durable consumer")
	flag.IntVar(&c.fetch, "fetch-batch", 128, "拉取批量")
	flag.DurationVar(&c.ackWait, "ack-wait", 30*time.Second, "ACK 等待")
	flag.DurationVar(&c.inactive, "consumer-inactive", 24*time.Hour, "消费者空闲回收阈值")
	flag.DurationVar(&c.reload, "reload-interval", time.Minute, "规则热加载周期")
	flag.StringVar(&c.logFormat, "log-format", "json", "日志格式 json/text")
	flag.BoolVar(&c.scriptEnabled, "script-enabled", false, "启用受限 script.run（默认关闭）")
	flag.StringVar(&c.commandOriginID, "command-origin-id", "", "command.send 路由源 ID；为空则禁用下行")
	flag.Parse()
	return c
}
func logger(format string) *slog.Logger {
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, nil))
}

func run(cfg config) error {
	log := logger(cfg.logFormat)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
	if err != nil {
		return err
	}
	defer pool.Close()
	if pending, err := pendingMigrations(ctx, pool); err != nil {
		return err
	} else if len(pending) > 0 {
		return fmt.Errorf("数据库有未应用迁移: %v", pending)
	}
	store, err := ruleconfig.NewStore(pool)
	if err != nil {
		return err
	}
	redisCfg, err := redis.ParseURL(cfg.redisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(redisCfg)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}
	var commandRouter command.Router
	var gatewayNode *cluster.Node
	if cfg.commandOriginID != "" {
		gatewayNode, err = cluster.New(ctx, cluster.Options{
			ID: cfg.commandOriginID, NATSURL: cfg.natsURL,
			Cursor: cluster.NewRedisCursor(rdb, "gw:offline:cursor"), Logger: log,
		}, cluster.NewRedisLocator(rdb, "gw:client", cluster.DefaultLocatorTTL))
		if err != nil {
			return err
		}
		defer gatewayNode.Close()
		commandRouter = gatewayNode
	}
	history, err := greptimedb.Open(ctx, cfg.historyDSN, 4)
	if err != nil {
		return err
	}
	defer history.Close()
	if err := history.CreateTable(ctx, tsdb.PlanJSON); err != nil {
		return fmt.Errorf("初始化规则历史表: %w", err)
	}
	latestStore := latest.NewRedisStore(rdb)
	pub, err := gateway.NewNATSPublisher(cfg.natsURL, "IOT_RULE")
	if err != nil {
		return err
	}
	defer pub.Close()
	if err := pub.EnsureStream(gateway.StreamSpec{Name: "IOT_RULE", Subjects: []string{"iot.rule.>"}, Replicas: 1, MaxAge: 24 * time.Hour, StrictSubjects: true}); err != nil {
		return err
	}
	engine, err := ruleengine.New(store, latestStore, history, pub, ruleengine.Config{ReloadEvery: cfg.reload, ScriptEnabled: cfg.scriptEnabled, CommandSender: command.Sender{Router: commandRouter}}, log)
	if err != nil {
		return err
	}
	dlqStore := dlq.New(pool)
	nc, err := nats.Connect(cfg.natsURL, nats.Name("svc-rule"), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	if err := command.EnsureReplyStream(js); err != nil {
		return fmt.Errorf("确保命令回执 Stream 失败: %w", err)
	}
	commandStore, err := command.NewStore(pool)
	if err != nil {
		return err
	}
	go func() {
		if err := command.ConsumeReplies(ctx, js, commandStore, "svc-rule-command-replies", log); err != nil && ctx.Err() == nil {
			log.Error("命令回执消费者退出", "error", err)
		}
	}()
	sub, err := natsjs.Subscribe(js, natsjs.Options{Subject: cfg.subject, Durable: cfg.durable, Stream: cfg.stream, AckWait: cfg.ackWait, Inactive: cfg.inactive, MaxDeliver: 5})
	if err != nil {
		return err
	}
	log.Info("svc-rule 已就绪", "subject", cfg.subject, "stream", cfg.stream, "durable", cfg.durable, "version", buildinfo.Version, "commit", buildinfo.Commit)
	for ctx.Err() == nil {
		msgs, err := sub.Fetch(cfg.fetch, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return err
		}
		for _, msg := range msgs {
			attempts := 1
			if meta, metaErr := msg.Metadata(); metaErr == nil && meta != nil && meta.NumDelivered > 0 {
				attempts = int(meta.NumDelivered)
			}
			var env envelope.Envelope
			if err := json.Unmarshal(msg.Data, &env); err != nil || env.DeviceKey == "" {
				reason := "规则信封解析失败"
				if err != nil {
					reason = err.Error()
				}
				if dlqErr := dlqStore.Put(ctx, dlq.Entry{Service: "svc-rule", Subject: cfg.subject, EntityType: "telemetry", Reason: reason, Attempts: attempts, Payload: string(msg.Data)}); dlqErr != nil {
					log.Error("规则毒消息写入 DLQ 失败", "error", dlqErr)
				}
				_ = msg.Ack()
				log.Warn("规则收到非法信封，已写入 DLQ 并 ACK", "error", reason)
				continue
			}
			if err := engine.Process(ctx, env); err != nil {
				if attempts >= 5 {
					if dlqErr := dlqStore.Put(ctx, dlq.Entry{ProjectID: env.ProjectID, Service: "svc-rule", Subject: cfg.subject, EntityType: "telemetry", TraceID: env.TraceID, Reason: err.Error(), Attempts: attempts, Payload: string(msg.Data)}); dlqErr != nil {
						log.Error("规则求值失败且 DLQ 写入失败", "error", dlqErr)
					}
					_ = msg.Ack()
					log.Error("规则消息达到最大重投次数，已写入 DLQ 并 ACK", "attempts", attempts, "error", err)
				} else {
					log.Warn("规则求值失败，重投", "attempts", attempts, "error", err)
					_ = msg.NakWithDelay(time.Second)
				}
				continue
			}
			_ = msg.Ack()
		}
	}
	return nil
}
