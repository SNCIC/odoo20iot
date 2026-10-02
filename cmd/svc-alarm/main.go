// Command svc-alarm 是 04 §2 的告警服务：推进告警五态状态机并对外发布告警事件。
//
// 职责边界：
//   - **只做状态推进与事件发布**。通道分发（webhook/邮件/短信/语音）属于
//     svc-notify（04 §2.3）；建工单属于 odoo-connector 消费 `iot.alarm.{project}`
//     之后的事（07 §6 S3）。
//   - 状态落在 PG 的 `t_alarm_active`（04 §2.5）。**没有内存索引需要「重建」** ——
//     权威状态本来就在库里，重启即恢复；04 §2.5 说的内存 map 只是加速层。
//
// 双通道（04 §2.1）：定时扫描（默认 5s）兜底；消费 `iot.rule.alarm` 保时效。
//
// 未实现（如实留白，不做假实现）：
//   - 通知策略解析与模板渲染（04 §2.3 第 1、2 步）：需要 `t_alarm_rule.notify`，
//     该表尚未建立；
//   - 未确认升级（30min → 上级 / 2h → P1）：依赖通知组配置，同上；
//   - `dedup_key` 哈希分片（04 §2.5）：当前靠扫描互斥 + CAS 保证**正确性**，
//     分片是规模问题不是正确性问题；
//   - 维护窗口（静默）的配置源：`Silences` 恒为空集（语义是「没有窗口」，
//     不是「静默全部」）。
//
// 用法（devbox 内）：
//
//	go run ./cmd/svc-alarm -nats-url nats://100.64.0.3:28222
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/natsjs"
	"github.com/SNCIC/odoo20iot/internal/pg"
)

// alarmStream 承载对外告警事件；下游 odoo-connector 靠它做持久订阅（07 §6 S3）。
const alarmStream = "IOT_ALARM"

// nakDelay 是引擎/存储类错误的重投退避。
const nakDelay = 500 * time.Millisecond

type config struct {
	pgDSN                 string
	natsURL               string
	httpAddr              string
	scanInterval          time.Duration
	scanTimeout           time.Duration
	eventRetention        time.Duration
	triggerSubject        string
	triggerStream         string
	triggerDurable        string
	ackWait               time.Duration
	fetchBatch            int
	readyProbe            time.Duration
	logFormat             string
	notifyEscalationAfter time.Duration
	p1EscalationAfter     time.Duration
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nsvc-alarm 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.pgDSN, "dsn", pg.DefaultDSN, "业务库 DSN（09 §2.4 的项目栈 iot-postgres）")
	flag.StringVar(&cfg.natsURL, "nats-url", "nats://100.64.0.3:28222", "NATS JetStream 地址")
	flag.StringVar(&cfg.httpAddr, "http-addr", "127.0.0.1:18092", "健康检查与指标监听地址")
	flag.DurationVar(&cfg.scanInterval, "scan-interval", 5*time.Second, "定时扫描周期（04 §2.1：5s）")
	flag.DurationVar(&cfg.scanTimeout, "scan-timeout", 30*time.Second, "单轮扫描超时")
	flag.DurationVar(&cfg.eventRetention, "event-retention", 24*time.Hour, "告警事件的流保留时长（须 ≥ 下游消费窗口）")
	flag.StringVar(&cfg.triggerSubject, "trigger-subject", alarm.TriggerSubject, "规则触发 subject；置空则只做定时扫描")
	flag.StringVar(&cfg.triggerStream, "trigger-stream", "IOT_RULE", "规则触发的流名")
	flag.StringVar(&cfg.triggerDurable, "trigger-durable", "svc-alarm", "durable consumer 名")
	flag.DurationVar(&cfg.notifyEscalationAfter, "notify-escalation-after", 30*time.Minute, "未确认告警首次升级等待时间")
	flag.DurationVar(&cfg.p1EscalationAfter, "p1-escalation-after", 2*time.Hour, "未确认告警 P1 升级等待时间")
	flag.DurationVar(&cfg.ackWait, "ack-wait", 30*time.Second, "总线等待 ACK 的上限")
	flag.IntVar(&cfg.fetchBatch, "fetch-batch", 256, "单次拉取的消息数")
	flag.DurationVar(&cfg.readyProbe, "ready-probe", 3*time.Second, "就绪探测超时")
	// ⚠️ 用字符串开关而不是 `-log-json` 布尔开关：布尔开关一旦写成
	// `-log-json false`，Go 的 flag 会把 `false` 当成**位置参数**并从那里
	// 停止解析 —— 其后所有参数（包括出站白名单这类安全配置）全部被
	// 静默丢弃，而服务照常启动。本项目已经踩过两次（见 09 §6）。
	flag.StringVar(&cfg.logFormat, "log-format", "json", "日志格式：json 或 text")
	flag.Parse()
	return cfg
}

// newLogger 按 `-log-format` 建日志器。
//
// ⚠️ 用字符串开关而不是 `-log-json` 布尔开关：布尔开关一旦被写成
// `-log-json false`，Go 的 flag 会把 `false` 当成**位置参数**并从这里
// 停止解析 —— 其后所有参数（包括出站白名单这种安全配置）全部被静默丢弃，
// 而服务照常启动。这个坑本项目已经踩过一次（见 09 §6）。
func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if strings.EqualFold(format, "text") {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

func run(cfg config) error {
	logger := newLogger(cfg.logFormat)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
	if err != nil {
		return err
	}
	defer pool.Close()

	// 迁移是**部署动作**（cmd/iot-migrate），服务不自动迁移；但启动时必须检查 ——
	// 否则会在落后的 schema 上跑，而每一轮扫描都会失败。
	if pending, err := pendingMigrations(ctx, pool); err != nil {
		return fmt.Errorf("检查迁移状态: %w", err)
	} else if len(pending) > 0 {
		return fmt.Errorf("数据库有 %d 个未应用的迁移 %v：请先执行 cmd/iot-migrate",
			len(pending), pending)
	}

	nc, err := nats.Connect(cfg.natsURL, nats.Name("svc-alarm"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("连接 NATS: %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("初始化 JetStream: %w", err)
	}
	if err := ensureStream(js, alarmStream,
		[]string{alarm.AlarmSubjectPrefix + ".>"}, cfg.eventRetention); err != nil {
		return err
	}
	if cfg.triggerSubject != "" {
		if err := ensureStream(js, cfg.triggerStream,
			[]string{wildcardOf(cfg.triggerSubject)}, cfg.eventRetention); err != nil {
			return err
		}
	}

	store, err := alarm.NewPGStore(pool)
	if err != nil {
		return err
	}
	metrics := new(alarm.Metrics)
	counts := new(counters)
	silences, err := alarm.LoadSilences(ctx, pool)
	if err != nil {
		return err
	}
	engine, err := alarm.New(alarm.Options{
		Store:    store,
		Silences: silences,
		Metrics:  metrics,
		Logger:   logger,
	})
	if err != nil {
		return err
	}
	app := &agent{
		engine:                engine,
		store:                 store,
		pool:                  pool,
		pub:                   &natsPublisher{js: js},
		lockKey:               pg.AdvisoryKey("svc-alarm:scan"),
		metrics:               metrics,
		counts:                counts,
		logger:                logger,
		now:                   time.Now,
		notifyEscalationAfter: cfg.notifyEscalationAfter,
		p1EscalationAfter:     cfg.p1EscalationAfter,
	}

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           routes(app, js, cfg),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var sub *natsjs.Subscription
	if cfg.triggerSubject != "" {
		// ⚠️ 退出时**不要** Unsubscribe：对 JetStream 订阅调用它会**删除消费者**，
		// 重启后新建的消费者从头投递 —— 流保留 24h，等于每次重启重放全天的事件。
		// 消费者是服务端状态，交给 nc.Close() 就行。
		sub, err = natsjs.Subscribe(js, natsjs.Options{
			Subject:  cfg.triggerSubject,
			Durable:  cfg.triggerDurable,
			Stream:   cfg.triggerStream,
			AckWait:  cfg.ackWait,
			Inactive: cfg.eventRetention,
		})
		if err != nil {
			return err
		}
	}

	logger.Info("svc-alarm 已就绪",
		"scan_interval", cfg.scanInterval.String(),
		"trigger_subject", cfg.triggerSubject,
		"http_addr", cfg.httpAddr,
		"version", buildinfo.Version, "commit", buildinfo.Commit)

	errCh := make(chan error, 3)
	go func() { errCh <- serveHTTP(srv, logger) }()
	go func() { errCh <- scanLoop(ctx, app, cfg, logger) }()
	if sub != nil {
		go func() { errCh <- triggerLoop(ctx, sub, app, cfg, logger) }()
	}

	select {
	case <-ctx.Done():
		logger.Info("收到退出信号")
	case err := <-errCh:
		if err != nil {
			logger.Error("主循环异常退出", "error", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	logger.Info("已退出",
		"scans", counts.Scans.Load(),
		"not_leader", counts.ScanSkipped.Load(),
		"published", counts.Published.Load(),
		"republished", counts.Republished.Load(),
		"publish_errors", counts.PublishErrors.Load())
	return nil
}

func serveHTTP(srv *http.Server, logger *slog.Logger) error {
	logger.Info("HTTP 已启动", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP 服务: %w", err)
	}
	return nil
}

// scanLoop 是定时扫描通道（04 §2.1）。
func scanLoop(ctx context.Context, app *agent, cfg config, logger *slog.Logger) error {
	t := time.NewTicker(cfg.scanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			scanCtx, cancel := context.WithTimeout(ctx, cfg.scanTimeout)
			err := app.scanOnce(scanCtx)
			cancel()
			if err != nil {
				// 扫描失败**不能退出**：PG 抖一下就把告警服务停掉，
				// 等于让告警在最需要它的时候下线。记错，下一轮再来。
				logger.Error("扫描失败，下轮重试", "error", err)
			}
		}
	}
}

// triggerLoop 是事件驱动通道（04 §2.1）：消费规则触发，保证时效。
func triggerLoop(ctx context.Context, sub *natsjs.Subscription, app *agent, cfg config, logger *slog.Logger) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := sub.Fetch(cfg.fetchBatch, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.Canceled) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("拉取触发消息失败", "error", err)
			continue
		}
		for _, m := range msgs {
			tr, err := alarm.ParseTrigger(m.Data)
			if err != nil {
				// 缺字段/非法 JSON 属于**上游或配置错误**，重投多少次都不会变对。
				// ACK 掉并计成毒消息 —— 不 ACK 会让它永远占着 PEL，
				// 而「永远占着」看起来像积压，会把注意力引向错误的方向。
				app.counts.PoisonTriggers.Add(1)
				logger.Error("丢弃非法触发消息（重投无意义）", "error", err, "subject", m.Subject)
				_ = m.Ack()
				continue
			}
			if err := app.observeTrigger(ctx, tr); err != nil {
				// 引擎/存储出错（多为 PG 抖动）：NAK 重投，恢复后补上。
				logger.Warn("处理触发失败，重投", "error", err, "rule", tr.RuleID)
				_ = m.NakWithDelay(nakDelay)
				continue
			}
			_ = m.Ack()
		}
	}
}

// pendingMigrations 返回尚未应用的迁移版本。
func pendingMigrations(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	migs, err := pg.LoadMigrations()
	if err != nil {
		return nil, err
	}
	applied, err := pg.AppliedVersions(ctx, pool)
	if err != nil {
		return nil, err
	}
	var pending []string
	for _, m := range migs {
		if _, ok := applied[m.Version]; !ok {
			pending = append(pending, m.Version)
		}
	}
	return pending, nil
}

// wildcardOf 把具体 subject 收敛成建流用的通配前缀：`iot.rule.alarm` → `iot.rule.>`。
//
// 取前两段而不是「去掉最后一段」：`iot.rule.alarm` 与将来的 `iot.rule.scene`
// 应落在同一个流里（它们同为规则产出），而按最后一段收敛会建出一堆
// 只有一条 subject 的流。
//
// ⚠️ 不足 3 段时**原样返回**：`iot.rule.>` 匹配不到 `iot.rule` 本身，
// 于是流的 subjects 不含实际要发布的 subject —— 表现是发布时静默失败
// （或报一个看不出原因的 no response from stream）。
func wildcardOf(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) < 3 {
		return subject
	}
	return parts[0] + "." + parts[1] + ".>"
}
