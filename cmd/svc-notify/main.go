// Command svc-notify 是 04 §2.3 的通知服务：消费告警事件，按策略分发到四条可选通道。
//
// 职责边界：
//   - 本服务**不判断告警状态**（那是 svc-alarm 的 FSM），也不决定「有没有告警」。
//     它只做一件事：把已经进入 active 的告警，按策略送到人手上。
//   - 通道支持 Webhook / 邮件 / 短信 / 语音；语音需配置兼容的 HTTP 语音网关。
//   - 死信落 `t_dlq`（04 §3.3），条目含原文、失败原因、重试历史与 trace_id。
//
// 订阅：`iot.alarm.>`（svc-alarm 对外事件）与 `iot.quota.alert.>`（svc-quota 阈值事件），
// 两者各用自己的 stream 绑定和 durable；配额事件与 usage 共用 `IOT_QUOTA` 流，靠 subject 隔离。
// 告警事件仍与 odoo-connector 共用 subject、使用各自 durable。
//
// 未实现（如实留白）：
//   - 通知策略来自配置文件而非 `t_alarm_rule.notify`（该表尚未建立）；
//   - 通知组（groups）未展开成收件人（依赖 `t_user` / `t_role`）；
//   - 未确认升级（30min → 上级 / 2h → P1）：仍需 svc-alarm 提供确认状态；
//     风暴熔断产生的 escalated=true 已支持按策略切换收件人；
//   - 统一出口代理（egress-proxy，05 §1）：SSRF 防护的另外三项已实现，
//     少的是集中出站审计；
//   - 策略热加载：改配置需重启；
//   - DLQ 重放工具（04 §3.3）与 7 天保留清理。
//
// 用法（devbox 内）：
//
//	go run ./cmd/svc-notify \
//	  -egress-allow 'hooks.example.com,*.dingtalk.com' \
//	  -webhook-url 'https://hooks.example.com/robot/send?access_token=xxx' \
//	  -smtp-addr 'smtp.example.com:587' -smtp-from 'iot@example.com' -smtp-require-tls
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/smtp"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/buildinfo"
	"github.com/SNCIC/odoo20iot/internal/dlq"
	"github.com/SNCIC/odoo20iot/internal/natsjs"
	"github.com/SNCIC/odoo20iot/internal/notify"
	"github.com/SNCIC/odoo20iot/internal/notifyconfig"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/quota"
	"github.com/SNCIC/odoo20iot/internal/secureconfig"
)

const nakDelay = 500 * time.Millisecond

type config struct {
	pgDSN   string
	natsURL string
	// 订阅
	subject      string
	stream       string
	durable      string
	quotaSubject string
	quotaStream  string
	quotaDurable string
	ackWait      time.Duration
	fetchWait    time.Duration
	// consumerInactive 是消费者的空闲回收阈值（见 natsjs.Subscribe 的说明）。
	consumerInactive time.Duration
	batch            int
	// 出站安全
	egressAllow   []string
	allowLoopback bool
	// Webhook
	webhookURL     string
	webhookTimeout time.Duration
	// 邮件
	smtpAddr       string
	smtpFrom       string
	smtpHelo       string
	smtpUser       string
	smtpPass       string
	smtpRequireTLS bool
	// 短信
	smsEndpoint string
	smsToken    string
	smsSender   string
	smsTemplate string
	// 语音
	voiceEndpoint string
	voiceToken    string
	voiceFrom     string
	voiceTimeout  time.Duration
	// 策略
	policyFile        string
	policySource      string
	defaultChannels   []string
	defaultWebhooks   string
	defaultEmails     string
	defaultSMSTo      string
	defaultVoiceTo    string
	defaultTemplate   string
	healthThreshold   float64
	healthMinSamples  int
	healthWindow      time.Duration
	dlqReplayInterval time.Duration
	dlqReplayBatch    int
	// HTTP
	httpAddr   string
	readyProbe time.Duration
	logFormat  string
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nsvc-notify 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	var egress, channels string
	flag.StringVar(&cfg.pgDSN, "dsn", pg.DefaultDSN, "业务库 DSN")
	flag.StringVar(&cfg.natsURL, "nats-url", "nats://100.64.0.3:28222", "NATS JetStream 地址")
	flag.StringVar(&cfg.subject, "subject", alarm.AlarmSubjectPrefix+".>", "订阅的告警事件 subject")
	flag.StringVar(&cfg.stream, "stream", "IOT_ALARM", "告警事件的流名")
	flag.StringVar(&cfg.durable, "durable", "svc-notify", "durable consumer 名")
	flag.StringVar(&cfg.quotaSubject, "quota-subject", quota.QuotaAlertSubjectPrefix+".>", "配额告警 subject")
	flag.StringVar(&cfg.quotaStream, "quota-stream", "IOT_QUOTA", "配额告警流名")
	flag.StringVar(&cfg.quotaDurable, "quota-durable", "svc-notify-quota", "配额告警 durable consumer 名")
	flag.DurationVar(&cfg.ackWait, "ack-wait", 30*time.Second, "总线等待 ACK 的上限")
	flag.DurationVar(&cfg.consumerInactive, "consumer-inactive", 24*time.Hour,
		"消费者空闲回收阈值；**必须 ≥ 服务可能的最长停机**，否则停机超过它就会重放整个保留窗口")
	flag.DurationVar(&cfg.fetchWait, "fetch-wait", 500*time.Millisecond, "单次拉取的等待上限")
	flag.IntVar(&cfg.batch, "fetch-batch", 64, "单次拉取的消息数")

	flag.StringVar(&egress, "egress-allow", os.Getenv("IOT_NOTIFY_EGRESS_ALLOW"), "出站白名单（逗号分隔：域名、*.域名 或 CIDR）。**必填**")
	flag.BoolVar(&cfg.allowLoopback, "allow-loopback", false, "放行回环地址（**仅本地联调**，生产必须为 false）")

	flag.StringVar(&cfg.webhookURL, "webhook-url", os.Getenv("IOT_NOTIFY_WEBHOOK_URL"), "默认策略里的 Webhook 地址（默认取 IOT_NOTIFY_WEBHOOK_URL）")
	flag.DurationVar(&cfg.webhookTimeout, "webhook-timeout", 5*time.Second, "Webhook 超时（04 §2.3：5s）")

	flag.StringVar(&cfg.smtpAddr, "smtp-addr", "", "SMTP 地址 host:port；留空则不启用邮件通道")
	flag.StringVar(&cfg.smtpFrom, "smtp-from", "", "邮件发件人")
	flag.StringVar(&cfg.smtpHelo, "smtp-helo", "", "HELO 名（留空用默认）")
	flag.StringVar(&cfg.smtpUser, "smtp-user", "", "SMTP 用户名（建议用环境变量 SMTP_PASS 传密码）")
	flag.BoolVar(&cfg.smtpRequireTLS, "smtp-require-tls", false, "要求 SMTP 必须支持 STARTTLS（公网必须开）")

	flag.StringVar(&cfg.smsEndpoint, "sms-endpoint", "", "短信网关地址；留空则不启用短信通道")
	flag.StringVar(&cfg.smsSender, "sms-sender", "", "短信签名")
	flag.StringVar(&cfg.smsTemplate, "sms-template", "", "短信模板 ID")
	flag.StringVar(&cfg.voiceEndpoint, "voice-endpoint", os.Getenv("IOT_VOICE_ENDPOINT"), "语音网关地址；留空则不启用语音通道")
	flag.StringVar(&cfg.voiceFrom, "voice-from", os.Getenv("IOT_VOICE_FROM"), "语音主叫号码或服务标识")
	flag.DurationVar(&cfg.voiceTimeout, "voice-timeout", 10*time.Second, "语音网关超时")

	flag.StringVar(&cfg.policyFile, "policy-file", "", "通知策略文件（JSON）；留空用下面的默认策略")
	flag.StringVar(&cfg.policySource, "policy-source", "db", "通知策略来源：db 或 file")
	flag.StringVar(&channels, "default-channels", "webhook", "默认通道优先级（逗号分隔）")
	flag.StringVar(&cfg.defaultWebhooks, "default-webhook-to", "", "默认 Webhook 收件人（逗号分隔 URL）")
	flag.StringVar(&cfg.defaultEmails, "default-email-to", "", "默认邮件收件人（逗号分隔）")
	flag.StringVar(&cfg.defaultSMSTo, "default-sms-to", "", "默认短信收件人（逗号分隔号码）")
	flag.StringVar(&cfg.defaultVoiceTo, "default-voice-to", "", "默认语音收件人（逗号分隔号码）")
	flag.StringVar(&cfg.defaultTemplate, "default-template", "alarm", "默认模板名")
	flag.Float64Var(&cfg.healthThreshold, "channel-failure-threshold", 0.3, "通道失败率阈值（04 §2.3：0.3）")
	flag.IntVar(&cfg.healthMinSamples, "channel-min-samples", 5, "通道健康判定的最小样本数")
	flag.DurationVar(&cfg.healthWindow, "channel-window", 10*time.Minute, "通道健康统计窗口")
	flag.DurationVar(&cfg.dlqReplayInterval, "dlq-replay-interval", 0, "DLQ 自动重放周期；0 表示关闭")
	flag.IntVar(&cfg.dlqReplayBatch, "dlq-replay-batch", 50, "单轮 DLQ 重放上限")

	flag.StringVar(&cfg.httpAddr, "http-addr", "127.0.0.1:18093", "健康检查与指标监听地址")
	flag.DurationVar(&cfg.readyProbe, "ready-probe", 3*time.Second, "就绪探测超时")
	// ⚠️ 用字符串开关而不是 `-log-json` 布尔开关：布尔开关一旦写成
	// `-log-json false`，Go 的 flag 会把 `false` 当成**位置参数**并从那里
	// 停止解析 —— 其后所有参数（包括出站白名单这类安全配置）全部被
	// 静默丢弃，而服务照常启动。本项目已经踩过两次（见 09 §6）。
	flag.StringVar(&cfg.logFormat, "log-format", "json", "日志格式：json 或 text")
	flag.Parse()

	cfg.egressAllow = splitList(egress)
	cfg.defaultChannels = splitList(channels)
	// 凭据优先取环境变量，避免出现在命令行历史与 ps 输出里。
	cfg.smtpPass = os.Getenv("SMTP_PASS")
	cfg.smsToken = os.Getenv("SMS_TOKEN")
	cfg.voiceToken = os.Getenv("IOT_VOICE_TOKEN")
	return cfg
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
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

// counters 是服务侧的运行计数（与 notify.Metrics 的业务计数分开）。
type counters struct {
	Received  atomic.Int64
	Delivered atomic.Int64
	// Skipped 是没有匹配到通知策略而被跳过的告警。
	Skipped atomic.Int64
	// Poison 是解析失败、重投无意义的事件。
	Poison atomic.Int64
	Errors atomic.Int64
}

func run(cfg config) error {
	logger := newLogger(cfg.logFormat)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 启动自检（06 §：依赖不可用则拒绝启动，避免带病运行）。
	// 出站白名单为空 = 一条通知也发不出去，此刻拒绝启动比
	// 「服务健康、告警全丢」要好得多。
	if len(cfg.egressAllow) == 0 {
		return fmt.Errorf("出站白名单为空（-egress-allow）：一条通知都发不出去，拒绝启动")
	}
	guard := &notify.Guard{AllowedHosts: cfg.egressAllow, AllowLoopback: cfg.allowLoopback}

	pool, err := pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
	if err != nil {
		return err
	}
	defer pool.Close()
	if pending, err := pendingMigrations(ctx, pool); err != nil {
		return fmt.Errorf("检查迁移状态: %w", err)
	} else if len(pending) > 0 {
		return fmt.Errorf("数据库有 %d 个未应用的迁移 %v：请先执行 cmd/iot-migrate", len(pending), pending)
	}

	channels, err := buildChannels(cfg, guard, logger)
	if err != nil {
		return err
	}

	var policies notify.PolicySource
	if strings.EqualFold(cfg.policySource, "file") {
		policies, err = loadPolicySource(cfg.policyFile, defaultPolicy(cfg))
		if err != nil {
			return err
		}
	} else if strings.EqualFold(cfg.policySource, "db") {
		var endpointStore *notifyconfig.Store
		if rawKey := os.Getenv("IOT_CONFIG_KEY"); rawKey != "" {
			key, keyErr := secureconfig.NewKey(rawKey)
			if keyErr != nil {
				return keyErr
			}
			endpointStore, err = notifyconfig.New(pool, key)
			if err != nil {
				return err
			}
		} else {
			logger.Warn("IOT_CONFIG_KEY 未设置，数据库通知策略仍可用，但不会读取加密通知端点")
		}
		policies = newDBPolicySource(pool, defaultPolicy(cfg), endpointStore)
	} else {
		return fmt.Errorf("未知策略来源 %q：只能是 db 或 file", cfg.policySource)
	}

	renderer := notify.NewRenderer()
	if err := renderer.Parse("alarm", notify.DefaultAlarmTemplate); err != nil {
		return err
	}

	metrics := new(notify.Metrics)
	dispatcher, err := notify.NewDispatcher(notify.Options{
		Channels: channels,
		DLQ:      dlq.New(pool),
		Logger:   logger,
		Metrics:  metrics,
		Health: notify.ChannelHealth{
			Threshold:  cfg.healthThreshold,
			Window:     cfg.healthWindow,
			MinSamples: cfg.healthMinSamples,
		},
	})
	if err != nil {
		return err
	}

	nc, err := nats.Connect(cfg.natsURL, nats.Name("svc-notify"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("连接 NATS: %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("初始化 JetStream: %w", err)
	}
	// ⚠️ 退出时**不要** Unsubscribe：对 JetStream 订阅调用它会**删除消费者**，
	// 重启后新建的消费者从头投递 —— 流保留 24h，等于每次重启把全天告警重发一遍
	// （对通知服务就是一场通知风暴）。
	sub, err := natsjs.Subscribe(js, natsjs.Options{
		Subject:  cfg.subject,
		Durable:  cfg.durable,
		Stream:   cfg.stream,
		AckWait:  cfg.ackWait,
		Inactive: cfg.consumerInactive,
	})
	if err != nil {
		return err
	}
	quotaSub, err := natsjs.Subscribe(js, natsjs.Options{
		Subject: cfg.quotaSubject, Durable: cfg.quotaDurable, Stream: cfg.quotaStream,
		AckWait: cfg.ackWait, Inactive: cfg.consumerInactive,
	})
	if err != nil {
		return err
	}

	counts := new(counters)
	app := &notifier{
		dispatcher: dispatcher, renderer: renderer, policies: policies,
		metrics: metrics, counts: counts, logger: logger,
	}
	replayer, err := dlq.NewReplayer(dlq.New(pool), 3)
	if err != nil {
		return err
	}
	if err := registerNotifyReplay(replayer, dispatcher, logger); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           routes(app, pool, cfg),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("svc-notify 已就绪",
		"subject", cfg.subject, "durable", cfg.durable,
		"channels", channelNames(channels),
		"policy_source", cfg.policySource,
		"egress_allow", cfg.egressAllow,
		"allow_loopback", cfg.allowLoopback,
		"version", buildinfo.Version, "commit", buildinfo.Commit)

	errCh := make(chan error, 4)
	go func() { errCh <- serveHTTP(srv, logger) }()
	go func() { errCh <- consumeLoop(ctx, sub, app, cfg, logger, app.handleEvent, "告警") }()
	go func() { errCh <- consumeLoop(ctx, quotaSub, app, cfg, logger, app.handleQuotaEvent, "配额告警") }()
	if cfg.dlqReplayInterval > 0 {
		go func() { errCh <- runNotifyDLQReplay(ctx, replayer, cfg.dlqReplayInterval, cfg.dlqReplayBatch, logger) }()
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
		"received", counts.Received.Load(),
		"delivered", counts.Delivered.Load(),
		"skipped_no_policy", counts.Skipped.Load(),
		"poison", counts.Poison.Load(),
		"dlq", metrics.DLQTotal.Load())
	return nil
}

// defaultPolicy 在没给策略文件时构造一份可跑的默认策略。
func defaultPolicy(cfg config) notify.Policy {
	return notify.Policy{
		Channels: cfg.defaultChannels,
		Template: cfg.defaultTemplate,
		Recipients: map[string][]string{
			notify.ChannelWebhook: appendIfSet(splitList(cfg.defaultWebhooks), cfg.webhookURL),
			notify.ChannelEmail:   splitList(cfg.defaultEmails),
			notify.ChannelSMS:     splitList(cfg.defaultSMSTo),
			notify.ChannelVoice:   splitList(cfg.defaultVoiceTo),
		},
	}
}

func appendIfSet(list []string, extra string) []string {
	if extra == "" {
		return list
	}
	return append(list, extra)
}

// buildChannels 按配置装配**已启用**的通道。
//
// 未配置的通道不注册：让「策略里写了 sms 但短信没配」表现为
// 启动日志里的通道列表缺一项 + 分发时的显式警告，
// 而不是一个永远静默成功的空通道。
func buildChannels(cfg config, guard *notify.Guard, logger *slog.Logger) (map[string]notify.Channel, error) {
	out := map[string]notify.Channel{
		notify.ChannelWebhook: notify.NewWebhookChannel(guard, cfg.webhookTimeout),
	}

	if cfg.smtpAddr != "" {
		var auth smtp.Auth
		if cfg.smtpUser != "" {
			host, _, _ := net.SplitHostPort(cfg.smtpAddr)
			auth = smtp.PlainAuth("", cfg.smtpUser, cfg.smtpPass, host)
		}
		ch, err := notify.NewEmailChannel(guard, cfg.smtpAddr, cfg.smtpFrom, cfg.smtpHelo,
			auth, 10*time.Second)
		if err != nil {
			return nil, err
		}
		ch.RequireTLS = cfg.smtpRequireTLS
		out[notify.ChannelEmail] = ch
	} else {
		logger.Warn("未配置 SMTP，邮件通道不可用")
	}

	if cfg.smsEndpoint != "" {
		out[notify.ChannelSMS] = notify.NewSMSChannel(guard, cfg.smsEndpoint,
			cfg.smsToken, cfg.smsSender, cfg.smsTemplate, 5*time.Second)
	} else {
		logger.Warn("未配置短信网关，短信通道不可用")
	}
	if cfg.voiceEndpoint != "" {
		out[notify.ChannelVoice] = notify.NewVoiceChannel(guard, cfg.voiceEndpoint,
			cfg.voiceToken, cfg.voiceFrom, cfg.voiceTimeout)
	} else {
		logger.Warn("未配置语音网关，语音通道不可用")
	}
	return out, nil
}

func channelNames(chans map[string]notify.Channel) []string {
	out := make([]string, 0, len(chans))
	for name := range chans {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// notifier 把一条告警事件走完「解析 → 取策略 → 渲染 → 分发」。
type notifier struct {
	dispatcher *notify.Dispatcher
	renderer   *notify.Renderer
	policies   notify.PolicySource
	metrics    *notify.Metrics
	counts     *counters
	logger     *slog.Logger
}

func consumeLoop(ctx context.Context, sub *natsjs.Subscription, app *notifier, cfg config, logger *slog.Logger, handle func(context.Context, []byte) error, kind string) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := sub.Fetch(cfg.batch, nats.MaxWait(cfg.fetchWait))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.Canceled) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("拉取通知事件失败", "kind", kind, "error", err)
			continue
		}

		for _, m := range msgs {
			app.counts.Received.Add(1)
			err := handle(ctx, m.Data)

			switch {
			case err == nil:
				_ = m.Ack()

			case notify.IsPermanent(err):
				// 配置缺口（没有策略）或坏消息：重投多少次都不会变对。
				// ACK 掉并**分别计数** —— 不 ACK 会让它们永远占着 PEL，
				// 表现成「积压」，把注意力引向完全错误的方向。
				if errors.Is(err, errNoPolicy) {
					app.counts.Skipped.Add(1)
					logger.Warn("该通知没有匹配的通知策略，已跳过（不重试、不进死信）", "kind", kind, "error", err)
				} else {
					app.counts.Poison.Add(1)
					logger.Error("丢弃非法通知事件（重投无意义）", "kind", kind, "error", err, "subject", m.Subject)
				}
				_ = m.Ack()

			case errors.Is(err, notify.ErrDLQUnavailable):
				// 既没送到人手上、也没留下任何记录 —— 这种必须重投，
				// 不能让消息离开总线，否则这条告警就彻底消失了。
				app.counts.Errors.Add(1)
				logger.Error("通知未送达且死信写入失败，重投", "error", err)
				_ = m.NakWithDelay(nakDelay)

			default:
				// 所有通道都失败、但死信**写进去了**：痕迹已经有了，
				// 恢复路径是 DLQ 的重放工具（04 §3.3），不是让总线重投整套阶梯。
				app.counts.Errors.Add(1)
				logger.Error("通知未能送达任何通道（已进死信）", "error", err)
				_ = m.Ack()
			}
		}
	}
}

// errNoPolicy 标记「没有匹配到通知策略」这一类永久失败。
var errNoPolicy = errors.New("no matching notify policy")

func (n *notifier) handleEvent(ctx context.Context, data []byte) error {
	var ev alarm.Event
	if err := json.Unmarshal(data, &ev); err != nil {
		// 事件由 svc-alarm 用同一份结构体发出；解析不出来说明总线上的东西
		// 不是我们的（或中间被改过）。重投无意义。
		return notify.Permanent("告警事件解析失败: %v", err)
	}
	if ev.AlarmID == "" || ev.ProjectID == "" {
		return notify.Permanent("告警事件缺少 alarm_id / project_id")
	}

	policy, err := n.policies.Resolve(ctx, notify.Request{
		ProjectID: ev.ProjectID, RuleID: ev.RuleID, Level: ev.Level, Escalated: ev.Escalated,
	})
	if err != nil {
		if notify.IsPermanent(err) {
			// 包一层哨兵，让消费循环能把「配置缺口」与「坏消息」分开计数 ——
			// 两者都 ACK，但它们的运维含义完全不同。
			return fmt.Errorf("%w: %w", errNoPolicy, err)
		}
		return err
	}
	if ev.EscalationStage == 1 && len(policy.AckEscalatedRecipients) > 0 {
		policy.Recipients = policy.AckEscalatedRecipients
	}
	if (ev.EscalationStage >= 2 || (ev.Escalated && ev.EscalationStage == 0)) && len(policy.EscalatedRecipients) > 0 {
		policy.Recipients = policy.EscalatedRecipients
	}

	at := ev.NotifiedTS
	if at.IsZero() {
		at = ev.FirstTS
	}
	msg := notify.Message{
		// 告警事件里没有 trace_id（接入链路尚未打通全链路追踪），
		// 用 alarm_id 当锚点：它已经能把日志、死信与 Odoo 工单串起来。
		TraceID: ev.AlarmID, AlarmID: ev.AlarmID, DedupKey: ev.DedupKey,
		ProjectID: ev.ProjectID, DeviceID: ev.DeviceID, RuleID: ev.RuleID,
		Level: ev.Level, Subject: eventTitle(ev), At: at,
	}
	if len(ev.MetricSnapshot) > 0 {
		var snap map[string]any
		if err := json.Unmarshal(ev.MetricSnapshot, &snap); err == nil {
			msg.Payload = snap
		}
	}

	body, err := n.renderer.Render(policy.Template, msg)
	if err != nil {
		return err
	}
	msg.Body = body

	results, err := n.dispatcher.Dispatch(ctx, msg, policy)
	for _, r := range results {
		if !r.OK() {
			continue
		}
		// 用**实际成功的那个通道**计数：记在主通道名下会让
		// 「主通道彻底挂了」这件事在指标上被抹平。
		n.metrics.RecordChannel(r.Channel)
		if r.DegradedFrom != "" {
			n.logger.Warn("通知靠降级送达",
				"alarm", ev.AlarmID, "degraded_from", r.DegradedFrom, "via", r.Channel)
		}
	}
	if err == nil {
		n.counts.Delivered.Add(1)
	}
	return err
}

func (n *notifier) handleQuotaEvent(ctx context.Context, data []byte) error {
	var ev quota.AlertEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return notify.Permanent("配额告警事件解析失败: %v", err)
	}
	if ev.ProjectID <= 0 || ev.Metric == "" || ev.Level == "" {
		return notify.Permanent("配额告警事件缺少 project_id / metric / level")
	}
	policy, err := n.policies.Resolve(ctx, notify.Request{
		ProjectID: ev.Project(), RuleID: ev.RuleID(), Level: ev.Level,
	})
	if err != nil {
		if notify.IsPermanent(err) {
			return fmt.Errorf("%w: %w", errNoPolicy, err)
		}
		return err
	}
	at := ev.WindowStart
	msg := notify.Message{
		TraceID:   ev.Project() + ":quota:" + ev.Metric + ":" + ev.Level + ":" + at.UTC().Format(time.RFC3339),
		ProjectID: ev.Project(), RuleID: ev.RuleID(), Level: ev.Level,
		Subject: "配额阈值告警 - " + ev.Metric,
		At:      at,
		Payload: map[string]any{"metric": ev.Metric, "usage": ev.Usage, "limit": ev.Limit, "window_start": ev.WindowStart},
	}
	msg.Body = fmt.Sprintf("配额指标 %s 达到%s阈值：当前用量 %d，阈值 %d，窗口开始 %s", ev.Metric, ev.Level, ev.Usage, ev.Limit, ev.WindowStart.Format(time.RFC3339))
	results, err := n.dispatcher.Dispatch(ctx, msg, policy)
	for _, r := range results {
		if r.OK() {
			n.metrics.RecordChannel(r.Channel)
		}
	}
	if err == nil {
		n.counts.Delivered.Add(1)
	}
	return err
}

func eventTitle(ev alarm.Event) string {
	if ev.RuleName != "" {
		return ev.RuleName
	}
	if ev.RuleID != "" {
		return ev.RuleID
	}
	return "告警"
}

func serveHTTP(srv *http.Server, logger *slog.Logger) error {
	logger.Info("HTTP 已启动", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP 服务: %w", err)
	}
	return nil
}

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

type notifyReplayPayload struct {
	Message notify.Message `json:"message"`
	Policy  notify.Policy  `json:"policy"`
	Channel string         `json:"channel"`
}

func registerNotifyReplay(replayer *dlq.Replayer, dispatcher *notify.Dispatcher, logger *slog.Logger) error {
	return replayer.Register("notification", func(ctx context.Context, row dlq.Record) error {
		var payload notifyReplayPayload
		if err := json.Unmarshal([]byte(row.Payload), &payload); err != nil {
			return notify.Permanent("解析通知 DLQ 载荷: %v", err)
		}
		if payload.Channel == "" || len(payload.Policy.Recipients[payload.Channel]) == 0 {
			return notify.Permanent("通知 DLQ 载荷缺少 channel 或 message")
		}
		policy := payload.Policy
		policy.Channels = []string{payload.Channel}
		if _, err := dispatcher.Replay(ctx, payload.Message, policy); err != nil {
			logger.Warn("通知 DLQ 重放失败", "dlq_id", row.ID, "channel", payload.Channel, "error", err)
			return err
		}
		logger.Info("通知 DLQ 重放成功", "dlq_id", row.ID, "channel", payload.Channel)
		return nil
	})
}

func runNotifyDLQReplay(ctx context.Context, replayer *dlq.Replayer, interval time.Duration, batch int, logger *slog.Logger) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	run := func() {
		result, err := replayer.Replay(ctx, "svc-notify", "", batch)
		if err != nil {
			logger.Error("通知 DLQ 扫描失败", "error", err)
			return
		}
		if result.Scanned > 0 {
			logger.Info("通知 DLQ 扫描完成", "scanned", result.Scanned, "succeeded", result.Succeeded, "failed", result.Failed, "skipped", result.Skipped)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			run()
		}
	}
}
