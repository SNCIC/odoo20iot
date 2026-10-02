package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/pg"
)

// publisher 是告警事件的发布端口。
//
// 定义成窄接口（而不是直接吃 *nats.Conn）是为了让扫描循环能用假发布器测
// 「哪些决策会发出去、发几次」—— 那正是本服务最容易写错的地方；
// 非要起真 NATS 才能测的话，实际上就不会有人测。
type publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// publishStore 是 agent 用到的那部分存储能力。
//
// 刻意不直接依赖 *alarm.PGStore：这三件事是**服务侧**的关注点
// （发布是状态机之外的事），把它们抽出来既让上面那些用例能跑在内存里，
// 也让 alarm 包继续保持「纯状态机」的边界。
type publishStore interface {
	MarkPublished(ctx context.Context, dedupKey string, at time.Time) error
	UnpublishedActive(ctx context.Context) ([]*alarm.Alarm, error)
	Active(ctx context.Context, states ...alarm.State) ([]*alarm.Alarm, error)
}

type escalationClaimer interface {
	ClaimEscalation(ctx context.Context, dedupKey string, stage int) (bool, error)
	Active(ctx context.Context, states ...alarm.State) ([]*alarm.Alarm, error)
}

// counters 是本服务的运行计数。
//
// 与 alarm.Metrics 分开：那些是**业务口径**（有效告警占比、合并率，04 §2.2.1），
// 这些是**服务健康**。混在一个结构里会让「告警质量」被服务重启清零。
type counters struct {
	Scans          atomic.Int64
	ScanErrors     atomic.Int64
	ScanSkipped    atomic.Int64
	Triggers       atomic.Int64
	TriggerErrors  atomic.Int64
	PoisonTriggers atomic.Int64
	Published      atomic.Int64
	PublishErrors  atomic.Int64
	Republished    atomic.Int64
}

// agent 把引擎、存储与发布器接成一个可运行的推进循环。
type agent struct {
	engine                *alarm.Engine
	store                 publishStore
	pool                  *pgxpool.Pool
	pub                   publisher
	lockKey               int64
	metrics               *alarm.Metrics
	counts                *counters
	logger                *slog.Logger
	now                   func() time.Time
	notifyEscalationAfter time.Duration
	p1EscalationAfter     time.Duration
}

// scanOnce 执行一轮扫描：抢锁 → Tick → 分发 → 补发。
func (a *agent) scanOnce(ctx context.Context) error {
	a.counts.Scans.Add(1)

	// 04 §2.5：定时扫描任务使用分布式锁，避免多实例重复扫描全量数据。
	lock, ok, err := pg.TryLock(ctx, a.pool, a.lockKey)
	if err != nil {
		a.counts.ScanErrors.Add(1)
		return fmt.Errorf("抢扫描锁: %w", err)
	}
	if !ok {
		// 别的实例正在扫。**不是错误**，也不该每 5s 记一条 warn ——
		// 那会把真正的问题淹掉，而这行日志本身完全正常。
		a.counts.ScanSkipped.Add(1)
		return nil
	}
	defer func() {
		if err := lock.Unlock(ctx); err != nil {
			a.logger.Warn("释放扫描锁失败", "error", err)
		}
	}()

	ds, err := a.engine.Tick(ctx)
	if err != nil {
		a.counts.ScanErrors.Add(1)
		return fmt.Errorf("扫描推进: %w", err)
	}
	a.dispatch(ctx, ds)
	if err := a.escalateUnconfirmed(ctx); err != nil {
		a.counts.ScanErrors.Add(1)
		return err
	}

	// 补发：把「已 active 但事件没发出去」的捞回来。
	// 没有这一步，一次 NATS 抖动就会静默丢掉那批工单。
	if err := a.republish(ctx); err != nil {
		a.counts.ScanErrors.Add(1)
		return err
	}
	return nil
}

func (a *agent) escalateUnconfirmed(ctx context.Context) error {
	claimer, ok := a.store.(escalationClaimer)
	if !ok {
		return nil
	}
	alarms, err := a.store.Active(ctx, alarm.StateActive)
	if err != nil {
		return fmt.Errorf("查询未确认告警: %w", err)
	}
	now := a.now()
	for _, current := range alarms {
		if current == nil || current.NotifiedTS.IsZero() || !current.AcknowledgedAt.IsZero() {
			continue
		}
		stage := 0
		if now.Sub(current.NotifiedTS) >= a.p1EscalationAfter {
			stage = 2
		} else if now.Sub(current.NotifiedTS) >= a.notifyEscalationAfter {
			stage = 1
		}
		if stage == 0 {
			continue
		}
		claimed, err := claimer.ClaimEscalation(ctx, current.DedupKey, stage)
		if err != nil {
			return err
		}
		if !claimed {
			continue
		}
		d := alarm.Decision{Alarm: current, Action: alarm.ActionNotified, Notify: true,
			Escalate: stage >= 2, EscalationStage: stage,
			Reason: fmt.Sprintf("未确认超过 %s，升级阶段 %d", now.Sub(current.NotifiedTS).Round(time.Second), stage)}
		a.publishDecision(ctx, d)
	}
	return nil
}

func (a *agent) dispatch(ctx context.Context, ds []alarm.Decision) {
	for _, d := range ds {
		notifyNormal := d.Notify
		if d.Escalate {
			// 04 §2.2：风暴熔断要「只发一条聚合通知并 P1 升级」。
			// 通过同一条线上事件携带 escalated=true，通知服务可按策略切换
			// P1 收件人，而不是只写一条没人消费的日志。
			project := ""
			if d.Alarm != nil {
				project = d.Alarm.ProjectID
			}
			a.logger.Warn("告警风暴熔断，发布 P1 升级事件",
				"project", project, "reason", d.Reason)
			escalation := d
			escalation.Notify = true
			if ev, ok := alarm.NewEvent(escalation); ok {
				data, err := json.Marshal(ev)
				if err != nil {
					a.logger.Error("序列化 P1 升级事件失败", "error", err)
				} else {
					subject := alarm.AlarmSubjectPrefix + ".escalation." + ev.ProjectID
					if err := a.pub.Publish(ctx, subject, data); err != nil {
						a.logger.Error("发布 P1 升级事件失败", "subject", subject, "error", err)
					}
				}
			}
		}
		if notifyNormal {
			a.publishDecision(ctx, d)
		}
	}
}

// publishDecision 发布一条告警事件；**发布成功才写发布标记**。
func (a *agent) publishDecision(ctx context.Context, d alarm.Decision) bool {
	ev, ok := alarm.NewEvent(d)
	if !ok {
		return false
	}
	data, err := json.Marshal(ev)
	if err != nil {
		// 结构体自己序列化失败属于编码错误，不该发生；但不能静默。
		a.logger.Error("序列化告警事件失败（不该发生）", "alarm", ev.AlarmID, "error", err)
		return false
	}

	subject := alarm.AlarmSubject(ev.ProjectID)
	if ev.Escalated || ev.EscalationStage > 0 {
		subject = alarm.AlarmSubjectPrefix + ".escalation." + ev.ProjectID
	}
	if err := a.pub.Publish(ctx, subject, data); err != nil {
		a.counts.PublishErrors.Add(1)
		// 不在这里重试：这一轮的状态已经落库为 active，扫描不会再产出这条决策。
		// 真正的兜底是 republish（按 published_at IS NULL 补发）——
		// 所以这里必须记 Error，否则一次抖动就悄悄少一张工单。
		a.logger.Error("发布告警事件失败，留待补发扫描",
			"subject", subject, "alarm", ev.AlarmID, "error", err)
		return false
	}

	if err := a.store.MarkPublished(ctx, ev.DedupKey, a.now()); err != nil {
		// 发布成功但标记失败 → 下一轮会**重复发布**。这是安全方向：
		// 下游按 07 §6 S3 的幂等键与 10min 合并窗口去重。
		// 反过来（先标记后发布）才会丢工单。
		a.logger.Warn("事件已发布但标记失败，下轮将重复投递（下游幂等兜住）",
			"alarm", ev.AlarmID, "error", err)
	}

	a.counts.Published.Add(1)
	a.logger.Info("已发布告警事件",
		"subject", subject, "alarm", ev.AlarmID,
		"level", ev.Level, "batch", ev.BatchID, "device", ev.DeviceID)
	return true
}

// republish 补发「已 active 但事件未发布」的告警。
//
// 语义是**至少一次**：崩溃或发布失败都会导致重复投递，由下游幂等兜住。
// 这也正是 07 §6 S3 定义 `idem:{tenant}:maintenance_req:eq-{eq}-{alarm_dedup_key}`
// 的原因 —— 幂等键不是预防性设计，而是为这一条路径存在的。
func (a *agent) republish(ctx context.Context) error {
	pending, err := a.store.UnpublishedActive(ctx)
	if err != nil {
		return fmt.Errorf("查询未发布的告警: %w", err)
	}
	for _, al := range pending {
		d := alarm.Decision{
			Alarm: al, Action: alarm.ActionNotified, Notify: true, Reason: "补发",
		}
		if al.BatchID != "" {
			d.Action = alarm.ActionBatched
		}
		if a.publishDecision(ctx, d) {
			a.counts.Republished.Add(1)
		}
	}
	return nil
}

// observeTrigger 处理一次规则触发（04 §2.1 的事件驱动通道）。
//
// 这一侧**不做 dedup_key 分片**（04 §2.5 要求按哈希分片保证同一告警只被一个实例
// 处理）。多副本同时订阅时，同一个 dedup_key 会被并发推进 —— 那不会算错
// （CAS 会挡住陈旧的写入），只是有一部推进白跑。分片是规模上来之后的事。
//
// 另外：这一侧只负责「尽快进入 confirmed」。通知仍由下一次扫描发出（≤5s），
// 因为 confirmed → active 属于时间推进，只走 Tick 一条路径 ——
// 两条路径各写一套推进逻辑是最难查的分裂（见 alarm 包注释）。
func (a *agent) observeTrigger(ctx context.Context, tr alarm.Trigger) error {
	a.counts.Triggers.Add(1)

	d, err := a.engine.Observe(ctx, tr)
	if err != nil {
		a.counts.TriggerErrors.Add(1)
		return err
	}
	if d.Notify {
		a.publishDecision(ctx, d)
	}
	return nil
}
