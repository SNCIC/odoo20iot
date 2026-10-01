package alarm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// DefaultMaxCASRetry 是乐观锁冲突后的重读重试次数（04 §2.5：最多 3 次）。
const DefaultMaxCASRetry = 3

// 抑制原因（同时作为幂等标记：原因没变就不必重复写库）。
const (
	reasonStormTripped = "告警风暴：单租户窗口内新增告警超阈值"
	reasonStormHolding = "风暴熔断中（风暴聚合通知已发出）"
)

// Config 是引擎参数。
type Config struct {
	Storm       StormConfig
	Aggregation AggregatorConfig
	MaxCASRetry int
}

func (c Config) normalize() Config {
	c.Storm = c.Storm.normalize()
	c.Aggregation = c.Aggregation.normalize()
	if c.MaxCASRetry <= 0 {
		c.MaxCASRetry = DefaultMaxCASRetry
	}
	return c
}

// Options 是引擎构造参数。
type Options struct {
	// Store 必填。
	Store Store
	// Silences 可选（不配就没有维护窗口）。
	Silences *Silences
	Config   Config
	Metrics  *Metrics
	Logger   *slog.Logger
	// Now 注入时钟（测试把 60s 观察期压成瞬时）。
	Now func() time.Time
}

// Engine 推进告警状态机。
type Engine struct {
	cfg     Config
	store   Store
	silence *Silences
	storm   *stormGuard
	agg     *aggregator
	metrics *Metrics
	now     func() time.Time
	logger  *slog.Logger
}

// New 构造引擎。
func New(opts Options) (*Engine, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("alarm: 需要 Store")
	}
	e := &Engine{
		cfg:     opts.Config.normalize(),
		store:   opts.Store,
		silence: opts.Silences,
		storm:   newStormGuard(opts.Config.Storm),
		agg:     newAggregator(opts.Config.Aggregation),
		metrics: opts.Metrics,
		now:     opts.Now,
		logger:  opts.Logger,
	}
	if e.metrics == nil {
		e.metrics = new(Metrics)
	}
	if e.now == nil {
		e.now = time.Now
	}
	if e.logger == nil {
		e.logger = slog.Default()
	}
	return e, nil
}

// Metrics 返回指标计数器。
func (e *Engine) Metrics() *Metrics { return e.metrics }

// Observe 处理一次规则触发（事件驱动通道，04 §2.1）。
func (e *Engine) Observe(ctx context.Context, tr Trigger) (Decision, error) {
	at := tr.At
	if at.IsZero() {
		at = e.now()
	}
	key := DedupKey(tr.ProjectID, tr.DeviceID, tr.RuleID)

	for attempt := 0; ; attempt++ {
		prev, err := e.store.Get(ctx, key)
		if err != nil {
			return Decision{}, fmt.Errorf("读取告警 %s: %w", key, err)
		}

		next, dec := e.observeTransition(prev, key, tr, at)

		// 风暴窗口只在**新增告警**上计数一次（04 §2.2 的口径：「新增告警」）。
		// 放在 CAS 之前：重试时若已变成「已存在」，分支就换成 Observed，不会再计。
		if dec.Action == ActionCreated {
			switch e.storm.hit(tr.ProjectID, at) {
			case StormTripped:
				dec.Escalate = true
				dec.Reason = reasonStormTripped
				e.metrics.Storms.Add(1)
			case StormHolding:
				next.Suppressed = true
				next.SuppressReason = reasonStormHolding
			}
		}

		expected := StateIdle
		if prev != nil {
			expected = prev.State
		}
		if err := e.store.Update(ctx, next, expected); err != nil {
			if errors.Is(err, ErrStateConflict) && attempt < e.cfg.MaxCASRetry {
				// 04 §2.5：冲突则重读重试（最多 3 次）。
				continue
			}
			return Decision{}, err
		}

		e.metrics.record(dec, at)
		return dec, nil
	}
}

// observeTransition 是 Observe 的纯迁移逻辑（无 IO，便于穷举测试）。
func (e *Engine) observeTransition(prev *Alarm, key string, tr Trigger, at time.Time) (*Alarm, Decision) {
	if prev == nil {
		a := &Alarm{
			DedupKey:     key,
			ProjectID:    tr.ProjectID,
			DeviceID:     tr.DeviceID,
			DeviceTypeID: tr.DeviceTypeID,
			RuleID:       tr.RuleID,
			RuleName:     tr.RuleName,
			Level:        tr.Level,
			ParentID:     tr.ParentID,
			Timing:       tr.Timing.Normalize(),
			State:        StateDetected,
			FirstTS:      at,
			LastTS:       at,
			StateTS:      at,
		}
		return a, Decision{Alarm: a, Action: ActionCreated, Reason: "条件首次满足，进入观察期"}
	}

	a := prev.clone()
	a.LastTS = at

	switch prev.State {
	case StateDetected:
		if a.Age(at) >= a.Timing.DetectWindow {
			a.State = StateConfirmed
			a.StateTS = at
			a.ConfirmedTS = at
			return a, Decision{Alarm: a, Action: ActionConfirmed, Reason: "观察期内持续满足"}
		}
		return a, Decision{Alarm: a, Action: ActionObserved, Reason: "观察期中，继续累计"}

	case StateConfirmed:
		return a, Decision{Alarm: a, Action: ActionObserved, Reason: "已确认，等待通知"}

	case StateActive:
		// 04 §2.2 抑制：active 状态下重复触发只更新 last_ts，不重复通知。
		return a, Decision{Alarm: a, Action: ActionObserved, Reason: "抑制期内不重复通知"}

	case StateResolved:
		// 文档未规定此边，本实现取「重新观察」（见 Recover 的说明）：
		// resolved 意味着恢复条件已满足，此时又触发说明在抖 —— 回到观察期
		// 比直接再通知一次稳（否则抖动会变成通知风暴）。
		a.FlapCount++
		a.State = StateDetected
		a.StateTS = at
		a.ResolvedTS = time.Time{}
		return a, Decision{Alarm: a, Action: ActionReopened, Reason: "抖动：恢复期内再次触发，重新观察"}
	}
	return a, Decision{Alarm: a, Action: ActionObserved}
}

// Recover 处理一次「恢复条件满足」（04 §2.1 的「恢复」边）。
//
// detected / confirmed 直接回 idle（ActionRecoveredQuiet）：这两个阶段
// **从未通知过**，进 resolved 再等 5 分钟自动关闭没有意义，
// 只会在库里留一条没人看过的记录。
//
// ⚠️ 04 §2.1 的 ASCII 图把 active 的「恢复」也画向 idle，但同节的表把 resolved
// 定义为「恢复条件满足，等待关闭」—— 两者只能取一。本实现取表：
// active + 恢复 → resolved。否则 resolved 永远无法进入，auto_close_s 也失去意义。
func (e *Engine) Recover(ctx context.Context, projectID, deviceID, ruleID string, at time.Time) (Decision, error) {
	if at.IsZero() {
		at = e.now()
	}
	key := DedupKey(projectID, deviceID, ruleID)

	for attempt := 0; ; attempt++ {
		prev, err := e.store.Get(ctx, key)
		if err != nil {
			return Decision{}, fmt.Errorf("读取告警 %s: %w", key, err)
		}
		if prev == nil {
			return Decision{Action: ActionObserved, Reason: "无活跃告警，忽略恢复"}, nil
		}

		next := prev.clone()
		var dec Decision
		switch prev.State {
		case StateDetected, StateConfirmed:
			next.State = StateIdle
			next.ClosedTS = at
			dec = Decision{Action: ActionRecoveredQuiet, Reason: "尚未通知即恢复"}
		case StateActive:
			next.State = StateResolved
			next.StateTS = at
			next.ResolvedTS = at
			dec = Decision{Action: ActionResolved, Reason: "恢复条件满足，等待关闭"}
		default:
			// 已在 resolved：等待自动关闭，重复恢复无意义。
			return Decision{Alarm: prev, Action: ActionObserved, Reason: "已在等待关闭"}, nil
		}

		if err := e.store.Update(ctx, next, prev.State); err != nil {
			if errors.Is(err, ErrStateConflict) && attempt < e.cfg.MaxCASRetry {
				continue
			}
			return Decision{}, err
		}
		dec.Alarm = next
		e.metrics.record(dec, at)
		return dec, nil
	}
}

// Tick 执行一次定时扫描（04 §2.1：每 5s，兜底防事件丢失）。
//
// 只推进**与触发无关、只与时间有关**的迁移：
//   - detected 满观察期 → confirmed（假设期间没有恢复事件，这正是「兜底」的含义）；
//   - confirmed → active（做抑制判定并发通知）或保持抑制；
//   - resolved 满自动关闭延迟 → idle。
func (e *Engine) Tick(ctx context.Context) ([]Decision, error) {
	at := e.now()

	alarms, err := e.store.Active(ctx, StateDetected, StateConfirmed, StateResolved)
	if err != nil {
		return nil, fmt.Errorf("扫描活跃告警: %w", err)
	}

	out := make([]Decision, 0, len(alarms))
	for _, prev := range alarms {
		next, dec, err := e.advance(ctx, prev, at)
		if err != nil {
			return out, err
		}
		if dec == nil {
			continue // 还不到时候 / 抑制状态没变
		}

		if err := e.store.Update(ctx, next, prev.State); err != nil {
			if errors.Is(err, ErrStateConflict) {
				// 事件通道或另一分片刚改过它 —— 跳过，下一轮再看。
				// 这里**不重试**：Tick 本身就是周期性的，重试只会与事件通道抢锁。
				e.logger.Debug("扫描时发现状态已被并发修改，跳过本轮", "alarm", prev.ID)
				continue
			}
			return out, err
		}
		e.metrics.record(*dec, at)
		out = append(out, *dec)
	}
	return out, nil
}

// advance 是 Tick 的纯迁移逻辑。
func (e *Engine) advance(ctx context.Context, prev *Alarm, at time.Time) (*Alarm, *Decision, error) {
	a := prev.clone()

	switch prev.State {
	case StateDetected:
		if a.Age(at) < a.Timing.DetectWindow {
			return nil, nil, nil
		}
		a.State = StateConfirmed
		a.StateTS = at
		a.ConfirmedTS = at
		return a, &Decision{Alarm: a, Action: ActionConfirmed, Reason: "定时扫描：观察期满"}, nil

	case StateConfirmed:
		reason, err := e.suppression(ctx, a, at)
		if err != nil {
			return nil, nil, err
		}
		if reason != "" {
			if a.Suppressed && a.SuppressReason == reason {
				return nil, nil, nil // 抑制原因没变，不必反复写库
			}
			a.Suppressed = true
			a.SuppressReason = reason
			return a, &Decision{Alarm: a, Action: ActionSuppressed, Reason: reason}, nil
		}

		a.State = StateActive
		a.StateTS = at
		a.NotifiedTS = at
		a.NotifyCount++
		a.Suppressed = false
		a.SuppressReason = ""

		// 聚合判定（04 §2.2）：同 device_type 下 ≥N 台不同设备在窗口内触发同一规则
		// → 合并为一条批量告警。本实现只做判定与计数，批量告警实体尚未落地。
		group := BatchGroupKey(a.ProjectID, a.DeviceTypeID, a.RuleID)
		devices, batchID, batched := e.agg.add(group, a.DeviceID, at)
		if batched {
			a.BatchID = batchID
			e.metrics.Batched.Add(1)
			return a, &Decision{
				Alarm: a, Action: ActionBatched, Notify: true,
				Reason: fmt.Sprintf("聚合：%d 台设备触发同一规则，合并为批量告警 %s", devices, batchID),
			}, nil
		}
		return a, &Decision{Alarm: a, Action: ActionNotified, Notify: true, Reason: "确认后通知"}, nil

	case StateResolved:
		if at.Sub(prev.ResolvedTS) < prev.Timing.AutoClose {
			return nil, nil, nil
		}
		a.State = StateIdle
		a.ClosedTS = at
		return a, &Decision{Alarm: a, Action: ActionClosed, Reason: "自动关闭"}, nil
	}
	return nil, nil, nil
}

// suppression 返回非空原因表示应抑制通知（04 §2.2 的静默窗口 / 根因 / 风暴）。
func (e *Engine) suppression(ctx context.Context, a *Alarm, at time.Time) (string, error) {
	// 1) 静默窗口：窗口内只记录不通知。
	if e.silence != nil {
		if w, ok := e.silence.Covering(a, at); ok {
			reason := "静默窗口"
			if w.Reason != "" {
				reason += "：" + w.Reason
			}
			return reason, nil
		}
	}

	// 2) 根因抑制：父告警活跃时子告警自动抑制
	// （如「设备离线」抑制其下所有指标告警）。
	// 「活跃」含 detected —— 设备离线一旦被检出，其下指标告警就没有诊断价值了，
	// 等它确认完再抑制已经晚了。
	if a.ParentID != "" {
		parent, err := e.store.GetByID(ctx, a.ParentID)
		if err != nil {
			return "", fmt.Errorf("查询父告警 %s: %w", a.ParentID, err)
		}
		if parent != nil && parent.State.Open() {
			return fmt.Sprintf("根因抑制：父告警 %s 处于 %s", parent.ID, parent.State), nil
		}
	}

	// 3) 风暴抑制：这里**只读**当前状态，不计数 ——
	// 计数只在「新增告警」时做一次（见 Observe），否则一次风暴会被数两遍。
	if e.storm.state(a.ProjectID, at) == StormHolding {
		return reasonStormHolding, nil
	}
	return "", nil
}

// SuppressPrefix 取抑制原因的前缀（调用方可据此归类统计）。
func SuppressPrefix(reason string) string {
	if i := strings.Index(reason, "："); i > 0 {
		return reason[:i]
	}
	return reason
}
