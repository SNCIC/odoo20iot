package alarm

import (
	"fmt"
	"sync"
	"time"
)

// Silence 是维护窗口（04 §2.2：支持定时/临时，窗口内只记录不通知）。
//
// 作用域用「零值即不限」表达，而不是多态过滤器 —— 运维配的多半是
// 「这台设备」「这个机型」这类直白条件，多态会让配置界面无从下手。
type Silence struct {
	ID string
	// ProjectID 为空表示不限租户（仅建议给全局维护窗口用）。
	ProjectID string
	// DeviceTypeID 为 0 / DeviceID 为空 / RuleID 为空，均表示该维度不限。
	DeviceTypeID int64
	DeviceID     string
	RuleID       string
	Start        time.Time
	End          time.Time
	Reason       string
}

// Covers 判定该窗口是否覆盖这条告警（at 时刻）。
func (s Silence) Covers(a *Alarm, at time.Time) bool {
	if a == nil {
		return false
	}
	if !s.Start.IsZero() && at.Before(s.Start) {
		return false
	}
	if !s.End.IsZero() && !at.Before(s.End) {
		return false
	}
	if s.ProjectID != "" && s.ProjectID != a.ProjectID {
		return false
	}
	if s.DeviceTypeID != 0 && s.DeviceTypeID != a.DeviceTypeID {
		return false
	}
	if s.DeviceID != "" && s.DeviceID != a.DeviceID {
		return false
	}
	if s.RuleID != "" && s.RuleID != a.RuleID {
		return false
	}
	return true
}

// Silences 是维护窗口集合（并发安全）。
type Silences struct {
	mu      sync.RWMutex
	windows []Silence
}

// NewSilences 构造空的窗口集合。
func NewSilences() *Silences { return &Silences{} }

// Add 追加一个窗口。
func (s *Silences) Add(w Silence) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows = append(s.windows, w)
}

// Replace 整体替换（控制台下发配置时用；避免增量同步的漏删）。
func (s *Silences) Replace(ws []Silence) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows = append([]Silence(nil), ws...)
}

// Covering 返回覆盖该告警的第一个窗口。
func (s *Silences) Covering(a *Alarm, at time.Time) (Silence, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, w := range s.windows {
		if w.Covers(a, at) {
			return w, true
		}
	}
	return Silence{}, false
}

// StormState 是风暴抑制的状态。
type StormState int

const (
	// StormOK：正常。
	StormOK StormState = iota
	// StormTripped：**刚刚**跨过阈值 —— 需发一条「告警风暴」聚合通知并 P1 升级。
	StormTripped
	// StormHolding：已在熔断窗口内 —— 只记录，不再重复发风暴通知。
	StormHolding
)

// StormConfig 是风暴抑制参数（04 §2.2：单租户 1 分钟内新增 > 500 条）。
type StormConfig struct {
	Threshold int
	Window    time.Duration
}

// DefaultStormConfig 是文档默认值。
func DefaultStormConfig() StormConfig {
	return StormConfig{Threshold: 500, Window: time.Minute}
}

func (c StormConfig) normalize() StormConfig {
	if c.Threshold <= 0 {
		c.Threshold = 500
	}
	if c.Window <= 0 {
		c.Window = time.Minute
	}
	return c
}

// stormGuard 实现风暴抑制。
//
// 为什么必须区分 Tripped 与 Holding：文档要求「只发一条告警风暴聚合通知」。
// 不区分的话，第 501 条之后的每一条都会再发一次风暴通知 ——
// 在风暴里再制造风暴，正是这类保护最典型的反效果。
type stormGuard struct {
	cfg StormConfig

	mu        sync.Mutex
	events    map[string][]time.Time
	trippedAt map[string]time.Time
}

func newStormGuard(cfg StormConfig) *stormGuard {
	return &stormGuard{
		cfg:       cfg.normalize(),
		events:    make(map[string][]time.Time),
		trippedAt: make(map[string]time.Time),
	}
}

// hit 记录一次新增告警并返回当前的风暴状态。
func (g *stormGuard) hit(projectID string, at time.Time) StormState {
	g.mu.Lock()
	defer g.mu.Unlock()

	// 已在熔断窗口内：不再重复通知（这是「只发一条」的落点）。
	if tripped, ok := g.trippedAt[projectID]; ok {
		if at.Sub(tripped) < g.cfg.Window {
			return StormHolding
		}
		delete(g.trippedAt, projectID)
	}

	cutoff := at.Add(-g.cfg.Window)
	evs := g.events[projectID]
	keep := evs[:0]
	for _, t := range evs {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	evs = append(keep, at)
	g.events[projectID] = evs

	if len(evs) <= g.cfg.Threshold {
		return StormOK
	}

	g.trippedAt[projectID] = at
	return StormTripped
}

// AggregatorConfig 是聚合参数（04 §2.2：同 device_type 下 ≥5 台设备在 60s 内
// 触发同一规则 → 合并为一条批量告警）。
type AggregatorConfig struct {
	Threshold int
	Window    time.Duration
}

// DefaultAggregatorConfig 是文档默认值。
func DefaultAggregatorConfig() AggregatorConfig {
	return AggregatorConfig{Threshold: 5, Window: 60 * time.Second}
}

func (c AggregatorConfig) normalize() AggregatorConfig {
	if c.Threshold <= 0 {
		c.Threshold = 5
	}
	if c.Window <= 0 {
		c.Window = 60 * time.Second
	}
	return c
}

// aggregator 统计「同一 device_type 下多少台**不同**设备在窗口内触发了同一规则」。
//
// ⚠️ 本实现只做**合并判定与计数**：批量告警实体本身（一条告警承载多台设备）
// 及其通知路径需要 t_alarm 的批量字段，尚未落地（见 09-handoff 遗留项）。
// 返回的 BatchID 让调用方至少能把同一批的通知合并成一条。
type aggregator struct {
	cfg AggregatorConfig

	mu     sync.Mutex
	groups map[string]map[string]time.Time // groupKey → deviceID → 首次触发时刻
}

func newAggregator(cfg AggregatorConfig) *aggregator {
	return &aggregator{cfg: cfg.normalize(), groups: make(map[string]map[string]time.Time)}
}

// add 记录一次触发，返回窗口内的**不同设备数**与合并信息。
//
// 用设备集合而不是计数：同一台设备刷 10 次不该被当成 10 台设备触发 ——
// 那会把「一台设备抖动」误判成「批量故障」，是聚合最容易写错的地方。
func (g *aggregator) add(groupKey, deviceID string, at time.Time) (devices int, batchID string, batched bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	devicesOf := g.groups[groupKey]
	if devicesOf == nil {
		devicesOf = make(map[string]time.Time)
		g.groups[groupKey] = devicesOf
	}

	cutoff := at.Add(-g.cfg.Window)
	earliest := at
	for id, first := range devicesOf {
		if !first.After(cutoff) {
			delete(devicesOf, id)
			continue
		}
		if first.Before(earliest) {
			earliest = first
		}
	}
	if _, seen := devicesOf[deviceID]; !seen {
		devicesOf[deviceID] = at
		if at.Before(earliest) {
			earliest = at
		}
	}

	if len(devicesOf) < g.cfg.Threshold {
		return len(devicesOf), "", false
	}
	// 同一窗口内 batchID 保持一致，便于把后续同批告警并进同一条通知。
	return len(devicesOf), fmt.Sprintf("batch:%s:%d", groupKey, earliest.Unix()), true
}

// state 只读地报告当前风暴状态（**不计数**）。
//
// 计数只在「新增告警」时做一次（04 §2.2 的口径）；若在抑制判定里再调 hit，
// 一次风暴会被数两遍。
func (g *stormGuard) state(projectID string, at time.Time) StormState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if tripped, ok := g.trippedAt[projectID]; ok && at.Sub(tripped) < g.cfg.Window {
		return StormHolding
	}
	return StormOK
}
