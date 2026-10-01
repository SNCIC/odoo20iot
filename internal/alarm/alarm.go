// Package alarm 实现 04 §2 的告警引擎：五态状态机（§2.1）、去重/聚合/抑制（§2.2）
// 与告警质量指标（§2.2.1）。
//
// 边界：本包是**纯状态机**，不做任何 IO —— 落库由 `Store` 抽象、通知由调用方
// （svc-alarm / svc-notify）根据 `Decision` 执行。这样状态迁移可以用确定性测试
// 穷举，而不是靠起服务观察。
//
// 推进双通道（04 §2.1）：`Observe` 走事件驱动（保时效），`Tick` 走定时扫描
// （每 5s，兜底防事件丢失）。两条路径共享同一套 `advance` 逻辑，
// 避免「扫描推进的规则和事件推进的不一样」这种最难查的分裂。
package alarm

import (
	"crypto/sha1"
	"encoding/hex"
	"time"
)

// State 是告警五态（04 §2.1）。
type State string

const (
	// StateIdle 表示无告警 —— 引擎里以「查不到记录」表示，不落库。
	StateIdle      State = "idle"
	StateDetected  State = "detected"
	StateConfirmed State = "confirmed"
	StateActive    State = "active"
	StateResolved  State = "resolved"
)

// Open 报告该状态是否占用去重键（04 §2.2：同一设备同一规则只允许一个活跃告警）。
//
// `resolved` 也算占用：它还在等自动关闭，此时放新告警进来会让抖动反复。
func (s State) Open() bool { return s != StateIdle }

// 时序默认值（04 §2.1 的「默认参数」列）。
const (
	DefaultDetectWindow = 60 * time.Second
	DefaultSuppress     = 10 * time.Minute
	DefaultAutoClose    = 5 * time.Minute
)

// Timing 是告警的时序参数（04 §2.4 的 `timing`）。
type Timing struct {
	// DetectWindow 是观察期：detected 持续满足多久进入 confirmed。
	DetectWindow time.Duration
	// Suppress 是抑制期：active 期间重复触发只更新 last_ts，不重复通知。
	Suppress time.Duration
	// AutoClose 是 resolved 后多久自动关闭回 idle（避免抖动反复）。
	AutoClose time.Duration
}

// Normalize 补齐零值（缺省取 04 §2.1 的默认参数）。
func (t Timing) Normalize() Timing {
	if t.DetectWindow <= 0 {
		t.DetectWindow = DefaultDetectWindow
	}
	if t.Suppress <= 0 {
		t.Suppress = DefaultSuppress
	}
	if t.AutoClose <= 0 {
		t.AutoClose = DefaultAutoClose
	}
	return t
}

// Alarm 是一条告警实例（PG `t_alarm` 一行的内存视图）。
type Alarm struct {
	ID       string
	DedupKey string

	ProjectID    string
	DeviceID     string
	DeviceTypeID int64
	RuleID       string
	RuleName     string
	Level        string

	// ParentID 支撑根因抑制（04 §2.2）：父告警活跃时子告警自动抑制
	// （如「设备离线」抑制其下所有指标告警）。
	ParentID string

	State State
	// Timing 随规则走，必须持久化 —— 否则重启后无法算出「还要等多久」。
	Timing Timing

	// FirstTS 首次触发；LastTS 最近一次触发；StateTS 进入当前状态的时间。
	FirstTS time.Time
	LastTS  time.Time
	StateTS time.Time
	// ConfirmedTS / NotifiedTS / ResolvedTS 是 04 §2.2.1 告警质量指标的原始数据
	// （如「平均确认时长 = confirmed_ts - first_ts」）。
	ConfirmedTS time.Time
	NotifiedTS  time.Time
	ResolvedTS  time.Time
	ClosedTS    time.Time

	// NotifyCount 通知次数（聚合时累加，04 §2.2）。
	NotifyCount int
	// FlapCount 抖动次数：resolved 期间再次触发。
	FlapCount int

	// Suppressed / SuppressReason 记录最近一次被抑制的原因，用于排障
	// （「为什么这条告警没发出去」必须有答案）。
	Suppressed     bool
	SuppressReason string

	// BatchID 非空表示这条告警已被聚合（04 §2.2）。
	BatchID string
}

// clone 复制一份，避免对 Store 返回的实例做原地修改 ——
// 原地改会让 CAS 失败时「已写入的预期状态」与实际分不清。
func (a *Alarm) clone() *Alarm {
	if a == nil {
		return nil
	}
	c := *a
	return &c
}

// Clone 是给外部（svc-alarm 的缓存层、Store 的调用方）用的安全复制：
// 调用方拿到的是副本，改它不会污染 Store 里的状态。
func (a *Alarm) Clone() *Alarm { return a.clone() }

// Trigger 是一次「规则触发」输入（由 svc-rule 的事件驱动通道送来）。
type Trigger struct {
	ProjectID    string
	DeviceID     string
	DeviceTypeID int64
	RuleID       string
	RuleName     string
	Level        string
	ParentID     string
	Timing       Timing
	// At 是触发时刻；零值取 Engine 的时钟（便于测试注入）。
	At time.Time
}

// Action 是本轮推进产生的动作。
type Action string

const (
	// ActionCreated：新建告警，进入 detected（观察期开始）。
	ActionCreated Action = "created"
	// ActionObserved：已在告警中，只更新 last_ts。
	ActionObserved Action = "observed"
	// ActionConfirmed：观察期满，进入 confirmed，待通知。
	ActionConfirmed Action = "confirmed"
	// ActionNotified：进入 active，**通知已发出**。
	ActionNotified Action = "notified"
	// ActionResolved：恢复条件满足，进入 resolved（等自动关闭）。
	ActionResolved Action = "resolved"
	// ActionClosed：自动关闭，回到 idle。
	ActionClosed Action = "closed"
	// ActionSuppressed：被静默窗口 / 根因 / 风暴抑制，只记录不通知。
	ActionSuppressed Action = "suppressed"
	// ActionBatched：被聚合进批量告警。
	ActionBatched Action = "batched"
	// ActionReopened：resolved 期间再次触发，重新进入观察期。
	ActionReopened Action = "reopened"
	// ActionRecoveredQuiet：detected/confirmed 阶段恢复 —— 从未通知过，
	// 直接回 idle，不产生 resolved（见 Engine.Recover 的说明）。
	ActionRecoveredQuiet Action = "recovered_quiet"
)

// Decision 告诉调用方该做什么。本包不做 IO：
// 落库、通知、指标都由调用方按 Decision 执行。
type Decision struct {
	Alarm  *Alarm
	Action Action
	// Notify 为 true 时调用方应发出通知（只有 ActionNotified / ActionBatched 会置位）。
	Notify bool
	// Escalate 为 true 时需 P1 升级（风暴熔断，04 §2.2）。
	Escalate bool
	Reason   string
}

// DedupKey 计算去重键（04 §2.2）：同一设备同一规则只允许一个活跃告警。
//
// ⚠️ **实现决策**：文档写的是 `sha1(project_id + device_id + rule_id)`，本实现
// 在三段之间插入 `\x00` 分隔符。裸拼接会让 `("a","bc","d")` 与 `("ab","c","d")`
// 算出**同一个键** —— 那意味着两台不同设备的告警会互相压制，
// 一个安静但危险的正确性缺陷。分隔符改变了摘要值，故与文档不同，
// 但保留了文档真正要的语义（稳定的 per-(project,device,rule) 键）。
func DedupKey(projectID, deviceID, ruleID string) string {
	h := sha1.New()
	// 逐段写入，段间用 0x00 —— 该字节不可能出现在 ID 里，故无歧义。
	for i, part := range []string{projectID, deviceID, ruleID} {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// BatchGroupKey 是聚合的分组键（04 §2.2：同一 device_type 下同一规则）。
func BatchGroupKey(projectID string, deviceTypeID int64, ruleID string) string {
	return DedupKey(projectID, itoa(deviceTypeID), ruleID)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
