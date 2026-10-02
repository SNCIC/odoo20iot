// Package notify 实现 04 §2.3 的通知分发：三条通道（Webhook / 邮件 / 短信）、
// 按优先级的降级、重试阶梯与死信。
//
// 边界：本包**不做状态机**（那是 svc-alarm 的事），也不决定「谁该被通知」
// （那是通知策略的事，见 PolicyResolver）。它只回答一个问题：
// 给定一条已渲染的通知与一条策略，**怎样把它可靠地送到人手上**。
//
// 三通道而非四通道：04 §2.3 列的是「Webhook → 邮件 → 短信 → 语音」四档，
// 而 06 的 Phase 2 验收项写的是「Webhook / 邮件 / 短信，3 通道」。
// 语音需要运营商语音网关，本期不做（如实留白，不做假实现）。
package notify

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// 通道名（策略里用它排序，指标里用它打标签）。
const (
	ChannelWebhook = "webhook"
	ChannelEmail   = "email"
	ChannelSMS     = "sms"
)

// ErrPermanent 表示重试无意义的失败。
//
// 必须与可重试失败分开：URL 未命中和白名单、模板渲染不出来、
// SMTP 说「收件人不存在」—— 这些重试三次只会浪费三次超时，
// 还会把 DLQ 灌满同一类噪音，让真正需要人看的条目沉下去。
var ErrPermanent = errors.New("notify: 永久失败（重试无意义）")

// Permanent 包装一个永久失败。
func Permanent(format string, args ...any) error {
	return fmt.Errorf(format+": %w", append(args, ErrPermanent)...)
}

// IsPermanent 报告错误是否属于永久失败。
func IsPermanent(err error) bool { return errors.Is(err, ErrPermanent) }

// Message 是一条待投递的通知。
//
// 同时带 Subject/Body（人读）与 Payload（机器读）：webhook 要 JSON、
// 邮件与短信要文本。让三个通道各自从同一份事实里取所需，
// 好过让上游为每个通道拼一份 —— 那样三者迟早会说不一样的话。
type Message struct {
	// TraceID 贯通日志与 DLQ 条目（04 §3.3：DLQ 条目含 trace_id）。
	TraceID string
	// AlarmID / DedupKey / ProjectID / DeviceID 来自告警事件。
	AlarmID   string
	DedupKey  string
	ProjectID string
	DeviceID  string
	RuleID    string
	Level     string

	// Subject 是标题，Body 是渲染后的正文。
	Subject string
	Body    string
	// Payload 是结构化载荷（webhook 直接发它）。
	Payload map[string]any

	At time.Time
}

// Policy 是一条告警的通知策略（04 §2.4 的 notify）。
type Policy struct {
	// Groups 是通知组名。正式实现里由 t_user / t_role 展开成收件人；
	// 当前仅在日志与指标里记录（见 09 的遗留项）。
	Groups []string
	// Channels 按**优先级从高到低**排列（04 §2.3：Webhook → 邮件 → 短信）。
	Channels []string
	// Template 是模板名（04 §2.4 的 notify.template）。
	Template string
	// Recipients 按通道名给收件人：webhook 是 URL、email 是地址、sms 是号码。
	Recipients map[string][]string
	// EscalatedRecipients 是升级事件的专用收件人；为空时沿用 Recipients。
	EscalatedRecipients map[string][]string
	// AckEscalatedRecipients 是首次未确认升级（阶段 1）的收件人。
	AckEscalatedRecipients map[string][]string
}

// Validate 检查策略是否可用。
func (p Policy) Validate() error {
	if len(p.Channels) == 0 {
		return Permanent("通知策略没有可用通道")
	}
	for _, c := range p.Channels {
		if len(p.Recipients[c]) == 0 {
			return Permanent("通道 %s 没有收件人", c)
		}
	}
	return nil
}

// Result 是一次通道投递的结果。
type Result struct {
	Channel  string
	Attempts int
	Err      error
	// Response 是被截断到 1 KB 的响应体（04 §2.3）。
	Response string
	// DegradedFrom 非空表示这条是靠降级发出去的（原通道是它）。
	DegradedFrom string
}

// OK 报告该通道是否投递成功。
func (r Result) OK() bool { return r.Err == nil }

// Channel 是一个通知通道。
type Channel interface {
	// Name 是通道名。
	Name() string
	// Send 投递一条通知；收件人由调用方按通道给出。
	//
	// 实现**不做重试**：重试与降级是 Dispatcher 的职责。
	// 放在通道里会让「这条通知到底试了几次」在指标上说不清，
	// 而重试次数正是排查「为什么人没收到」时第一个要看的数。
	Send(ctx context.Context, msg Message, recipients []string) error
}

// maxResponseBytes 是记录响应体的上限（04 §2.3：响应体截断至 1 KB）。
//
// 「截断」不是为了省存储，而是因为出站响应是**不可信输入**：
// 对方可以返回 100MB 把内存打满，也可以塞一大段 HTML 把日志搅乱。
const maxResponseBytes = 1 << 10

// PartialError 表示「部分收件人被拒」：消息已经投出去了（给被接受的那部分），
// 但没有覆盖全部收件人。
//
// 它既**不该重试**（会给已经收到的人再发一遍，凌晨三点的重复告警是最招人恨的），
// 也**不该降级**（降级意味着「原通道整体不可用」，而它其实好好的）。
// 所以需要这第三种结果，而不是硬塞进「成功 / 失败」二选一。
type PartialError struct {
	Channel  string
	Rejected []string
	Accepted int
}

func (e *PartialError) Error() string {
	return fmt.Sprintf("%s: %d 个收件人被接受，%d 个被拒: %v",
		e.Channel, e.Accepted, len(e.Rejected), e.Rejected)
}

// IsPartial 报告错误是否属于部分投递。
func IsPartial(err error) bool {
	var pe *PartialError
	return errors.As(err, &pe)
}
