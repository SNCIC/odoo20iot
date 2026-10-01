package natsjs

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// StreamSpec 描述要保证存在的 JetStream Stream。
//
// 用结构体而非位置参数：字段多且有几个同为整型/时长，位置参数很容易写反，
// 而且每加一个字段就要改一遍全部调用点。
//
// **零值表示「不约束」**：`MaxAge<=0` / `MaxMsgsPerSubject<=0` / `Discard==0`
// 都表示「不检查、也不改」，只会影响新建时的取值。要显式表达「无限保留」
// 或「DiscardOld」，就不传该字段 —— 因为对一个**既存**流来说，
// 「没设」与「部署侧另有安排」在服务端看起来是一样的，服务不该替部署侧做决定。
type StreamSpec struct {
	// Name 是 Stream 名。
	Name string
	// Subjects 是 Stream 捕获的 subject（可含通配）。
	Subjects []string
	// Replicas 是副本数。<=0 取 1；**既存流的副本数不会被改**（属部署期按 RPO 决定）。
	Replicas int
	// MaxAge 是消息保留时长；<=0 不约束。
	MaxAge time.Duration
	// MaxMsgsPerSubject 是单 subject 的消息上限（防洪）；<=0 不约束。
	MaxMsgsPerSubject int64
	// Discard 是达到上限时的丢弃策略；0（DiscardOld）不约束。
	// 要「防洪时丢新不丢旧」需显式传 nats.DiscardNew。
	Discard nats.DiscardPolicy
	// StrictSubjects=true 时，既存流的 subjects 不覆盖 Subjects 就**报错**，
	// 不自动改订阅范围 —— 改 subjects 会影响其他消费者，属部署动作。
	// false 时按需校正 subjects（适用于「本服务独占这个流」的场景）。
	StrictSubjects bool
}

// EnsureStream 幂等地保证 Stream 存在，并把**保留口径**校正到 spec。
//
// 存在的理由：早期各服务都写「不存在才创建」，于是**先于口径代码建出来的流**
// 永远停在旧配置 —— 典型后果是 `MaxAge=0`（无限保留）：磁盘随写入无上限增长，
// 且是「重启即重放」的放大器（保留窗口越长，一次误删消费者的代价越大）。
//
// ⚠️ 校正 `MaxAge` 会**立即删除超期消息**，且可能缩短其他消费者的可重放窗口，
// 因此每次校正都打一条 WARN 留痕（策略：自动校正 + 告警，而不是静默改）。
func EnsureStream(js nats.JetStreamContext, spec StreamSpec) error {
	if spec.Name == "" || len(spec.Subjects) == 0 {
		return fmt.Errorf("natsjs: stream 名与 subjects 都不能为空")
	}
	replicas := spec.Replicas
	if replicas <= 0 {
		replicas = 1
	}

	info, err := js.StreamInfo(spec.Name)
	switch {
	case err == nil:
		return reconcileStream(js, info, spec)
	case errors.Is(err, nats.ErrStreamNotFound):
	default:
		return fmt.Errorf("natsjs: 查询 stream %s: %w", spec.Name, err)
	}

	cfg := &nats.StreamConfig{
		Name:      spec.Name,
		Subjects:  spec.Subjects,
		Replicas:  replicas,
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   spec.Discard,
		MaxAge:    spec.MaxAge,
	}
	if spec.MaxMsgsPerSubject > 0 {
		cfg.MaxMsgsPerSubject = spec.MaxMsgsPerSubject
	}
	if _, err := js.AddStream(cfg); err != nil {
		return fmt.Errorf("natsjs: 创建 stream %s: %w", spec.Name, err)
	}
	return nil
}

// reconcileStream 把既存 Stream 校正到期望口径；已一致则什么都不做。
//
// 在 `info.Config` 上原地改而不是整体替换 —— 否则会把部署期设的其它字段
// （副本数、去重窗口、存储后端等）一并抹回本函数的默认值。
func reconcileStream(js nats.JetStreamContext, info *nats.StreamInfo, spec StreamSpec) error {
	cur := info.Config
	changes := make([]any, 0, 6)

	if !spec.StrictSubjects && !sameStringSet(cur.Subjects, spec.Subjects) {
		changes = append(changes, "subjects", fmt.Sprint(cur.Subjects), "→", fmt.Sprint(spec.Subjects))
		cur.Subjects = spec.Subjects
	}
	if spec.StrictSubjects && !coversSubjects(cur.Subjects, spec.Subjects) {
		return fmt.Errorf("natsjs: stream %s 未覆盖 subject %v（现有 %v）："+
			"改订阅范围属部署动作，本服务不自动改",
			spec.Name, spec.Subjects, cur.Subjects)
	}
	if spec.MaxAge > 0 && cur.MaxAge != spec.MaxAge {
		changes = append(changes, "max_age", cur.MaxAge, "→", spec.MaxAge)
		cur.MaxAge = spec.MaxAge
	}
	if spec.MaxMsgsPerSubject > 0 && cur.MaxMsgsPerSubject != spec.MaxMsgsPerSubject {
		changes = append(changes, "max_msgs_per_subject", cur.MaxMsgsPerSubject, "→", spec.MaxMsgsPerSubject)
		cur.MaxMsgsPerSubject = spec.MaxMsgsPerSubject
	}
	if spec.Discard != 0 && cur.Discard != spec.Discard {
		changes = append(changes, "discard", cur.Discard, "→", spec.Discard)
		cur.Discard = spec.Discard
	}

	if len(changes) == 0 {
		return nil
	}

	attrs := append([]any{"stream", spec.Name, "note", "缩短 max_age 会立即删除超期消息"}, changes...)
	slog.Warn("Stream 配置与期望口径不一致，正在校正", attrs...)
	if _, err := js.UpdateStream(&cur); err != nil {
		return fmt.Errorf("natsjs: 校正 stream %s 配置: %w", spec.Name, err)
	}
	return nil
}

// coversSubjects 判断既存 subjects 是否覆盖了全部期望值。
//
// 覆盖判定与 03 的坑一致：`foo.>` 覆盖 `foo.bar`，但**不覆盖 `foo` 本身**
// （`>` 至少匹配一层），故不能用简单前缀比较。
func coversSubjects(have, want []string) bool {
	for _, w := range want {
		covered := false
		for _, h := range have {
			if subjectCovers(h, w) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

// subjectCovers 判断 h 是否覆盖 w：完全相同，或 h 是可以覆盖 w 的通配。
func subjectCovers(h, w string) bool {
	if h == w {
		return true
	}
	if !strings.HasSuffix(h, ">") {
		return false
	}
	prefix := strings.TrimSuffix(h, ">") // 保留末尾的点，故 `foo.>` 只覆盖 `foo.` 开头的
	return strings.HasPrefix(w, prefix) && len(w) > len(prefix)
}

// sameStringSet 判断两个字符串集合是否相等（顺序无关、忽略重复）。
//
// 用集合而不是切片比较：NATS 返回 subjects 的顺序不保证与写入一致，
// 直接按位比较会把「内容相同、顺序不同」误判成漂移，每次启动都重建一次配置。
func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	if len(set) != len(a) {
		return false
	}
	for _, s := range b {
		if _, ok := set[s]; !ok {
			return false
		}
	}
	return true
}
