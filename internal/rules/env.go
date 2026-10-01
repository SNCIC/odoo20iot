package rules

import "time"

// 命名空间常量：与 04 §1.2 的变量表一致，全小写。
const (
	nsMsg    = "msg"
	nsMeta   = "meta"
	nsPrev   = "prev"
	nsWindow = "window"
	nsState  = "state"
)

// Env 是规则表达式的求值环境：5 个小写命名空间 → 各自的取值映射。
//
// **它必须是 map，不能是 Go 结构体** —— 这是 C1 实测出来的硬约束：
// expr 的成员访问大小写敏感，而 DSL 的命名空间（`msg`）与物模型字段
// （`meta.device_id`）都是小写；Go 结构体字段必须导出（`Msg`），
// 用结构体做环境会让 `msg.temperature` 直接报 `unknown name msg`。
// 详见 04 §1.2.1。
//
// 热路径可以复用同一个 Env（见 Bind），从而做到「每条消息一次分配」，
// 而不是「每条规则一次分配」。
type Env map[string]any

// NewEnv 构造一个已预置五个命名空间键的环境。
//
// 五个键一律给空映射（**包括 prev**）：expr 的类型检查会从环境取值反推类型，
// 若模板里把 prev 写成 nil，检查器会把 prev 收窄成 `nil` 类型，
// 于是 `prev.temperature` 报 `type nil has no field temperature`。
// 「prev 可能为空」是运行时事实，由 Bind 传 nil 表达，编译期一律按映射看待。
func NewEnv() Env {
	return Env{
		nsMsg:    map[string]any{},
		nsMeta:   map[string]any{},
		nsPrev:   map[string]any{},
		nsWindow: map[string]any{},
		nsState:  map[string]any{},
	}
}

// Bind 就地写入五个命名空间并返回自身。
//
// 同一条消息通常会命中多条规则，复用同一个 Env 即可把环境构造的开销
// 摊到「每条消息一次」。
func (e Env) Bind(msg, meta, prev, window, state map[string]any) Env {
	e[nsMsg], e[nsMeta], e[nsPrev], e[nsWindow], e[nsState] = msg, meta, prev, window, state
	return e
}

// NewMeta 构造 meta 命名空间。键名与变量表一致（小写 + 下划线）。
func NewMeta(deviceID, deviceTypeID, projectID, companyID int64, group string, groups []any, tags map[string]any, ts time.Time, seq int64) map[string]any {
	return map[string]any{
		"device_id":      deviceID,
		"device_type_id": deviceTypeID,
		"project_id":     projectID,
		"company_id":     companyID,
		"group":          group,
		"groups":         groups,
		"tags":           tags,
		"ts":             ts,
		"seq":            seq,
	}
}

// Eval 在给定环境上求值。
//
// 返回值语义（04 §1.5 的 error_policy 据此分派）：
//   - (true/false, nil) ⇒ 正常判定；
//   - (_, err)          ⇒ **求值失败**：例如设备未上报被引用的指标（nil 参与比较）、
//     白名单函数参数非法。调用方**不得**把失败当成 false ——
//     两者在「是否触发动作」上的后果完全相反。
func (p *Program) Eval(env Env) (bool, error) {
	return asBool(p.run(env))
}

// Runner.Eval 与 Program.Eval 的唯一差别是 VM 的来源，因此共用同一套
// 结果判定逻辑，避免两条路径的语义分叉。
func asBool(out any, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	b, ok := out.(bool)
	if !ok {
		return false, errUnexpectedResult{got: out}
	}
	return b, nil
}

type errUnexpectedResult struct{ got any }

func (e errUnexpectedResult) Error() string {
	return "规则求值结果不是布尔值（编译期类型检查与运行时不一致）"
}
