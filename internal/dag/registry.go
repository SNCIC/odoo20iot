package dag

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Action 是一个可执行的编排动作。
//
// 执行体必须是**编译进二进制的 Go 函数**（04 §1.3）：平台不执行用户提交的
// 编排代码。这是安全边界的根本所在 ——「新增动作 = 写 Go 代码 + 注册 + 上线」，
// 而不是让用户在平台上写脚本。
type Action interface {
	// Name 是注册名（04 §1.3 的动作表：alarm.raise / command.send / ...）。
	Name() string
	// Do 执行动作。params 里的变量引用已被解析为字面值。
	Do(ctx context.Context, params map[string]any) error
	// Idempotent 声明该动作是否幂等。
	//
	// 重试策略要读它：**对不幂等的动作重试等于制造重复副作用**（04 §1.3 的动作表
	// 逐项标了幂等性）。本包不因此拒绝重试配置 —— 约束表里没有这一条 ——
	// 但在重试不幂等动作时会告警。
	Idempotent() bool
}

// Registry 是动作白名单（04 §1.3）。
//
// 非注册表内的 `action` 在**保存阶段**即被拒绝（04 §1.6），
// 而不是等执行时才发现「这个动作不存在」。
type Registry struct {
	actions map[string]Action
}

// NewRegistry 构造注册表。重复名、空名、nil 动作都会被拒绝 ——
// 它们都会让白名单失去意义（后注册者静默覆盖先注册者）。
func NewRegistry(actions ...Action) (*Registry, error) {
	r := &Registry{actions: make(map[string]Action, len(actions))}
	for _, a := range actions {
		if a == nil {
			return nil, fmt.Errorf("dag: 注册表不接受 nil 动作")
		}
		name := strings.TrimSpace(a.Name())
		if name == "" {
			return nil, fmt.Errorf("dag: 动作名不能为空")
		}
		if _, dup := r.actions[name]; dup {
			return nil, fmt.Errorf("dag: 动作 %q 重复注册", name)
		}
		r.actions[name] = a
	}
	return r, nil
}

// Get 查找动作。
func (r *Registry) Get(name string) (Action, bool) {
	if r == nil {
		return nil, false
	}
	a, ok := r.actions[name]
	return a, ok
}

// Has 判定动作是否在白名单内。
func (r *Registry) Has(name string) bool {
	_, ok := r.Get(name)
	return ok
}

// Names 返回全部动作名（字典序，便于错误信息稳定可读）。
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.actions))
	for name := range r.actions {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Func 把普通函数适配成 Action，便于注册表构造与测试。
type Func struct {
	ActionName string
	Run        func(ctx context.Context, params map[string]any) error
	// Idem 声明幂等性。
	Idem bool
}

var _ Action = Func{}

// Name 实现 Action。
func (f Func) Name() string { return f.ActionName }

// Do 实现 Action。
func (f Func) Do(ctx context.Context, params map[string]any) error {
	if f.Run == nil {
		return fmt.Errorf("dag: 动作 %s 未实现", f.ActionName)
	}
	return f.Run(ctx, params)
}

// Idempotent 实现 Action。
func (f Func) Idempotent() bool { return f.Idem }
