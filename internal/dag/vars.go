package dag

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/SNCIC/odoo20iot/internal/rules"
)

// varRefPattern 匹配唯一的合法变量引用形式：`$path.to.value`。
var varRefPattern = regexp.MustCompile(`^\$[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z0-9_]+)*$`)

// ValidateVarRef 校验一个参数值是否是合法的变量引用。
//
// 规则（04 §1.3 / §1.6）：**仅允许 `$path.to.value`，禁止表达式拼接**。
// 这条约束是安全边界的一部分：一旦允许拼接，params 就变成第二个表达式引擎，
// 绕过 L1 的白名单与类型校验。
//
// ⚠️ **实现决策**：以 `$` 开头的字符串**一律视为变量引用**，因此字面量 `"$5"`
// 会被拒绝。需要一个以 `$` 开头的字面量时，请改写数据形态（例如把金额放进
// 结构化字段），而不是给解析器加转义 —— 少一层语法就少一处歧义。
// 不以 `$` 开头的字符串是普通字面量，直接放行。
func ValidateVarRef(s string) error {
	if s == "" || !strings.HasPrefix(s, "$") {
		return nil
	}
	if !varRefPattern.MatchString(s) {
		return fmt.Errorf("只允许 $path.to.value 形式的变量引用（禁止表达式拼接），得到 %q", s)
	}
	return nil
}

// ResolveVar 从 env 里取出 `$path.to.value` 的值。
//
// 取不到值**返回错误而不是零值**：把「变量名写错」静默变成 `nil`，
// 会让动作收到一个看似合法但完全错误的参数，比直接失败难排查得多。
func ResolveVar(ref string, env rules.Env) (any, error) {
	path := strings.Split(strings.TrimPrefix(ref, "$"), ".")
	if len(path) == 0 || path[0] == "" {
		return nil, fmt.Errorf("变量引用 %q 缺少路径", ref)
	}

	var cur any = map[string]any(env)
	for i, seg := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("变量 %s 的第 %d 段 %q 无法定位（上一级不是对象）", ref, i+1, seg)
		}
		v, ok := m[seg]
		if !ok {
			return nil, fmt.Errorf("变量 %s 不存在：缺少 %q", ref, strings.Join(path[:i+1], "."))
		}
		cur = v
	}
	return cur, nil
}

// ResolveParams 把参数树里的变量引用替换为 env 里的实际值。
//
// 返回的是**深拷贝**：动作实现若改动参数，不该回写进编译好的图 ——
// 否则同一张图在多次执行之间会互相污染（规则/场景都是长期驻留的对象）。
func ResolveParams(params map[string]any, env rules.Env) (map[string]any, error) {
	if len(params) == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(params))
	for k, v := range params {
		rv, err := resolveValue(v, env)
		if err != nil {
			return nil, fmt.Errorf("参数 %s: %w", k, err)
		}
		out[k] = rv
	}
	return out, nil
}

func resolveValue(v any, env rules.Env) (any, error) {
	switch t := v.(type) {
	case string:
		// 只在它确实是变量引用时才解析（ValidateVarRef 已在编译期保证语法）。
		if strings.HasPrefix(t, "$") {
			return ResolveVar(t, env)
		}
		return t, nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, sub := range t {
			rv, err := resolveValue(sub, env)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			out[k] = rv
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, sub := range t {
			rv, err := resolveValue(sub, env)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			out[i] = rv
		}
		return out, nil
	default:
		return v, nil
	}
}
