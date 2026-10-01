// Package rules 实现 04 §1.2 的 L1 条件 DSL（规则表达式）的编译与求值。
//
// # 为什么要在 expr 之上再写一层校验（C1 的核心结论）
//
// 04 §1.2 原本的设计是「用 `expr.Env(RuleEnv{})` 的类型化环境做编译期校验」。
// 实测（见 §1.2.1）表明**这条路走不通**：
//
//   - expr 的 Env 类型检查依赖 Go 的静态类型，字段名**大小写敏感**；
//   - 物模型的指标键是小写字符串（`temperature`），而 Go 的结构体字段必须导出
//     （`Temperature`）——`reflect.StructOf` 直接拒绝小写字段名；
//   - 把 env 换成 `map[string]any` 后，expr **完全不做字段与类型校验**：
//     `msg.typo > 60`、`msg.temperature > 'abc'`、`msg.running && msg.temperature`
//     全部编译通过。
//
// 因此编译期校验必须**由我们自己基于 AST + 物模型类型表实现**（gate.go），
// expr 只负责解析、编译成字节码与求值。这不是重复劳动：我们的校验器能给出
// 「是否想用 msg.temperature」这类建议，而库的类型错误做不到。
package rules

import (
	"fmt"
	"strings"
)

// Kind 是 DSL 可见的取值类型。
type Kind uint8

const (
	KindUnknown Kind = iota
	KindNumber
	KindBool
	KindString
	KindTime
	KindList
	// KindAny 表示运行时才确定（如 state 的取值）。与任何标量比较都放行，
	// 但**不参与** && / || 之外的宽松推断，避免把错误掩盖过去。
	KindAny
)

func (k Kind) String() string {
	switch k {
	case KindNumber:
		return "数值"
	case KindBool:
		return "布尔"
	case KindString:
		return "字符串"
	case KindTime:
		return "时间"
	case KindList:
		return "列表"
	case KindAny:
		return "动态值"
	default:
		return "未知"
	}
}

// DeviceSchema 描述「这条规则能引用哪些字段、各是什么类型」。
//
// 它是编译期校验的唯一依据，由物模型 + 规则配置构造，**不接受用户直接提交**。
type DeviceSchema struct {
	// Metrics 是物模型指标：小写键 → 类型。只允许 Number / Bool / String。
	Metrics map[string]Kind

	// TagKeys 是已知的标签键。为空表示不校验标签键
	//（标签由租户自定义，强制枚举会把合法规则挡在门外）。
	TagKeys []string

	// WindowAggs 是**本规则**声明的窗口聚合键（如 avg / max）。
	// 未声明窗口的规则引用 window.* 会被编译器拒绝 —— 这正是
	// 04 §1.5 里 window 配置与表达式必须一致的要求。
	WindowAggs []string

	// OptionalMetrics 是「设备可能不上报」的指标。
	//
	// 引用可选指标时**必须**给出兜底值（`(msg.x ?? 0) > 60`），否则编译期报错。
	// 理由：缺失指标在运行时是 nil，`msg.x > 60` 会直接求值失败，
	// 整条规则按 error_policy 被丢弃 —— 这类故障在线上极难定位，
	// 不如在保存规则时就挡住。必填指标不需要出现在本表里。
	OptionalMetrics map[string]bool
}

// Object 是表达式里可访问的四个命名空间。
type Object uint8

const (
	ObjMsg Object = iota
	ObjMeta
	ObjPrev
	ObjWindow
	ObjState
)

func (o Object) String() string {
	return [...]string{"msg", "meta", "prev", "window", "state"}[o]
}

// MetaField 是 meta 命名空间的静态字段表（04 §1.2「可用变量」）。
var metaFields = map[string]Kind{
	"device_id":      KindNumber,
	"device_type_id": KindNumber,
	"project_id":     KindNumber,
	"company_id":     KindNumber,
	"group":          KindString,
	"groups":         KindList,
	"tags":           KindAny, // map[string]string，键动态
	"ts":             KindTime,
	"seq":            KindNumber,
}

// windowFields 是窗口聚合键的封闭白名单（键的**启用**由 DeviceSchema.WindowAggs 决定）。
var windowFields = map[string]Kind{
	"avg":   KindNumber,
	"min":   KindNumber,
	"max":   KindNumber,
	"sum":   KindNumber,
	"count": KindNumber,
	"last":  KindNumber,
	"first": KindNumber,
}

var objectNames = map[string]Object{
	"msg":    ObjMsg,
	"meta":   ObjMeta,
	"prev":   ObjPrev,
	"window": ObjWindow,
	"state":  ObjState,
}

// suggestion 在一组候选名里找最接近的一个，用于「是否想用 X？」。
//
// 用 Levenshtein 距离，阈值取「候选长度的 1/3 或 2，取大者」——
// 阈值太松会给出离谱建议，太紧则在拼错两个字母时沉默。
func suggestion(typo string, candidates []string) string {
	best, bestDist := "", -1
	prefix := ""

	for _, c := range candidates {
		d := levenshtein(typo, c)
		if bestDist < 0 || d < bestDist {
			best, bestDist = c, d
		}
		// 前缀缩写是最常见的拼法（`temp` → `temperature`），
		// 但它与完整词的编辑距离很大，单靠距离阈值会被漏掉。
		if strings.HasPrefix(c, typo) && (prefix == "" || len(c) < len(prefix)) {
			prefix = c
		}
	}

	if prefix != "" {
		return prefix
	}

	limit := len(best) / 3
	if limit < 2 {
		limit = 2
	}
	if best == "" || bestDist > limit {
		return ""
	}
	return best
}

func levenshtein(a, b string) int {
	ar, br := []rune(a), []rune(b)
	prev := make([]int, len(br)+1)
	cur := make([]int, len(br)+1)

	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ar); i++ {
		cur[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, min(cur[j-1]+1, prev[j-1]+cost))
		}
		prev, cur = cur, prev
	}
	return prev[len(br)]
}

// hintSuffix 把建议拼成统一的错误后缀。
func hintSuffix(candidate string) string {
	if candidate == "" {
		return ""
	}
	return fmt.Sprintf("，是否想用 %q？", candidate)
}
