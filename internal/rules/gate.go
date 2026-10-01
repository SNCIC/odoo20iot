package rules

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/file"
)

// maxRegexLen 是 04 §1.2 对正则字面量的长度上限（配合 Go 原生 RE2 无回溯爆炸的特性）。
const maxRegexLen = 512

// Issue 是一条编译期校验问题，Loc 是源文本中的字节区间。
type Issue struct {
	Message string
	Loc     file.Location
}

// CompileError 汇总一个表达式的全部校验问题。
//
// 一次性返回**所有**问题而不是遇到第一个就停：用户在控制台改一条表达式时，
// 逐个报错的往返成本很高（04 §1.6 把「保存即校验」定位为最好的质检）。
type CompileError struct {
	Source string
	Issues []Issue
}

func (e *CompileError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "规则表达式校验失败（%d 处）", len(e.Issues))

	for _, is := range e.Issues {
		line, col := offsetToLineCol(e.Source, is.Loc.From)
		fmt.Fprintf(&b, "\n  第 %d 行第 %d 列：%s", line, col, is.Message)
		if snip, ok := snippet(e.Source, line); ok {
			fmt.Fprintf(&b, "\n      %s", snip)
			if pad := col - 1; pad > 0 && pad < 200 {
				fmt.Fprintf(&b, "\n      %s^", strings.Repeat(" ", pad))
			}
		}
	}
	return b.String()
}

func offsetToLineCol(src string, offset int) (line, col int) {
	if offset < 0 {
		offset = 0
	}
	if offset > len(src) {
		offset = len(src)
	}
	line = 1
	last := 0
	for i := 0; i < offset; i++ {
		if src[i] == '\n' {
			line++
			last = i + 1
		}
	}
	return line, offset - last + 1
}

func snippet(src string, line int) (string, bool) {
	lines := strings.Split(src, "\n")
	if line < 1 || line > len(lines) {
		return "", false
	}
	return strings.TrimRight(lines[line-1], "\r"), true
}

// gate 是静态校验器。
type gate struct {
	sch    *DeviceSchema
	src    string
	issues []Issue
}

func (g *gate) addf(loc file.Location, format string, args ...any) {
	g.issues = append(g.issues, Issue{Message: fmt.Sprintf(format, args...), Loc: loc})
}

// check 校验一棵已解析的表达式树。返回 nil 表示通过。
func check(src string, tree *ast.Node, sch *DeviceSchema) *CompileError {
	g := &gate{sch: sch, src: src}

	g.scanBanned(tree)
	g.scanRegex(tree)
	g.scanOptionalMetrics(tree)

	if kind := g.check(*tree, false); kind != KindBool && kind != KindAny {
		g.addf((*tree).Location(), "规则表达式必须返回布尔值，实际是%s", kind)
	}

	if len(g.issues) == 0 {
		return nil
	}
	sort.SliceStable(g.issues, func(i, j int) bool { return g.issues[i].Loc.From < g.issues[j].Loc.From })
	return &CompileError{Source: src, Issues: g.issues}
}

// ---------- 第一遍：禁用语法 ----------

// scanBanned 拦截 expr 里存在、但我们不允许的语法。
//
// 注意 PredicateNode：`all/filter/map/none/any/one` 在**解析阶段**就变成谓词节点，
// 绕过了 expr 的 Builtins 表 —— 实测 `expr.DisableAllBuiltins()` 拦不住它们
// （见 04 §1.2.1）。因此必须在 AST 层单独封禁。
func (g *gate) scanBanned(tree *ast.Node) {
	ast.Walk(tree, visitorFn(func(node *ast.Node) {
		switch n := (*node).(type) {
		case *ast.PredicateNode:
			g.addf(n.Location(), "谓词语法（all/filter/map/none/any/one）不在白名单内，请改用显式比较")

		case *ast.PointerNode:
			// `#` 只出现在谓词内部；这里兜底一次，避免谓词被封禁后仍有遗漏路径。
			g.addf(n.Location(), "`#` 谓词占位符不在白名单内")

		case *ast.VariableDeclaratorNode:
			// expr 本身支持 `let x = ...`，我们主动禁用（04 §1.2「禁用赋值」）：
			// 保留纯表达式形态才能稳定做静态分析，也避免规则复杂度失控。
			g.addf(n.Location(), "不允许使用 let 声明变量：条件 DSL 只支持纯表达式")

		case *ast.BuiltinNode:
			if _, ok := whitelist[n.Name]; !ok {
				g.addf(n.Location(), "函数 %s 不在内置函数白名单内", n.Name)
			}

		case *ast.MemberNode:
			if n.Method {
				g.addf(n.Location(), "不允许调用成员方法")
			}
		}
	}))
}

// scanRegex 校验正则字面量长度（04 §1.2：正则安全）。
//
// `matches` 在 expr 里是**中缀运算符**，不是函数（`matches(a,b)` 编译不过）。
// 右操作数必须是字面量时才可静态判定长度；动态拼接的正则直接拒绝。
func (g *gate) scanRegex(tree *ast.Node) {
	ast.Walk(tree, visitorFn(func(node *ast.Node) {
		n, ok := (*node).(*ast.BinaryNode)
		if !ok || (n.Operator != "matches" && n.Operator != "not matches") {
			return
		}
		lit, ok := n.Right.(*ast.StringNode)
		if !ok {
			g.addf(n.Right.Location(), "matches 的右侧必须是正则**字面量**（不允许拼接，否则无法做静态长度校验）")
			return
		}
		if len(lit.Value) > maxRegexLen {
			g.addf(lit.Location(), "正则字面量长度 %d 超过上限 %d", len(lit.Value), maxRegexLen)
		}
	}))
}

// scanOptionalMetrics 要求对「可能缺失」的指标显式给出兜底值。
//
// 这是运行时语义倒逼出来的编译期规则：设备未上报某指标时，`msg.x` 求值为 nil，
// `nil > 60` 会让**整条规则**求值失败并按 error_policy 丢弃。这类故障在线上
// 表现为「规则偶尔不生效」，极难定位。判定「是否已经兜底」只看该成员访问
// 是否直接位于 `??` 的左侧 —— 保守但不会放过漏网情况。
func (g *gate) scanOptionalMetrics(tree *ast.Node) {
	if len(g.sch.OptionalMetrics) == 0 {
		return
	}

	coalesced := map[ast.Node]bool{}
	ast.Walk(tree, visitorFn(func(node *ast.Node) {
		if b, ok := (*node).(*ast.BinaryNode); ok && b.Operator == "??" {
			coalesced[b.Left] = true
		}
	}))

	ast.Walk(tree, visitorFn(func(node *ast.Node) {
		m, ok := (*node).(*ast.MemberNode)
		if !ok {
			return
		}
		root, props, dyn, id := memberChain(m)
		if dyn || id == nil || len(props) != 1 {
			return
		}
		if root != "msg" && root != "prev" {
			return
		}
		if !g.sch.OptionalMetrics[props[0]] || coalesced[ast.Node(m)] {
			return
		}
		g.addf(m.Location(),
			"指标 %q 可能缺失，必须先给兜底值：写成 `(%s.%s ?? 0) ...`，否则缺失时整条规则会求值失败",
			props[0], root, props[0])
	}))
}

type visitorFn func(*ast.Node)

func (f visitorFn) Visit(n *ast.Node) { f(n) }

// ---------- 第二遍：类型与字段校验 ----------

// check 递归推导节点类型，同时校验字段名、函数调用与运算符类型。
//
// prevGuarded 表示当前位置是否处于「prev 已判空」的短路分支内。
func (g *gate) check(n ast.Node, prevGuarded bool) Kind {
	switch v := n.(type) {
	case nil:
		return KindUnknown

	case *ast.NilNode:
		return KindUnknown

	case *ast.IntegerNode, *ast.FloatNode:
		return KindNumber

	case *ast.BoolNode:
		return KindBool

	case *ast.StringNode:
		return KindString

	case *ast.IdentifierNode:
		return g.checkIdentifier(v)

	case *ast.MemberNode:
		return g.checkMember(v, prevGuarded)

	case *ast.CallNode:
		return g.checkCall(v, prevGuarded)

	case *ast.BinaryNode:
		return g.checkBinary(v, prevGuarded)

	case *ast.UnaryNode:
		k := g.check(v.Node, prevGuarded)
		switch v.Operator {
		case "!", "not":
			if k != KindBool && k != KindAny {
				g.addf(v.Location(), "运算符 %s 需要布尔操作数，实际是%s", v.Operator, k)
			}
			return KindBool
		case "-", "+":
			if k != KindNumber && k != KindAny {
				g.addf(v.Location(), "运算符 %s 需要数值操作数，实际是%s", v.Operator, k)
			}
			return KindNumber
		default:
			g.addf(v.Location(), "不支持的运算符 %s", v.Operator)
			return KindUnknown
		}

	case *ast.ConditionalNode:
		if ck := g.check(v.Cond, prevGuarded); ck != KindBool && ck != KindAny {
			g.addf(v.Cond.Location(), "三元表达式的条件必须是布尔值，实际是%s", ck)
		}
		// 三元是 prev 判空的另一个合法短路形式：
		//   prev == nil ? false : prev.temperature > 5
		op, ok := prevNilTest(v.Cond)
		lk := g.check(v.Exp1, prevGuarded || (ok && op == "!="))
		rk := g.check(v.Exp2, prevGuarded || (ok && op == "=="))
		return mergeKinds(lk, rk)

	case *ast.ArrayNode:
		var elem Kind
		for _, item := range v.Nodes {
			k := g.check(item, prevGuarded)
			elem = mergeKinds(elem, k)
		}
		return KindList

	case *ast.MapNode:
		for _, p := range v.Pairs {
			if pair, ok := p.(*ast.PairNode); ok {
				g.check(pair.Key, prevGuarded)
				g.check(pair.Value, prevGuarded)
			}
		}
		return KindAny

	case *ast.SliceNode:
		g.check(v.Node, prevGuarded)
		return KindList

	case *ast.BuiltinNode:
		// 与 expr 内置同名的白名单函数（len/int/...）在解析阶段就是 BuiltinNode，
		// 必须同样走签名校验，否则参数类型会漏过去（例如 len(msg.temperature)）。
		spec, ok := whitelist[v.Name]
		if !ok {
			return KindAny // scanBanned 已报错
		}
		g.checkArgs(v.Name, spec, v.Arguments, v.Location(), prevGuarded)
		return spec.ret

	case *ast.PredicateNode, *ast.PointerNode, *ast.VariableDeclaratorNode:
		return KindAny // 已在 scanBanned 报错

	default:
		g.addf(n.Location(), "不支持的表达式（%T）", n)
		return KindUnknown
	}
}

func (g *gate) checkIdentifier(n *ast.IdentifierNode) Kind {
	if _, ok := objectNames[n.Value]; ok {
		return KindAny
	}
	if _, ok := whitelist[n.Value]; ok {
		g.addf(n.Location(), "函数 %s 必须被调用（写成 %s(...)）", n.Value, n.Value)
		return KindUnknown
	}
	g.addf(n.Location(), "未知的变量 %q%s", n.Value, hintSuffix(suggestion(n.Value, g.rootNames())))
	return KindUnknown
}

func (g *gate) rootNames() []string {
	out := make([]string, 0, len(objectNames))
	for k := range objectNames {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// memberChain 把 `msg.a.b` 拆成根标识符 + 属性路径。
// ok=false 表示属性不是字面量（动态下标），由调用方决定是否放行。
func memberChain(n *ast.MemberNode) (root string, props []string, dyn bool, id *ast.IdentifierNode) {
	var rev []string
	var cur ast.Node = n

	for {
		m, ok := cur.(*ast.MemberNode)
		if !ok {
			break
		}
		switch p := m.Property.(type) {
		case *ast.StringNode:
			rev = append(rev, p.Value)
		case *ast.IntegerNode:
			// 数组下标：如 wgs84_to_gcj02(...)[0]；或用整数键访问 map。
			rev = append(rev, strconv.Itoa(p.Value))
		default:
			dyn = true
		}
		cur = m.Node
	}

	id, _ = cur.(*ast.IdentifierNode)
	if id == nil {
		return "", nil, true, nil
	}

	for i := len(rev) - 1; i >= 0; i-- {
		props = append(props, rev[i])
	}
	return id.Value, props, dyn, id
}

func (g *gate) checkMember(n *ast.MemberNode, prevGuarded bool) Kind {
	root, props, dyn, id := memberChain(n)

	if id == nil {
		// 形如 f(x).y —— 只允许函数返回列表再取下标（如 wgs84_to_gcj02(...)[0]）。
		if p, ok := n.Property.(*ast.IntegerNode); ok {
			_, _ = p, ok
			g.check(n.Node, prevGuarded)
			return KindNumber
		}
		g.addf(n.Location(), "不支持在函数返回值上做成员访问")
		return KindUnknown
	}

	obj, known := objectNames[root]
	if !known {
		g.addf(id.Location(), "未知的变量 %q%s", root, hintSuffix(suggestion(root, g.rootNames())))
		return KindUnknown
	}

	if dyn {
		// 动态下标：只有 state / meta.tags 这类「键本身是数据」的映射才允许。
		switch obj {
		case ObjState:
			return KindAny
		case ObjMeta:
			if len(props) == 1 && props[0] == "tags" {
				return KindString
			}
		}
		g.addf(n.Location(), "%s 不支持动态下标访问（指标名必须是字面量，才能做静态校验）", root)
		return KindUnknown
	}

	if len(props) == 0 {
		return KindAny
	}

	switch obj {
	case ObjMsg, ObjPrev:
		if obj == ObjPrev && !prevGuarded {
			g.addf(n.Location(),
				"引用 prev.%s 前必须先判空：写成 `prev != nil && prev.%s ...`（首条消息下 prev 为空，取值会运行时报错）",
				props[0], props[0])
		}
		if len(props) > 1 {
			g.addf(n.Location(), "%s.%s 是标量，不能再取成员 .%s", root, props[0], props[1])
			return KindUnknown
		}
		k, ok := g.sch.Metrics[props[0]]
		if !ok {
			g.addf(n.Location(), "物模型里没有指标 %q%s", props[0], hintSuffix(suggestion(props[0], metricNames(g.sch))))
			return KindUnknown
		}
		return k

	case ObjMeta:
		k, ok := metaFields[props[0]]
		if !ok {
			g.addf(n.Location(), "meta 没有字段 %q%s", props[0], hintSuffix(suggestion(props[0], mapKeys(metaFields))))
			return KindUnknown
		}
		if props[0] == "tags" && len(props) == 2 && len(g.sch.TagKeys) > 0 {
			if !contains(g.sch.TagKeys, props[1]) {
				g.addf(n.Location(), "标签 %q 未在设备类型中定义%s", props[1], hintSuffix(suggestion(props[1], g.sch.TagKeys)))
				return KindUnknown
			}
			return KindString
		}
		if len(props) > 1 {
			g.addf(n.Location(), "meta.%s 不能再取成员 .%s", props[0], props[1])
			return KindUnknown
		}
		return k

	case ObjWindow:
		k, ok := windowFields[props[0]]
		if !ok {
			g.addf(n.Location(), "窗口没有聚合 %q%s", props[0], hintSuffix(suggestion(props[0], mapKeys(windowFields))))
			return KindUnknown
		}
		if !contains(g.sch.WindowAggs, props[0]) {
			g.addf(n.Location(), "本规则未声明聚合 %q 的窗口（当前已声明：%s）", props[0], strings.Join(g.sch.WindowAggs, ", "))
			return KindUnknown
		}
		return k

	case ObjState:
		return KindAny
	}

	return KindUnknown
}

func (g *gate) checkCall(n *ast.CallNode, prevGuarded bool) Kind {
	id, ok := n.Callee.(*ast.IdentifierNode)
	if !ok {
		if root, props, _, _ := memberChainOf(n.Callee); root != "" && len(props) > 0 {
			if obj, known := objectNames[root]; known && obj == ObjState {
				// 04 §1.2 的变量表里写的是 `state.get(key)`；实测该写法在 expr 下
				// 不可用（Go 的方法名必须导出，`state.get` 解析不到）。
				// 这里不照抄错误写法，直接给出可用形式。
				g.addf(n.Location(), "state 是映射，不支持 state.get(...)：请写成 state.%s 或 state[%q]", props[0], props[0])
				return KindAny
			}
		}
		g.addf(n.Location(), "只允许调用白名单内的具名函数")
		return KindUnknown
	}

	spec, ok := whitelist[id.Value]
	if !ok {
		g.addf(id.Location(), "函数 %q 不在内置函数白名单内%s", id.Value, hintSuffix(suggestion(id.Value, mapKeys(whitelist))))
		for _, a := range n.Arguments {
			g.check(a, prevGuarded)
		}
		return KindUnknown
	}

	g.checkArgs(id.Value, spec, n.Arguments, n.Location(), prevGuarded)
	return spec.ret
}

// checkArgs 校验参数个数与类型（CallNode 与 BuiltinNode 共用）。
//
// loc 是调用点位置：参数为空时用调用点，否则用第一个参数，避免出现「第 0 列」。
func (g *gate) checkArgs(name string, spec funcSpec, args []ast.Node, callLoc file.Location, prevGuarded bool) {
	if len(args) < spec.minArgs || (spec.maxArgs >= 0 && len(args) > spec.maxArgs) {
		loc := callLoc
		if len(args) > 0 {
			loc = args[0].Location()
		}
		g.addf(loc, "%s 的参数个数不对：期望 %s，实际 %d", name, arityText(spec), len(args))
	}

	for i, a := range args {
		k := g.check(a, prevGuarded)
		if want, ok := argKind(spec, i); ok {
			assertAssignable(g, a.Location(), name, i+1, want, k)
		}
	}
}

// memberChainOf 是 memberChain 的宽松版本：解析失败时返回空 root。
func memberChainOf(n ast.Node) (string, []string, bool, *ast.IdentifierNode) {
	m, ok := n.(*ast.MemberNode)
	if !ok {
		return "", nil, false, nil
	}
	return memberChain(m)
}

func arityText(spec funcSpec) string {
	switch {
	case spec.maxArgs < 0:
		return fmt.Sprintf("至少 %d 个", spec.minArgs)
	case spec.minArgs == spec.maxArgs:
		return fmt.Sprintf("%d 个", spec.minArgs)
	default:
		return fmt.Sprintf("%d~%d 个", spec.minArgs, spec.maxArgs)
	}
}

func argKind(spec funcSpec, i int) (Kind, bool) {
	if i < len(spec.args) {
		return spec.args[i], true
	}
	if len(spec.args) > 0 && spec.maxArgs < 0 {
		return spec.args[len(spec.args)-1], true
	}
	return KindUnknown, false
}

func assertAssignable(g *gate, loc file.Location, fn string, pos int, want, got Kind) {
	if want == KindAny || got == KindAny || want == got {
		return
	}
	g.addf(loc, "%s 的第 %d 个参数需要%s，实际是%s", fn, pos, want, got)
}

func (g *gate) checkBinary(n *ast.BinaryNode, prevGuarded bool) Kind {
	// && / || 的短路保护：左侧判空则右侧的 prev.* 才安全。
	guardLeft, guardRight := prevGuarded, prevGuarded
	// 短路保护判定的是**左操作数**：`prev != nil && <这里才安全>`。
	if op, ok := prevNilTest(n.Left); ok {
		switch n.Operator {
		case "&&":
			if op == "!=" {
				guardRight = true
			}
		case "||":
			if op == "==" {
				guardRight = true
			}
		}
	}

	lk := g.check(n.Left, guardLeft)
	rk := g.check(n.Right, guardRight)

	switch n.Operator {
	case "&&", "||":
		assertBool(g, n.Left.Location(), n.Operator, lk)
		assertBool(g, n.Right.Location(), n.Operator, rk)
		return KindBool

	case "==", "!=":
		if !comparable(lk, rk) {
			g.addf(n.Location(), "运算符 %s 两侧类型不兼容（%s 与 %s）", n.Operator, lk, rk)
		}
		return KindBool

	case "<", "<=", ">", ">=":
		if lk != rk && lk != KindAny && rk != KindAny {
			g.addf(n.Location(), "运算符 %s 两侧类型必须一致（%s 与 %s）", n.Operator, lk, rk)
		} else if lk == KindBool {
			g.addf(n.Location(), "布尔值不能做大小比较（%s）", n.Operator)
		}
		return KindBool

	case "+", "-", "*", "/", "%", "**":
		assertNumber(g, n.Left.Location(), n.Operator, lk)
		assertNumber(g, n.Right.Location(), n.Operator, rk)
		return KindNumber

	case "in", "not in":
		if rk != KindList && rk != KindAny {
			g.addf(n.Right.Location(), "运算符 %s 右侧必须是列表或区间，实际是%s", n.Operator, rk)
		}
		return KindBool

	case "matches", "not matches":
		if lk != KindString && lk != KindAny {
			g.addf(n.Left.Location(), "matches 左侧必须是字符串，实际是%s", lk)
		}
		return KindBool

	case "??":
		return mergeKinds(lk, rk)

	case "..":
		assertNumber(g, n.Left.Location(), n.Operator, lk)
		assertNumber(g, n.Right.Location(), n.Operator, rk)
		return KindList

	default:
		g.addf(n.Location(), "不支持的运算符 %q", n.Operator)
		return KindUnknown
	}
}

func assertBool(g *gate, loc file.Location, op string, k Kind) {
	if k != KindBool && k != KindAny {
		g.addf(loc, "运算符 %s 需要布尔操作数，实际是%s", op, k)
	}
}

func assertNumber(g *gate, loc file.Location, op string, k Kind) {
	if k != KindNumber && k != KindAny {
		g.addf(loc, "运算符 %s 需要数值操作数，实际是%s", op, k)
	}
}

func comparable(a, b Kind) bool {
	return a == b || a == KindAny || b == KindAny
}

func mergeKinds(a, b Kind) Kind {
	switch {
	case a == KindUnknown:
		return b
	case b == KindUnknown:
		return a
	case a == b:
		return a
	case a == KindAny || b == KindAny:
		return KindAny
	default:
		return KindAny
	}
}

// prevNilTest 判断表达式是否是 `prev == nil` / `prev != nil` 形式，返回其运算符。
func prevNilTest(n ast.Node) (string, bool) {
	b, ok := n.(*ast.BinaryNode)
	if !ok || (b.Operator != "==" && b.Operator != "!=") {
		return "", false
	}
	if isRootIdent(b.Left, "prev") {
		if _, ok := b.Right.(*ast.NilNode); ok {
			return b.Operator, true
		}
	}
	if isRootIdent(b.Right, "prev") {
		if _, ok := b.Left.(*ast.NilNode); ok {
			return b.Operator, true
		}
	}
	return "", false
}

func isRootIdent(n ast.Node, name string) bool {
	id, ok := n.(*ast.IdentifierNode)
	return ok && id.Value == name
}

func metricNames(sch *DeviceSchema) []string {
	out := make([]string, 0, len(sch.Metrics))
	for k := range sch.Metrics {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func mapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}
