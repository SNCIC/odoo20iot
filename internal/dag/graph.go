// Package dag 实现 04 §1.3 的 L2 自研 DAG 编排执行器。
//
// 它同时服务两条产品线，**且必须是同一套代码**（04 §1.3 / §3）：
//   - 规则引擎的「触发之后做什么」（条件 → 动作链 → 分支 / 延时 / 并行 / 重试）；
//   - 场景联动的预置模板（场景 = 预置 DAG + 定时/事件触发源）。
//
// 「禁止出现第二套编排引擎」是 ADR-003 选自研而非引外部编排框架的核心原因。
//
// 安全边界：节点执行体**全部是编译进二进制的 Go 函数**，平台不执行用户提交的
// 编排代码。用户能提交的只有图的形状与参数（04 §1.3）。
//
// condition 节点复用 L1 的 expr（`internal/rules`），**不另建表达式引擎** ——
// 否则同一套语法会有两种语义，这是比性能更值得避免的事。
package dag

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/rules"
)

// 节点类型（04 §1.3）。
const (
	NodeCondition = "condition"
	NodeAction    = "action"
	NodeDelay     = "delay"
	NodeParallel  = "parallel"
	NodeBranch    = "branch"
	NodeEnd       = "end"
)

// 构建时强制校验的约束（04 §1.3 的约束表）。
const (
	// MaxNodes 是节点数上限。
	MaxNodes = 32
	// MaxDepth 是深度上限（entry 记为第 1 层）。
	MaxDepth = 8
	// MaxParallel 是并行上限。
	MaxParallel = 5
	// DefaultBranchLimit 是 parallel 节点未写 limit 时的默认并发度。
	DefaultBranchLimit = 5
	// DefaultNodeTimeout 是单节点超时。
	DefaultNodeTimeout = 5 * time.Second
	// MaxNodeTimeout 是单节点超时的硬上限。
	MaxNodeTimeout = 5 * time.Second
	// MaxDelay 是 delay 节点的上限（更长延时改走定时任务）。
	MaxDelay = 5 * time.Minute
	// MaxRetry 是单节点重试上限。
	MaxRetry = 3
)

// Definition 是规则/场景里的 DAG 定义（04 §1.5 的 `dag` 字段）。
type Definition struct {
	Entry string `json:"entry"`
	Nodes []Node `json:"nodes"`
}

// Node 是一个编排节点。字段按类型分组，未用的字段留空，
// 由 Compile 按类型校验 —— 用一个大结构体而不是多态解码，
// 是因为约束表本身就是「按类型的字段组合」。
type Node struct {
	ID   string `json:"id"`
	Type string `json:"type"`

	// condition
	Expr    string `json:"expr,omitempty"`
	OnTrue  string `json:"on_true,omitempty"`
	OnFalse string `json:"on_false,omitempty"`

	// action
	Action    string         `json:"action,omitempty"`
	Params    map[string]any `json:"params,omitempty"`
	Retry     *Retry         `json:"retry,omitempty"`
	TimeoutMS int            `json:"timeout_ms,omitempty"`

	// delay
	MS   int    `json:"ms,omitempty"`
	Next string `json:"next,omitempty"`

	// parallel
	Branches []string `json:"branches,omitempty"`
	Limit    int      `json:"limit,omitempty"`

	// branch（多路分支 / switch）
	Cases   []Case `json:"cases,omitempty"`
	Default string `json:"default,omitempty"`
}

// Case 是 branch 节点的一路分支。
//
// ⚠️ 04 §1.3 只写了 `cases[]` 与 `default`，未定义 case 的字段名；
// 本实现取 `{expr, next}`（与 condition 的 `expr` 保持一致），
// 是**实现决策**，改动需与规则编辑器同步。
type Case struct {
	Expr string `json:"expr"`
	Next string `json:"next"`
}

// Retry 是 action 节点的重试策略（04 §1.5 的 `retry`）。
type Retry struct {
	Max     int    `json:"max"`
	Backoff string `json:"backoff"` // exponential | fixed
	BaseMS  int    `json:"base_ms"`
}

// Issue 是一条编译期校验问题。
type Issue struct {
	Node    string
	Message string
}

func (i Issue) String() string {
	if i.Node == "" {
		return i.Message
	}
	return fmt.Sprintf("节点 %s: %s", i.Node, i.Message)
}

// CompileError 聚合一次编译里的全部问题。
//
// 与 `rules.CompileError` 同一取向：**一次性返回所有问题**而不是遇到第一个就停 ——
// 用户在控制台改一条 DAG 时，逐个报错的往返成本很高（04 §1.6 把「保存即校验」
// 定位为最好的质检）。
type CompileError struct {
	Issues []Issue
}

func (e *CompileError) Error() string {
	parts := make([]string, 0, len(e.Issues))
	for _, i := range e.Issues {
		parts = append(parts, i.String())
	}
	return fmt.Sprintf("DAG 校验失败（%d 处）: %s", len(e.Issues), strings.Join(parts, "; "))
}

// compiledNode 是校验通过后的节点：condition/case 的表达式已编译成 L1 程序。
type compiledNode struct {
	node *Node
	// program 是 condition 节点的表达式程序；branch 节点则为 nil（各 case 各自编译）。
	program *rules.Program
	// casePrograms 与 Node.Cases 一一对应。
	casePrograms []*rules.Program
	// action 是解析后的动作实现（已过白名单）。
	action Action
}

// Graph 是校验通过、可直接执行的图。
type Graph struct {
	entry string
	nodes map[string]*compiledNode
	order []string // 拓扑序，便于稳定遍历与排障
	depth map[string]int
	// warnings 是**不阻断保存**的提示（当前只有「不可达节点」）。
	// 与 Issue 分开：约束表（04 §1.3）没有把不可达列为拒绝项，
	// 自行加一条拒绝规则会把文档认为合法的配置挡在门外。
	warnings []Issue
}

// Entry 返回入口节点 id。
func (g *Graph) Entry() string { return g.entry }

// Nodes 返回全部节点 id（拓扑序）。
func (g *Graph) Nodes() []string { return append([]string(nil), g.order...) }

// Depth 返回某节点的深度（entry 为 1）。
func (g *Graph) Depth(id string) (int, bool) {
	d, ok := g.depth[id]
	return d, ok
}

// Warnings 返回不阻断保存的提示。
func (g *Graph) Warnings() []Issue { return append([]Issue(nil), g.warnings...) }

// Deps 是编译 DAG 所需的外部依赖。
type Deps struct {
	// Registry 是动作白名单（04 §1.3）。nil 表示**不做动作白名单校验**，
	// 只用于纯结构校验的测试场景；生产必须传。
	Registry *Registry
	// Compiler 是 L1 的表达式编译器（带缓存）。condition/case 用它编译。
	Compiler *rules.Compiler
	// Schema 是物模型 schema，供 L1 做类型校验。
	Schema *rules.DeviceSchema
	// KeyPrefix 是编译缓存键前缀（建议 `ruleID@version`）。
	// 每个 condition 节点用 `KeyPrefix + ":" + nodeID` 作为键。
	KeyPrefix string
}

// Compile 校验并编译一个 DAG 定义。
//
// 校验项与 04 §1.3 的约束表一一对应；表达式部分交给 L1 的 Compiler，
// 因此「未知字段/类型不符」也在这一步被拦住（04 §1.6 要求保存即阻断）。
func Compile(def Definition, deps Deps) (*Graph, error) {
	c := &compiler{deps: deps, byID: make(map[string]*Node, len(def.Nodes))}
	c.run(def)

	if len(c.issues) > 0 {
		return nil, &CompileError{Issues: c.issues}
	}

	g := &Graph{
		entry:    def.Entry,
		nodes:    c.compiled,
		order:    c.order,
		depth:    c.depth,
		warnings: c.warnings,
	}
	return g, nil
}

// ParseDefinition 从 JSON 解析 DAG 定义。
func ParseDefinition(raw []byte) (Definition, error) {
	var def Definition
	if err := json.Unmarshal(raw, &def); err != nil {
		return Definition{}, fmt.Errorf("解析 DAG 定义: %w", err)
	}
	return def, nil
}

type compiler struct {
	deps Deps

	entry    string
	byID     map[string]*Node
	compiled map[string]*compiledNode
	order    []string
	depth    map[string]int
	issues   []Issue
	warnings []Issue
}

func (c *compiler) addf(nodeID, format string, args ...any) {
	c.issues = append(c.issues, Issue{Node: nodeID, Message: fmt.Sprintf(format, args...)})
}

func (c *compiler) compileExpr(nodeID, src string) *rules.Program {
	if c.deps.Compiler == nil || c.deps.Schema == nil {
		// 未提供编译器时只做结构校验；调用方（测试）需自行保证不执行 condition。
		return nil
	}
	key := c.deps.KeyPrefix + ":" + nodeID
	prog, err := c.deps.Compiler.Compile(key, src, c.deps.Schema)
	if err != nil {
		c.addf(nodeID, "表达式编译失败: %v", err)
		return nil
	}
	return prog
}

// ---------------------------------------------------------------------------
// 校验流程
// ---------------------------------------------------------------------------

func (c *compiler) run(def Definition) {
	c.entry = def.Entry
	c.compiled = make(map[string]*compiledNode, len(def.Nodes))

	c.checkShape(def)
	if len(c.issues) > 0 {
		// 形状不对（缺 entry / 节点 id 重复 / 超上限）时节点表本身不可信，
		// 继续做引用与图分析只会产生一屏噪音。
		return
	}

	c.buildNodes(def)
	defIssues := len(c.issues)

	// 引用校验**始终执行**：它是纯局部的「目标 id 存在吗」，与节点字段写得对不对
	// 无关，而且正是用户最常犯的错（少写 / 写错 next）。在这里提前返回，会逼用户
	// 「改一次、报一次」，与 04 §1.6「保存即阻断」想要的一次报全正好相反。
	c.checkReferences()

	// 图分析（环 / 深度 / 可达）只在**所有节点定义都合法**后执行：
	// 边集不完整时算出来的「环」或「不可达」是误导，不如不报。
	if defIssues > 0 {
		return
	}
	c.analyzeGraph()
}

func (c *compiler) checkShape(def Definition) {
	if strings.TrimSpace(def.Entry) == "" {
		c.addf("", "缺少 entry")
	}
	if len(def.Nodes) == 0 {
		c.addf("", "nodes 为空")
		return
	}
	if len(def.Nodes) > MaxNodes {
		c.addf("", "节点数 %d 超过上限 %d", len(def.Nodes), MaxNodes)
	}

	seen := make(map[string]bool, len(def.Nodes))
	for i := range def.Nodes {
		n := &def.Nodes[i]
		if strings.TrimSpace(n.ID) == "" {
			c.addf("", "第 %d 个节点缺少 id", i+1)
			continue
		}
		if seen[n.ID] {
			c.addf(n.ID, "节点 id 重复")
			continue
		}
		seen[n.ID] = true
		c.byID[n.ID] = n
	}
}

func (c *compiler) buildNodes(def Definition) {
	for i := range def.Nodes {
		n := &def.Nodes[i]
		if n.ID == "" {
			continue
		}
		cn := &compiledNode{node: n}

		switch n.Type {
		case NodeCondition:
			c.checkCondition(n, cn)
		case NodeAction:
			c.checkAction(n, cn)
		case NodeDelay:
			c.checkDelay(n)
		case NodeParallel:
			c.checkParallel(n)
		case NodeBranch:
			c.checkBranch(n, cn)
		case NodeEnd:
			// end 无需字段；「不得有出边」由 checkReferences 统一处理。
		default:
			c.addf(n.ID, "未知节点类型 %q（可选：condition/action/delay/parallel/branch/end）", n.Type)
		}

		c.compiled[n.ID] = cn
	}
}

func (c *compiler) checkCondition(n *Node, cn *compiledNode) {
	if strings.TrimSpace(n.Expr) == "" {
		c.addf(n.ID, "condition 缺少 expr")
	}
	if strings.TrimSpace(n.OnTrue) == "" {
		c.addf(n.ID, "condition 缺少 on_true")
	}
	if strings.TrimSpace(n.OnFalse) == "" {
		c.addf(n.ID, "condition 缺少 on_false")
	}
	if n.Expr != "" {
		cn.program = c.compileExpr(n.ID, n.Expr)
	}
}

func (c *compiler) checkAction(n *Node, cn *compiledNode) {
	if strings.TrimSpace(n.Action) == "" {
		c.addf(n.ID, "action 节点缺少 action")
		return
	}
	if c.deps.Registry != nil {
		act, ok := c.deps.Registry.Get(n.Action)
		if !ok {
			c.addf(n.ID, "动作 %q 不在注册表内（04 §1.3 白名单），可用: %s",
				n.Action, strings.Join(c.deps.Registry.Names(), "/"))
			return
		}
		cn.action = act
	}

	if n.TimeoutMS < 0 {
		c.addf(n.ID, "timeout_ms 不能为负")
	}
	if n.TimeoutMS > int(MaxNodeTimeout/time.Millisecond) {
		c.addf(n.ID, "timeout_ms %d 超过单节点上限 %dms", n.TimeoutMS, MaxNodeTimeout/time.Millisecond)
	}

	if n.Retry != nil {
		if n.Retry.Max < 0 || n.Retry.Max > MaxRetry {
			c.addf(n.ID, "retry.max %d 超出范围 0..%d", n.Retry.Max, MaxRetry)
		}
		switch n.Retry.Backoff {
		case "", "fixed", "exponential":
		default:
			c.addf(n.ID, "retry.backoff 只支持 fixed / exponential，得到 %q", n.Retry.Backoff)
		}
		if n.Retry.BaseMS < 0 {
			c.addf(n.ID, "retry.base_ms 不能为负")
		}
	}

	c.checkParams(n.ID, n.Params)
}

func (c *compiler) checkDelay(n *Node) {
	if n.MS <= 0 {
		c.addf(n.ID, "delay 的 ms 必须为正")
	}
	if time.Duration(n.MS)*time.Millisecond > MaxDelay {
		c.addf(n.ID, "delay %dms 超过上限 %s（更长延时改走定时任务）", n.MS, MaxDelay)
	}
	if strings.TrimSpace(n.Next) == "" {
		c.addf(n.ID, "delay 缺少 next")
	}
}

func (c *compiler) checkParallel(n *Node) {
	if len(n.Branches) == 0 {
		c.addf(n.ID, "parallel 缺少 branches")
	}
	if len(n.Branches) > MaxParallel {
		c.addf(n.ID, "parallel 分支数 %d 超过上限 %d", len(n.Branches), MaxParallel)
	}
	if n.Limit < 0 {
		c.addf(n.ID, "parallel 的 limit 不能为负")
	}
	if n.Limit > MaxParallel {
		c.addf(n.ID, "parallel 的 limit %d 超过上限 %d", n.Limit, MaxParallel)
	}
	if n.Limit > 0 && len(n.Branches) > 0 && n.Limit > len(n.Branches) {
		c.addf(n.ID, "parallel 的 limit %d 大于分支数 %d（多余的并发度没有意义）", n.Limit, len(n.Branches))
	}
}

func (c *compiler) checkBranch(n *Node, cn *compiledNode) {
	if len(n.Cases) == 0 {
		c.addf(n.ID, "branch 缺少 cases")
	}
	cn.casePrograms = make([]*rules.Program, len(n.Cases))
	for i := range n.Cases {
		cs := &n.Cases[i]
		if strings.TrimSpace(cs.Expr) == "" {
			c.addf(n.ID, "第 %d 路 case 缺少 expr", i+1)
			continue
		}
		if strings.TrimSpace(cs.Next) == "" {
			c.addf(n.ID, "第 %d 路 case 缺少 next", i+1)
		}
		cn.casePrograms[i] = c.compileExpr(fmt.Sprintf("%s#case%d", n.ID, i), cs.Expr)
	}
	if strings.TrimSpace(n.Default) == "" {
		c.addf(n.ID, "branch 缺少 default（必须给兜底去处，否则会出现「什么都不做」的静默分支）")
	}
}

// checkParams 校验参数里的变量引用语法（04 §1.3 / §1.6）：
// **仅允许 `$path.to.value`，禁止表达式拼接**。
//
// 这条约束是安全边界的一部分：一旦允许拼接，params 就变成第二个表达式引擎，
// 绕过了 L1 的白名单与类型校验。
func (c *compiler) checkParams(nodeID string, params map[string]any) {
	for key, val := range params {
		c.checkParamValue(nodeID, key, val)
	}
}

func (c *compiler) checkParamValue(nodeID, path string, val any) {
	switch v := val.(type) {
	case string:
		if err := ValidateVarRef(v); err != nil {
			c.addf(nodeID, "参数 %s: %v", path, err)
		}
	case map[string]any:
		for k, sub := range v {
			c.checkParamValue(nodeID, path+"."+k, sub)
		}
	case []any:
		for i, sub := range v {
			c.checkParamValue(nodeID, fmt.Sprintf("%s[%d]", path, i), sub)
		}
	}
}

// ---------------------------------------------------------------------------
// 引用与图分析
// ---------------------------------------------------------------------------

func (c *compiler) targets(n *Node) []string {
	var out []string
	switch n.Type {
	case NodeCondition:
		out = append(out, n.OnTrue, n.OnFalse)
	case NodeAction, NodeDelay:
		// action 的 next 可选（缺省 = 链到此为止）；delay 的 next 必填
		// ——「必填」由 checkDelay 负责，这里统一取边。
		out = append(out, n.Next)
	case NodeParallel:
		out = append(out, n.Branches...)
	case NodeBranch:
		for _, cs := range n.Cases {
			out = append(out, cs.Next)
		}
		out = append(out, n.Default)
	}
	return out
}

func (c *compiler) checkReferences() {
	for _, id := range c.sortedIDs() {
		n := c.byID[id]
		for _, t := range c.targets(n) {
			if strings.TrimSpace(t) == "" {
				continue // 缺字段已在 buildNodes 报过
			}
			if _, ok := c.byID[t]; !ok {
				c.addf(id, "引用了不存在的节点 %q", t)
			}
		}
		if n.Type == NodeEnd && strings.TrimSpace(n.Next) != "" {
			// targets() 对 end 返回空，故这里直接看字段 —— 否则这条校验永远不会触发。
			c.addf(id, "end 节点不能有出边")
		}
	}
	if _, ok := c.byID[c.entry]; !ok && c.entry != "" {
		// entry 为空已在 checkShape 报过。
		c.addf("", "entry 指向不存在的节点 %q", c.entry)
	}
}

// analyzeGraph 做环检测、深度计算与可达性提示。
//
// 用 Kahn 拓扑排序而不是朴素 DFS：拓扑序同时给出「有无环」与「稳定的节点顺序」，
// 深度可以在同一遍里 DP 出来，省一次遍历。
func (c *compiler) analyzeGraph() {
	indeg := make(map[string]int, len(c.byID))
	adj := make(map[string][]string, len(c.byID))
	for id := range c.byID {
		indeg[id] = 0
	}
	for id, n := range c.byID {
		for _, t := range c.targets(n) {
			if _, ok := c.byID[t]; !ok {
				continue // 引用错误已报过，拓扑分析跳过
			}
			adj[id] = append(adj[id], t)
			indeg[t]++
		}
	}

	// 稳定顺序：入度为零的节点按 id 排序出队，保证同一 DAG 每次编译结果一致。
	queue := make([]string, 0, len(indeg))
	for id, d := range indeg {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	c.depth = make(map[string]int, len(c.byID))
	if _, ok := c.byID[c.entry]; ok {
		c.depth[c.entry] = 1
	}

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		c.order = append(c.order, id)

		next := append([]string(nil), adj[id]...)
		sort.Strings(next)
		for _, t := range next {
			if d := c.depth[id]; d > 0 && d+1 > c.depth[t] {
				c.depth[t] = d + 1
			}
			indeg[t]--
			if indeg[t] == 0 {
				queue = append(queue, t)
			}
		}
	}

	if len(c.order) != len(c.byID) {
		// 有环节点没被排出来。定位它们，给出可操作的提示。
		cycle := make([]string, 0, len(c.byID))
		for id := range c.byID {
			if indeg[id] > 0 {
				cycle = append(cycle, id)
			}
		}
		sort.Strings(cycle)
		c.addf("", "DAG 存在环，涉及节点: %s", strings.Join(cycle, "/"))
		return
	}

	for id, d := range c.depth {
		if d > MaxDepth {
			c.addf(id, "深度 %d 超过上限 %d", d, MaxDepth)
		}
	}

	// 可达性：不可达节点是明确的编写错误，但 04 §1.3 的约束表没把它列为拒绝项，
	// 故只提示、不阻断（自行加拒绝规则会把文档认为合法的配置挡在门外）。
	reachable := make(map[string]bool, len(c.byID))
	if _, ok := c.byID[c.entry]; ok {
		var walk func(string)
		walk = func(id string) {
			if reachable[id] {
				return
			}
			reachable[id] = true
			for _, t := range adj[id] {
				walk(t)
			}
		}
		walk(c.entry)
	}
	var unreachable []string
	for id := range c.byID {
		if !reachable[id] {
			unreachable = append(unreachable, id)
		}
	}
	if len(unreachable) > 0 {
		sort.Strings(unreachable)
		c.warnings = append(c.warnings, Issue{
			Message: fmt.Sprintf("存在从 entry 不可达的节点（不会被任何路径执行）: %s",
				strings.Join(unreachable, "/")),
		})
	}

	c.warnParallelOverlap(adj)
}

// warnParallelOverlap 提示「并行分支可达同一节点」。
//
// 本实现**不支持 join**（见 executor.runParallel 的说明：04 §1.3 未定义汇聚语义）。
// 两条分支若可达同一节点，该节点会被执行两次 —— 对 `alarm.raise`（幂等）无害，
// 对 `notify.send`（不幂等）就是重复通知。用户必须提前知道，而不是事后排查。
func (c *compiler) warnParallelOverlap(adj map[string][]string) {
	reach := func(start string) map[string]bool {
		seen := make(map[string]bool)
		var walk func(string)
		walk = func(id string) {
			if seen[id] {
				return
			}
			seen[id] = true
			for _, t := range adj[id] {
				walk(t)
			}
		}
		walk(start)
		return seen
	}

	for _, id := range c.sortedIDs() {
		n := c.byID[id]
		if n.Type != NodeParallel || len(n.Branches) < 2 {
			continue
		}

		sets := make([]map[string]bool, len(n.Branches))
		for i, b := range n.Branches {
			sets[i] = reach(b)
		}

		overlap := make(map[string]bool)
		for i := 0; i < len(sets); i++ {
			for j := i + 1; j < len(sets); j++ {
				for nodeID := range sets[i] {
					if sets[j][nodeID] {
						overlap[nodeID] = true
					}
				}
			}
		}
		if len(overlap) == 0 {
			continue
		}

		names := make([]string, 0, len(overlap))
		for k := range overlap {
			names = append(names, k)
		}
		sort.Strings(names)
		c.warnings = append(c.warnings, Issue{
			Node: id,
			Message: fmt.Sprintf("并行分支可达同一节点（会被执行两次 —— 本实现不支持 join）: %s",
				strings.Join(names, "/")),
		})
	}
}

func (c *compiler) sortedIDs() []string {
	out := make([]string, 0, len(c.byID))
	for id := range c.byID {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
