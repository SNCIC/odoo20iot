package dag

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/rules"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testSchema() *rules.DeviceSchema {
	return &rules.DeviceSchema{
		Metrics:    map[string]rules.Kind{"temperature": rules.KindNumber},
		WindowAggs: []string{"avg"},
	}
}

// record 记录动作调用。
type record struct {
	mu    sync.Mutex
	calls []string
	args  map[string][]map[string]any
}

func newRecord() *record { return &record{args: map[string][]map[string]any{}} }

func (r *record) add(name string, p map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
	r.args[name] = append(r.args[name], p)
}

func (r *record) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *record) count(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.args[name])
}

func (r *record) paramsOf(name string) []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any(nil), r.args[name]...)
}

// newRegistry 构造一个含 04 §1.3 动作表子集的注册表。
//
// run 为 nil 时只记录调用；否则由它决定成败（用于重试/失败路径测试）。
func newRegistry(t *testing.T, run func(name string, params map[string]any) error) *Registry {
	t.Helper()
	mk := func(name string, idem bool) Func {
		n := name
		return Func{ActionName: n, Idem: idem, Run: func(_ context.Context, p map[string]any) error {
			if run == nil {
				return nil
			}
			return run(n, p)
		}}
	}
	r, err := NewRegistry(
		mk("alarm.raise", true),
		mk("alarm.clear", true),
		mk("command.send", true),
		mk("notify.send", false), // 04 §1.3：notify.send 不幂等
	)
	if err != nil {
		t.Fatalf("构造注册表失败: %v", err)
	}
	return r
}

func testDeps(t *testing.T, reg *Registry) Deps {
	t.Helper()
	return Deps{
		Registry:  reg,
		Compiler:  rules.NewCompiler(64),
		Schema:    testSchema(),
		KeyPrefix: "test@1",
	}
}

func compileErr(t *testing.T, def Definition, deps Deps) *CompileError {
	t.Helper()
	_, err := Compile(def, deps)
	if err == nil {
		t.Fatal("期望编译失败，但通过了")
	}
	var ce *CompileError
	if !errors.As(err, &ce) {
		t.Fatalf("期望 *CompileError，得到 %T: %v", err, err)
	}
	return ce
}

func hasIssue(ce *CompileError, substr string) bool {
	for _, i := range ce.Issues {
		if strings.Contains(i.Message, substr) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 合法路径
// ---------------------------------------------------------------------------

// TestCompile_合法DAG 用 04 §1.5 示例的形状验证编译通过与深度计算。
func TestCompile_合法DAG(t *testing.T) {
	def := Definition{
		Entry: "n1",
		Nodes: []Node{
			{ID: "n1", Type: NodeCondition, Expr: "window.avg > 60", OnTrue: "n2", OnFalse: "end"},
			{ID: "n2", Type: NodeAction, Action: "alarm.raise",
				Params: map[string]any{"level": "warn", "value": "$window.avg"}, Next: "n3"},
			{ID: "n3", Type: NodeAction, Action: "command.send", Next: "end"},
			{ID: "end", Type: NodeEnd},
		},
	}

	g, err := Compile(def, testDeps(t, newRegistry(t, nil)))
	if err != nil {
		t.Fatalf("应编译通过，得到 %v", err)
	}
	if g.Entry() != "n1" {
		t.Fatalf("entry 错误: %s", g.Entry())
	}
	if len(g.Warnings()) != 0 {
		t.Fatalf("不该有警告: %v", g.Warnings())
	}
	// n1=1 → n2=2 → n3=3 → end=4（end 也可由 n1 直接到达，取较深的那条）
	if d, ok := g.Depth("end"); !ok || d != 4 {
		t.Fatalf("end 的深度应为 4，得到 %d(ok=%v)", d, ok)
	}
	if len(g.Nodes()) != 4 {
		t.Fatalf("拓扑序应含 4 个节点，得到 %v", g.Nodes())
	}
}

// TestCompile_解析JSON 验证与 04 §1.5 一致的 JSON 形状可解析。
func TestCompile_解析JSON(t *testing.T) {
	raw := []byte(`{
	  "entry": "n1",
	  "nodes": [
	    {"id":"n1","type":"condition","expr":"msg.temperature > 60","on_true":"n2","on_false":"end"},
	    {"id":"n2","type":"action","action":"notify.send","params":{"group":"oncall_a"},"next":"end"},
	    {"id":"end","type":"end"}
	  ]
	}`)

	def, err := ParseDefinition(raw)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if _, err := Compile(def, testDeps(t, newRegistry(t, nil))); err != nil {
		t.Fatalf("应编译通过: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 约束表（04 §1.3）
// ---------------------------------------------------------------------------

func TestCompile_环检测(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "alarm.raise", Next: "b"},
		{ID: "b", Type: NodeAction, Action: "alarm.raise", Next: "a"},
	}}
	ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "环") {
		t.Fatalf("应报环，得到 %v", ce.Issues)
	}
}

func TestCompile_深度超限(t *testing.T) {
	// 10 个节点的直链，深度 10 > 8
	nodes := make([]Node, 0, 10)
	for i := 0; i < 10; i++ {
		n := Node{ID: fmt.Sprintf("a%d", i), Type: NodeAction, Action: "alarm.raise"}
		if i < 9 {
			n.Next = fmt.Sprintf("a%d", i+1)
		}
		nodes = append(nodes, n)
	}
	ce := compileErr(t, Definition{Entry: "a0", Nodes: nodes}, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "深度") {
		t.Fatalf("应报深度超限，得到 %v", ce.Issues)
	}
}

func TestCompile_节点数超限(t *testing.T) {
	nodes := make([]Node, 0, MaxNodes+1)
	for i := 0; i <= MaxNodes; i++ {
		nodes = append(nodes, Node{ID: fmt.Sprintf("a%d", i), Type: NodeEnd})
	}
	ce := compileErr(t, Definition{Entry: "a0", Nodes: nodes}, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "节点数") {
		t.Fatalf("应报节点数超限，得到 %v", ce.Issues)
	}
}

func TestCompile_引用不存在的节点(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeCondition, Expr: "msg.temperature > 1", OnTrue: "缺失", OnFalse: "end"},
		{ID: "end", Type: NodeEnd},
	}}
	ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "不存在的节点") {
		t.Fatalf("应报引用不存在，得到 %v", ce.Issues)
	}
}

func TestCompile_entry不存在(t *testing.T) {
	def := Definition{Entry: "缺失", Nodes: []Node{{ID: "a", Type: NodeEnd}}}
	ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "entry 指向不存在") {
		t.Fatalf("应报 entry 不存在，得到 %v", ce.Issues)
	}
}

func TestCompile_end有出边(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "alarm.raise", Next: "end"},
		{ID: "end", Type: NodeEnd, Next: "a"},
	}}
	ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "end 节点不能有出边") {
		t.Fatalf("应报 end 有出边，得到 %v", ce.Issues)
	}
}

// TestCompile_动作白名单 是 04 §1.6 的「保存阶段即阻断」：
// 非注册表内的 action 不允许保存，而不是等执行时才发现。
func TestCompile_动作白名单(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "os.exec", Next: "end"},
		{ID: "end", Type: NodeEnd},
	}}
	ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "不在注册表内") {
		t.Fatalf("应报动作不在白名单，得到 %v", ce.Issues)
	}
	// 错误信息要给出可用动作，否则用户只能靠猜。
	if !hasIssue(ce, "alarm.raise") {
		t.Fatalf("错误信息应列出可用动作，得到 %v", ce.Issues)
	}
}

func TestCompile_delay超上限(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeDelay, MS: int(MaxDelay/time.Millisecond) + 1, Next: "end"},
		{ID: "end", Type: NodeEnd},
	}}
	ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "超过上限") {
		t.Fatalf("应报 delay 超上限，得到 %v", ce.Issues)
	}
}

func TestCompile_parallel约束(t *testing.T) {
	t.Run("分支数超限", func(t *testing.T) {
		branches := make([]string, 0, MaxParallel+1)
		nodes := make([]Node, 0, MaxParallel+2)
		for i := 0; i <= MaxParallel; i++ {
			branches = append(branches, fmt.Sprintf("b%d", i))
			nodes = append(nodes, Node{ID: fmt.Sprintf("b%d", i), Type: NodeEnd})
		}
		nodes = append(nodes, Node{ID: "p", Type: NodeParallel, Branches: branches})
		ce := compileErr(t, Definition{Entry: "p", Nodes: nodes}, testDeps(t, newRegistry(t, nil)))
		if !hasIssue(ce, "分支数") {
			t.Fatalf("应报分支数超限，得到 %v", ce.Issues)
		}
	})

	t.Run("limit超过分支数", func(t *testing.T) {
		def := Definition{Entry: "p", Nodes: []Node{
			{ID: "p", Type: NodeParallel, Branches: []string{"b1"}, Limit: 3},
			{ID: "b1", Type: NodeEnd},
		}}
		ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
		if !hasIssue(ce, "大于分支数") {
			t.Fatalf("应报 limit 大于分支数，得到 %v", ce.Issues)
		}
	})
}

// TestCompile_变量引用禁止拼接 是 04 §1.3 的安全约束：
// 一旦允许拼接，params 就成了第二个表达式引擎，绕过 L1 的白名单与类型校验。
func TestCompile_变量引用禁止拼接(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "notify.send",
			Params: map[string]any{"text": "$msg.a + $msg.b", "nested": map[string]any{"v": "$msg.x.y"}},
			Next:   "end"},
		{ID: "end", Type: NodeEnd},
	}}
	ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
	if !hasIssue(ce, "禁止表达式拼接") {
		t.Fatalf("应报变量拼接非法，得到 %v", ce.Issues)
	}
	// 合法的 `$msg.x.y` 不该被误报。
	for _, i := range ce.Issues {
		if strings.Contains(i.Message, "$msg.x.y") {
			t.Fatalf("合法引用被误报: %v", i)
		}
	}
}

// TestCompile_一次返回全部问题 验证「保存即校验」不做往返消耗。
func TestCompile_一次返回全部问题(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "os.exec", Next: "缺失"},
		{ID: "b", Type: NodeAction, Action: "也不存在"},
		{ID: "c", Type: NodeDelay, MS: 1, Next: "缺失2"},
	}}
	ce := compileErr(t, def, testDeps(t, newRegistry(t, nil)))
	if len(ce.Issues) < 3 {
		t.Fatalf("应一次报出全部问题（≥3），得到 %d: %v", len(ce.Issues), ce.Issues)
	}
}

// TestCompile_不可达只是警告 验证不把文档认为合法的配置挡在门外。
func TestCompile_不可达只是警告(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "alarm.raise", Next: "end"},
		{ID: "end", Type: NodeEnd},
		{ID: "orphan", Type: NodeAction, Action: "alarm.raise"},
	}}
	g, err := Compile(def, testDeps(t, newRegistry(t, nil)))
	if err != nil {
		t.Fatalf("不可达不该阻断保存，得到 %v", err)
	}
	if len(g.Warnings()) != 1 || !strings.Contains(g.Warnings()[0].Message, "orphan") {
		t.Fatalf("应给出不可达警告，得到 %v", g.Warnings())
	}
}

// TestCompile_并行分支重叠警告 验证「不支持 join」的后果被提前告知：
// 重复执行对不幂等动作（notify.send）就是重复通知。
func TestCompile_并行分支重叠警告(t *testing.T) {
	def := Definition{Entry: "p", Nodes: []Node{
		{ID: "p", Type: NodeParallel, Branches: []string{"b1", "b2"}},
		{ID: "b1", Type: NodeAction, Action: "alarm.raise", Next: "shared"},
		{ID: "b2", Type: NodeAction, Action: "alarm.raise", Next: "shared"},
		{ID: "shared", Type: NodeAction, Action: "notify.send"},
	}}
	g, err := Compile(def, testDeps(t, newRegistry(t, nil)))
	if err != nil {
		t.Fatalf("应编译通过: %v", err)
	}
	found := false
	for _, w := range g.Warnings() {
		if strings.Contains(w.Message, "join") {
			found = true
		}
	}
	if !found {
		t.Fatalf("应给出分支重叠警告，得到 %v", g.Warnings())
	}
}

// ---------------------------------------------------------------------------
// 注册表
// ---------------------------------------------------------------------------

func TestNewRegistry_拒绝重复与空名(t *testing.T) {
	if _, err := NewRegistry(Func{ActionName: "a"}, Func{ActionName: "a"}); err == nil {
		t.Fatal("重复注册应报错（否则白名单会被静默覆盖）")
	}
	if _, err := NewRegistry(Func{ActionName: "  "}); err == nil {
		t.Fatal("空名应报错")
	}
	if _, err := NewRegistry(nil); err == nil {
		t.Fatal("nil 动作应报错")
	}
}

func TestRegistry_Names稳定有序(t *testing.T) {
	r, err := NewRegistry(Func{ActionName: "b"}, Func{ActionName: "a"})
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	names := r.Names()
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("Names 应字典序，得到 %v", names)
	}
}
