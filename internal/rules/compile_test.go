package rules

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/expr-lang/expr"
)

func testSchema() *DeviceSchema {
	return &DeviceSchema{
		Metrics: map[string]Kind{
			"temperature": KindNumber,
			"humidity":    KindNumber,
			"running":     KindBool,
			"mode":        KindString,
			"reported_at": KindTime,
		},
		TagKeys:         []string{"site", "line"},
		WindowAggs:      []string{"avg", "max"},
		OptionalMetrics: map[string]bool{"humidity": true},
	}
}

func mustCompile(t *testing.T, src string) *Program {
	t.Helper()
	p, err := Compile("test", src, testSchema(), nil)
	if err != nil {
		t.Fatalf("表达式应编译通过: %s\n%v", src, err)
	}
	return p
}

func compileErr(t *testing.T, src string) *CompileError {
	t.Helper()
	_, err := Compile("test", src, testSchema(), nil)
	if err == nil {
		t.Fatalf("表达式应被拒绝: %s", src)
	}
	var ce *CompileError
	if !errors.As(err, &ce) {
		t.Fatalf("期望 *CompileError，得到 %T: %v", err, err)
	}
	return ce
}

// TestCompile_合法表达式 覆盖 04 §1.2 变量表与「可用语法」清单中的写法。
func TestCompile_合法表达式(t *testing.T) {
	valid := []string{
		"msg.temperature > 60",
		"msg.temperature > 60 && meta.tags.site == 'sh'",
		"msg.temperature in [20, 25, 30]",
		"msg.temperature not in [20, 25]",
		"(msg.temperature - 20) / 5 > 1",
		"abs(msg.temperature - 30) < 0.5",
		"msg.running && msg.temperature > 60",
		"msg.mode == 'auto'",
		"msg.mode matches '^sh.*'",
		"not (msg.temperature > 60)",
		"prev != nil && msg.temperature - prev.temperature > 5",
		"prev == nil ? false : msg.temperature > prev.temperature",
		"window.avg > 60",
		"window.max > 60 && window.avg > 50",
		"meta.device_id == 100234",
		"meta.group == '车间A'",
		"meta.seq > 0",
		"time_diff_s(time_now(), msg.reported_at) > 3600",
		"state.alarm == true",
		"state['alarm'] == true",
		"len(msg.mode) > 0",
		"has_prefix(msg.mode, 'au')",
		"(msg.humidity ?? 0) > 70",
		"convert(msg.temperature, 'c', 'f') > 140",
		"distance_m(31.23, 121.47, 31.24, 121.48) < 2000",
	}

	for _, src := range valid {
		if _, err := Compile("test", src, testSchema(), nil); err != nil {
			t.Errorf("应通过但被拒绝: %s\n%v", src, err)
		}
	}
}

// TestCompile_字段建议 是「最好的质检」这条设计目标的可执行形式：
// 拼错字段时必须给出「是否想用 X」。
func TestCompile_字段建议(t *testing.T) {
	ce := compileErr(t, "msg.temp > 60")
	msg := ce.Error()
	if !strings.Contains(msg, `"temperature"`) {
		t.Fatalf("应建议 temperature，实际:\n%s", msg)
	}
	if !strings.Contains(msg, "第 1 行第 5 列") {
		t.Fatalf("应给出定位，实际:\n%s", msg)
	}
	if !strings.Contains(msg, "msg.temp > 60") {
		t.Fatalf("应回显源码片段，实际:\n%s", msg)
	}
}

func TestCompile_类型校验(t *testing.T) {
	cases := []struct {
		src  string
		want string
	}{
		{"msg.temperature > 'abc'", "两侧类型必须一致"},
		{"msg.temperature && true", "需要布尔操作数"},
		{"msg.running > true", "布尔值不能做大小比较"},
		{"msg.temperature + msg.running > 1", "需要数值操作数"},
		{"msg.temperature in 5", "右侧必须是列表"},
		{"msg.temperature matches '^1'", "matches 左侧必须是字符串"},
		{"!msg.temperature", "需要布尔操作数"},
		{"msg.temperature > 60 ? 1 : 'a'", ""}, // 分支类型不一致目前归并为动态值，不报错（见下）
		{"time_diff_s(msg.temperature, time_now()) > 0", "需要时间"},
	}
	for _, c := range cases {
		if c.want == "" {
			continue
		}
		ce := compileErr(t, c.src)
		if !strings.Contains(ce.Error(), c.want) {
			t.Errorf("%s 的报错应包含 %q，实际:\n%s", c.src, c.want, ce.Error())
		}
	}
}

// TestCompile_函数白名单 是安全边界的回归测试。
//
// 其中 all/filter/map 尤其重要：它们是 PredicateNode，
// `expr.DisableAllBuiltins()` 拦不住（04 §1.2.1 记录了这个缺口）。
func TestCompile_函数白名单(t *testing.T) {
	banned := []string{
		"all([1, 2], # > 0)",
		"none([1, 2], # > 0)",
		"any([1, 2], # > 0)",
		"one([1, 2], # > 0)",
		"filter([1, 2], # > 0)[0] > 0",
		"map([1, 2], # * 2)[0] > 0",
		"parseInt('1', 16) > 0",
		"int('3') > 0",
		"string(1) == '1'",
		"get([1, 2], 0) > 0",
		"unknown_fn(1) > 0",
	}
	for _, src := range banned {
		if _, err := Compile("test", src, testSchema(), nil); err == nil {
			t.Errorf("非白名单函数应被拒绝: %s", src)
		}
	}

	// 白名单内的同名函数（len 与 expr 内置同名）必须可用，
	// 且要走我们自己的签名校验，而不是被 expr 的内置实现接管。
	mustCompile(t, "len(msg.mode) > 2")
	mustCompile(t, "trim(msg.mode) == 'auto'")

	typeErr := compileErr(t, "len(msg.temperature) > 2")
	if !strings.Contains(typeErr.Error(), "需要字符串") {
		t.Fatalf("白名单函数的参数类型必须被校验，实际:\n%s", typeErr.Error())
	}

	// 参数个数错误必须报出来。
	ce := compileErr(t, "abs() > 0")
	if !strings.Contains(ce.Error(), "参数个数不对") {
		t.Fatalf("应报参数个数，实际:\n%s", ce.Error())
	}
}

func TestCompile_禁用let(t *testing.T) {
	ce := compileErr(t, "let x = 1; x > 0")
	if !strings.Contains(ce.Error(), "let") {
		t.Fatalf("应说明不允许 let，实际:\n%s", ce.Error())
	}
}

// TestCompile_正则 覆盖 04 §1.2 的「正则安全」两条规定。
func TestCompile_正则(t *testing.T) {
	// 长度上限：512 通过、513 拒绝。
	mustCompile(t, "msg.mode matches '"+strings.Repeat("a", 512)+"'")
	ce := compileErr(t, "msg.mode matches '"+strings.Repeat("a", 513)+"'")
	if !strings.Contains(ce.Error(), "超过上限") {
		t.Fatalf("应报长度超限，实际:\n%s", ce.Error())
	}

	// 动态拼接的正则无法静态判定长度，直接拒绝。
	ce = compileErr(t, "msg.mode matches msg.mode")
	if !strings.Contains(ce.Error(), "必须是正则**字面量**") {
		t.Fatalf("应要求字面量，实际:\n%s", ce.Error())
	}
}

// TestCompile_prev必须判空 是 04 §1.2 「表达式写法约束」的可执行形式。
func TestCompile_prev必须判空(t *testing.T) {
	ok := []string{
		"prev != nil && msg.temperature - prev.temperature > 5",
		"prev == nil || msg.temperature > prev.temperature",
		"prev == nil ? false : msg.temperature > prev.temperature",
		"prev != nil && (prev.temperature > 10 && msg.temperature > prev.temperature)",
	}
	for _, src := range ok {
		mustCompile(t, src)
	}

	bad := []string{
		"prev.temperature > 5",                        // 完全没判空
		"msg.temperature > 5 && prev.temperature > 1", // 判空缺失
		"prev.temperature > 1 || prev == nil",         // 判空在右侧，短路保护不了左侧
	}
	for _, src := range bad {
		ce := compileErr(t, src)
		if !strings.Contains(ce.Error(), "必须先判空") {
			t.Errorf("%s 应被要求判空，实际:\n%s", src, ce.Error())
		}
	}
}

func TestCompile_窗口必须声明(t *testing.T) {
	ce := compileErr(t, "window.count > 10")
	if !strings.Contains(ce.Error(), "未声明聚合") {
		t.Fatalf("应报未声明窗口，实际:\n%s", ce.Error())
	}

	ce = compileErr(t, "window.median > 10")
	if !strings.Contains(ce.Error(), "窗口没有聚合") {
		t.Fatalf("应报未知聚合，实际:\n%s", ce.Error())
	}
}

// TestCompile_可选指标必须兜底 覆盖「缺失指标的运行时语义」这一缺口。
func TestCompile_可选指标必须兜底(t *testing.T) {
	mustCompile(t, "(msg.humidity ?? 0) > 70")
	mustCompile(t, "msg.temperature > 60 && (msg.humidity ?? 0) > 70")

	ce := compileErr(t, "msg.humidity > 70")
	if !strings.Contains(ce.Error(), "可能缺失") {
		t.Fatalf("应要求兜底值，实际:\n%s", ce.Error())
	}
}

func TestCompile_根必须是布尔(t *testing.T) {
	ce := compileErr(t, "msg.temperature")
	if !strings.Contains(ce.Error(), "必须返回布尔值") {
		t.Fatalf("应报根类型，实际:\n%s", ce.Error())
	}
}

// TestCompile_一次返回所有问题 避免用户在控制台反复试错。
func TestCompile_一次返回所有问题(t *testing.T) {
	ce := compileErr(t, "msg.temp > 60 && msg.humid > 70 && weidow.avg > 1")
	if len(ce.Issues) < 3 {
		t.Fatalf("应一次报出 3 个问题，实际 %d 个:\n%s", len(ce.Issues), ce.Error())
	}
}

// TestCompile_文档写法勘误 把探测中发现的三处文档写法问题固定成回归用例。
func TestCompile_文档写法勘误(t *testing.T) {
	// 1) 文档里的 `prev == null` 写法在 expr 里不成立（只有 nil）。
	ce := compileErr(t, "prev == null")
	if !strings.Contains(ce.Error(), "未知的变量") {
		t.Fatalf("null 应被识别为未知变量，实际:\n%s", ce.Error())
	}

	// 2) `state.get(key)` 不可用（Go 方法名泄漏），改用 state 映射。
	ce = compileErr(t, "state.get('alarm')")
	if !strings.Contains(ce.Error(), "state") {
		t.Fatalf("state.get 应被拒绝并提示 state 用法，实际:\n%s", ce.Error())
	}

	// 3) 时间不能直接与数值相减。
	ce = compileErr(t, "msg.reported_at > time_now() - 60")
	if !strings.Contains(ce.Error(), "需要数值操作数") {
		t.Fatalf("时间相减应被拒绝，实际:\n%s", ce.Error())
	}
}

// ---------- 求值语义 ----------

func testEnv(t *testing.T) Env {
	t.Helper()
	return NewEnv().Bind(
		map[string]any{"temperature": 65.0, "running": true, "mode": "auto", "humidity": 72.0},
		NewMeta(100234, 55, 10231, 1, "车间A", []any{"车间A"},
			map[string]any{"site": "sh"}, time.Now().UTC(), 42),
		map[string]any{"temperature": 58.0},
		map[string]any{"avg": 62.5, "max": 70.0},
		map[string]any{"alarm": true},
	)
}

func TestEval_正常路径(t *testing.T) {
	env := testEnv(t)

	cases := []struct {
		src  string
		want bool
	}{
		{"msg.temperature > 60", true},
		{"msg.temperature > 70", false},
		{"msg.temperature > 60 && meta.tags.site == 'sh'", true},
		{"msg.temperature > 60 && meta.tags.site == 'bj'", false},
		{"prev != nil && msg.temperature - prev.temperature > 5", true},
		{"window.avg > 60", true},
		{"state.alarm == true", true},
		{"(msg.humidity ?? 0) > 70", true},
		{"abs(msg.temperature - 30) < 0.5", false},
		{"time_diff_s(time_now(), meta.ts) < 60", true},
	}
	for _, c := range cases {
		p := mustCompile(t, c.src)
		got, err := p.Eval(env)
		if err != nil {
			t.Errorf("%s 求值出错: %v", c.src, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s = %v，期望 %v", c.src, got, c.want)
		}
	}
}

// TestEval_首条消息prev为空 验证 04 §1.2 中「prev 判空」规则的运行时必要性：
// 若没有短路保护，求值会失败 —— 这正是编译期强制判空要防的事。
func TestEval_首条消息prev为空(t *testing.T) {
	env := testEnv(t)
	env[nsPrev] = nil

	guarded := mustCompile(t, "prev != nil && msg.temperature - prev.temperature > 5")
	got, err := guarded.Eval(env)
	if err != nil {
		t.Fatalf("已判空的表达式不应失败: %v", err)
	}
	if got {
		t.Fatal("prev 为空时应短路为 false")
	}

	// 绕过门禁直接编译未判空的写法，确认运行时确实会失败（证明门禁不是多余的）。
	raw, err := compileWithoutGate("prev.temperature > 5")
	if err != nil {
		t.Fatalf("绕过门禁编译失败: %v", err)
	}
	if _, err := raw.Eval(env); err == nil {
		t.Fatal("未判空的 prev 引用应求值失败 —— 门禁的必要性依赖此行为")
	}
}

// TestEval_缺失指标 固化「设备未上报」的运行时语义（文档此前未定义）。
func TestEval_缺失指标(t *testing.T) {
	env := testEnv(t)
	delete(env[nsMsg].(map[string]any), "temperature")

	direct := mustCompile(t, "msg.temperature > 60")
	if _, err := direct.Eval(env); err == nil {
		t.Fatal("指标缺失时直接比较应失败（nil 参与比较），不能被当成 false")
	}

	fallback := mustCompile(t, "(msg.temperature ?? 0) > 60")
	got, err := fallback.Eval(env)
	if err != nil {
		t.Fatalf("带兜底的表达式不应失败: %v", err)
	}
	if got {
		t.Fatal("兜底值 0 不应触发阈值")
	}
}

// ---------- 缓存 ----------

func TestCache_键与命中(t *testing.T) {
	c := NewCompiler(4)
	sch := testSchema()
	src := "msg.temperature > 60"

	p1, err := c.Compile(CacheKey("r1", 1, src), src, sch)
	if err != nil {
		t.Fatalf("首次编译失败: %v", err)
	}
	p2, err := c.Compile(CacheKey("r1", 1, src), src, sch)
	if err != nil {
		t.Fatalf("二次编译失败: %v", err)
	}
	if p1 != p2 {
		t.Fatal("相同键应命中缓存，返回同一个 Program")
	}

	st := c.Stats()
	if st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("命中统计不对: %+v", st)
	}
	if st.HitRate() != 0.5 {
		t.Fatalf("命中率不对: %v", st.HitRate())
	}
}

// TestCache_版本或表达式变化即失效 验证 04 §1.2 的缓存键口径。
func TestCache_版本或表达式变化即失效(t *testing.T) {
	src := "msg.temperature > 60"

	keys := map[string]bool{
		CacheKey("r1", 1, src):     true,
		CacheKey("r1", 2, src):     true, // 版本变
		CacheKey("r1", 1, src+" "): true, // 表达式变
		CacheKey("r2", 1, src):     true, // 规则变
	}
	if len(keys) != 4 {
		t.Fatal("缓存键未随 规则/版本/表达式 变化，会导致旧规则被错误复用")
	}
}

func TestCache_LRU淘汰(t *testing.T) {
	c := NewCompiler(2)
	sch := testSchema()

	_, _ = c.Compile(CacheKey("r1", 1, "msg.temperature > 60"), "msg.temperature > 60", sch)
	_, _ = c.Compile(CacheKey("r2", 1, "msg.temperature > 61"), "msg.temperature > 61", sch)
	// 再写一条，容量为 2，最早的应被淘汰。
	_, _ = c.Compile(CacheKey("r3", 1, "msg.temperature > 62"), "msg.temperature > 62", sch)

	if st := c.Stats(); st.Size != 2 {
		t.Fatalf("容量应被限制为 2，实际 %d", st.Size)
	}
}

// compileWithoutGate 绕过静态门禁直接交给 expr 编译。
//
// 它存在的意义是「反证」：用于验证门禁挡住的确实是**运行时会炸**的写法，
// 而不是我们一厢情愿的洁癖。
func compileWithoutGate(src string) (*Program, error) {
	prog, err := expr.Compile(src, expr.Env(NewEnv()), expr.AsBool())
	if err != nil {
		return nil, err
	}
	return &Program{prog: prog, src: src}, nil
}
