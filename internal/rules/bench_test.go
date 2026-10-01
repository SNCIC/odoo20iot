package rules

import (
	"os"
	"sort"
	"testing"
	"time"

	"github.com/expr-lang/expr/vm"
)

// 04 §1.7 的性能模型：L1 表达式求值（已编译）≤ 2 µs / 次。
const targetEvalP99 = 2 * time.Microsecond

func benchSchema() *DeviceSchema {
	return &DeviceSchema{
		Metrics: map[string]Kind{
			"temperature": KindNumber,
			"humidity":    KindNumber,
			"voltage":     KindNumber,
			"running":     KindBool,
			"mode":        KindString,
		},
		TagKeys:    []string{"site", "line"},
		WindowAggs: []string{"avg", "max"},
	}
}

func benchEnv() Env {
	return NewEnv().Bind(
		map[string]any{"temperature": 65.4, "humidity": 58.1, "voltage": 6.02, "running": true, "mode": "auto"},
		NewMeta(100234, 55, 10231, 1, "车间A", []any{"车间A"},
			map[string]any{"site": "sh"}, time.Now().UTC(), 42),
		map[string]any{"temperature": 58.0, "humidity": 55.0, "voltage": 6.1, "running": false, "mode": "auto"},
		map[string]any{"avg": 62.5, "max": 70.0},
		map[string]any{},
	)
}

// evalSources 覆盖 04 §1.2 的典型形状，性能结论必须按「混合负载」看，
// 只测最简单的那条会给出过于乐观的数字。
var evalSources = []string{
	"msg.temperature > 60",
	"msg.temperature > 60 && meta.tags.site == 'sh'",
	"prev != nil && msg.temperature - prev.temperature > 5",
	"abs(msg.temperature - 30) < 0.5 && window.avg > 60 && meta.seq > 0",
	"msg.temperature in [20, 25, 30] || msg.mode matches '^au'",
}

func compileAll(tb testing.TB, sch *DeviceSchema) []*Program {
	tb.Helper()

	progs := make([]*Program, 0, len(evalSources))
	for _, src := range evalSources {
		p, err := Compile("bench", src, sch, nil)
		if err != nil {
			tb.Fatalf("编译 %s 失败: %v", src, err)
		}
		progs = append(progs, p)
	}
	return progs
}

func BenchmarkEval_单条件(b *testing.B) {
	p, err := Compile("bench", "msg.temperature > 60", benchSchema(), nil)
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnv()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Eval(env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEval_典型(b *testing.B) {
	p, err := Compile("bench", "msg.temperature > 60 && meta.tags.site == 'sh'", benchSchema(), nil)
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnv()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Eval(env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEval_含prev与窗口(b *testing.B) {
	p, err := Compile("bench",
		"abs(msg.temperature - 30) < 0.5 && window.avg > 60 && meta.seq > 0 && prev != nil && msg.temperature - prev.temperature > 5",
		benchSchema(), nil)
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnv()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Eval(env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEval_混合五条(b *testing.B) {
	progs := compileAll(b, benchSchema())
	env := benchEnv()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := progs[i%len(progs)].Eval(env); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompile_缓存命中 对应 04 §1.7 的「编译缓存命中率 ≥ 99.99%」。
func BenchmarkCompile_缓存命中(b *testing.B) {
	c := NewCompiler(10000)
	sch := benchSchema()
	src := evalSources[1]
	key := CacheKey("r1", 1, src)

	if _, err := c.Compile(key, src, sch); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Compile(key, src, sch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompile_冷编译 是保存规则时的成本（控制台交互，预算宽松）。
func BenchmarkCompile_冷编译(b *testing.B) {
	sch := benchSchema()
	src := evalSources[1]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Compile("bench", src, sch, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// TestPerf_EvalP99 是 C1 验收线的可执行形式：**求值 P99 ≤ 2 µs**。
//
// 默认只测量并输出，不硬失败 —— 共享宿主上的绝对耗时波动很大，
// 把阈值塞进 CI 只会制造假警报。需要作为门禁时显式开启：
//
//	IOT_PERF_ASSERT=1 go test ./internal/rules -run TestPerf -v
func TestPerf_EvalP99(t *testing.T) {
	progs := compileAll(t, benchSchema())
	env := benchEnv()

	const rounds = 200_000
	lats := make([]time.Duration, 0, rounds)

	// 预热：让 sync.Pool 与 CPU 频率先稳定下来。
	warm := NewRunner()
	for i := 0; i < 20_000; i++ {
		_, _ = warm.Eval(progs[i%len(progs)], env)
	}

	runner := NewRunner()
	for i := 0; i < rounds; i++ {
		p := progs[i%len(progs)]
		start := time.Now()
		if _, err := runner.Eval(p, env); err != nil {
			t.Fatalf("求值失败: %v", err)
		}
		lats = append(lats, time.Since(start))
	}

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p50, p90, p99, p999 := pct(lats, 0.50), pct(lats, 0.90), pct(lats, 0.99), pct(lats, 0.999)

	t.Logf("求值延迟（%d 次混合，5 条表达式轮流，Runner 复用 VM）：", rounds)
	t.Logf("  P50=%v  P90=%v  P99=%v  P99.9=%v  max=%v", p50, p90, p99, p999, lats[len(lats)-1])
	t.Logf("  验收线 P99 ≤ %v → %s（%.2f× 余量）", targetEvalP99, verdictText(p99 <= targetEvalP99), float64(targetEvalP99)/float64(p99))

	if os.Getenv("IOT_PERF_ASSERT") == "" {
		t.Log("（未设置 IOT_PERF_ASSERT，仅测量不判定）")
		return
	}
	if p99 > targetEvalP99 {
		t.Fatalf("P99 = %v 超过验收线 %v", p99, targetEvalP99)
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func verdictText(ok bool) string {
	if ok {
		return "达标"
	}
	return "不达标"
}

// BenchmarkEval_常量 是 expr.Run 的**成本地板**：不访问任何字段，
// 用于把「VM 调度 + 装箱」与「表达式本身的成本」区分开。
func BenchmarkEval_常量(b *testing.B) {
	p, err := Compile("bench", "1 > 0", benchSchema(), nil)
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnv()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Eval(env); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEval_复用VM 对比「每次走 expr.Run（内部从 sync.Pool 取 VM）」
// 与「调用方持有 Runner 反复用」——这是决定要不要额外暴露 Runner 的依据。
func BenchmarkEval_复用VM(b *testing.B) {
	p, err := Compile("bench", "msg.temperature > 60 && meta.tags.site == 'sh'", benchSchema(), nil)
	if err != nil {
		b.Fatal(err)
	}
	env := map[string]any(benchEnv())
	shared := new(vm.VM)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := shared.Run(p.prog, env); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEval_Runner 是上面那条对照的「正式 API 版」。
func BenchmarkEval_Runner(b *testing.B) {
	p, err := Compile("bench", "msg.temperature > 60 && meta.tags.site == 'sh'", benchSchema(), nil)
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnv()
	r := NewRunner()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Eval(p, env); err != nil {
			b.Fatal(err)
		}
	}
}
