package rules

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/parser"
	"github.com/expr-lang/expr/vm"
)

// MaxNodes 是单条表达式的 AST 节点上限。
//
// 04 §1.2 原本规定「设 20 ms 超时作防御性兜底」。C1 实测后改为**编译期节点上限**：
//   - expr 无循环、无 IO，求值时间上界由 AST 规模决定；
//   - 用 goroutine + select 做运行时超时，每次求值要多付一次调度与 channel 开销，
//     直接把 P99 ≤ 2 µs 的预算吃掉（实测见 04 §1.2.1）。
//
// 因此把「不可控」换成「可控」：编译期限死规模，运行时不再有超时分支。
const MaxNodes = 512

// Program 是一条编译通过的规则表达式。
type Program struct {
	prog *vm.Program
	src  string
	key  string
}

// Source 返回原始表达式文本（控制台回显用）。
func (p *Program) Source() string { return p.src }

// run 执行字节码。单独抽出来是为了让 Eval 的类型断言逻辑保持在一处。
func (p *Program) run(env Env) (any, error) {
	// expr 的 VM 对环境做 `map[string]any` 类型断言，命名类型不满足；
	// 这里是一次零成本的底层类型转换。
	return expr.Run(p.prog, envMap(env))
}

// envMap 把命名类型转成 VM 认得的底层类型（零成本转换）。
func envMap(env Env) map[string]any { return map[string]any(env) }

// Runner 是**非并发安全**的求值器，内部持有一个可复用的 expr VM。
//
// 为什么需要它：`expr.Run` 每次求值都要从 sync.Pool 取一次 VM，
// 实测这一进一出加上结果断言要花掉约 0.3 µs —— 在 04 §1.7 的
// 「≤ 2 µs / 次」预算里占 15%+，是纯浪费。
//
// 用法：每个消费 goroutine 持有一个 Runner（不可跨 goroutine 共享），
// 在它上面反复调用 Eval。拿不准并发模型时直接用 Program.Eval 即可。
type Runner struct {
	vm *vm.VM
}

// NewRunner 构造求值器。
func NewRunner() *Runner { return &Runner{vm: new(vm.VM)} }

// Eval 复用内部 VM 求值。语义与 Program.Eval 完全一致。
func (r *Runner) Eval(p *Program, env Env) (bool, error) {
	return asBool(r.vm.Run(p.prog, envMap(env)))
}

// compileOptions 组装 expr 的编译选项。
//
// 注意这里**不是**唯一防线：expr 的内置函数被整体关闭只是纵深防御，
// 真正的白名单校验在我们的 AST 门禁里（gate.go）。原因见 §1.2.1 ——
// `expr.DisableAllBuiltins()` 挡不住 all/filter/map 这类谓词语法。
func compileOptions() []expr.Option {
	opts := []expr.Option{
		// 环境只提供「五个命名空间存在」这一事实；字段与类型的校验
		// 完全在我们的门禁里（gate.go），不依赖 expr 的反射式检查。
		expr.Env(NewEnv()),
		expr.AsBool(),
		expr.DisableAllBuiltins(),
		expr.MaxNodes(MaxNodes),
		// 关闭常量折叠以外的优化？保持默认（optimizer 会做常量折叠，
		// 对形如 `x > 60 && true` 的表达式有收益，且不改变语义）。
	}
	return append(opts, functionOptions()...)
}

// Compiler 负责「校验 → 编译 → 缓存」。
//
// 缓存键是 `rule_id + version + 表达式哈希`（04 §1.2 规定），
// 因此改表达式或改版本都会自然失效，不需要显式清除。
type Compiler struct {
	opts     []expr.Option
	capacity int

	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List // 前端 = 最近使用

	hits   atomic.Int64
	misses atomic.Int64
}

// CacheStats 是编译缓存的命中情况（用于 §1.7 的「命中率 ≥ 99.99%」口径）。
type CacheStats struct {
	Hits     int64
	Misses   int64
	Size     int
	Capacity int
}

// HitRate 返回命中率；没有任何请求时返回 1。
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 1
	}
	return float64(s.Hits) / float64(total)
}

// NewCompiler 构造编译器。capacity ≤ 0 时取 10000（04 §1.2 的规定值）。
func NewCompiler(capacity int) *Compiler {
	if capacity <= 0 {
		capacity = 10000
	}
	return &Compiler{
		opts:     compileOptions(),
		capacity: capacity,
		entries:  make(map[string]*list.Element, capacity),
		lru:      list.New(),
	}
}

// CacheKey 按 04 §1.2 的口径生成缓存键。
func CacheKey(ruleID string, version int, src string) string {
	sum := sha256.Sum256([]byte(src))
	return fmt.Sprintf("%s:%d:%s", ruleID, version, hex.EncodeToString(sum[:8]))
}

// Compile 校验并编译一条规则表达式。
//
// 返回的 error 可能是：
//   - *CompileError —— 我们的静态校验没通过（含字段建议）；
//   - expr 自身的解析/编译错误（语法错误等）。
func (c *Compiler) Compile(key, src string, sch *DeviceSchema) (*Program, error) {
	if p := c.lookup(key); p != nil {
		return p, nil
	}

	prog, err := Compile(key, src, sch, c.opts)
	if err != nil {
		return nil, err
	}

	c.store(key, prog)
	return prog, nil
}

// Compile 是一次性的「校验 + 编译」，不走缓存。
//
// 校验发生在 expr 编译**之前**：这样报错信息是我们的（带字段建议与源码定位），
// 而不是库的反射式错误。
func Compile(key, src string, sch *DeviceSchema, opts []expr.Option) (*Program, error) {
	if sch == nil {
		sch = &DeviceSchema{}
	}

	tree, err := parser.Parse(src)
	if err != nil {
		return nil, fmt.Errorf("表达式语法错误: %w", err)
	}
	if cerr := check(src, &tree.Node, sch); cerr != nil {
		return nil, cerr
	}

	if opts == nil {
		opts = compileOptions()
	}
	prog, err := expr.Compile(src, opts...)
	if err != nil {
		// 走到这里说明门禁放过了库不接受的东西 —— 属于我们的漏洞，
		// 原样透出以便定位，不要吞掉。
		return nil, fmt.Errorf("表达式编译失败（门禁未拦住，可能是校验器缺陷）: %w", err)
	}

	return &Program{prog: prog, src: src, key: key}, nil
}

func (c *Compiler) lookup(key string) *Program {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.entries[key]
	if !ok {
		c.misses.Add(1)
		return nil
	}
	c.lru.MoveToFront(el)
	c.hits.Add(1)
	return el.Value.(*Program)
}

func (c *Compiler) store(key string, p *Program) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.entries[key]; ok {
		el.Value = p
		c.lru.MoveToFront(el)
		return
	}

	c.entries[key] = c.lru.PushFront(p)
	for c.lru.Len() > c.capacity {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.lru.Remove(back)
		delete(c.entries, back.Value.(*Program).key)
	}
}

// Stats 返回缓存统计。
func (c *Compiler) Stats() CacheStats {
	c.mu.Lock()
	size := c.lru.Len()
	c.mu.Unlock()

	return CacheStats{
		Hits:     c.hits.Load(),
		Misses:   c.misses.Load(),
		Size:     size,
		Capacity: c.capacity,
	}
}
