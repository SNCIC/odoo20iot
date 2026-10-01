package dag

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/SNCIC/odoo20iot/internal/rules"
)

// DefaultTotalTimeout 是整体超时（04 §1.3）。
const DefaultTotalTimeout = 30 * time.Second

// ErrorPolicy 是规则级的失败策略（04 §1.5 的 `error_policy`）。
type ErrorPolicy string

const (
	// PolicyDrop：节点失败则跳过、继续后续节点。
	PolicyDrop ErrorPolicy = "drop"
	// PolicyRetry：按节点 `retry` 配置重试（上限 MaxRetry）；耗尽后同 drop 继续，
	// 但结果标记为失败。
	PolicyRetry ErrorPolicy = "retry"
	// PolicyDLQ：节点失败立即中断 DAG，由调用方负责投 DLQ。
	PolicyDLQ ErrorPolicy = "dlq"
)

// StepStatus 是一条执行轨迹的状态。
type StepStatus string

const (
	StatusOK      StepStatus = "ok"
	StatusFailed  StepStatus = "failed"
	StatusSkipped StepStatus = "skipped"
)

// Step 是一条执行轨迹。
//
// 04 §1.3 要求「整体超时 → 中断 DAG，**写执行日志**」，所以轨迹是一等产物，
// 不是调试附属物。
type Step struct {
	NodeID   string
	Type     string
	Status   StepStatus
	Attempts int
	Duration time.Duration
	Err      error
}

// Result 是一次执行的结果。
type Result struct {
	Steps []Step
	// LastNode 是最后一个执行到的节点。
	LastNode string
	// Failed 表示有节点失败（drop/retry 策略下 DAG 仍会跑完）。
	Failed bool
	// TimedOut 表示因整体超时被中断。
	TimedOut bool
}

// RunOptions 是单次执行的参数。
type RunOptions struct {
	// Policy 默认 drop。
	Policy ErrorPolicy
	// Timeout 是整体超时，默认 DefaultTotalTimeout（30s）。
	Timeout time.Duration
	Logger  *slog.Logger
}

// Executor 执行编译好的 DAG。
//
// 与 `rules.Runner` 一样**非并发安全**（04 §1.2.1 第 4 条），
// 故每个并发执行者持有自己的 Executor。
type Executor struct {
	runner *rules.Runner
	logger *slog.Logger
}

// NewExecutor 构造执行器。runner 为 nil 时自建一个（仅供单 goroutine 使用）。
func NewExecutor(runner *rules.Runner, logger *slog.Logger) *Executor {
	if runner == nil {
		runner = rules.NewRunner()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Executor{runner: runner, logger: logger}
}

// Run 执行 DAG。
//
// 返回的 error 只表示「DAG 无法执行」（整体超时）；**动作本身的失败体现在
// Result.Steps 与 Result.Failed 上** —— 两者性质不同，混在一个返回值里
// 会让调用方分不清「该重投」还是「该告警」。
func (e *Executor) Run(ctx context.Context, g *Graph, env rules.Env, opts RunOptions) (Result, error) {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTotalTimeout
	}
	if opts.Policy == "" {
		opts.Policy = PolicyDrop
	}

	logger := opts.Logger
	if logger == nil {
		logger = e.logger
	}

	runCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	res := e.runChain(runCtx, g, g.entry, env, opts)

	// 只有**整体**超时才算 TimedOut：单节点的 5s 超时是节点失败，
	// 不该被误报成「整个 DAG 被中断」。
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		res.TimedOut = true
		logger.Warn("DAG 整体超时被中断", "timeout", opts.Timeout,
			"last_node", res.LastNode, "steps", len(res.Steps))
	}
	return res, nil
}

// runChain 从 start 开始沿**单链**执行，直到链条结束或被中断。
//
// parallel 节点会在内部把分支跑完（返回空后继），故本函数只处理单链。
func (e *Executor) runChain(ctx context.Context, g *Graph, start string, env rules.Env, opts RunOptions) Result {
	var res Result
	cur := start

	for cur != "" {
		if ctx.Err() != nil {
			return res
		}

		cn, ok := g.nodes[cur]
		if !ok {
			// Compile 已保证引用存在；走到这里说明图被外部改过。
			e.logger.Error("执行到未编译的节点，中断", "node", cur)
			return res
		}

		steps, next := e.execNode(ctx, g, cn, env, opts)
		for _, step := range steps {
			if step.Err != nil {
				res.Failed = true
			}
			res.Steps = append(res.Steps, step)
		}
		res.LastNode = cn.node.ID

		if res.Failed && opts.Policy == PolicyDLQ && lastFailed(steps) {
			// 04 §1.5：dlq = 立即进 DLQ。中断后由调用方投递。
			e.logger.Warn("节点失败且策略为 dlq，中断 DAG", "node", cn.node.ID)
			return res
		}

		switch len(next) {
		case 0:
			return res
		case 1:
			cur = next[0]
		default:
			// 只有 parallel 会产生多后继，而它已在 execNode 内部跑完分支。
			e.logger.Error("节点返回了多个后继，本实现不支持 join", "node", cn.node.ID)
			return res
		}
	}
	return res
}

func lastFailed(steps []Step) bool {
	for _, s := range steps {
		if s.Err != nil {
			return true
		}
	}
	return false
}

// execNode 执行单个节点，返回本节点产生的轨迹与后续节点。
func (e *Executor) execNode(ctx context.Context, g *Graph, cn *compiledNode, env rules.Env, opts RunOptions) ([]Step, []string) {
	started := time.Now()
	step := Step{NodeID: cn.node.ID, Type: cn.node.Type, Status: StatusOK}

	switch cn.node.Type {
	case NodeEnd:
		step.Duration = time.Since(started)
		return []Step{step}, nil

	case NodeCondition:
		branch, err := e.evalCondition(cn, env)
		if err != nil {
			step.Status, step.Err = StatusFailed, err
			return []Step{step}, nil
		}
		step.Duration = time.Since(started)
		if branch {
			return []Step{step}, []string{cn.node.OnTrue}
		}
		return []Step{step}, []string{cn.node.OnFalse}

	case NodeBranch:
		next, err := e.evalBranch(cn, env)
		if err != nil {
			step.Status, step.Err = StatusFailed, err
			return []Step{step}, nil
		}
		step.Duration = time.Since(started)
		return []Step{step}, []string{next}

	case NodeAction:
		attempts, err := e.execAction(ctx, cn, env, opts)
		step.Attempts = attempts
		if err != nil {
			step.Status, step.Err = StatusFailed, err
		}
		step.Duration = time.Since(started)
		// ⚠️ action 的 `next` 是**可选**的（04 §1.5 的示例里 action 就没有 next），
		// 缺省表示「链到此为止」。这是本实现在文档缺口处的取值，已记入文档。
		return []Step{step}, nonEmpty(cn.node.Next)

	case NodeDelay:
		if err := sleepCtx(ctx, time.Duration(cn.node.MS)*time.Millisecond); err != nil {
			step.Status, step.Err = StatusFailed, err
			step.Duration = time.Since(started)
			return []Step{step}, nil
		}
		step.Duration = time.Since(started)
		return []Step{step}, []string{cn.node.Next}

	case NodeParallel:
		inner := e.runParallel(ctx, g, cn, env, opts)
		step.Attempts = 1
		for _, s := range inner {
			if s.Err != nil {
				step.Status, step.Err = StatusFailed, fmt.Errorf("并行分支中节点 %s 失败", s.NodeID)
				break
			}
		}
		step.Duration = time.Since(started)

		// 分支轨迹附在 parallel 节点之后，保持「一条链一套完整轨迹」的可读性。
		out := make([]Step, 0, len(inner)+1)
		out = append(out, step)
		out = append(out, inner...)
		return out, nil
	}

	step.Duration = time.Since(started)
	return []Step{step}, nonEmpty(cn.node.Next)
}

func (e *Executor) evalCondition(cn *compiledNode, env rules.Env) (bool, error) {
	if cn.program == nil {
		return false, fmt.Errorf("节点 %s 的表达式未编译", cn.node.ID)
	}
	// condition 的求值环境就是 L1 的 Env（msg/meta/prev/window/state）——
	// 复用 L1 的表达式引擎，不另建一套（04 §1.3）。
	return cn.program.Eval(env)
}

func (e *Executor) evalBranch(cn *compiledNode, env rules.Env) (string, error) {
	for i, cs := range cn.node.Cases {
		if i >= len(cn.casePrograms) || cn.casePrograms[i] == nil {
			return "", fmt.Errorf("节点 %s 第 %d 路 case 的表达式未编译", cn.node.ID, i+1)
		}
		ok, err := cn.casePrograms[i].Eval(env)
		if err != nil {
			return "", fmt.Errorf("节点 %s 第 %d 路 case 求值失败: %w", cn.node.ID, i+1, err)
		}
		if ok {
			return cs.Next, nil
		}
	}
	return cn.node.Default, nil
}

// runParallel 并发执行各分支，返回分支产生的轨迹。
//
// ⚠️ 语义：**每个分支各自执行到链条结束，本实现不支持 join（多分支汇聚）** ——
// 04 §1.3 定义了「并发执行多个分支」与并发度 `limit`，但**没有定义汇聚语义**
// （全等？任一？）。凭空定一个会让用户的预期与实现不一致，故如实留白：
// 若两条分支可达同一节点，该节点会被执行两次；Compile 会就此给出警告。
func (e *Executor) runParallel(ctx context.Context, g *Graph, cn *compiledNode, env rules.Env, opts RunOptions) []Step {
	limit := cn.node.Limit
	if limit <= 0 {
		limit = DefaultBranchLimit
	}
	if limit > len(cn.node.Branches) {
		limit = len(cn.node.Branches)
	}

	sem := make(chan struct{}, limit)
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		inner []Step
	)

	for _, branch := range cn.node.Branches {
		wg.Add(1)
		go func(start string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			sub := e.runChain(ctx, g, start, env, opts)
			mu.Lock()
			defer mu.Unlock()
			inner = append(inner, sub.Steps...)
		}(branch)
	}
	wg.Wait()
	return inner
}

// execAction 执行 action 节点并返回尝试次数。
func (e *Executor) execAction(ctx context.Context, cn *compiledNode, env rules.Env, opts RunOptions) (int, error) {
	if cn.action == nil {
		return 0, fmt.Errorf("节点 %s 的动作 %q 未解析", cn.node.ID, cn.node.Action)
	}

	params, err := ResolveParams(cn.node.Params, env)
	if err != nil {
		// 变量取不到值属于**配置错误**（04 §1.6：报错回显给用户），
		// 重试没有意义 —— 参数不会因为再试一次就存在。
		return 0, err
	}

	timeout := DefaultNodeTimeout
	if cn.node.TimeoutMS > 0 {
		timeout = time.Duration(cn.node.TimeoutMS) * time.Millisecond
	}

	// 重试只在 error_policy=retry 时生效（04 §1.5：drop/retry/dlq 三选一）。
	retries := 0
	if opts.Policy == PolicyRetry && cn.node.Retry != nil {
		retries = cn.node.Retry.Max
		if retries > MaxRetry {
			retries = MaxRetry
		}
	}

	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			if !cn.action.Idempotent() {
				// 04 §1.3 的动作表逐项标了幂等性：对不幂等的动作重试等于
				// 制造重复副作用（重复通知、重复下发命令）。
				e.logger.Warn("对声明为不幂等的动作重试，可能产生重复副作用",
					"node", cn.node.ID, "action", cn.node.Action, "attempt", attempt+1)
			}
			if err := sleepCtx(ctx, backoffDelay(cn.node.Retry, attempt)); err != nil {
				return attempt, err
			}
		}

		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		lastErr = cn.action.Do(attemptCtx, params)
		cancel()
		if lastErr == nil {
			return attempt + 1, nil
		}
		if ctx.Err() != nil {
			return attempt + 1, ctx.Err()
		}
	}
	return retries + 1, lastErr
}

// backoffDelay 计算第 attempt 次重试（从 1 开始）前的等待。
func backoffDelay(r *Retry, attempt int) time.Duration {
	if r == nil || r.BaseMS <= 0 {
		return 0
	}
	base := time.Duration(r.BaseMS) * time.Millisecond
	if r.Backoff == "exponential" {
		// MaxRetry 上限为 3，移位不会溢出。
		return base << uint(attempt-1)
	}
	return base
}

func nonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

// sleepCtx 等待 d；d<=0 时立即返回（并如实反映 ctx 状态）。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
