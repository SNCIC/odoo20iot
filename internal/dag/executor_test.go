package dag

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/rules"
)

func testEnv() rules.Env {
	return rules.NewEnv().Bind(
		map[string]any{"temperature": 65.0},
		map[string]any{"device_id": int64(7)},
		nil,
		map[string]any{"avg": 72.5},
		map[string]any{"mode": "auto"},
	)
}

// mustCompile 编译一个测试用 DAG。
func mustCompile(t *testing.T, reg *Registry, def Definition) *Graph {
	t.Helper()
	g, err := Compile(def, testDeps(t, reg))
	if err != nil {
		t.Fatalf("编译失败: %v", err)
	}
	return g
}

// ---------------------------------------------------------------------------
// 基本节点
// ---------------------------------------------------------------------------

func TestExecutor_condition分支(t *testing.T) {
	rec := newRecord()
	reg := newRegistry(t, func(name string, p map[string]any) error { rec.add(name, p); return nil })

	def := Definition{Entry: "n1", Nodes: []Node{
		{ID: "n1", Type: NodeCondition, Expr: "window.avg > 60", OnTrue: "yes", OnFalse: "no"},
		{ID: "yes", Type: NodeAction, Action: "alarm.raise"},
		{ID: "no", Type: NodeAction, Action: "alarm.clear"},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if got := rec.names(); len(got) != 1 || got[0] != "alarm.raise" {
		t.Fatalf("window.avg=72.5 > 60 应走 true 分支，实际调用 %v", got)
	}
	if res.Failed {
		t.Fatalf("不该失败: %+v", res.Steps)
	}
	if res.LastNode != "yes" {
		t.Fatalf("LastNode 应为 yes，得到 %s", res.LastNode)
	}
}

func TestExecutor_branch多路分支(t *testing.T) {
	rec := newRecord()
	reg := newRegistry(t, func(name string, p map[string]any) error { rec.add(name, p); return nil })

	def := Definition{Entry: "sw", Nodes: []Node{
		{ID: "sw", Type: NodeBranch, Cases: []Case{
			{Expr: "msg.temperature > 100", Next: "hot"},
			{Expr: "msg.temperature > 50", Next: "warm"},
		}, Default: "cold"},
		{ID: "hot", Type: NodeAction, Action: "alarm.raise"},
		{ID: "warm", Type: NodeAction, Action: "command.send"},
		{ID: "cold", Type: NodeAction, Action: "alarm.clear"},
	}}
	g := mustCompile(t, reg, def)

	// temperature=65 → 第一路 false、第二路 true → warm
	if _, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{}); err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if got := rec.names(); len(got) != 1 || got[0] != "command.send" {
		t.Fatalf("应命中第二路 case（warm），实际 %v", got)
	}

	// 都不命中 → default
	env := rules.NewEnv().Bind(map[string]any{"temperature": 1.0}, nil, nil, nil, nil)
	if _, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, env, RunOptions{}); err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if got := rec.names(); len(got) != 2 || got[1] != "alarm.clear" {
		t.Fatalf("都不命中应走 default，实际 %v", got)
	}
}

// TestExecutor_参数变量解析 验证 `$window.avg` 这类引用在动作里拿到的是值。
func TestExecutor_参数变量解析(t *testing.T) {
	rec := newRecord()
	reg := newRegistry(t, func(name string, p map[string]any) error { rec.add(name, p); return nil })

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "command.send", Params: map[string]any{
			"device_id": "$meta.device_id",
			"data":      map[string]any{"speed": 3, "avg": "$window.avg"},
			"tags":      []any{"x", "$state.mode"},
		}},
	}}
	g := mustCompile(t, reg, def)

	if _, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{}); err != nil {
		t.Fatalf("执行失败: %v", err)
	}

	params := rec.paramsOf("command.send")[0]
	if got := params["data"].(map[string]any)["avg"]; got != 72.5 {
		t.Fatalf("$window.avg 应解析为 72.5，得到 %v", got)
	}
	if got := params["tags"].([]any)[1]; got != "auto" {
		t.Fatalf("$state.mode 应解析为 auto，得到 %v", got)
	}
}

func TestExecutor_delay(t *testing.T) {
	rec := newRecord()
	reg := newRegistry(t, func(name string, p map[string]any) error { rec.add(name, p); return nil })

	def := Definition{Entry: "d", Nodes: []Node{
		{ID: "d", Type: NodeDelay, MS: 30, Next: "a"},
		{ID: "a", Type: NodeAction, Action: "alarm.raise"},
	}}
	g := mustCompile(t, reg, def)

	started := time.Now()
	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 25*time.Millisecond {
		t.Fatalf("delay 应真的等待，实际只用了 %s", elapsed)
	}
	if len(res.Steps) != 2 || res.Steps[0].Type != NodeDelay {
		t.Fatalf("轨迹不符: %+v", res.Steps)
	}
}

// TestExecutor_parallel并发上限 验证 limit 真的限制了并发度。
func TestExecutor_parallel并发上限(t *testing.T) {
	var (
		mu        sync.Mutex
		active    int
		maxActive int
		done      int
	)
	reg := newRegistry(t, func(_ string, _ map[string]any) error {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()

		time.Sleep(30 * time.Millisecond)

		mu.Lock()
		active--
		done++
		mu.Unlock()
		return nil
	})

	def := Definition{Entry: "p", Nodes: []Node{
		{ID: "p", Type: NodeParallel, Branches: []string{"b1", "b2", "b3"}, Limit: 2},
		{ID: "b1", Type: NodeAction, Action: "alarm.raise"},
		{ID: "b2", Type: NodeAction, Action: "alarm.raise"},
		{ID: "b3", Type: NodeAction, Action: "alarm.raise"},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if done != 3 {
		t.Fatalf("三个分支都应执行，实际 %d", done)
	}
	if maxActive != 2 {
		t.Fatalf("并发度应为 limit=2，实际峰值 %d", maxActive)
	}
	// 轨迹：1 个 parallel 节点 + 3 个分支动作
	if len(res.Steps) != 4 {
		t.Fatalf("轨迹应含 parallel + 3 分支，得到 %d", len(res.Steps))
	}
}

// ---------------------------------------------------------------------------
// 失败策略（04 §1.5）
// ---------------------------------------------------------------------------

func TestExecutor_drop策略失败继续(t *testing.T) {
	rec := newRecord()
	reg := newRegistry(t, func(name string, p map[string]any) error {
		rec.add(name, p)
		if name == "command.send" {
			return errors.New("设备离线")
		}
		return nil
	})

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "command.send", Next: "b"},
		{ID: "b", Type: NodeAction, Action: "notify.send"},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{Policy: PolicyDrop})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if !res.Failed {
		t.Fatal("应有失败标记")
	}
	// drop：失败后继续执行后续节点
	if got := rec.names(); len(got) != 2 {
		t.Fatalf("drop 策略下应继续执行 b，实际调用 %v", got)
	}
}

func TestExecutor_dlq策略立即中断(t *testing.T) {
	rec := newRecord()
	reg := newRegistry(t, func(name string, p map[string]any) error {
		rec.add(name, p)
		return errors.New("炸了")
	})

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "command.send", Next: "b"},
		{ID: "b", Type: NodeAction, Action: "notify.send"},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{Policy: PolicyDLQ})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if got := rec.names(); len(got) != 1 {
		t.Fatalf("dlq 策略应立即中断，实际调用 %v", got)
	}
	if !res.Failed || res.LastNode != "a" {
		t.Fatalf("结果不符: %+v", res)
	}
}

func TestExecutor_retry策略重试到成功(t *testing.T) {
	var attempts int
	reg := newRegistry(t, func(_ string, _ map[string]any) error {
		attempts++
		if attempts < 3 {
			return errors.New("抖动")
		}
		return nil
	})

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "alarm.raise", // 幂等
			Retry: &Retry{Max: 3, Backoff: "fixed", BaseMS: 1}},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{Policy: PolicyRetry})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("应第 3 次成功，实际尝试 %d 次", attempts)
	}
	if res.Failed {
		t.Fatal("最终成功不该标记失败")
	}
	if res.Steps[0].Attempts != 3 {
		t.Fatalf("轨迹应记 3 次尝试，得到 %d", res.Steps[0].Attempts)
	}
}

func TestExecutor_retry策略耗尽(t *testing.T) {
	var attempts int
	reg := newRegistry(t, func(_ string, _ map[string]any) error {
		attempts++
		return errors.New("一直失败")
	})

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "alarm.raise",
			Retry: &Retry{Max: 2, Backoff: "exponential", BaseMS: 1}},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{Policy: PolicyRetry})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	// Max=2 是**重试次数**（不含首次），故总尝试 3 次。
	if attempts != 3 {
		t.Fatalf("Max=2 应共尝试 3 次，实际 %d", attempts)
	}
	if !res.Failed {
		t.Fatal("耗尽后应标记失败")
	}
}

// TestExecutor_不重试时只执行一次 验证 drop 策略下节点自带的 retry 配置不生效
// （04 §1.5：重试只在 error_policy=retry 时发生）。
func TestExecutor_不重试时只执行一次(t *testing.T) {
	var attempts int
	reg := newRegistry(t, func(_ string, _ map[string]any) error {
		attempts++
		return errors.New("失败")
	})

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "alarm.raise",
			Retry: &Retry{Max: 3, Backoff: "fixed", BaseMS: 1}},
	}}
	g := mustCompile(t, reg, def)

	if _, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{Policy: PolicyDrop}); err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("drop 策略下不该重试，实际 %d 次", attempts)
	}
}

// TestExecutor_变量缺失不重试 验证「配置错误」不会浪费重试次数 ——
// 参数不会因为再试一次就存在（04 §1.6：这类错误应在保存期挡住）。
func TestExecutor_变量缺失不重试(t *testing.T) {
	var attempts int
	reg := newRegistry(t, func(_ string, _ map[string]any) error { attempts++; return nil })

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "alarm.raise",
			Params: map[string]any{"v": "$window.nope"},
			Retry:  &Retry{Max: 3, Backoff: "fixed", BaseMS: 1}},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{Policy: PolicyRetry})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if attempts != 0 {
		t.Fatalf("变量解析失败时不该调用动作，实际 %d 次", attempts)
	}
	if !res.Failed || res.Steps[0].Attempts != 0 {
		t.Fatalf("结果不符: %+v", res.Steps)
	}
}

// ---------------------------------------------------------------------------
// 超时（04 §1.3：单节点 5s / 整体 30s）
// ---------------------------------------------------------------------------

func TestExecutor_整体超时(t *testing.T) {
	reg := newRegistry(t, func(_ string, _ map[string]any) error { return nil })

	def := Definition{Entry: "d", Nodes: []Node{
		{ID: "d", Type: NodeDelay, MS: 500, Next: "end"},
		{ID: "end", Type: NodeEnd},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(),
		RunOptions{Timeout: 40 * time.Millisecond})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if !res.TimedOut {
		t.Fatalf("应标记整体超时，得到 %+v", res)
	}
}

// TestExecutor_单节点超时不算整体超时 验证两者不会被混为一谈 ——
// 混了会让「一个动作慢」被误报成「整个 DAG 被中断」。
func TestExecutor_单节点超时不算整体超时(t *testing.T) {
	// 这个动作**尊重 ctx**：真实动作（HTTP 调用、命令下发）都必须如此，
	// 否则节点超时形同虚设 —— 本包的其余测试用不感知 ctx 的桩，是刻意的简化。
	blocking := Func{ActionName: "command.send", Idem: true,
		Run: func(ctx context.Context, _ map[string]any) error {
			select {
			case <-time.After(200 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
	reg, err := NewRegistry(blocking)
	if err != nil {
		t.Fatalf("构造注册表失败: %v", err)
	}

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "command.send", TimeoutMS: 30},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if res.TimedOut {
		t.Fatal("单节点超时不该标记为整体超时")
	}
	if !res.Failed {
		t.Fatal("该节点应记为失败")
	}
}

// ---------------------------------------------------------------------------
// 动作白名单与注册表
// ---------------------------------------------------------------------------

// TestExecutor_未注册动作无法编译 验证白名单在保存期生效，
// 而不是等执行时才发现「这个动作不存在」。
func TestExecutor_未注册动作无法编译(t *testing.T) {
	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "rm.rf"},
	}}
	if _, err := Compile(def, testDeps(t, newRegistry(t, nil))); err == nil {
		t.Fatal("未注册动作应编译失败")
	}
}

func TestNewExecutor_默认日志与runner(t *testing.T) {
	e := NewExecutor(nil, nil)
	if e.runner == nil || e.logger == nil {
		t.Fatal("应补齐默认依赖")
	}
}

// TestExecutor_轨迹含失败信息 验证 04 §1.3「写执行日志」有可用的载体。
func TestExecutor_轨迹含失败信息(t *testing.T) {
	reg := newRegistry(t, func(_ string, _ map[string]any) error { return errors.New("设备离线") })

	def := Definition{Entry: "a", Nodes: []Node{
		{ID: "a", Type: NodeAction, Action: "command.send", Next: "end"},
		{ID: "end", Type: NodeEnd},
	}}
	g := mustCompile(t, reg, def)

	res, err := NewExecutor(nil, testLogger()).Run(context.Background(), g, testEnv(), RunOptions{Policy: PolicyDrop})
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if res.Steps[0].Status != StatusFailed || res.Steps[0].Err == nil {
		t.Fatalf("轨迹应含失败与原因: %+v", res.Steps[0])
	}
	if !strings.Contains(res.Steps[0].Err.Error(), "设备离线") {
		t.Fatalf("失败原因应可读: %v", res.Steps[0].Err)
	}
	if res.Steps[1].Status != StatusOK {
		t.Fatalf("后续节点应正常执行: %+v", res.Steps[1])
	}
	_ = fmt.Sprint(res.Steps)
}
