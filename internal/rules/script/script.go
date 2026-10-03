// Package script 提供规则 L3 逃生舱。
//
// 脚本默认关闭。启用时只把四个 JSON 风格命名空间注入 JS VM，绝不暴露
// Go 对象、网络客户端、文件系统或服务依赖；执行超时由 goja interrupt 强制中断。
package script

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dop251/goja"
)

var ErrDisabled = errors.New("rules: script.run 未启用")

type Config struct {
	Enabled      bool
	Timeout      time.Duration
	MaxSourceLen int
}

type Input struct {
	Msg          map[string]any
	Meta         map[string]any
	Prev         map[string]any
	State        map[string]any
	Capabilities []string
}

type Engine struct{ cfg Config }

type EmitFunc func(context.Context, string, any) error

// Action 是 DAG 注册表可直接使用的 script.run 动作适配器。
// 参数约定：source 为脚本正文，msg/meta/prev/state 为可选输入映射。
type Action struct {
	Engine *Engine
	Emit   EmitFunc
}

func (Action) Name() string     { return "script.run" }
func (Action) Idempotent() bool { return false }

func (a Action) Do(ctx context.Context, params map[string]any) error {
	if a.Engine == nil {
		return fmt.Errorf("rules: script.run 未配置引擎")
	}
	source, ok := params["source"].(string)
	if !ok {
		return fmt.Errorf("rules: script.run 缺少 source")
	}
	_, err := a.Engine.run(ctx, source, Input{
		Msg:          mapParam(params, "msg"),
		Meta:         mapParam(params, "meta"),
		Prev:         mapParam(params, "prev"),
		State:        mapParam(params, "state"),
		Capabilities: stringSliceParam(params, "capabilities"),
	}, func(ctx context.Context, kind string, value any) error {
		if kind != "alarm" {
			return fmt.Errorf("rules: emit 类型 %q 未实现", kind)
		}
		payload, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("rules: emit alarm payload 必须是对象")
		}
		for _, key := range []string{"project_id", "device_id", "device_type_id", "rule_id", "rule_name"} {
			if trusted, exists := params[key]; exists {
				payload[key] = trusted
			}
		}
		if a.Emit == nil {
			return fmt.Errorf("rules: emit alarm 未配置输出处理器")
		}
		return a.Emit(ctx, kind, payload)
	})
	if err != nil {
		return err
	}
	return nil
}

func mapParam(params map[string]any, key string) map[string]any {
	value, _ := params[key].(map[string]any)
	return value
}

func stringSliceParam(params map[string]any, key string) []string {
	var values []string
	switch raw := params[key].(type) {
	case []string:
		return append(values, raw...)
	case []any:
		for _, item := range raw {
			if value, ok := item.(string); ok {
				values = append(values, value)
			}
		}
	}
	return values
}

func New(cfg Config) (*Engine, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 50 * time.Millisecond
	}
	if cfg.MaxSourceLen <= 0 {
		cfg.MaxSourceLen = 32 * 1024
	}
	if cfg.Timeout > time.Second {
		return nil, fmt.Errorf("rules: script 超时上限为 1s")
	}
	return &Engine{cfg: cfg}, nil
}

func (e *Engine) Run(ctx context.Context, source string, in Input) (any, error) {
	return e.run(ctx, source, in, nil)
}

func (e *Engine) run(ctx context.Context, source string, in Input, emitter EmitFunc) (any, error) {
	if !e.cfg.Enabled {
		return nil, ErrDisabled
	}
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("rules: script 不能为空")
	}
	if len(source) > e.cfg.MaxSourceLen {
		return nil, fmt.Errorf("rules: script 超过 %d 字节", e.cfg.MaxSourceLen)
	}
	if err := validateCapabilities(in.Capabilities); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()

	vm := goja.New()
	if err := vm.Set("msg", in.Msg); err != nil {
		return nil, err
	}
	if err := vm.Set("meta", in.Meta); err != nil {
		return nil, err
	}
	if err := vm.Set("prev", in.Prev); err != nil {
		return nil, err
	}
	state := in.State
	if state == nil {
		state = map[string]any{}
	}
	if hasCapability(in.Capabilities, "state") {
		stateObject := vm.NewObject()
		for key, value := range state {
			if err := stateObject.Set(key, value); err != nil {
				return nil, err
			}
		}
		if err := stateObject.Set("get", func(key string) any { return state[key] }); err != nil {
			return nil, err
		}
		if err := stateObject.Set("set", func(key string, value any) { state[key] = value }); err != nil {
			return nil, err
		}
		if err := vm.Set("state", stateObject); err != nil {
			return nil, err
		}
	} else if err := vm.Set("state", state); err != nil {
		return nil, err
	}
	// L3 只允许纯计算：关闭动态代码执行入口；网络、文件和 Go 对象
	// 不会注入 VM，因此 require/import 也不可用。
	if err := vm.Set("eval", nil); err != nil {
		return nil, err
	}
	if err := vm.Set("Function", nil); err != nil {
		return nil, err
	}
	if err := vm.Set("emit", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 2 {
			panic(vm.NewGoError(fmt.Errorf("rules: emit 需要 kind 和 payload 两个参数")))
		}
		kindValue, ok := call.Argument(0).Export().(string)
		if !ok || kindValue == "" {
			panic(vm.NewGoError(fmt.Errorf("rules: emit kind 必须是非空字符串")))
		}
		kind := kindValue
		capability := "emit:" + kind
		if !hasCapability(in.Capabilities, capability) {
			panic(vm.NewGoError(fmt.Errorf("rules: emit %q 未声明 capability %q", kind, capability)))
		}
		if emitter == nil {
			panic(vm.NewGoError(fmt.Errorf("rules: emit %q 未配置输出处理器", kind)))
		}
		payload := call.Argument(1).Export()
		if _, ok := payload.(map[string]any); !ok {
			panic(vm.NewGoError(fmt.Errorf("rules: emit payload 必须是对象")))
		}
		if err := emitter(ctx, kind, payload); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	}); err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			vm.Interrupt(ctx.Err())
		case <-done:
		}
	}()
	defer close(done)

	value, err := vm.RunString("(function(){\n" + source + "\n})()")
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("rules: script 执行超时或取消: %w", ctx.Err())
		}
		return nil, fmt.Errorf("rules: script 执行失败: %w", err)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("rules: script 执行超时或取消: %w", ctx.Err())
	}
	return value.Export(), nil
}

func validateCapabilities(capabilities []string) error {
	for _, capability := range capabilities {
		if capability != "script.run" && capability != "state" && capability != "emit:alarm" {
			return fmt.Errorf("rules: script capability %q 未允许", capability)
		}
	}
	return nil
}

func hasCapability(capabilities []string, target string) bool {
	for _, capability := range capabilities {
		if capability == target {
			return true
		}
	}
	return false
}
