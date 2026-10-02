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
	Msg   map[string]any
	Meta  map[string]any
	Prev  map[string]any
	State map[string]any
}

type Engine struct{ cfg Config }

// Action 是 DAG 注册表可直接使用的 script.run 动作适配器。
// 参数约定：source 为脚本正文，msg/meta/prev/state 为可选输入映射。
type Action struct{ Engine *Engine }

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
	value, err := a.Engine.Run(ctx, source, Input{
		Msg:   mapParam(params, "msg"),
		Meta:  mapParam(params, "meta"),
		Prev:  mapParam(params, "prev"),
		State: mapParam(params, "state"),
	})
	if err != nil {
		return err
	}
	if value == nil {
		return nil
	}
	return nil
}

func mapParam(params map[string]any, key string) map[string]any {
	value, _ := params[key].(map[string]any)
	return value
}

func New(cfg Config) (*Engine, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 100 * time.Millisecond
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
	if !e.cfg.Enabled {
		return nil, ErrDisabled
	}
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("rules: script 不能为空")
	}
	if len(source) > e.cfg.MaxSourceLen {
		return nil, fmt.Errorf("rules: script 超过 %d 字节", e.cfg.MaxSourceLen)
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
	if err := vm.Set("state", in.State); err != nil {
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
