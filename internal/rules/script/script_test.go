package script

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDisabledByDefault(t *testing.T) {
	engine, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(context.Background(), "return 1", Input{})
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("err=%v", err)
	}
}

func TestRunUsesWhitelistedNamespaces(t *testing.T) {
	engine, err := New(Config{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := engine.Run(context.Background(), "return msg.temperature > prev.temperature && meta.site === 'A'", Input{
		Msg:  map[string]any{"temperature": 21.0},
		Prev: map[string]any{"temperature": 20.0},
		Meta: map[string]any{"site": "A"},
	})
	if err != nil || got != true {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestRunRejectsDynamicCode(t *testing.T) {
	engine, err := New(Config{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"return eval('1+1')", "return Function('return 1')()", "return require('fs')"} {
		if _, err := engine.Run(context.Background(), source, Input{}); err == nil {
			t.Fatalf("source %q unexpectedly succeeded", source)
		}
	}
}

func TestRunHonorsTimeoutAndSourceLimit(t *testing.T) {
	engine, err := New(Config{Enabled: true, Timeout: 10 * time.Millisecond, MaxSourceLen: 8})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), strings.Repeat("x", 9), Input{}); err == nil {
		t.Fatal("oversized source unexpectedly succeeded")
	}

	engine, err = New(Config{Enabled: true, Timeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), "for (;;) {}", Input{}); err == nil {
		t.Fatal("infinite loop unexpectedly succeeded")
	}
}

func TestAction(t *testing.T) {
	engine, err := New(Config{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	action := Action{Engine: engine}
	if action.Name() != "script.run" || action.Idempotent() {
		t.Fatal("script.run action metadata incorrect")
	}
	if err := action.Do(context.Background(), map[string]any{
		"source":       "if (msg.temperature < 20) throw new Error('bad')",
		"msg":          map[string]any{"temperature": 25.0},
		"capabilities": []any{"script.run"},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRejectsUndeclaredCapabilities(t *testing.T) {
	engine, err := New(Config{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(context.Background(), "return 1", Input{Capabilities: []string{"script.emit.command"}})
	if err == nil || !strings.Contains(err.Error(), "未允许") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStateCapabilityProvidesScopedAccessors(t *testing.T) {
	engine, err := New(Config{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := engine.Run(context.Background(), "state.set('last', 42); return state.get('last')", Input{
		Capabilities: []string{"script.run", "state"},
		State:        map[string]any{},
	})
	if err != nil || got != int64(42) && got != float64(42) {
		t.Fatalf("got=%v (%T) err=%v", got, got, err)
	}
}

func TestEmitRequiresCapabilityAndHandler(t *testing.T) {
	engine, err := New(Config{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.run(context.Background(), "emit('alarm', {level: 'critical'})", Input{Capabilities: []string{"script.run"}}, func(context.Context, string, any) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "未声明") {
		t.Fatalf("missing capability error=%v", err)
	}
	_, err = engine.run(context.Background(), "emit('alarm', {level: 'critical'})", Input{Capabilities: []string{"script.run", "emit:alarm"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "未配置") {
		t.Fatalf("missing handler error=%v", err)
	}
}

func TestEmitValidatesArgumentsAndPayload(t *testing.T) {
	engine, err := New(Config{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	emitter := func(context.Context, string, any) error { return nil }
	cases := []string{
		"emit()",
		"emit(1, {})",
		"emit('alarm', 'not-an-object')",
		"emit('command', {})",
	}
	for _, source := range cases {
		if _, err := engine.run(context.Background(), source, Input{Capabilities: []string{"script.run", "emit:alarm"}}, emitter); err == nil {
			t.Errorf("source %q unexpectedly succeeded", source)
		}
	}
}
