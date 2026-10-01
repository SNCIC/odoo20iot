package dag

import (
	"strings"
	"testing"

	"github.com/SNCIC/odoo20iot/internal/rules"
)

// TestValidateVarRef 覆盖 04 §1.3 的「仅允许 $path.to.value，禁止表达式拼接」。
func TestValidateVarRef(t *testing.T) {
	ok := []string{
		"",
		"普通字面量",
		"a $ b",  // 不以 $ 开头 → 字面量
		"$msg",   // 单段
		"$msg.a", // 两段
		"$msg.a.b_c",
		"$window.avg",
		"$meta.device_id",
		"$state.mode",
		"$_x.y", // 下划线开头
	}
	for _, s := range ok {
		if err := ValidateVarRef(s); err != nil {
			t.Errorf("%q 应放行，得到 %v", s, err)
		}
	}

	bad := []string{
		"$msg.a + $msg.b", // 拼接
		"$msg.a+1",
		"$",              // 缺路径
		"$.a",            // 缺根
		"$msg.",          // 尾点
		"$msg..a",        // 空段
		"$1abc",          // 数字开头
		"$msg.a b",       // 含空格
		"$msg['a']",      // 下标语法
		"$msg.a|upper()", // 函数调用
	}
	for _, s := range bad {
		if err := ValidateVarRef(s); err == nil {
			t.Errorf("%q 应被拒绝", s)
		}
	}
}

func TestResolveVar_取值(t *testing.T) {
	env := testEnv()

	cases := []struct {
		ref  string
		want any
	}{
		{"$msg.temperature", 65.0},
		{"$window.avg", 72.5},
		{"$state.mode", "auto"},
		{"$meta.device_id", int64(7)},
	}
	for _, c := range cases {
		got, err := ResolveVar(c.ref, env)
		if err != nil {
			t.Errorf("%s 解析失败: %v", c.ref, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s 期望 %v，得到 %v", c.ref, c.want, got)
		}
	}
}

// TestResolveVar_缺失即报错 验证变量名写错不会被静默变成 nil ——
// 那会让动作收到一个看似合法但完全错误的参数。
func TestResolveVar_缺失即报错(t *testing.T) {
	env := testEnv()

	cases := []struct {
		ref  string
		want string
	}{
		{"$msg.nope", "缺少"},
		{"$nope.a", "缺少"},
		{"$msg.temperature.deep", "上一级不是对象"},
	}
	for _, c := range cases {
		_, err := ResolveVar(c.ref, env)
		if err == nil {
			t.Errorf("%s 应报错", c.ref)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s 的错误信息应含 %q，得到 %v", c.ref, c.want, err)
		}
	}
}

// TestResolveParams_深拷贝 验证动作改动参数不会回写进编译好的图 ——
// 规则与场景都是长期驻留对象，回写会让多次执行互相污染。
func TestResolveParams_深拷贝(t *testing.T) {
	original := map[string]any{
		"a": 1,
		"nested": map[string]any{
			"b": "$window.avg",
			"c": []any{"$state.mode", 3},
		},
	}

	got, err := ResolveParams(original, testEnv())
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}

	if got["nested"].(map[string]any)["b"] != 72.5 {
		t.Fatalf("嵌套变量未解析: %v", got)
	}
	if got["nested"].(map[string]any)["c"].([]any)[0] != "auto" {
		t.Fatalf("数组内变量未解析: %v", got)
	}

	// 原图必须原样不动
	if original["nested"].(map[string]any)["b"] != "$window.avg" {
		t.Fatal("原参数被改写了 —— 会导致多次执行互相污染")
	}

	// 改动结果不应影响原图
	got["nested"].(map[string]any)["b"] = "changed"
	if original["nested"].(map[string]any)["b"] != "$window.avg" {
		t.Fatal("结果与原图共享了底层 map")
	}
}

func TestResolveParams_空参数(t *testing.T) {
	got, err := ResolveParams(nil, testEnv())
	if err != nil {
		t.Fatalf("空参数不该报错: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("应返回空 map 而非 nil，得到 %#v", got)
	}
}

// TestResolveParams_错误带路径 验证报错能定位到具体参数，
// 否则用户面对一棵嵌套参数树无从下手。
func TestResolveParams_错误带路径(t *testing.T) {
	_, err := ResolveParams(map[string]any{
		"outer": map[string]any{"inner": "$msg.nope"},
	}, testEnv())
	if err == nil {
		t.Fatal("应报错")
	}
	if !strings.Contains(err.Error(), "outer") || !strings.Contains(err.Error(), "inner") {
		t.Fatalf("错误信息应带参数路径，得到 %v", err)
	}
}

// 确保 testEnv 的形态与 rules.Env 一致（编译期约束）。
var _ rules.Env = testEnv()
