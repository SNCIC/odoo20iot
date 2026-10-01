package buildinfo

import (
	"runtime"
	"strings"
	"testing"
)

func TestRuntimeContainsGoVersionAndPlatform(t *testing.T) {
	got := Runtime()

	if !strings.HasPrefix(got, runtime.Version()) {
		t.Fatalf("Runtime() = %q，期望以 %q 开头", got, runtime.Version())
	}
	want := runtime.GOOS + "/" + runtime.GOARCH
	if !strings.HasSuffix(got, want) {
		t.Fatalf("Runtime() = %q，期望以 %q 结尾", got, want)
	}
}

func TestDefaultsAreInjected(t *testing.T) {
	// 默认值必须非空，否则构建脚本漏注入时日志会缺失关键字段。
	for name, v := range map[string]string{
		"Version":   Version,
		"Commit":    Commit,
		"BuildTime": BuildTime,
	} {
		if v == "" {
			t.Fatalf("%s 为空，构建脚本可能漏了 -ldflags 注入", name)
		}
	}
}
