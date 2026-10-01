package pg

import (
	"strings"
	"testing"
)

func TestLoadMigrationsOrderedAndStable(t *testing.T) {
	migs, err := LoadMigrations()
	if err != nil {
		t.Fatalf("LoadMigrations: %v", err)
	}
	if len(migs) == 0 {
		t.Fatal("应至少有一个内嵌迁移")
	}

	// 版本按字典序升序 —— 迁移必须按零填充命名（0001、0002…），
	// 否则 "10_x" 会排在 "2_x" 前面，执行顺序静默错乱。
	for i := 1; i < len(migs); i++ {
		if migs[i-1].Version >= migs[i].Version {
			t.Fatalf("应按版本升序：%s 排在 %s 之前", migs[i-1].Version, migs[i].Version)
		}
	}

	for _, m := range migs {
		if len(m.Checksum) != 64 {
			t.Fatalf("%s 的 checksum 应为 sha256 十六进制（64 字符），得 %d", m.Version, len(m.Checksum))
		}
		if strings.TrimSpace(m.SQL) == "" {
			t.Fatalf("%s 内容为空", m.Version)
		}
		if strings.Contains(m.Version, ".") {
			t.Fatalf("%s：版本号不应含扩展名", m.Version)
		}
	}

	// 校验和必须稳定：不稳定的话每次启动都会误报「迁移被改过」。
	again, err := LoadMigrations()
	if err != nil {
		t.Fatalf("二次 LoadMigrations: %v", err)
	}
	if len(again) != len(migs) {
		t.Fatalf("两次加载数量不一致：%d vs %d", len(migs), len(again))
	}
	for i := range migs {
		if migs[i].Checksum != again[i].Checksum {
			t.Fatalf("%s 校验和不稳定", migs[i].Version)
		}
	}
}

func TestShortTruncatesForReadableErrors(t *testing.T) {
	if got := short("0123456789abcdef"); got != "01234567" {
		t.Fatalf("得 %q", got)
	}
	if got := short("abc"); got != "abc" {
		t.Fatalf("短摘要应原样返回，得 %q", got)
	}
}
