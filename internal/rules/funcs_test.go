package rules

import (
	"math"
	"testing"
	"time"
)

// call 直接调用白名单实现，绕开编译与 VM —— 这里测的是函数本身的行为，
// 不是门禁（门禁由 compile_test.go 覆盖）。
func call(t *testing.T, name string, args ...any) any {
	t.Helper()

	spec, ok := whitelist[name]
	if !ok {
		t.Fatalf("白名单里没有 %s", name)
	}
	out, err := spec.impl(args)
	if err != nil {
		t.Fatalf("%s(%v) 出错: %v", name, args, err)
	}
	return out
}

func wantNum(t *testing.T, got any, want float64) {
	t.Helper()
	v, ok := got.(float64)
	if !ok {
		t.Fatalf("期望 float64，得到 %T", got)
	}
	if math.Abs(v-want) > 1e-9 {
		t.Fatalf("期望 %v，得到 %v", want, v)
	}
}

// TestFuncs_二进制读取 覆盖 02 里 raw_parsers 的主力函数。
//
// 报文用十六进制字符串给出：`payload` 会先尝试按 hex 解码，
// 这正是 04 §1.2 里 `int16_be(payload, 1)` 期望的形态。
func TestFuncs_二进制读取(t *testing.T) {
	// 00 01 FE FF 80 40
	const payloadHex = "0001feff8040"

	// 每个用例的报文字面量都刻意写得「一眼可验」，避免偏移量算错的假失败。
	cases := []struct {
		fn   string
		args []any
		want float64
	}{
		{"uint8", []any{"00", 0}, 0x00},
		{"uint8", []any{"fe", 0}, 0xfe},
		{"int8", []any{"fe", 0}, -2},           // 0xfe 的有符号解释
		{"int16_be", []any{"0102", 0}, 0x0102}, // 高字节在前
		{"int16_le", []any{"0102", 0}, 0x0201}, // 低字节在前
		{"uint16_be", []any{"feff", 0}, 0xfeff},
		{"uint16_le", []any{"feff", 0}, 0xfffe},
		{"int16_le", []any{"feff", 0}, -2}, // 0xfffe 的有符号解释
		{"int32_be", []any{"7f000001", 0}, 0x7f000001},
		{"uint32_le", []any{"04030201", 0}, 0x01020304},
		{"int32_be", []any{"feff8040", 0}, -16809920},     // 0xfeff8040 的有符号解释
		{"int32_be", []any{"0001feff8040", 2}, -16809920}, // 带偏移量的等价写法
	}

	for _, c := range cases {
		wantNum(t, call(t, c.fn, c.args...), c.want)
	}

	// 浮点：0x40490FDB = 3.14159265
	wantNum(t, call(t, "float32_be", "40490fdb", 0), float64(float32(math.Pi)))
	wantNum(t, call(t, "float64_be", "400921fb54442d18", 0), math.Pi)

	if got := call(t, "hex_bytes", "0001feff", 2, 2); got != "feff" {
		t.Fatalf("hex_bytes 期望 feff，得到 %v", got)
	}

	// 越界必须报错而不是静默返回 0。
	spec := whitelist["int32_be"]
	if _, err := spec.impl([]any{"0001feff", 4}); err == nil {
		t.Fatal("越界读取必须报错")
	}
}

func TestFuncs_BCD(t *testing.T) {
	// BCD：0x12 0x34 → 1234
	wantNum(t, call(t, "bcd_to_int", "1234"), 1234)
	wantNum(t, call(t, "bcd_to_int", "001999", 0, 3), 1999)

	spec := whitelist["bcd_to_int"]
	if _, err := spec.impl([]any{"1f"}); err == nil {
		t.Fatal("非法 BCD 必须报错")
	}
}

func TestFuncs_校验和(t *testing.T) {
	// Modbus CRC-16："123456789" 的标准校验值 0x4B37。
	wantNum(t, call(t, "crc16", "313233343536373839"), 0x4B37)
	// CRC-32 IEEE："123456789" 的标准校验值 0xCBF43926。
	wantNum(t, call(t, "crc32", "313233343536373839"), 0xCBF43926)

	if got := call(t, "luhn_ok", "4111111111111111"); got != true {
		t.Fatalf("合法卡号应通过 Luhn，得到 %v", got)
	}
	if got := call(t, "luhn_ok", "4111111111111112"); got != false {
		t.Fatalf("非法卡号不应通过 Luhn，得到 %v", got)
	}
}

func TestFuncs_数值与字符串(t *testing.T) {
	wantNum(t, call(t, "abs", -3.5), 3.5)
	wantNum(t, call(t, "ceil", 3.2), 4)
	wantNum(t, call(t, "floor", 3.8), 3)
	wantNum(t, call(t, "round", 3.14159, 2), 3.14)
	wantNum(t, call(t, "clamp", 15.0, 0.0, 10.0), 10.0)
	wantNum(t, call(t, "min", 3.0, 1.0, 2.0), 1.0)
	wantNum(t, call(t, "max", 3.0, 1.0, 2.0), 3.0)
	wantNum(t, call(t, "pow", 2.0, 10.0), 1024)

	// len 是**字节数**（与 Go 的 len 一致）：DSL 里 len 的主要用途是校验
	// 原始报文长度，按字节计数才是可预测的语义。
	if got := call(t, "len", "上海"); got != float64(6) {
		t.Fatalf("len 应按字节数（UTF-8），得到 %v", got)
	}
	if got := call(t, "len", "auto"); got != float64(4) {
		t.Fatalf("len 失败: %v", got)
	}
	if got := call(t, "has_prefix", "shanghai", "sh"); got != true {
		t.Fatalf("has_prefix 失败: %v", got)
	}
	if got := call(t, "has_suffix", "shanghai", "ai"); got != true {
		t.Fatalf("has_suffix 失败: %v", got)
	}
	if got := call(t, "upper", "sh"); got != "SH" {
		t.Fatalf("upper 失败: %v", got)
	}
	if got := call(t, "trim", "  x  "); got != "x" {
		t.Fatalf("trim 失败: %v", got)
	}
	if got := call(t, "contains", "abc", "b"); got != true {
		t.Fatalf("contains 失败: %v", got)
	}
}

func TestFuncs_单位换算(t *testing.T) {
	// 同量纲
	wantNum(t, call(t, "convert", 1.0, "bar", "kpa"), 100)
	wantNum(t, call(t, "convert", 100.0, "c", "f"), 212)
	wantNum(t, call(t, "convert", 0.0, "c", "k"), 273.15)
	wantNum(t, call(t, "convert", 1000.0, "l", "m3"), 1)
	wantNum(t, call(t, "convert", 1000.0, "wh", "kwh"), 1)

	// 跨量纲必须报错，而不是给出看似合理的数字。
	spec := whitelist["convert"]
	if _, err := spec.impl([]any{1.0, "bar", "c"}); err == nil {
		t.Fatal("跨量纲换算必须报错")
	}
	if _, err := spec.impl([]any{1.0, "bar", "furlong"}); err == nil {
		t.Fatal("未知单位必须报错")
	}
}

func TestFuncs_时间(t *testing.T) {
	ts := time.Date(2026, 10, 1, 8, 30, 45, 0, time.UTC)

	if got := call(t, "time_format", ts, "2006-01-02"); got != "2026-10-01" {
		t.Fatalf("time_format 失败: %v", got)
	}
	wantNum(t, call(t, "time_diff_s", ts.Add(90*time.Second), ts), 90)

	trunc, ok := call(t, "time_trunc", ts, "h").(time.Time)
	if !ok || !trunc.Equal(time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("time_trunc 失败: %v", trunc)
	}

	if _, ok := call(t, "time_now").(time.Time); !ok {
		t.Fatal("time_now 应返回时间")
	}
}

func TestFuncs_地理(t *testing.T) {
	// 上海人民广场附近：WGS-84 → GCJ-02 的偏移量级在数百米内。
	glat, glng := wgs84ToGCJ02(31.2304, 121.4737)
	if math.Abs(glat-31.2304) > 0.01 || math.Abs(glng-121.4737) > 0.01 {
		t.Fatalf("GCJ-02 偏移量级异常: %v %v", glat, glng)
	}
	if glat == 31.2304 && glng == 121.4737 {
		t.Fatal("境内坐标应发生偏移")
	}

	// 境外坐标不加偏。
	olat, olng := wgs84ToGCJ02(48.8566, 2.3522)
	if olat != 48.8566 || olng != 2.3522 {
		t.Fatal("境外坐标不应加偏")
	}

	// 北京天安门到上海人民广场约 1068 km。
	d, _ := call(t, "distance_m", 39.9087, 116.3975, 31.2304, 121.4737).(float64)
	if d < 1_060_000 || d > 1_080_000 {
		t.Fatalf("distance_m 结果异常: %.0f m", d)
	}
}

// TestFuncs_白名单规模 锁定白名单条数：它是安全边界，不能悄悄变多。
func TestFuncs_白名单规模(t *testing.T) {
	const want = 41
	if len(whitelist) != want {
		names := mapKeys(whitelist)
		t.Fatalf("白名单应有 %d 个函数，实际 %d：%v", want, len(whitelist), names)
	}
	for name, spec := range whitelist {
		if spec.impl == nil {
			t.Errorf("%s 没有实现", name)
		}
		if spec.minArgs < 0 {
			t.Errorf("%s 的 minArgs 非法", name)
		}
		if spec.maxArgs >= 0 && spec.maxArgs < spec.minArgs {
			t.Errorf("%s 的 maxArgs < minArgs", name)
		}
	}
}
