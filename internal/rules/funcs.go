package rules

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"strings"
	"time"

	"github.com/expr-lang/expr"
)

// funcSpec 描述一个白名单函数的**静态签名**（供编译期校验）与实现。
//
// 签名是安全边界的一部分：只有本表内的函数可以被调用，且参数个数/类型在编译期
// 就校验完。因此 expr 侧不再声明函数类型（我们的校验器是权威），
// `expr.Function` 只负责把实现塞进运行时。
type funcSpec struct {
	name    string
	minArgs int
	maxArgs int // -1 表示不限
	args    []Kind
	ret     Kind
	impl    func(args []any) (any, error)
}

// whitelist 是 04 §1.2「内置函数白名单」的实现。
//
// 全部无副作用、无 IO、无全局状态 —— 这也是 expr 层不需要沙箱的原因。
var whitelist = buildWhitelist()

func buildWhitelist() map[string]funcSpec {
	n := KindNumber
	b := KindBool
	s := KindString
	t := KindTime
	any_ := KindAny

	table := []funcSpec{
		// ---- 二进制读取（raw_parsers 主力，替代 02 早期示例里的 JS 写法）----
		binaryReader("int8", 1, func(v uint64) float64 { return float64(int8(uint8(v))) }),
		binaryReader("uint8", 1, func(v uint64) float64 { return float64(uint8(v)) }),
		binaryReader("int16_be", 2, func(v uint64) float64 { return float64(int16(uint16(v))) }),
		binaryReader("uint16_be", 2, func(v uint64) float64 { return float64(uint16(v)) }),
		binaryReader("int16_le", 2, func(v uint64) float64 { return float64(int16(uint16(v))) }),
		binaryReader("uint16_le", 2, func(v uint64) float64 { return float64(uint16(v)) }),
		binaryReader("int32_be", 4, func(v uint64) float64 { return float64(int32(uint32(v))) }),
		binaryReader("uint32_be", 4, func(v uint64) float64 { return float64(uint32(v)) }),
		binaryReader("int32_le", 4, func(v uint64) float64 { return float64(int32(uint32(v))) }),
		binaryReader("uint32_le", 4, func(v uint64) float64 { return float64(uint32(v)) }),
		{name: "float32_be", minArgs: 2, maxArgs: 2, args: []Kind{s, n}, ret: n,
			impl: func(a []any) (any, error) {
				buf, off, err := bytesAt(a, 0, 4)
				if err != nil {
					return nil, err
				}
				return float64(math.Float32frombits(binary.BigEndian.Uint32(buf[off:]))), nil
			}},
		{name: "float64_be", minArgs: 2, maxArgs: 2, args: []Kind{s, n}, ret: n,
			impl: func(a []any) (any, error) {
				buf, off, err := bytesAt(a, 0, 8)
				if err != nil {
					return nil, err
				}
				return math.Float64frombits(binary.BigEndian.Uint64(buf[off:])), nil
			}},
		{name: "hex_bytes", minArgs: 1, maxArgs: 3, args: []Kind{s, n, n}, ret: s,
			impl: func(a []any) (any, error) {
				buf, err := payload(a[0])
				if err != nil {
					return nil, err
				}
				off, length, err := offsetLen(a, len(buf))
				if err != nil {
					return nil, err
				}
				return hex.EncodeToString(buf[off : off+length]), nil
			}},
		{name: "bcd_to_int", minArgs: 1, maxArgs: 3, args: []Kind{s, n, n}, ret: n,
			impl: func(a []any) (any, error) {
				buf, err := payload(a[0])
				if err != nil {
					return nil, err
				}
				off, length, err := offsetLen(a, len(buf))
				if err != nil {
					return nil, err
				}
				var out float64
				for _, by := range buf[off : off+length] {
					hi, lo := by>>4, by&0x0f
					if hi > 9 || lo > 9 {
						return nil, fmt.Errorf("bcd_to_int: 0x%02x 不是合法 BCD", by)
					}
					out = out*100 + float64(hi)*10 + float64(lo)
				}
				return out, nil
			}},

		// ---- 数值 ----
		{name: "abs", minArgs: 1, maxArgs: 1, args: []Kind{n}, ret: n,
			impl: unaryNum(math.Abs)},
		{name: "ceil", minArgs: 1, maxArgs: 1, args: []Kind{n}, ret: n,
			impl: unaryNum(math.Ceil)},
		{name: "floor", minArgs: 1, maxArgs: 1, args: []Kind{n}, ret: n,
			impl: unaryNum(math.Floor)},
		{name: "round", minArgs: 1, maxArgs: 2, args: []Kind{n, n}, ret: n,
			impl: func(a []any) (any, error) {
				v, err := num(a[0])
				if err != nil {
					return nil, err
				}
				if len(a) == 1 {
					return math.Round(v), nil
				}
				digits, err := num(a[1])
				if err != nil {
					return nil, err
				}
				shift := math.Pow(10, digits)
				return math.Round(v*shift) / shift, nil
			}},
		{name: "clamp", minArgs: 3, maxArgs: 3, args: []Kind{n, n, n}, ret: n,
			impl: func(a []any) (any, error) {
				v, lo, hi, err := threeNums(a)
				if err != nil {
					return nil, err
				}
				return math.Min(math.Max(v, lo), hi), nil
			}},
		{name: "min", minArgs: 2, maxArgs: -1, args: []Kind{n}, ret: n, impl: foldNum(math.Min)},
		{name: "max", minArgs: 2, maxArgs: -1, args: []Kind{n}, ret: n, impl: foldNum(math.Max)},
		{name: "pow", minArgs: 2, maxArgs: 2, args: []Kind{n, n}, ret: n,
			impl: func(a []any) (any, error) {
				x, y, err := twoNums(a)
				if err != nil {
					return nil, err
				}
				return math.Pow(x, y), nil
			}},

		// ---- 单位换算 ----
		{name: "convert", minArgs: 3, maxArgs: 3, args: []Kind{n, s, s}, ret: n, impl: convertImpl},

		// ---- 字符串 ----
		// len 返回**字节数**（与 Go 的 len 一致）。DSL 里它的主要用途是校验原始报文
		// 长度，按字节计数才可预测；若某天需要字符数，应新增 rune_len 而不是改语义。
		{name: "len", minArgs: 1, maxArgs: 1, args: []Kind{s}, ret: n,
			impl: func(a []any) (any, error) {
				v, err := str(a[0])
				if err != nil {
					return nil, err
				}
				return float64(len(v)), nil
			}},
		{name: "contains", minArgs: 2, maxArgs: 2, args: []Kind{s, s}, ret: b, impl: strPair(strings.Contains)},
		{name: "has_prefix", minArgs: 2, maxArgs: 2, args: []Kind{s, s}, ret: b, impl: strPair(strings.HasPrefix)},
		{name: "has_suffix", minArgs: 2, maxArgs: 2, args: []Kind{s, s}, ret: b, impl: strPair(strings.HasSuffix)},
		{name: "upper", minArgs: 1, maxArgs: 1, args: []Kind{s}, ret: s, impl: strFn(strings.ToUpper)},
		{name: "lower", minArgs: 1, maxArgs: 1, args: []Kind{s}, ret: s, impl: strFn(strings.ToLower)},
		{name: "trim", minArgs: 1, maxArgs: 1, args: []Kind{s}, ret: s, impl: strFn(strings.TrimSpace)},

		// ---- 时间 ----
		{name: "time_now", minArgs: 0, maxArgs: 0, ret: t,
			impl: func([]any) (any, error) { return time.Now().UTC(), nil }},
		{name: "time_format", minArgs: 2, maxArgs: 2, args: []Kind{t, s}, ret: s,
			impl: func(a []any) (any, error) {
				ts, err := toTime(a[0])
				if err != nil {
					return nil, err
				}
				layout, err := str(a[1])
				if err != nil {
					return nil, err
				}
				return ts.Format(layout), nil
			}},
		{name: "time_add", minArgs: 2, maxArgs: 2, args: []Kind{t, n}, ret: t,
			impl: func(a []any) (any, error) {
				ts, err := toTime(a[0])
				if err != nil {
					return nil, err
				}
				secs, err := num(a[1])
				if err != nil {
					return nil, err
				}
				return ts.Add(time.Duration(secs * float64(time.Second))), nil
			}},
		{name: "time_diff_s", minArgs: 2, maxArgs: 2, args: []Kind{t, t}, ret: n,
			impl: func(a []any) (any, error) {
				x, err := toTime(a[0])
				if err != nil {
					return nil, err
				}
				y, err := toTime(a[1])
				if err != nil {
					return nil, err
				}
				return x.Sub(y).Seconds(), nil
			}},
		{name: "time_trunc", minArgs: 2, maxArgs: 2, args: []Kind{t, s}, ret: t, impl: timeTruncImpl},

		// ---- 校验 ----
		{name: "crc16", minArgs: 1, maxArgs: 3, args: []Kind{s, n, n}, ret: n,
			impl: func(a []any) (any, error) {
				buf, err := payload(a[0])
				if err != nil {
					return nil, err
				}
				off, length, err := offsetLen(a, len(buf))
				if err != nil {
					return nil, err
				}
				// Modbus CRC-16（poly 0xA001，初值 0xFFFF）—— IoT 报文里最常见的一种。
				var crc uint16 = 0xFFFF
				for _, by := range buf[off : off+length] {
					crc ^= uint16(by)
					for i := 0; i < 8; i++ {
						if crc&1 != 0 {
							crc = crc>>1 ^ 0xA001
						} else {
							crc >>= 1
						}
					}
				}
				return float64(crc), nil
			}},
		{name: "crc32", minArgs: 1, maxArgs: 3, args: []Kind{s, n, n}, ret: n,
			impl: func(a []any) (any, error) {
				buf, err := payload(a[0])
				if err != nil {
					return nil, err
				}
				off, length, err := offsetLen(a, len(buf))
				if err != nil {
					return nil, err
				}
				return float64(crc32.ChecksumIEEE(buf[off : off+length])), nil
			}},
		{name: "luhn_ok", minArgs: 1, maxArgs: 1, args: []Kind{s}, ret: b,
			impl: func(a []any) (any, error) {
				v, err := str(a[0])
				if err != nil {
					return nil, err
				}
				return luhnOK(v), nil
			}},

		// ---- 地理 ----
		{name: "wgs84_to_gcj02", minArgs: 2, maxArgs: 2, args: []Kind{n, n}, ret: any_,
			impl: func(a []any) (any, error) {
				lat, lng, err := twoNums(a)
				if err != nil {
					return nil, err
				}
				glat, glng := wgs84ToGCJ02(lat, lng)
				return []any{glat, glng}, nil
			}},
		{name: "gcj02_to_bd09", minArgs: 2, maxArgs: 2, args: []Kind{n, n}, ret: any_,
			impl: func(a []any) (any, error) {
				lat, lng, err := twoNums(a)
				if err != nil {
					return nil, err
				}
				blat, blng := gcj02ToBD09(lat, lng)
				return []any{blat, blng}, nil
			}},
		{name: "distance_m", minArgs: 4, maxArgs: 4, args: []Kind{n, n, n, n}, ret: n,
			impl: func(a []any) (any, error) {
				nums := make([]float64, 4)
				for i := range nums {
					v, err := num(a[i])
					if err != nil {
						return nil, err
					}
					nums[i] = v
				}
				return haversineM(nums[0], nums[1], nums[2], nums[3]), nil
			}},
	}

	out := make(map[string]funcSpec, len(table))
	for _, f := range table {
		out[f.name] = f
	}
	return out
}

// options 生成 expr 的函数注册项；参数类型不在 expr 侧声明 ——
// 权威是上面的静态签名表（编译期校验），expr 只负责调用实现。
func functionOptions() []expr.Option {
	opts := make([]expr.Option, 0, len(whitelist))
	for name, spec := range whitelist {
		impl := spec.impl
		opts = append(opts, expr.Function(name, func(args ...any) (any, error) {
			return impl(args)
		}))
	}
	return opts
}

// ---------- 构造辅助 ----------

type intReader struct {
	bytes int
	order binary.ByteOrder
	conv  func(uint64) float64
}

func binaryReader(name string, size int, conv func(uint64) float64) funcSpec {
	order := binary.ByteOrder(binary.BigEndian)
	if strings.HasSuffix(name, "_le") {
		order = binary.LittleEndian
	}
	r := intReader{bytes: size, order: order, conv: conv}

	return funcSpec{
		name: name, minArgs: 2, maxArgs: 2, args: []Kind{KindString, KindNumber}, ret: KindNumber,
		impl: func(a []any) (any, error) {
			buf, off, err := bytesAt(a, 0, r.bytes)
			if err != nil {
				return nil, err
			}
			return r.conv(readUint(buf[off:], r.bytes, r.order)), nil
		},
	}
}

func readUint(b []byte, size int, order binary.ByteOrder) uint64 {
	switch size {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(order.Uint16(b))
	default:
		return uint64(order.Uint32(b))
	}
}

func unaryNum(f func(float64) float64) func([]any) (any, error) {
	return func(a []any) (any, error) {
		v, err := num(a[0])
		if err != nil {
			return nil, err
		}
		return f(v), nil
	}
}

func foldNum(f func(a, b float64) float64) func([]any) (any, error) {
	return func(a []any) (any, error) {
		out, err := num(a[0])
		if err != nil {
			return nil, err
		}
		for _, v := range a[1:] {
			x, err := num(v)
			if err != nil {
				return nil, err
			}
			out = f(out, x)
		}
		return out, nil
	}
}

func strFn(f func(string) string) func([]any) (any, error) {
	return func(a []any) (any, error) {
		v, err := str(a[0])
		if err != nil {
			return nil, err
		}
		return f(v), nil
	}
}

func strPair(f func(a, b string) bool) func([]any) (any, error) {
	return func(a []any) (any, error) {
		x, err := str(a[0])
		if err != nil {
			return nil, err
		}
		y, err := str(a[1])
		if err != nil {
			return nil, err
		}
		return f(x, y), nil
	}
}

// ---------- 取值转换（防御式：参数来自 VM，不信任）----------

func num(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case float32:
		return float64(x), nil
	case time.Duration:
		return x.Seconds(), nil
	default:
		return 0, fmt.Errorf("需要数值，得到 %T", v)
	}
}

func str(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	default:
		return "", fmt.Errorf("需要字符串，得到 %T", v)
	}
}

func toTime(v any) (time.Time, error) {
	switch x := v.(type) {
	case time.Time:
		return x, nil
	case int64:
		return time.Unix(x, 0).UTC(), nil
	case float64:
		return time.Unix(int64(x), 0).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("需要时间，得到 %T", v)
	}
}

// payload 把「要解析的报文」统一成字节切片：既接受原始字节，也接受十六进制字符串。
func payload(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		if b, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(x), "0x")); err == nil {
			return b, nil
		}
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("需要报文字节或十六进制字符串，得到 %T", v)
	}
}

func offsetLen(a []any, defaultLen int) (int, int, error) {
	off := 0
	if len(a) > 1 && a[1] != nil {
		v, err := num(a[1])
		if err != nil {
			return 0, 0, err
		}
		off = int(v)
	}
	length := defaultLen
	if len(a) > 2 && a[2] != nil {
		v, err := num(a[2])
		if err != nil {
			return 0, 0, err
		}
		length = int(v)
	}
	return off, length, nil
}

func bytesAt(a []any, payloadIdx, size int) ([]byte, int, error) {
	buf, err := payload(a[payloadIdx])
	if err != nil {
		return nil, 0, err
	}
	off, _, err := offsetLen(a[payloadIdx:], size)
	if err != nil {
		return nil, 0, err
	}
	if off < 0 || off+size > len(buf) {
		return nil, 0, fmt.Errorf("读取越界：报文长度 %d，偏移 %d，需要 %d 字节", len(buf), off, size)
	}
	return buf, off, nil
}

func twoNums(a []any) (float64, float64, error) {
	x, err := num(a[0])
	if err != nil {
		return 0, 0, err
	}
	y, err := num(a[1])
	if err != nil {
		return 0, 0, err
	}
	return x, y, nil
}

func threeNums(a []any) (float64, float64, float64, error) {
	x, err := num(a[0])
	if err != nil {
		return 0, 0, 0, err
	}
	y, err := num(a[1])
	if err != nil {
		return 0, 0, 0, err
	}
	z, err := num(a[2])
	if err != nil {
		return 0, 0, 0, err
	}
	return x, y, z, nil
}

// ---------- 单位换算 ----------

type unitDef struct {
	dim      string
	toBase   func(float64) float64
	fromBase func(float64) float64
}

func linearUnit(dim string, factor float64) unitDef {
	return unitDef{dim: dim,
		toBase:   func(v float64) float64 { return v * factor },
		fromBase: func(v float64) float64 { return v / factor }}
}

var units = map[string]unitDef{
	// 压强，基准 Pa
	"pa":  linearUnit("pressure", 1),
	"kpa": linearUnit("pressure", 1000),
	"mpa": linearUnit("pressure", 1e6),
	"bar": linearUnit("pressure", 1e5),
	"psi": linearUnit("pressure", 6894.757293168),
	// 温度，基准 ℃（仿射，单独处理）
	"c": {"temperature", func(v float64) float64 { return v }, func(v float64) float64 { return v }},
	"f": {"temperature", func(v float64) float64 { return (v - 32) / 1.8 }, func(v float64) float64 { return v*1.8 + 32 }},
	"k": {"temperature", func(v float64) float64 { return v - 273.15 }, func(v float64) float64 { return v + 273.15 }},
	// 体积，基准 m³
	"m3": linearUnit("volume", 1),
	"l":  linearUnit("volume", 0.001),
	// 能量，基准 kWh
	"kwh": linearUnit("energy", 1),
	"wh":  linearUnit("energy", 0.001),
	"mj":  linearUnit("energy", 1.0/3.6),
}

func convertImpl(a []any) (any, error) {
	v, err := num(a[0])
	if err != nil {
		return nil, err
	}
	from, err := str(a[1])
	if err != nil {
		return nil, err
	}
	to, err := str(a[2])
	if err != nil {
		return nil, err
	}

	fu, ok := units[strings.ToLower(from)]
	if !ok {
		return nil, fmt.Errorf("convert: 未知源单位 %q", from)
	}
	tu, ok := units[strings.ToLower(to)]
	if !ok {
		return nil, fmt.Errorf("convert: 未知目标单位 %q", to)
	}
	if fu.dim != tu.dim {
		return nil, fmt.Errorf("convert: %q(%s) 与 %q(%s) 不同量纲，不能换算", from, fu.dim, to, tu.dim)
	}
	return tu.fromBase(fu.toBase(v)), nil
}

// ---------- 时间 ----------

func timeTruncImpl(a []any) (any, error) {
	ts, err := toTime(a[0])
	if err != nil {
		return nil, err
	}
	unit, err := str(a[1])
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(unit) {
	case "s", "sec":
		return ts.Truncate(time.Second), nil
	case "m", "min":
		return ts.Truncate(time.Minute), nil
	case "h", "hour":
		return ts.Truncate(time.Hour), nil
	case "d", "day":
		return time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, ts.Location()), nil
	default:
		return nil, fmt.Errorf("time_trunc: 未知单位 %q（可选 s/m/h/d）", unit)
	}
}

// ---------- 校验 ----------

func luhnOK(s string) bool {
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
	if len(digits) < 2 {
		return false
	}

	sum, alt := 0, false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if alt {
			if d *= 2; d > 9 {
				d -= 9
			}
		}
		sum += d
		alt = !alt
	}
	return sum%10 == 0
}

// ---------- 地理 ----------

const (
	gcjA  = 6378245.0
	gcjEE = 0.00669342162296594323
)

// wgs84ToGCJ02 是国测局坐标偏移算法（GCJ-02 加偏）。
//
// 中国大陆范围内才需要加偏；境外坐标原样返回。
func wgs84ToGCJ02(lat, lng float64) (float64, float64) {
	if outOfChina(lat, lng) {
		return lat, lng
	}
	dLat := transformLat(lng-105.0, lat-35.0)
	dLng := transformLng(lng-105.0, lat-35.0)

	radLat := lat / 180.0 * math.Pi
	magic := math.Sin(radLat)
	magic = 1 - gcjEE*magic*magic
	sqrtMagic := math.Sqrt(magic)

	dLat = (dLat * 180.0) / ((gcjA * (1 - gcjEE)) / (magic * sqrtMagic) * math.Pi)
	dLng = (dLng * 180.0) / (gcjA / sqrtMagic * math.Cos(radLat) * math.Pi)

	return lat + dLat, lng + dLng
}

func gcj02ToBD09(lat, lng float64) (float64, float64) {
	z := math.Sqrt(lng*lng+lat*lat) + 0.00002*math.Sin(lat*math.Pi*3000.0/180.0)
	theta := math.Atan2(lat, lng) + 0.000003*math.Cos(lng*math.Pi*3000.0/180.0)
	return z*math.Sin(theta) + 0.006, z*math.Cos(theta) + 0.0065
}

func outOfChina(lat, lng float64) bool {
	return lng < 72.004 || lng > 137.8347 || lat < 0.8293 || lat > 55.8271
}

func transformLat(x, y float64) float64 {
	ret := -100.0 + 2.0*x + 3.0*y + 0.2*y*y + 0.1*x*y + 0.2*math.Sqrt(math.Abs(x))
	ret += (20.0*math.Sin(6.0*x*math.Pi) + 20.0*math.Sin(2.0*x*math.Pi)) * 2.0 / 3.0
	ret += (20.0*math.Sin(y*math.Pi) + 40.0*math.Sin(y/3.0*math.Pi)) * 2.0 / 3.0
	ret += (160.0*math.Sin(y/12.0*math.Pi) + 320*math.Sin(y*math.Pi/30.0)) * 2.0 / 3.0
	return ret
}

func transformLng(x, y float64) float64 {
	ret := 300.0 + x + 2.0*y + 0.1*x*x + 0.1*x*y + 0.1*math.Sqrt(math.Abs(x))
	ret += (20.0*math.Sin(6.0*x*math.Pi) + 20.0*math.Sin(2.0*x*math.Pi)) * 2.0 / 3.0
	ret += (20.0*math.Sin(x*math.Pi) + 40.0*math.Sin(x/3.0*math.Pi)) * 2.0 / 3.0
	ret += (150.0*math.Sin(x/12.0*math.Pi) + 300.0*math.Sin(x/30.0*math.Pi)) * 2.0 / 3.0
	return ret
}

// haversineM 返回两点间大圆距离（米），地球半径取 WGS-84 平均半径。
func haversineM(lat1, lng1, lat2, lng2 float64) float64 {
	const r = 6371008.8

	rad := math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLng := (lng2 - lng1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return 2 * r * math.Asin(math.Min(1, math.Sqrt(a)))
}
