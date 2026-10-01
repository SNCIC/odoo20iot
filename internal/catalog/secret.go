package catalog

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/SNCIC/odoo20iot/internal/auth"
)

// secretHashPrefix 是编码格式的 scheme 段。
const secretHashPrefix = "argon2id"

// EncodeSecretHash 把 Argon2id 摘要编码成可落 `t_device.secret_hash` 的文本。
//
// 格式：`argon2id$t=<time>,m=<memoryKiB>,p=<threads>$<b64 salt>$<b64 hash>`
//
// 摘要**自带参数**（auth.Digest 的设计），因此参数升级不会破坏存量凭据 ——
// 编码里必须把参数一起存下来，否则老设备在换参数后就再也验不过。
func EncodeSecretHash(d auth.Digest) string {
	return strings.Join([]string{
		secretHashPrefix,
		fmt.Sprintf("t=%d,m=%d,p=%d", d.Params.Time, d.Params.Memory, d.Params.Threads),
		base64.RawStdEncoding.EncodeToString(d.Salt),
		base64.RawStdEncoding.EncodeToString(d.Hash),
	}, "$")
}

// DecodeSecretHash 解析 EncodeSecretHash 的输出。任何格式问题都返回错误 ——
// 绝不「解析失败就当空摘要」，那会让认证退化成免校验。
func DecodeSecretHash(s string) (auth.Digest, error) {
	var d auth.Digest
	parts := strings.Split(s, "$")
	if len(parts) != 4 {
		return d, fmt.Errorf("catalog: 摘要格式非法（期望 4 段，得到 %d）", len(parts))
	}
	if parts[0] != secretHashPrefix {
		return d, fmt.Errorf("catalog: 未知的摘要算法 %q", parts[0])
	}

	params := auth.Params{KeyLen: auth.DefaultParams.KeyLen}
	for _, kv := range strings.Split(parts[1], ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return d, fmt.Errorf("catalog: 摘要参数段非法 %q", kv)
		}
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return d, fmt.Errorf("catalog: 摘要参数 %q 不是整数: %w", kv, err)
		}
		switch k {
		case "t":
			params.Time = uint32(n)
		case "m":
			params.Memory = uint32(n)
		case "p":
			if n > 255 {
				return d, fmt.Errorf("catalog: 摘要参数 p=%d 超范围", n)
			}
			params.Threads = uint8(n)
		default:
			return d, fmt.Errorf("catalog: 未知的摘要参数 %q", k)
		}
	}
	if params.Time == 0 || params.Memory == 0 || params.Threads == 0 {
		return d, fmt.Errorf("catalog: 摘要参数不完整（t/m/p 都必须为正）")
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil {
		return d, fmt.Errorf("catalog: 盐不是合法 base64: %w", err)
	}
	hash, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil {
		return d, fmt.Errorf("catalog: 摘要不是合法 base64: %w", err)
	}
	if len(salt) == 0 || len(hash) == 0 {
		return d, fmt.Errorf("catalog: 盐或摘要为空")
	}

	d.Params = params
	d.Salt = salt
	d.Hash = hash
	if params.KeyLen == 0 {
		d.Params.KeyLen = uint32(len(hash))
	}
	return d, nil
}
