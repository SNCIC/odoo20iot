// Package auth 实现 03 §2.1 的三档设备认证与 §2.2 的物模型驱动 ACL。
//
// 三档（ADR-008，默认 B 档设备级）：
//
//	A. project     项目级共享凭据，**显式开启**，仅用于试用/PoC
//	B. per_device  设备级 secret（生产默认）
//	C. mtls        双向证书，CN = device_key
//
// 本包只负责「凭据校验」与「身份判定」，不关心凭据从哪来 ——
// 那是 Directory 的职责（本地缓存 / Redis / svc-auth）。
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"

	"golang.org/x/crypto/argon2"
)

// Mode 是认证档位。
type Mode string

const (
	// ModeProject 是 A 档：项目级共享凭据。
	ModeProject Mode = "project"
	// ModePerDevice 是 B 档：设备级 secret，生产默认。
	ModePerDevice Mode = "per_device"
	// ModeMTLS 是 C 档：双向证书。
	ModeMTLS Mode = "mtls"
)

// Valid 判断档位是否是已知取值。
func (m Mode) Valid() bool {
	switch m {
	case ModeProject, ModePerDevice, ModeMTLS:
		return true
	default:
		return false
	}
}

// Params 是 Argon2id 参数。Memory 的单位是 KiB（与 x/crypto/argon2 一致）。
type Params struct {
	Time    uint32
	Memory  uint32
	Threads uint8
	KeyLen  uint32
}

// MemoryBytes 返回**单次校验**的内存占用上界。
//
// 这是容量规划的关键数字：并发校验数 × MemoryBytes 就是网关的内存峰值。
// 03 §2.1 把它列为 Phase 0 必验项，正是因为 64 MiB 的量级足以在重连风暴中打爆网关。
func (p Params) MemoryBytes() int64 { return int64(p.Memory) * 1024 }

// String 给出人类可读的参数描述（进日志与文档）。
func (p Params) String() string {
	return fmt.Sprintf("argon2id t=%d m=%dMiB p=%d", p.Time, p.Memory/1024, p.Threads)
}

// DefaultParams 是按实测结论选定的参数（03 §2.1.1）。
//
// 文档原值 `t=3, m=64MiB, p=4` **已被实测否决**：
//   - 单次 132ms、内存 64MiB —— 两项都是本方案最差的；
//   - 认证吞吐 26 次/秒（最优并发槽位下），8 万设备冷启动要 51 个节点；
//   - `p=4` 没有带来收益：Argon2 的瓶颈是内存带宽，线程越多争用越重，
//     `p=2` 反而更快（51 次/秒 vs 26 次/秒）。
//
// 选定值满足 OWASP 的 Argon2id 建议（m ≥ 19MiB、t ≥ 2、p ≥ 1），
// 同时把单次成本压到 116ms / 32MiB。参数可以随硬件与威胁模型调整；
// 已落库的摘要自带参数（见 Digest），改默认值不会破坏存量凭据。
var DefaultParams = Params{Time: 3, Memory: 32 * 1024, Threads: 2, KeyLen: 32}

// Digest 是慢哈希摘要，**自带参数与盐**。
//
// 自带参数是为了让参数升级不破坏存量凭据：老摘要用老参数校验，
// 新凭据用新参数；不需要一次性全量重算。
type Digest struct {
	Params Params
	Salt   []byte
	Hash   []byte
}

// HashSecret 用给定盐与参数计算 secret 的摘要。
func HashSecret(secret string, p Params, salt []byte) Digest {
	return Digest{
		Params: p,
		Salt:   salt,
		Hash:   argon2.IDKey([]byte(secret), salt, p.Time, p.Memory, p.Threads, p.KeyLen),
	}
}

// Verify 校验 secret。使用常数时间比较，避免通过耗时侧信道逐字节猜摘要。
func (d Digest) Verify(secret string) bool {
	if len(d.Hash) == 0 || len(d.Salt) == 0 {
		// 没有摘要一律拒绝：空摘要不得被当成「免校验」。
		return false
	}
	p := d.Params
	if p.KeyLen == 0 {
		p.KeyLen = uint32(len(d.Hash))
	}
	got := argon2.IDKey([]byte(secret), d.Salt, p.Time, p.Memory, p.Threads, p.KeyLen)
	return subtle.ConstantTimeCompare(got, d.Hash) == 1
}

// NewSalt 生成 16 字节随机盐。
func NewSalt() ([]byte, error) {
	s := make([]byte, 16)
	if _, err := rand.Read(s); err != nil {
		return nil, fmt.Errorf("生成盐失败: %w", err)
	}
	return s, nil
}

// HashToken 计算访问令牌的摘要。
//
// A 档的 AccessToken 是高熵随机串（不是人类口令），用 SHA-256 即可 ——
// 慢哈希在这里只增加成本、不增加安全性（03 §2.1 凭据存储规则）。
func HashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// VerifyToken 常数时间比较令牌摘要。
func VerifyToken(hash []byte, token string) bool {
	if len(hash) == 0 || token == "" {
		return false
	}
	return subtle.ConstantTimeCompare(HashToken(token), hash) == 1
}

// GenerateSecret 生成 32 字节高熵 secret（Base64 编码，43 字符）。
//
// 03 §2.1 的 B 档安全性建立在「secret 是 32 字节高熵随机串，不是人类密码」之上，
// 因此这个长度是安全论证的一部分，不要为了好记而缩短。
func GenerateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("生成 secret 失败: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
