package apiauth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"sync"
	"time"
)

// KeySource 按 kid/alg 提供验签公钥。
type KeySource interface {
	Key(ctx context.Context, kid, alg string) (crypto.PublicKey, error)
}

// JWKSConfig 配置 JWKS 来源。
type JWKSConfig struct {
	// URL 与 File 至少给一个。File 供测试与离线环境使用。
	URL  string
	File string
	// TTL 是缓存有效期，默认 10m。
	TTL time.Duration
	// Client 用于拉取 URL；nil 用默认（5s 超时）。
	Client *http.Client
	// MaxBytes 限制 JWKS 文档大小，默认 1 MiB。
	MaxBytes int64
}

// JWKS 是 JWK Set 的缓存与解析器。
//
// 只实现验签所需的 RSA / EC(P-256) 两类键 —— **只用 stdlib 的 crypto 解析，不自己实现密码学**。
type JWKS struct {
	cfg JWKSConfig

	mu        sync.Mutex
	keys      map[string]crypto.PublicKey
	soleKey   crypto.PublicKey
	fetchedAt time.Time
}

var _ KeySource = (*JWKS)(nil)

// NewJWKS 构造并**立即拉取一次**：拿不到键就拒绝启动（与 auth.LoadFile 的取向一致，
// 让配置错误在启动时暴露，而不是在第一个请求上）。
func NewJWKS(cfg JWKSConfig) (*JWKS, error) {
	if cfg.URL == "" && cfg.File == "" {
		return nil, fmt.Errorf("apiauth: JWKS 需要 url 或 file 之一")
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 10 * time.Minute
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 5 * time.Second}
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1 << 20
	}
	j := &JWKS{cfg: cfg}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.refreshLocked(context.Background()); err != nil {
		return nil, fmt.Errorf("apiauth: 初始化 JWKS: %w", err)
	}
	return j, nil
}

// Key 返回 kid 对应的公钥。
//
// 两种情形：命中的键即使缓存过期也**先返回旧键、顺手刷新**（JWKS 抖动不该打死全部请求）；
// 未命中的 kid 则强制刷新一次再找，仍没有才报错。
func (j *JWKS) Key(ctx context.Context, kid, alg string) (crypto.PublicKey, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if k, ok := j.lookupLocked(kid); ok {
		if time.Since(j.fetchedAt) > j.cfg.TTL {
			_ = j.refreshLocked(ctx) // 失败不致命：旧键仍可用
			if k2, ok2 := j.lookupLocked(kid); ok2 {
				return k2, nil
			}
		}
		return k, nil
	}

	if err := j.refreshLocked(ctx); err != nil {
		return nil, fmt.Errorf("%w: 刷新 JWKS: %v", ErrAuthUnavailable, err)
	}
	if k, ok := j.lookupLocked(kid); ok {
		return k, nil
	}
	return nil, fmt.Errorf("apiauth: 未知的 kid %q", kid)
}

func (j *JWKS) lookupLocked(kid string) (crypto.PublicKey, bool) {
	if kid == "" {
		if j.soleKey != nil {
			return j.soleKey, true
		}
		return nil, false
	}
	k, ok := j.keys[kid]
	return k, ok
}

func (j *JWKS) refreshLocked(ctx context.Context) error {
	raw, err := j.read(ctx)
	if err != nil {
		return err
	}
	keys, sole, err := parseJWKS(raw)
	if err != nil {
		return err
	}
	j.keys, j.soleKey, j.fetchedAt = keys, sole, time.Now()
	return nil
}

func (j *JWKS) read(ctx context.Context) ([]byte, error) {
	if j.cfg.File != "" {
		b, err := os.ReadFile(j.cfg.File)
		if err != nil {
			return nil, fmt.Errorf("读取 JWKS 文件: %w", err)
		}
		if int64(len(b)) > j.cfg.MaxBytes {
			return nil, fmt.Errorf("JWKS 文件超过 %d 字节", j.cfg.MaxBytes)
		}
		return b, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.cfg.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("构造 JWKS 请求: %w", err)
	}
	resp, err := j.cfg.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("拉取 JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("拉取 JWKS: HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, j.cfg.MaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("读取 JWKS 响应: %w", err)
	}
	if int64(len(b)) > j.cfg.MaxBytes {
		return nil, fmt.Errorf("JWKS 响应超过 %d 字节", j.cfg.MaxBytes)
	}
	return b, nil
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}

func parseJWKS(raw []byte) (map[string]crypto.PublicKey, crypto.PublicKey, error) {
	var set jwkSet
	if err := json.Unmarshal(raw, &set); err != nil {
		return nil, nil, fmt.Errorf("解析 JWKS: %w", err)
	}
	if len(set.Keys) == 0 {
		return nil, nil, fmt.Errorf("JWKS 不含任何键")
	}

	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for i, k := range set.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue // 只收签名用途的键
		}
		pub, err := k.publicKey()
		if err != nil {
			return nil, nil, fmt.Errorf("第 %d 个 JWK: %w", i, err)
		}
		if k.Kid == "" {
			// 无 kid 的键只可能作为「唯一键」使用。
			continue
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		// 允许「只有一个无 kid 的键」的 JWKS。
		for _, k := range set.Keys {
			if k.Kid == "" && (k.Use == "" || k.Use == "sig") {
				pub, err := k.publicKey()
				if err != nil {
					return nil, nil, err
				}
				return keys, pub, nil
			}
		}
		return nil, nil, fmt.Errorf("JWKS 不含可用的签名键")
	}

	var sole crypto.PublicKey
	if len(keys) == 1 {
		for _, v := range keys {
			sole = v
		}
	}
	return keys, sole, nil
}

func (k jwk) publicKey() (crypto.PublicKey, error) {
	switch k.Kty {
	case "RSA":
		n, err := b64big(k.N)
		if err != nil {
			return nil, fmt.Errorf("解析 RSA n: %w", err)
		}
		e, err := b64int(k.E)
		if err != nil {
			return nil, fmt.Errorf("解析 RSA e: %w", err)
		}
		return &rsa.PublicKey{N: n, E: e}, nil
	case "EC":
		if k.Crv != "P-256" {
			return nil, fmt.Errorf("不支持的 EC 曲线 %q（只支持 P-256）", k.Crv)
		}
		x, err := b64big(k.X)
		if err != nil {
			return nil, fmt.Errorf("解析 EC x: %w", err)
		}
		y, err := b64big(k.Y)
		if err != nil {
			return nil, fmt.Errorf("解析 EC y: %w", err)
		}
		curve := elliptic.P256()
		if !curve.IsOnCurve(x, y) {
			return nil, fmt.Errorf("EC 公钥不在 P-256 曲线上")
		}
		return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
	default:
		return nil, fmt.Errorf("不支持的 kty %q", k.Kty)
	}
}

func b64big(s string) (*big.Int, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("空值")
	}
	return new(big.Int).SetBytes(b), nil
}

func b64int(s string) (int, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return 0, err
	}
	if len(b) == 0 || len(b) > 8 {
		return 0, fmt.Errorf("指数长度非法")
	}
	v := 0
	for _, c := range b {
		v = v<<8 | int(c)
	}
	return v, nil
}
