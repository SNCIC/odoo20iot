package apiauth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// 只接受这两种算法。**明确拒绝 HS256 与 none** ——
// 前者会把「用公钥当 HMAC 密钥」的经典混淆漏洞引进来，后者根本不验签。
const (
	algRS256 = "RS256"
	algES256 = "ES256"
)

// JWTConfig 配置 JWT 校验。
type JWTConfig struct {
	// Issuers 是 iss 允许列表（精确匹配）。
	Issuers []string
	// Audience 是必须命中的 aud（05 §3.3 的 `aud iot-api`）。
	Audience string
	// Leeway 是时间声明容差，默认 30s。
	Leeway time.Duration
	// Keys 提供验签公钥。
	Keys KeySource
	// Revoker 提供 jti 吊销查询；nil 表示不做吊销检查（仅开发）。
	Revoker Revoker
	// MaxTokenBytes 限制令牌长度，默认 8192。
	MaxTokenBytes int
}

// JWTVerifier 按 05 §3.3 校验 JWT。
type JWTVerifier struct {
	cfg JWTConfig
}

var _ Verifier = (*JWTVerifier)(nil)

// NewJWTVerifier 构造校验器。**Revoker 为 nil 时不做吊销检查** —— 生产必须配。
func NewJWTVerifier(cfg JWTConfig) (*JWTVerifier, error) {
	if len(cfg.Issuers) == 0 {
		return nil, fmt.Errorf("apiauth: JWT 需要 iss 允许列表")
	}
	if cfg.Audience == "" {
		return nil, fmt.Errorf("apiauth: JWT 需要 audience")
	}
	if cfg.Keys == nil {
		return nil, fmt.Errorf("apiauth: JWT 需要 KeySource")
	}
	if cfg.MaxTokenBytes <= 0 {
		cfg.MaxTokenBytes = 8192
	}
	if cfg.Leeway <= 0 {
		cfg.Leeway = 30 * time.Second
	}
	return &JWTVerifier{cfg: cfg}, nil
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

type jwtClaims struct {
	Iss string          `json:"iss"`
	Sub string          `json:"sub"`
	Aud json.RawMessage `json:"aud"`
	Exp *float64        `json:"exp"`
	Nbf *float64        `json:"nbf"`
	Iat *float64        `json:"iat"`
	Jti string          `json:"jti"`
	// tenant 是**强制**声明（05 §3.3）：无它一律拒绝。
	Tenant json.RawMessage `json:"tenant"`
	Scope  json.RawMessage `json:"scope"`
}

// Verify 校验令牌。校验顺序刻意为「先便宜后昂贵」：
// 结构 → 算法白名单 → 取钥验签 → 时间 → 签发方 → 受众 → 租户 → jti → 吊销。
func (v *JWTVerifier) Verify(ctx context.Context, token string) (Identity, error) {
	if token == "" || len(token) > v.cfg.MaxTokenBytes {
		return Identity{}, fmt.Errorf("%w: 令牌缺失或过长", ErrUnauthenticated)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Identity{}, fmt.Errorf("%w: 不是三段式 JWT", ErrUnauthenticated)
	}

	headerRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Identity{}, fmt.Errorf("%w: header 不是 base64url", ErrUnauthenticated)
	}
	var hdr jwtHeader
	if err := json.Unmarshal(headerRaw, &hdr); err != nil {
		return Identity{}, fmt.Errorf("%w: header 不是合法 JSON", ErrUnauthenticated)
	}
	if hdr.Alg != algRS256 && hdr.Alg != algES256 {
		return Identity{}, fmt.Errorf("%w: 不支持的 alg %q", ErrUnauthenticated, hdr.Alg)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Identity{}, fmt.Errorf("%w: 签名不是 base64url", ErrUnauthenticated)
	}
	key, err := v.cfg.Keys.Key(ctx, hdr.Kid, hdr.Alg)
	if err != nil {
		if errors.Is(err, ErrAuthUnavailable) {
			return Identity{}, err
		}
		return Identity{}, fmt.Errorf("%w: 取验签公钥失败: %v", ErrUnauthenticated, err)
	}
	if err := verifySignature(hdr.Alg, key, []byte(parts[0]+"."+parts[1]), sig); err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}

	payloadRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Identity{}, fmt.Errorf("%w: payload 不是 base64url", ErrUnauthenticated)
	}
	var c jwtClaims
	if err := json.Unmarshal(payloadRaw, &c); err != nil {
		return Identity{}, fmt.Errorf("%w: payload 不是合法 JSON", ErrUnauthenticated)
	}

	now := time.Now()
	if c.Exp == nil {
		return Identity{}, fmt.Errorf("%w: 缺少 exp（不接受永不过期的令牌）", ErrUnauthenticated)
	}
	if now.After(time.Unix(int64(*c.Exp), 0).Add(v.cfg.Leeway)) {
		return Identity{}, fmt.Errorf("%w: 令牌已过期", ErrUnauthenticated)
	}
	if c.Nbf != nil && now.Before(time.Unix(int64(*c.Nbf), 0).Add(-v.cfg.Leeway)) {
		return Identity{}, fmt.Errorf("%w: 令牌尚未生效", ErrUnauthenticated)
	}

	if !containsString(v.cfg.Issuers, c.Iss) {
		return Identity{}, fmt.Errorf("%w: 未知的签发方 %q", ErrUnauthenticated, c.Iss)
	}
	auds, err := decodeStringList(c.Aud)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: aud 非法: %v", ErrUnauthenticated, err)
	}
	if !containsString(auds, v.cfg.Audience) {
		return Identity{}, fmt.Errorf("%w: aud 不含 %q", ErrUnauthenticated, v.cfg.Audience)
	}
	projectID, err := decodeTenant(c.Tenant)
	if err != nil || projectID <= 0 {
		return Identity{}, fmt.Errorf("%w: tenant 声明缺失或非法（05 §3.3 强制）", ErrUnauthenticated)
	}
	if strings.TrimSpace(c.Jti) == "" {
		return Identity{}, fmt.Errorf("%w: 缺少 jti（无法做吊销判定）", ErrUnauthenticated)
	}
	if c.Sub == "" {
		return Identity{}, fmt.Errorf("%w: 缺少 sub", ErrUnauthenticated)
	}

	// 吊销查询放最后：前面的本地校验都过了才值得打一次 Redis。
	if v.cfg.Revoker != nil {
		revoked, err := v.cfg.Revoker.Revoked(ctx, c.Jti)
		if err != nil {
			// fail-closed：查不到就当作「可能已吊销」（05 §3.3 / 02 §5.3）。
			return Identity{}, fmt.Errorf("%w: 吊销表查询失败: %v", ErrAuthUnavailable, err)
		}
		if revoked {
			return Identity{}, fmt.Errorf("%w: 令牌已被吊销", ErrUnauthenticated)
		}
	}

	actorType, actorID := splitSubject(c.Sub)
	scopes, _ := decodeStringList(c.Scope)
	return Identity{
		ProjectID: projectID,
		ActorType: actorType,
		ActorID:   actorID,
		JTI:       c.Jti,
		Scopes:    scopes,
		ExpiresAt: int64(*c.Exp),
	}, nil
}

func verifySignature(alg string, key crypto.PublicKey, signed, sig []byte) error {
	digest := sha256.Sum256(signed)
	switch alg {
	case algRS256:
		pub, ok := key.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("算法 RS256 但密钥类型是 %T", key)
		}
		if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
			return fmt.Errorf("RS256 验签失败")
		}
		return nil
	case algES256:
		pub, ok := key.(*ecdsa.PublicKey)
		if !ok {
			return fmt.Errorf("算法 ES256 但密钥类型是 %T", key)
		}
		if len(sig) != 64 {
			return fmt.Errorf("ES256 签名长度应为 64，得到 %d", len(sig))
		}
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		if !ecdsa.Verify(pub, digest[:], r, s) {
			return fmt.Errorf("ES256 验签失败")
		}
		return nil
	default:
		return fmt.Errorf("不支持的算法 %q", alg)
	}
}

// splitSubject 把 `sub` 拆成 `{actor_type}:{actor_id}`；没有冒号时整串当 actor_id。
func splitSubject(sub string) (actorType, actorID string) {
	if t, id, ok := strings.Cut(sub, ":"); ok {
		return t, id
	}
	return "", sub
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// decodeStringList 接受「字符串数组」或「空格分隔的字符串」两种形态。
// 空输入返回 nil（不是错误）—— aud 缺失要在调用点单独判。
func decodeStringList(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err == nil {
		return list, nil
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return strings.Fields(single), nil
	}
	return nil, fmt.Errorf("既不是字符串也不是字符串数组")
}

// decodeTenant 接受数字或字符串形式的 project_id。
func decodeTenant(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("tenant 缺失")
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		var parsed int64
		if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &parsed); err != nil {
			return 0, fmt.Errorf("tenant 字符串不是数字: %q", s)
		}
		return parsed, nil
	}
	return 0, fmt.Errorf("tenant 既不是数字也不是字符串")
}
