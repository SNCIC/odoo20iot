package apiauth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---- 测试用的签名/密钥工具（只用于「造令牌」，不参与生产验签）----

type testKey struct {
	kid  string
	alg  string
	priv crypto.PrivateKey
	jwk  map[string]any
}

func newRSAKey(t *testing.T, kid string) testKey {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成 RSA 密钥失败: %v", err)
	}
	pub := &priv.PublicKey
	return testKey{kid: kid, alg: algRS256, priv: priv, jwk: map[string]any{
		"kty": "RSA", "kid": kid, "alg": algRS256, "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}}
}

func newECKey(t *testing.T, kid string) testKey {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("生成 EC 密钥失败: %v", err)
	}
	pub := &priv.PublicKey
	size := (pub.Curve.Params().BitSize + 7) / 8
	return testKey{kid: kid, alg: algES256, priv: priv, jwk: map[string]any{
		"kty": "EC", "kid": kid, "alg": algES256, "use": "sig", "crv": "P-256",
		"x": base64.RawURLEncoding.EncodeToString(pub.X.FillBytes(make([]byte, size))),
		"y": base64.RawURLEncoding.EncodeToString(pub.Y.FillBytes(make([]byte, size))),
	}}
}

// sign 用给定 header 覆盖项与 claims 造一个 JWT。
func (k testKey) sign(t *testing.T, header map[string]any, claims map[string]any) string {
	t.Helper()
	if header == nil {
		header = map[string]any{"alg": k.alg, "kid": k.kid, "typ": "JWT"}
	}
	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(claims)
	signing := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb)
	digest := sha256.Sum256([]byte(signing))

	var sig []byte
	switch k.alg {
	case algRS256:
		var err error
		sig, err = rsa.SignPKCS1v15(rand.Reader, k.priv.(*rsa.PrivateKey), crypto.SHA256, digest[:])
		if err != nil {
			t.Fatalf("RS256 签名失败: %v", err)
		}
	case algES256:
		r, s, err := ecdsa.Sign(rand.Reader, k.priv.(*ecdsa.PrivateKey), digest[:])
		if err != nil {
			t.Fatalf("ES256 签名失败: %v", err)
		}
		size := (k.priv.(*ecdsa.PrivateKey).Curve.Params().BitSize + 7) / 8
		sig = append(r.FillBytes(make([]byte, size)), s.FillBytes(make([]byte, size))...)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func writeJWKS(t *testing.T, keys ...testKey) string {
	t.Helper()
	set := map[string]any{"keys": []any{}}
	for _, k := range keys {
		set["keys"] = append(set["keys"].([]any), k.jwk)
	}
	b, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("序列化 JWKS 失败: %v", err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("写 JWKS 失败: %v", err)
	}
	return path
}

func newVerifier(t *testing.T, jwksFile string, revoker Revoker) *JWTVerifier {
	t.Helper()
	ks, err := NewJWKS(JWKSConfig{File: jwksFile})
	if err != nil {
		t.Fatalf("构造 JWKS 失败: %v", err)
	}
	v, err := NewJWTVerifier(JWTConfig{
		Issuers:  []string{"https://auth.example.com"},
		Audience: "iot-api",
		Keys:     ks,
		Revoker:  revoker,
	})
	if err != nil {
		t.Fatalf("构造 JWTVerifier 失败: %v", err)
	}
	return v
}

func goodClaims(overrides map[string]any) map[string]any {
	c := map[string]any{
		"iss":    "https://auth.example.com",
		"sub":    "user:42",
		"aud":    "iot-api",
		"exp":    float64(time.Now().Add(time.Hour).Unix()),
		"jti":    "jti-1",
		"tenant": float64(7),
		"scope":  []string{"iot:read"},
	}
	for k, v := range overrides {
		if v == nil {
			delete(c, k)
			continue
		}
		c[k] = v
	}
	return c
}

// TestJWTVerifier_HappyPath 校验 RS256/ES256 两条正常路径与声明解析。
func TestJWTVerifier_HappyPath(t *testing.T) {
	rsaKey := newRSAKey(t, "rsa-1")
	ecKey := newECKey(t, "ec-1")
	file := writeJWKS(t, rsaKey, ecKey)
	v := newVerifier(t, file, nil)
	ctx := context.Background()

	for _, k := range []testKey{rsaKey, ecKey} {
		t.Run(k.alg, func(t *testing.T) {
			id, err := v.Verify(ctx, k.sign(t, nil, goodClaims(nil)))
			if err != nil {
				t.Fatalf("合法令牌不应报错: %v", err)
			}
			if id.ProjectID != 7 {
				t.Errorf("tenant 应解析为 project_id=7，得到 %d", id.ProjectID)
			}
			if id.ActorType != "user" || id.ActorID != "42" {
				t.Errorf("sub 应拆成 user/42，得到 %q/%q", id.ActorType, id.ActorID)
			}
			if !id.HasScope("iot:read") || id.HasScope("iot:write") {
				t.Errorf("scope 解析不符: %v", id.Scopes)
			}
			if id.JTI != "jti-1" {
				t.Errorf("jti 应为 jti-1，得到 %q", id.JTI)
			}
		})
	}
}

// TestJWTVerifier_Rejects 覆盖各类非法令牌。
func TestJWTVerifier_Rejects(t *testing.T) {
	key := newRSAKey(t, "rsa-1")
	other := newRSAKey(t, "rsa-2")
	file := writeJWKS(t, key)
	v := newVerifier(t, file, nil)
	ctx := context.Background()

	cases := []struct {
		name  string
		token string
	}{
		{"空令牌", ""},
		{"不是三段式", "abc.def"},
		{"header 非 base64", "!!!." + "e30" + ".x"},
		{"header 非 JSON", base64.RawURLEncoding.EncodeToString([]byte("not-json")) + ".e30.eA"},
		{"alg=none", key.sign(t, map[string]any{"alg": "none", "kid": "rsa-1"}, goodClaims(nil))},
		{"alg=HS256", key.sign(t, map[string]any{"alg": "HS256", "kid": "rsa-1"}, goodClaims(nil))},
		{"未知 kid", other.sign(t, nil, goodClaims(nil))},
		{"签名被篡改", key.sign(t, nil, goodClaims(nil))[:len(key.sign(t, nil, goodClaims(nil)))-4] + "AAAA"},
		{"已过期", key.sign(t, nil, goodClaims(map[string]any{"exp": float64(time.Now().Add(-time.Hour).Unix())}))},
		{"尚未生效", key.sign(t, nil, goodClaims(map[string]any{"nbf": float64(time.Now().Add(time.Hour).Unix())}))},
		{"缺 exp", key.sign(t, nil, goodClaims(map[string]any{"exp": nil}))},
		{"签发方不对", key.sign(t, nil, goodClaims(map[string]any{"iss": "https://evil.example"}))},
		{"受众不对", key.sign(t, nil, goodClaims(map[string]any{"aud": "other-api"}))},
		{"缺 jti", key.sign(t, nil, goodClaims(map[string]any{"jti": nil}))},
		{"缺 sub", key.sign(t, nil, goodClaims(map[string]any{"sub": nil}))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := v.Verify(ctx, c.token); err == nil {
				t.Fatal("必须被拒绝")
			} else if !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("应归为 ErrUnauthenticated，得到 %v", err)
			}
		})
	}
}

// TestJWTVerifier_TenantIsMandatory 校验 tenant 声明（05 §3.3 强制）的三种坏形态。
func TestJWTVerifier_TenantIsMandatory(t *testing.T) {
	key := newRSAKey(t, "rsa-1")
	v := newVerifier(t, writeJWKS(t, key), nil)
	ctx := context.Background()

	for name, tenant := range map[string]any{
		"缺失":  nil,
		"为零":  float64(0),
		"为负":  float64(-1),
		"非数字": "abc",
	} {
		t.Run(name, func(t *testing.T) {
			tok := key.sign(t, nil, goodClaims(map[string]any{"tenant": tenant}))
			if _, err := v.Verify(ctx, tok); err == nil {
				t.Fatal("tenant 非法必须被拒绝")
			}
		})
	}

	// 字符串形式的 tenant 应当被接受（不同签发方序列化习惯不同）。
	tok := key.sign(t, nil, goodClaims(map[string]any{"tenant": "42"}))
	id, err := v.Verify(ctx, tok)
	if err != nil {
		t.Fatalf("字符串 tenant 应被接受: %v", err)
	}
	if id.ProjectID != 42 {
		t.Fatalf("期望 project_id=42，得到 %d", id.ProjectID)
	}
}

// TestJWTVerifier_AudienceForms 校验 aud 的字符串/数组两种形态。
func TestJWTVerifier_AudienceForms(t *testing.T) {
	key := newRSAKey(t, "rsa-1")
	v := newVerifier(t, writeJWKS(t, key), nil)
	ctx := context.Background()

	for name, aud := range map[string]any{
		"字符串命中": "iot-api",
		"数组命中":  []string{"other", "iot-api"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := v.Verify(ctx, key.sign(t, nil, goodClaims(map[string]any{"aud": aud}))); err != nil {
				t.Fatalf("应通过: %v", err)
			}
		})
	}

	tok := key.sign(t, nil, goodClaims(map[string]any{"aud": []string{"a", "b"}}))
	if _, err := v.Verify(ctx, tok); err == nil {
		t.Fatal("数组不含目标 aud 必须被拒绝")
	}
}

// TestJWTVerifier_Leeway 校验时间容差：刚过期一点点应仍通过。
func TestJWTVerifier_Leeway(t *testing.T) {
	key := newRSAKey(t, "rsa-1")
	ks, _ := NewJWKS(JWKSConfig{File: writeJWKS(t, key)})
	v, _ := NewJWTVerifier(JWTConfig{
		Issuers: []string{"https://auth.example.com"}, Audience: "iot-api", Keys: ks, Leeway: 30 * time.Second,
	})
	ctx := context.Background()

	tok := key.sign(t, nil, goodClaims(map[string]any{"exp": float64(time.Now().Add(-10 * time.Second).Unix())}))
	if _, err := v.Verify(ctx, tok); err != nil {
		t.Fatalf("过期 10s 在 30s 容差内应通过: %v", err)
	}
	tok = key.sign(t, nil, goodClaims(map[string]any{"exp": float64(time.Now().Add(-time.Minute).Unix())}))
	if _, err := v.Verify(ctx, tok); err == nil {
		t.Fatal("过期 60s 超出容差必须被拒绝")
	}
}

// TestJWTVerifier_Revocation 校验 jti 吊销与 fail-closed。
func TestJWTVerifier_Revocation(t *testing.T) {
	key := newRSAKey(t, "rsa-1")
	file := writeJWKS(t, key)
	revoker := NewMemRevoker()
	v := newVerifier(t, file, revoker)
	ctx := context.Background()
	tok := key.sign(t, nil, goodClaims(nil))

	if _, err := v.Verify(ctx, tok); err != nil {
		t.Fatalf("未吊销应通过: %v", err)
	}
	revoker.Revoke("jti-1")
	if _, err := v.Verify(ctx, tok); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("已吊销应归为 ErrUnauthenticated，得到 %v", err)
	}

	// 吊销表不可用 ⇒ fail-closed：既不是 401 也不是放行，而是 ErrAuthUnavailable。
	revoker.Fail(errors.New("redis down"))
	if _, err := v.Verify(ctx, key.sign(t, nil, goodClaims(map[string]any{"jti": "jti-2"}))); !errors.Is(err, ErrAuthUnavailable) {
		t.Fatalf("吊销依赖故障应归为 ErrAuthUnavailable（fail-closed），得到 %v", err)
	}
}

// TestJWTVerifier_OversizeToken 校验超长令牌被拒。
func TestJWTVerifier_OversizeToken(t *testing.T) {
	key := newRSAKey(t, "rsa-1")
	ks, _ := NewJWKS(JWKSConfig{File: writeJWKS(t, key)})
	v, _ := NewJWTVerifier(JWTConfig{
		Issuers: []string{"https://auth.example.com"}, Audience: "iot-api", Keys: ks, MaxTokenBytes: 64,
	})
	big := key.sign(t, nil, goodClaims(map[string]any{"pad": fmt.Sprintf("%0100d", 0)}))
	if _, err := v.Verify(context.Background(), big); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("超长令牌应被拒绝，得到 %v", err)
	}
}

// TestJWKS_SoleKeyWithoutKid 校验无 kid 的单键集合可用（header 也不带 kid）。
func TestJWKS_SoleKeyWithoutKid(t *testing.T) {
	key := newRSAKey(t, "rsa-1")
	// 造一个不带 kid 的 JWKS（模拟只发布一个键、且省略 kid）。
	set := map[string]any{"keys": []any{}}
	bare := map[string]any{}
	for k, v := range key.jwk {
		if k != "kid" {
			bare[k] = v
		}
	}
	set["keys"] = append(set["keys"].([]any), bare)
	raw, _ := json.Marshal(set)
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("写 JWKS 失败: %v", err)
	}

	v := newVerifier(t, path, nil)
	tok := key.sign(t, map[string]any{"alg": algRS256, "typ": "JWT"}, goodClaims(nil))
	if _, err := v.Verify(context.Background(), tok); err != nil {
		t.Fatalf("唯一键（无 kid）应可用: %v", err)
	}
}

// TestJWTVerifier_RequiresConfig 校验构造期的必填项。
func TestJWTVerifier_RequiresConfig(t *testing.T) {
	ks, _ := NewJWKS(JWKSConfig{File: writeJWKS(t, newRSAKey(t, "k"))})
	if _, err := NewJWTVerifier(JWTConfig{Audience: "iot-api", Keys: ks}); err == nil {
		t.Error("缺 Issuers 必须报错")
	}
	if _, err := NewJWTVerifier(JWTConfig{Issuers: []string{"i"}, Keys: ks}); err == nil {
		t.Error("缺 Audience 必须报错")
	}
	if _, err := NewJWTVerifier(JWTConfig{Issuers: []string{"i"}, Audience: "a"}); err == nil {
		t.Error("缺 Keys 必须报错")
	}
	if _, err := NewJWKS(JWKSConfig{}); err == nil {
		t.Error("JWKS 既无 url 也无 file 必须报错")
	}
}
