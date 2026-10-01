// Package apiauth 是**用户侧 HTTP API** 的认证层（与 internal/auth 的设备认证无关）。
//
// 生产路径按 `05 §3.3` 校验 JWT：ES256/RS256、`iss` 允许列表、`aud=iot-api`、
// **`tenant`（= project_id）声明强制存在**、`jti` 吊销表按 Redis 查且**失败即拒绝**（fail-closed）。
// 另提供**开发用静态令牌**（常量时间比较），默认只绑 127.0.0.1 并在启动/每次认证时打 WARN。
//
// ⚠️ 身份源头（Odoo OIDC / svc-auth）尚未建立，因此本包的 JWT 校验器**只做校验**，
// 不负责签发；开发环境用静态令牌顶上。
package apiauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
)

// Identity 是认证通过后的调用方身份。
type Identity struct {
	ProjectID int64 // 租户，来自 JWT 的 tenant 声明（或 dev 令牌的配置）
	ActorType string
	ActorID   string
	JTI       string
	Scopes    []string
	ExpiresAt int64 // Unix 秒；dev 令牌为 0
	Dev       bool
}

// HasScope 判断是否含某 scope。空 scope 集合视为「无任何 scope」。
func (id Identity) HasScope(scope string) bool {
	for _, s := range id.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

var (
	// ErrUnauthenticated 表示令牌缺失/非法/过期/被吊销 → 401。
	ErrUnauthenticated = errors.New("apiauth: 未认证")
	// ErrAuthUnavailable 表示**认证依赖**（吊销表 / JWKS）不可用。
	// 按 05 §3.3 的 fail-closed：宁可不放行 → 503。
	ErrAuthUnavailable = errors.New("apiauth: 认证依赖不可用")
)

// Verifier 校验原始令牌串并返回身份。
type Verifier interface {
	Verify(ctx context.Context, token string) (Identity, error)
}

// Metrics 记录认证结果（由服务侧汇总暴露）。
type Metrics struct {
	AuthFailures    atomic.Int64
	AuthUnavailable atomic.Int64
}

// Options 组装中间件。
type Options struct {
	Verifier Verifier
	Metrics  *Metrics
	Logger   *slog.Logger
	// Realm 出现在 WWW-Authenticate 头里（默认 "iot-api"）。
	Realm string
}

// RequireAuth 是要求 `Authorization: Bearer <token>` 的中间件。
//
// 失败时**直接写错误响应并中断**，不把请求交给下游 —— 下游因此可以无条件相信
// `IdentityFrom(ctx)` 拿到的租户（05 §3.3 的「先校验 project_id」）。
func RequireAuth(o Options) func(http.Handler) http.Handler {
	logger := o.Logger
	if logger == nil {
		logger = slog.Default()
	}
	realm := o.Realm
	if realm == "" {
		realm = "iot-api"
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := r.Header.Get("Authorization")
			token, ok := bearerToken(raw)
			if !ok {
				writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "缺少 Authorization: Bearer 令牌", realm)
				return
			}
			if o.Verifier == nil {
				writeAuthError(w, http.StatusServiceUnavailable, "AUTH_UNAVAILABLE", "认证未配置", realm)
				return
			}

			id, err := o.Verifier.Verify(r.Context(), token)
			if err != nil {
				switch {
				case errors.Is(err, ErrAuthUnavailable):
					if o.Metrics != nil {
						o.Metrics.AuthUnavailable.Add(1)
					}
					logger.Error("认证依赖不可用（fail-closed）", "error", err)
					writeAuthError(w, http.StatusServiceUnavailable, "AUTH_UNAVAILABLE", "认证依赖不可用", realm)
				default:
					if o.Metrics != nil {
						o.Metrics.AuthFailures.Add(1)
					}
					// 不把内部原因回给客户端（避免区分「签名错」与「过期」这类信息）。
					writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "令牌无效", realm)
				}
				return
			}

			if id.Dev {
				logger.Warn("开发令牌认证通过（仅开发/PoC，禁止用于生产）",
					"project_id", id.ProjectID, "path", r.URL.Path)
			}
			next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
		})
	}
}

type ctxKey struct{}

// WithIdentity 把身份放进 context。
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// IdentityFrom 取出身份。
func IdentityFrom(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

// bearerToken 从 Authorization 头里取出 Bearer 令牌。
func bearerToken(raw string) (string, bool) {
	const prefix = "Bearer "
	if len(raw) <= len(prefix) || !strings.EqualFold(raw[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(raw[len(prefix):])
	return token, token != ""
}

func writeAuthError(w http.ResponseWriter, status int, code, msg, realm string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Bearer realm=%q", realm))
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": false, "code": code, "message": msg,
	})
}
