package apiauth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStaticTokenVerifier(t *testing.T) {
	v, err := NewStaticTokenVerifier("dev-token", 9)
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	ctx := context.Background()

	id, err := v.Verify(ctx, "dev-token")
	if err != nil {
		t.Fatalf("正确令牌应通过: %v", err)
	}
	if id.ProjectID != 9 || !id.Dev {
		t.Fatalf("身份不符: %+v", id)
	}

	for _, bad := range []string{"", "dev-toke", "dev-tokenx", "DEV-TOKEN"} {
		if _, err := v.Verify(ctx, bad); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("令牌 %q 应被拒绝，得到 %v", bad, err)
		}
	}

	if _, err := NewStaticTokenVerifier("", 1); err == nil {
		t.Error("空令牌必须报错")
	}
	if _, err := NewStaticTokenVerifier("x", 0); err == nil {
		t.Error("project_id<=0 必须报错")
	}
}

// fakeVerifier 用于多校验器测试。
type fakeVerifier struct {
	id  Identity
	err error
}

func (f fakeVerifier) Verify(context.Context, string) (Identity, error) { return f.id, f.err }

// TestMultiVerifier 校验依次尝试，且 ErrAuthUnavailable 立刻上抛。
func TestMultiVerifier(t *testing.T) {
	ctx := context.Background()

	m := MultiVerifier{
		fakeVerifier{err: ErrUnauthenticated},
		fakeVerifier{id: Identity{ProjectID: 5}},
	}
	id, err := m.Verify(ctx, "x")
	if err != nil || id.ProjectID != 5 {
		t.Fatalf("应落到第二个校验器，得到 %+v / %v", id, err)
	}

	// 第一个校验器报「依赖不可用」时不得被后面的成功掩盖。
	m = MultiVerifier{
		fakeVerifier{err: ErrAuthUnavailable},
		fakeVerifier{id: Identity{ProjectID: 5}},
	}
	if _, err := m.Verify(ctx, "x"); !errors.Is(err, ErrAuthUnavailable) {
		t.Fatalf("ErrAuthUnavailable 必须立刻上抛，得到 %v", err)
	}

	// 全失败 → ErrUnauthenticated。
	m = MultiVerifier{fakeVerifier{err: ErrUnauthenticated}, nil}
	if _, err := m.Verify(ctx, "x"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("应归为 ErrUnauthenticated，得到 %v", err)
	}
}

// TestRequireAuth 校验中间件的 401/503 与身份注入。
func TestRequireAuth(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := IdentityFrom(r.Context())
		if !ok {
			t.Error("下游应能从 context 拿到身份")
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"project_id": id.ProjectID})
	})

	do := func(h http.Handler, auth string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/devices", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	devVerifier, _ := NewStaticTokenVerifier("dev-token", 3)
	metrics := new(Metrics)
	h := RequireAuth(Options{Verifier: devVerifier, Metrics: metrics})(next)

	t.Run("合法令牌", func(t *testing.T) {
		rec := do(h, "Bearer dev-token")
		if rec.Code != http.StatusOK {
			t.Fatalf("期望 200，得到 %d（%s）", rec.Code, rec.Body.String())
		}
		if !containsStr(rec.Body.String(), `"project_id":3`) {
			t.Fatalf("下游应拿到 tenant=3，得到 %s", rec.Body.String())
		}
	})
	t.Run("大小写不敏感的 Bearer", func(t *testing.T) {
		if rec := do(h, "bearer dev-token"); rec.Code != http.StatusOK {
			t.Fatalf("期望 200，得到 %d", rec.Code)
		}
	})
	t.Run("缺头 401", func(t *testing.T) {
		rec := do(h, "")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("期望 401，得到 %d", rec.Code)
		}
		if rec.Header().Get("WWW-Authenticate") == "" {
			t.Error("401 应带 WWW-Authenticate")
		}
	})
	t.Run("令牌错 401", func(t *testing.T) {
		if rec := do(h, "Bearer nope"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("期望 401，得到 %d", rec.Code)
		}
		if metrics.AuthFailures.Load() == 0 {
			t.Error("认证失败应计数")
		}
	})
	t.Run("非 Bearer 方案 401", func(t *testing.T) {
		if rec := do(h, "Basic abc"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("期望 401，得到 %d", rec.Code)
		}
	})

	t.Run("依赖不可用 503（fail-closed）", func(t *testing.T) {
		h2 := RequireAuth(Options{Verifier: fakeVerifier{err: ErrAuthUnavailable}, Metrics: metrics})(next)
		rec := do(h2, "Bearer anything")
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("期望 503，得到 %d", rec.Code)
		}
		if !containsStr(rec.Body.String(), "AUTH_UNAVAILABLE") {
			t.Fatalf("错误码应为 AUTH_UNAVAILABLE，得到 %s", rec.Body.String())
		}
	})

	t.Run("未配置校验器 503", func(t *testing.T) {
		h3 := RequireAuth(Options{})(next)
		if rec := do(h3, "Bearer x"); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("期望 503，得到 %d", rec.Code)
		}
	})
}

func containsStr(s, sub string) bool { return strings.Contains(s, sub) }
