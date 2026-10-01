package apiauth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
)

// StaticTokenVerifier 是**开发用**的共享令牌校验器。
//
// 定位与 internal/connector 的 webhook Bearer 一致：仅供本机开发/PoC，
// 不是 05 §3.3 的 JWT 方案。服务的默认监听地址必须绑 127.0.0.1，
// 且启动与每次认证都要打 WARN —— 这东西不该出现在任何生产路径上。
type StaticTokenVerifier struct {
	token     [32]byte
	projectID int64
	actorType string
	actorID   string
}

var _ Verifier = (*StaticTokenVerifier)(nil)

// NewStaticTokenVerifier 构造开发令牌校验器。
func NewStaticTokenVerifier(token string, projectID int64) (*StaticTokenVerifier, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("apiauth: 开发令牌不能为空")
	}
	if projectID <= 0 {
		return nil, fmt.Errorf("apiauth: 开发令牌必须绑定正的 project_id")
	}
	return &StaticTokenVerifier{
		token:     sha256.Sum256([]byte(token)),
		projectID: projectID,
		actorType: "dev",
		actorID:   "static-token",
	}, nil
}

// Verify 常数时间比较令牌摘要。
//
// 先哈希再比较：`subtle.ConstantTimeCompare` 对不等长输入会立刻返回，
// 会把令牌长度泄漏出去；哈希后长度恒为 32 字节。
func (v *StaticTokenVerifier) Verify(_ context.Context, got string) (Identity, error) {
	sum := sha256.Sum256([]byte(got))
	if subtle.ConstantTimeCompare(sum[:], v.token[:]) != 1 {
		return Identity{}, fmt.Errorf("%w: 开发令牌不匹配", ErrUnauthenticated)
	}
	return Identity{
		ProjectID: v.projectID,
		ActorType: v.actorType,
		ActorID:   v.actorID,
		Dev:       true,
	}, nil
}

// MultiVerifier 依次尝试多个校验器（典型配置：[dev 静态令牌, JWT]）。
type MultiVerifier []Verifier

var _ Verifier = MultiVerifier(nil)

// Verify 逐个尝试。
//
// ⚠️ 认证依赖不可用（ErrAuthUnavailable）必须**立刻上抛**：
// 若被后面的校验器失败掩盖成 401，就会把「依赖挂了」误报成「令牌不对」，
// 排障时会一直盯着令牌看。
func (m MultiVerifier) Verify(ctx context.Context, token string) (Identity, error) {
	var lastErr error
	for _, v := range m {
		if v == nil {
			continue
		}
		id, err := v.Verify(ctx, token)
		if err == nil {
			return id, nil
		}
		if errors.Is(err, ErrAuthUnavailable) {
			return Identity{}, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = ErrUnauthenticated
	}
	return Identity{}, lastErr
}
