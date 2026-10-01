package gateway

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/SNCIC/odoo20iot/internal/auth"
)

// AuthHook 把 03 §2.1 的三档认证与 §2.2 的 ACL 接到 mochi-mqtt 上。
//
// 它替换掉 Phase 0 里临时使用的 `auth.AllowHook`。两个 Hook 方法的分工：
//   - `OnConnectAuthenticate` 判定「你是谁」（凭据）；
//   - `OnACLCheck` 判定「你能碰到哪些 topic」（物模型白名单）。
//
// 认证结果按 clientID 存在本 Hook 内，由 `OnDisconnect` 回收 ——
// mochi 的 `Client` 没有可供业务使用的自定义字段，因此结果不能挂在 client 上。
type AuthHook struct {
	mqtt.HookBase

	baseCtx context.Context
	auth    *auth.Authenticator
	logger  *slog.Logger
	metrics *Metrics

	mu     sync.RWMutex
	grants map[string]*auth.Result
}

var _ mqtt.Hook = (*AuthHook)(nil)

// NewAuthHook 构造认证 Hook。baseCtx 用于取消在途的目录查询与校验排队。
func NewAuthHook(baseCtx context.Context, a *auth.Authenticator, metrics *Metrics, log *slog.Logger) *AuthHook {
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if metrics == nil {
		metrics = new(Metrics)
	}
	if log == nil {
		log = slog.Default()
	}
	return &AuthHook{
		baseCtx: baseCtx,
		auth:    a,
		logger:  log,
		metrics: metrics,
		grants:  make(map[string]*auth.Result, 1024),
	}
}

// ID 实现 mqtt.Hook。
func (h *AuthHook) ID() string { return "iot-device-auth" }

// Provides 声明接管认证、ACL 与断连回收。
func (h *AuthHook) Provides(b byte) bool {
	switch b {
	case mqtt.OnConnectAuthenticate, mqtt.OnACLCheck, mqtt.OnDisconnect:
		return true
	default:
		return false
	}
}

// OnConnectAuthenticate 实现 §2.1 的认证入口。
//
// 返回 false 会让 mochi 回 CONNACK 失败码并断开连接 —— 这正是要的 fail-closed：
// 认证不确定时返回 false，绝不「先放行再说」。
func (h *AuthHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	req := h.buildRequest(cl, pk)

	res, err := h.auth.Authenticate(h.baseCtx, req)
	if err != nil {
		var f *auth.Failure
		reason := string(auth.ReasonInternal)
		if errors.As(err, &f) {
			reason = string(f.Reason)
		}

		h.metrics.ConnectFailTotal.Add(1)
		h.metrics.recordConnectFail(reason)

		// 过载与后端不可用是「平台侧问题」，用 warn 而不是 error ——
		// 重连风暴下把这类事件刷成错误日志海，恰恰会掩盖真正的问题。
		if reason == string(auth.ReasonOverloaded) || reason == string(auth.ReasonDirectoryDown) {
			h.logger.Warn("设备认证被拒（平台侧原因）",
				"client", req.ClientID, "remote", req.RemoteIP, "reason", reason, "error", err)
		} else {
			h.logger.Info("设备认证被拒",
				"client", req.ClientID, "remote", req.RemoteIP, "reason", reason, "error", err)
		}
		return false
	}

	h.mu.Lock()
	h.grants[cl.ID] = res
	h.mu.Unlock()

	h.metrics.AuthSuccessTotal.Add(1)
	h.logger.Debug("设备认证通过",
		"client", cl.ID, "device_key", res.DeviceKey,
		"project", res.ProjectID, "mode", res.Mode)
	return true
}

// OnACLCheck 实现 §2.2 的物模型驱动 ACL。
//
// 这里**只查内存**：白名单在认证时已经算好，ACL 检查必须能留在收包热路径上。
func (h *AuthHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	h.mu.RLock()
	res, ok := h.grants[cl.ID]
	h.mu.RUnlock()

	if !ok || res.ACL == nil {
		// 没有授权记录却走到这里，说明白名单没建起来 —— 拒绝，不猜测。
		h.metrics.ACLDeniedTotal.Add(1)
		h.logger.Warn("ACL 拒绝：连接无授权记录", "client", cl.ID, "topic", topic)
		return false
	}

	if res.ACL.Allow(topic, write) {
		return true
	}

	h.metrics.ACLDeniedTotal.Add(1)
	h.logger.Info("ACL 拒绝",
		"client", cl.ID, "device_key", res.DeviceKey, "topic", topic, "write", write)
	return false
}

// OnDisconnect 回收授权记录。
func (h *AuthHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	h.mu.Lock()
	delete(h.grants, cl.ID)
	h.mu.Unlock()
}

// grantCount 返回当前持有授权记录的连接数（观测与测试用）。
//
// 它是「授权记录是否会随连接数泄漏」这一问题的直接可观测形式。
func (h *AuthHook) grantCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.grants)
}

// buildRequest 从 CONNECT 报文与连接信息组装认证请求。
func (h *AuthHook) buildRequest(cl *mqtt.Client, pk packets.Packet) auth.Request {
	req := auth.Request{
		ClientID: pk.Connect.ClientIdentifier,
		Username: string(pk.Connect.Username),
		Password: string(pk.Connect.Password),
		RemoteIP: remoteIP(cl.Net.Remote),
	}

	// C 档（mTLS）的信息来自 TLS 层。读到 CONNECT 时握手必然已完成，
	// 因此这里能拿到已校验的证书链。
	if tc, ok := cl.Net.Conn.(*tls.Conn); ok {
		st := tc.ConnectionState()
		req.TLSVerified = st.HandshakeComplete && len(st.VerifiedChains) > 0
		if len(st.PeerCertificates) > 0 {
			req.TLSCommonName = st.PeerCertificates[0].Subject.CommonName
		}
	}
	return req
}

// remoteIP 去掉端口得到纯 IP；失败时原样返回。
func remoteIP(remote string) string {
	if host, _, err := net.SplitHostPort(remote); err == nil {
		return host
	}
	return remote
}
