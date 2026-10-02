package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Reason 是认证失败的原因分类，直接进 `gw_connect_fail_total{reason}`（06 §4）。
//
// **分类必须准确**：容量不足与凭据错误混在一起，会把正常设备在重连风暴中
// 打成黑名单设备 —— 这是本模块最容易犯、后果最严重的错误。
type Reason string

const (
	ReasonBadRequest    Reason = "bad_request"
	ReasonUnknownDevice Reason = "unknown_device"
	ReasonBadCredential Reason = "bad_credential"
	ReasonRevoked       Reason = "revoked"
	ReasonModeMismatch  Reason = "mode_mismatch"
	ReasonBlacklisted   Reason = "blacklisted"
	ReasonOverloaded    Reason = "overloaded"
	ReasonDirectoryDown Reason = "directory_unavailable"
	ReasonInternal      Reason = "internal"
)

// Failure 是认证失败。
type Failure struct {
	Reason Reason
	Err    error
}

func (f *Failure) Error() string {
	if f.Err == nil {
		return string(f.Reason)
	}
	return fmt.Sprintf("%s: %v", f.Reason, f.Err)
}

func (f *Failure) Unwrap() error { return f.Err }

func fail(reason Reason, err error) *Failure { return &Failure{Reason: reason, Err: err} }

// Request 是一次认证请求。字段与 MQTT CONNECT 报文一一对应。
type Request struct {
	ClientID string
	Username string
	Password string
	RemoteIP string

	// C 档：由 TLS 层提供。
	TLSVerified   bool
	TLSCommonName string
}

// Result 是认证通过的判定结果，同时携带 ACL。
type Result struct {
	ProjectID    int64
	DeviceID     int64
	DeviceTypeID int64
	DeviceKey    string
	Mode         Mode

	ACL *ACL

	// CredentialVersion 供控制面判断本地缓存是否已过期。
	CredentialVersion int64
}

// Policy 是认证策略。
type Policy struct {
	// MaxConcurrentVerify 限制**同时进行**的 Argon2 校验数。
	//
	// 这是本模块最重要的一个参数，两个约束同时压着它：
	//   - 内存：每次校验占用 Params.MemoryBytes，不限并发时重连风暴会让网关 OOM；
	//   - **内存带宽**：Argon2 是 memory-hard KDF，并发越高越互相拖慢 ——
	//     实测同一节点上槽位从 4 提到 32，认证吞吐反而从 51 次/秒掉到 19 次/秒
	//     （CPU 利用率封顶 25%，瓶颈不在核数）。
	//
	// 因此默认值取 8（实测峰值附近），而**不是**按 CPU 核数来定。
	// ≤ 0 表示不限（仅测试用）。
	MaxConcurrentVerify int

	// VerifyQueueTimeout 是等待校验槽位的上限。超时按「过载」处理：
	// 拒绝连接但**不计入失败计数**（否则重连风暴会把正常设备拉黑）。
	VerifyQueueTimeout time.Duration

	// AllowProjectMode 允许 A 档。
	//
	// 03 §2.1：A 档必须**显式开启**，且设备数 > 1000 时禁止开启。
	// 默认 false —— 默认值必须是安全的那个。
	AllowProjectMode bool

	// 失败计数与黑名单（03 §2.1「更严限流」与「紧急封禁」）。
	FailWindow             time.Duration
	FailLimitPerDevice     int
	FailLimitPerIP         int
	FailLimitPerProjectKey int
	BlacklistTTL           time.Duration
}

// DefaultPolicy 给出保守的默认值。
func DefaultPolicy() Policy {
	return Policy{
		MaxConcurrentVerify:    8,
		VerifyQueueTimeout:     2 * time.Second,
		AllowProjectMode:       false,
		FailWindow:             time.Minute,
		FailLimitPerDevice:     10,
		FailLimitPerIP:         30,
		FailLimitPerProjectKey: 20,
		BlacklistTTL:           10 * time.Minute,
	}
}

// Metrics 是认证模块的计数器。
type Metrics struct {
	Attempts   atomic.Int64
	Success    atomic.Int64
	Overloaded atomic.Int64 // 排队超时被拒（非凭据问题）
	Degraded   atomic.Int64 // 目录降级放行
	// CounterSweeps 是失败计数表被迫重置的次数（见 maybeSweepLocked）。
	// 它非 0 说明有人在用伪造 clientID 冲刷计数表，应当告警。
	CounterSweeps atomic.Int64

	mu       sync.Mutex
	failures map[Reason]int64
}

// FailureCount 返回某原因下的失败次数。
func (m *Metrics) FailureCount(r Reason) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failures[r]
}

// Failures 返回全部失败计数快照。
func (m *Metrics) Failures() map[Reason]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make(map[Reason]int64, len(m.failures))
	for k, v := range m.failures {
		out[k] = v
	}
	return out
}

func (m *Metrics) countFailure(r Reason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failures == nil {
		m.failures = make(map[Reason]int64, 4)
	}
	m.failures[r]++
}

// Authenticator 实现 03 §2.1 的三档认证。
type Authenticator struct {
	dir     Directory
	policy  Policy
	metrics *Metrics
	now     func() time.Time

	// verifySlots 限制并发 Argon2 校验数。
	verifySlots chan struct{}

	mu         sync.Mutex
	failCounts map[string]*failCounter
	blacklist  map[string]time.Time
}

type failCounter struct {
	n       int
	resetAt time.Time
}

// NewAuthenticator 构造认证器。
func NewAuthenticator(dir Directory, policy Policy, metrics *Metrics) *Authenticator {
	if metrics == nil {
		metrics = new(Metrics)
	}
	a := &Authenticator{
		dir:        dir,
		policy:     policy,
		metrics:    metrics,
		now:        time.Now,
		failCounts: make(map[string]*failCounter, 1024),
		blacklist:  make(map[string]time.Time, 64),
	}
	if policy.MaxConcurrentVerify > 0 {
		a.verifySlots = make(chan struct{}, policy.MaxConcurrentVerify)
	}
	return a
}

// Metrics 返回计数器。
func (a *Authenticator) Metrics() *Metrics { return a.metrics }

// Authenticate 执行一次认证。
//
// 返回 (nil, *Failure) 表示拒绝；错误一律是 *Failure，调用方据此分类上报。
func (a *Authenticator) Authenticate(ctx context.Context, req Request) (*Result, error) {
	a.metrics.Attempts.Add(1)

	if req.ClientID == "" {
		return a.reject(ReasonBadRequest, errors.New("clientID 为空"))
	}
	if a.isBlacklisted(req.ClientID, req.RemoteIP, 0) {
		return a.reject(ReasonBlacklisted, nil)
	}

	id, err := a.dir.Lookup(ctx, req.ClientID)
	if err != nil {
		// 无法判定 → fail-closed。**不计入失败计数**：这是平台侧问题，
		// 不是设备拿错凭据；计进去会在后端抖动时误伤全部设备。
		return a.reject(ReasonDirectoryDown, err)
	}
	if id == nil {
		return a.rejectFor(ReasonUnknownDevice, nil, req.ClientID, req.RemoteIP, 0)
	}
	if id.Revoked {
		return a.rejectFor(ReasonRevoked, nil, req.ClientID, req.RemoteIP, 0)
	}
	if !id.Mode.Valid() {
		return a.reject(ReasonInternal, fmt.Errorf("设备 %s 的 auth_mode 非法: %q", id.DeviceKey, id.Mode))
	}

	switch id.Mode {
	case ModeProject:
		if !a.policy.AllowProjectMode {
			// 默认拒绝 A 档：它必须被显式开启（ADR-008 决策反转）。
			return a.reject(ReasonModeMismatch, errors.New("项目级凭据（A 档）未在本网关开启"))
		}
		if a.isBlacklisted(req.ClientID, req.RemoteIP, id.ProjectID) {
			return a.reject(ReasonBlacklisted, nil)
		}
		if err := a.verifyProject(ctx, id, req); err != nil {
			return a.rejectFor(classifyVerifyErr(err), err, req.ClientID, req.RemoteIP, id.ProjectID)
		}

	case ModePerDevice:
		if req.ClientID != req.Username || req.Username != id.DeviceKey {
			// 03 §2.1：clientID == username == device_key 的强校验阻止跨设备冒用。
			return a.rejectFor(ReasonBadCredential,
				fmt.Errorf("clientID/username/device_key 不一致"), req.ClientID, req.RemoteIP, 0)
		}
		if err := a.verifyArgon2(ctx, id.Secret, req.Password); err != nil {
			return a.rejectFor(classifyVerifyErr(err), err, req.ClientID, req.RemoteIP, 0)
		}

	case ModeMTLS:
		if !req.TLSVerified || req.TLSCommonName != id.DeviceKey {
			return a.rejectFor(ReasonBadCredential,
				fmt.Errorf("mTLS 证书未校验或 CN(%q) 与 device_key(%q) 不一致", req.TLSCommonName, id.DeviceKey),
				req.ClientID, req.RemoteIP, 0)
		}
	}

	projectID := int64(0)
	if id.Mode == ModeProject {
		projectID = id.ProjectID
	}
	a.clearFailures(req.ClientID, req.RemoteIP, projectID)
	a.metrics.Success.Add(1)

	return &Result{
		ProjectID:         id.ProjectID,
		DeviceID:          id.DeviceID,
		DeviceTypeID:      id.DeviceTypeID,
		DeviceKey:         id.DeviceKey,
		Mode:              id.Mode,
		ACL:               NewACL(id),
		CredentialVersion: id.CredentialVersion,
	}, nil
}

// verifyProject 校验 A 档凭据。
func (a *Authenticator) verifyProject(ctx context.Context, id *Identity, req Request) error {
	if !VerifyToken(id.ProjectTokenHash, req.Username) {
		return errors.New("AccessToken 不匹配")
	}
	return a.verifyArgon2(ctx, id.ProjectKey, req.Password)
}

// verifyArgon2 在并发上限内执行一次慢哈希校验。
//
// 排队超时返回 errOverloaded —— 调用方必须把它归类为「过载」而不是「凭据错误」。
func (a *Authenticator) verifyArgon2(ctx context.Context, d Digest, secret string) error {
	if len(d.Hash) == 0 {
		// 没有摘要 → 拒绝。设备在注册表里但没有凭据摘要，属于配置错误。
		return errors.New("设备未配置凭据摘要")
	}

	release, err := a.acquireVerifySlot(ctx)
	if err != nil {
		return err
	}
	defer release()

	if !d.Verify(secret) {
		return errBadCredential
	}
	return nil
}

var (
	errBadCredential = errors.New("凭据不匹配")
	errOverloaded    = errors.New("认证校验排队超时（本网关过载）")
)

func (a *Authenticator) acquireVerifySlot(ctx context.Context) (func(), error) {
	if a.verifySlots == nil {
		return func() {}, nil
	}

	timeout := a.policy.VerifyQueueTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case a.verifySlots <- struct{}{}:
		return func() { <-a.verifySlots }, nil
	case <-timer.C:
		a.metrics.Overloaded.Add(1)
		return nil, errOverloaded
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// classifyVerifyErr 保证「凭据错误」与「过载」不会被混为一谈。
func classifyVerifyErr(err error) Reason {
	if errors.Is(err, errOverloaded) {
		return ReasonOverloaded
	}
	return ReasonBadCredential
}

func (a *Authenticator) reject(r Reason, err error) (*Result, error) {
	a.metrics.countFailure(r)
	return nil, fail(r, err)
}

// rejectFor 在计数失败的同时维护失败窗口与黑名单。
func (a *Authenticator) rejectFor(r Reason, err error, clientID, ip string, projectID int64) (*Result, error) {
	a.metrics.countFailure(r)

	// 过载与后端不可用**不计入**失败窗口：它们与设备行为无关。
	if r != ReasonOverloaded && r != ReasonDirectoryDown {
		a.recordFailure(clientID, ip, projectID)
	}
	return nil, fail(r, err)
}

// ---------- 失败计数与黑名单 ----------

// maxFailCounters 是失败窗口计数表的上限。
//
// 需要上限的原因很现实：伪造 clientID 的连接风暴会让这张表无限增长 ——
// 一个防滥用的机制自己变成内存泄漏点，是典型的安全设计反面教材。
const maxFailCounters = 50_000

func (a *Authenticator) recordFailure(clientID, ip string, projectID int64) {
	now := a.now()

	a.mu.Lock()
	defer a.mu.Unlock()

	a.maybeSweepLocked(now)

	if a.bumpLocked("dev:"+clientID, now, a.policy.FailLimitPerDevice) {
		a.blacklist["dev:"+clientID] = now.Add(a.policy.BlacklistTTL)
	}
	if ip != "" && a.bumpLocked("ip:"+ip, now, a.policy.FailLimitPerIP) {
		a.blacklist["ip:"+ip] = now.Add(a.policy.BlacklistTTL)
	}
	if projectID > 0 && a.bumpLocked(fmt.Sprintf("project:%d", projectID), now, a.policy.FailLimitPerProjectKey) {
		a.blacklist[fmt.Sprintf("project:%d", projectID)] = now.Add(a.policy.BlacklistTTL)
	}
}

// maybeSweepLocked 在超限时清理：先删过期项，仍然超限就整表重置。
//
// 整表重置是刻意的「丢计数保内存」：失败计数的漏计只会延迟黑名单生效，
// 而内存耗尽会让整个网关不可用 —— 两者不对称，取舍是明确的。
func (a *Authenticator) maybeSweepLocked(now time.Time) {
	if len(a.failCounts) < maxFailCounters {
		return
	}
	for k, c := range a.failCounts {
		if now.After(c.resetAt) {
			delete(a.failCounts, k)
		}
	}
	if len(a.failCounts) >= maxFailCounters {
		clear(a.failCounts)
		a.metrics.CounterSweeps.Add(1)
	}
}

// bumpLocked 累加窗口计数，返回是否达到阈值。
func (a *Authenticator) bumpLocked(key string, now time.Time, limit int) bool {
	c, ok := a.failCounts[key]
	if !ok || now.After(c.resetAt) {
		c = &failCounter{resetAt: now.Add(a.policy.FailWindow)}
		a.failCounts[key] = c
	}
	c.n++
	return limit > 0 && c.n >= limit
}

// clearFailures 在认证成功后清空该设备的窗口计数，
// 避免「偶发失败累积到阈值」误伤正常设备。
func (a *Authenticator) clearFailures(clientID, ip string, projectID int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	delete(a.failCounts, "dev:"+clientID)
	if ip != "" {
		delete(a.failCounts, "ip:"+ip)
	}
	if projectID > 0 {
		delete(a.failCounts, fmt.Sprintf("project:%d", projectID))
	}
}

func (a *Authenticator) isBlacklisted(clientID, ip string, projectID int64) bool {
	now := a.now()

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.blacklistedLocked("dev:"+clientID, now) {
		return true
	}
	if ip != "" && a.blacklistedLocked("ip:"+ip, now) {
		return true
	}
	if projectID > 0 && a.blacklistedLocked(fmt.Sprintf("project:%d", projectID), now) {
		return true
	}
	return false
}

func (a *Authenticator) blacklistedLocked(key string, now time.Time) bool {
	until, ok := a.blacklist[key]
	if !ok {
		return false
	}
	if now.After(until) {
		delete(a.blacklist, key)
		return false
	}
	return true
}

// BlacklistSize 返回当前黑名单条数（观测用）。
func (a *Authenticator) BlacklistSize() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.blacklist)
}
