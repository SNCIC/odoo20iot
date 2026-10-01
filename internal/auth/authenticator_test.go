package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// 单测用轻量参数：Argon2id 的慢正是被验证的对象，
// 功能用例没必要为此付出 64 MiB × 数十毫秒的代价。
var testParams = Params{Time: 1, Memory: 8 * 1024, Threads: 1, KeyLen: 32}

func mustDevice(t *testing.T, key, secret string, mode Mode) *Identity {
	t.Helper()

	salt, err := NewSalt()
	if err != nil {
		t.Fatalf("生成盐失败: %v", err)
	}
	return &Identity{
		ProjectID:    10231,
		DeviceTypeID: 55,
		DeviceKey:    key,
		Mode:         mode,
		Secret:       HashSecret(secret, testParams, salt),
	}
}

func newTestAuth(t *testing.T, ids ...*Identity) (*Authenticator, *Metrics) {
	t.Helper()

	byKey := make(map[string]*Identity, len(ids))
	for _, id := range ids {
		byKey[id.DeviceKey] = id
	}

	dir := DirectoryFunc(func(_ context.Context, clientID string) (*Identity, error) {
		return byKey[clientID], nil
	})

	policy := DefaultPolicy()
	policy.MaxConcurrentVerify = 4
	metrics := new(Metrics)
	return NewAuthenticator(dir, policy, metrics), metrics
}

func TestAuthenticate_PerDevice(t *testing.T) {
	id := mustDevice(t, "dev-A", "s3cret-A", ModePerDevice)
	a, _ := newTestAuth(t, id)

	res, err := a.Authenticate(context.Background(), Request{
		ClientID: "dev-A", Username: "dev-A", Password: "s3cret-A", RemoteIP: "10.0.0.1",
	})
	if err != nil {
		t.Fatalf("应认证通过: %v", err)
	}
	if res.DeviceKey != "dev-A" || res.ProjectID != 10231 || res.Mode != ModePerDevice {
		t.Fatalf("认证结果不对: %+v", res)
	}
	if res.ACL == nil || !res.ACL.Allow("v1/devices/dev-A/telemetry", true) {
		t.Fatal("认证结果应带可用 ACL")
	}

	// 密码错误
	_, err = a.Authenticate(context.Background(), Request{
		ClientID: "dev-A", Username: "dev-A", Password: "wrong", RemoteIP: "10.0.0.2",
	})
	if reasonOf(t, err) != ReasonBadCredential {
		t.Fatalf("期望 bad_credential，得到 %v", err)
	}
}

// TestAuthenticate_ClientID与Username必须一致 是 03 §2.1 的防串号要求。
func TestAuthenticate_ClientID与Username必须一致(t *testing.T) {
	id := mustDevice(t, "dev-A", "s3cret", ModePerDevice)
	a, _ := newTestAuth(t, id)

	cases := []Request{
		{ClientID: "dev-A", Username: "dev-B", Password: "s3cret"}, // 冒用别人的 clientID
		{ClientID: "dev-A", Username: "", Password: "s3cret"},      // 不填 username
		{ClientID: "dev-A", Username: "dev-A", Password: "s3cret", RemoteIP: "1.1.1.1"},
	}
	if _, err := a.Authenticate(context.Background(), cases[0]); reasonOf(t, err) != ReasonBadCredential {
		t.Fatalf("username 不一致应拒绝，得到 %v", err)
	}
	if _, err := a.Authenticate(context.Background(), cases[1]); reasonOf(t, err) != ReasonBadCredential {
		t.Fatalf("username 为空应拒绝，得到 %v", err)
	}
	if _, err := a.Authenticate(context.Background(), cases[2]); err != nil {
		t.Fatalf("三者一致应通过，得到 %v", err)
	}
}

// TestAuthenticate_项目级默认关闭 验证 ADR-008 的决策反转落到了默认值上。
func TestAuthenticate_项目级默认关闭(t *testing.T) {
	id := mustDevice(t, "dev-A", "", ModeProject)
	// A 档的凭据挂在项目上：AccessToken 走 SHA-256，ProjectKey 走 Argon2id。
	id.ProjectTokenHash = HashToken("access-token")
	salt, err := NewSalt()
	if err != nil {
		t.Fatalf("生成盐失败: %v", err)
	}
	id.ProjectKey = HashSecret("project-key", testParams, salt)
	a, _ := newTestAuth(t, id)

	req := Request{ClientID: "dev-A", Username: "access-token", Password: "project-key", RemoteIP: "10.0.0.3"}

	_, err = a.Authenticate(context.Background(), req)
	if reasonOf(t, err) != ReasonModeMismatch {
		t.Fatalf("A 档默认应关闭，得到 %v", err)
	}

	// 显式开启后应通过 —— 证明拒绝的原因是策略而不是实现缺陷。
	byKey := map[string]*Identity{"dev-A": id}
	policy := DefaultPolicy()
	policy.AllowProjectMode = true
	policy.MaxConcurrentVerify = 2
	a2 := NewAuthenticator(DirectoryFunc(func(_ context.Context, k string) (*Identity, error) {
		return byKey[k], nil
	}), policy, new(Metrics))

	if _, err := a2.Authenticate(context.Background(), req); err != nil {
		t.Fatalf("显式开启 A 档后应通过: %v", err)
	}

	// 令牌错误
	req.Username = "wrong-token"
	if _, err := a2.Authenticate(context.Background(), req); reasonOf(t, err) != ReasonBadCredential {
		t.Fatalf("错误 AccessToken 应拒绝，得到 %v", err)
	}
}

// TestAuthenticate_mTLS 覆盖 C 档：CN 必须与 device_key 一致且证书已验证。
func TestAuthenticate_mTLS(t *testing.T) {
	id := mustDevice(t, "dev-A", "", ModeMTLS)
	a, _ := newTestAuth(t, id)

	ok := Request{ClientID: "dev-A", TLSVerified: true, TLSCommonName: "dev-A"}
	if _, err := a.Authenticate(context.Background(), ok); err != nil {
		t.Fatalf("mTLS 应通过: %v", err)
	}

	bad := []Request{
		{ClientID: "dev-A", TLSVerified: true, TLSCommonName: "dev-B"},  // CN 不符
		{ClientID: "dev-A", TLSVerified: false, TLSCommonName: "dev-A"}, // 证书未校验
	}
	for _, r := range bad {
		if _, err := a.Authenticate(context.Background(), r); reasonOf(t, err) != ReasonBadCredential {
			t.Errorf("应拒绝 %+v，得到 %v", r, err)
		}
	}
}

func TestAuthenticate_撤销与查无此设备(t *testing.T) {
	revoked := mustDevice(t, "dev-A", "s", ModePerDevice)
	revoked.Revoked = true
	a, _ := newTestAuth(t, revoked)

	_, err := a.Authenticate(context.Background(), Request{
		ClientID: "dev-A", Username: "dev-A", Password: "s", RemoteIP: "10.0.0.9"})
	if reasonOf(t, err) != ReasonRevoked {
		t.Fatalf("期望 revoked，得到 %v", err)
	}

	_, err = a.Authenticate(context.Background(), Request{
		ClientID: "ghost", Username: "ghost", Password: "s", RemoteIP: "10.0.0.9"})
	if reasonOf(t, err) != ReasonUnknownDevice {
		t.Fatalf("期望 unknown_device，得到 %v", err)
	}
}

// TestAuthenticate_后端不可用必须fail_closed 是 03 §2.1「降级」的核心：
// 仅靠 L1 缓存不足以放行，缓存没有的业务必须被拒。
func TestAuthenticate_后端不可用必须fail_closed(t *testing.T) {
	id := mustDevice(t, "dev-A", "s", ModePerDevice)
	backed := &flakyDirectory{ids: map[string]*Identity{"dev-A": id}}

	cached := NewCachedDirectory(backed, WithCacheTTL(time.Minute))
	a := NewAuthenticator(cached, DefaultPolicy(), new(Metrics))

	// 先成功一次，把身份放进 L1。
	if _, err := a.Authenticate(context.Background(), Request{
		ClientID: "dev-A", Username: "dev-A", Password: "s", RemoteIP: "10.0.0.1"}); err != nil {
		t.Fatalf("首次认证应通过: %v", err)
	}

	// 后端挂掉：已缓存设备仍可接入（L1 降级）。
	backed.setDown(true)
	if _, err := a.Authenticate(context.Background(), Request{
		ClientID: "dev-A", Username: "dev-A", Password: "s", RemoteIP: "10.0.0.1"}); err != nil {
		t.Fatalf("后端不可用时已缓存设备应放行: %v", err)
	}

	// 未缓存设备一律拒绝（fail-closed），且归类为目录不可用而非凭据错误。
	_, err := a.Authenticate(context.Background(), Request{
		ClientID: "dev-B", Username: "dev-B", Password: "s", RemoteIP: "10.0.0.1"})
	if reasonOf(t, err) != ReasonDirectoryDown {
		t.Fatalf("期望 directory_unavailable，得到 %v", err)
	}
}

// TestAuthenticate_过载不得计入失败计数 是本模块最重要的一条断言。
//
// 若把「网关过载」当成「凭据错误」计数，重连风暴会把**全部正常设备**打进黑名单 ——
// 平台自己的容量问题会升级成全网设备离线。
func TestAuthenticate_过载不得计入失败计数(t *testing.T) {
	id := mustDevice(t, "dev-A", "s", ModePerDevice)
	byKey := map[string]*Identity{"dev-A": id}

	policy := DefaultPolicy()
	policy.MaxConcurrentVerify = 1
	policy.VerifyQueueTimeout = 30 * time.Millisecond
	policy.FailLimitPerDevice = 3

	a := NewAuthenticator(DirectoryFunc(func(_ context.Context, k string) (*Identity, error) {
		return byKey[k], nil
	}), policy, new(Metrics))

	// 占满唯一的校验槽位，制造过载。
	release := make(chan struct{})
	blocked := make(chan struct{})
	a.verifySlots <- struct{}{}
	go func() {
		close(blocked)
		<-release
		<-a.verifySlots
	}()
	<-blocked

	// 连续 10 次必然排队超时。
	const attempts = 10
	for i := 0; i < attempts; i++ {
		_, err := a.Authenticate(context.Background(), Request{
			ClientID: "dev-A", Username: "dev-A", Password: "s", RemoteIP: "10.0.0.1"})
		if reasonOf(t, err) != ReasonOverloaded {
			t.Fatalf("第 %d 次应为 overloaded，得到 %v", i, err)
		}
	}

	m := a.Metrics()
	if got := m.FailureCount(ReasonOverloaded); got != attempts {
		t.Fatalf("过载计数应为 %d，得到 %d", attempts, got)
	}
	if got := m.FailureCount(ReasonBadCredential); got != 0 {
		t.Fatalf("过载不得计入凭据失败，实际 %d", got)
	}
	if a.BlacklistSize() != 0 {
		t.Fatalf("过载不得触发黑名单，实际 %d 条", a.BlacklistSize())
	}

	// 过载过去之后，正常凭据必须仍然可用。
	close(release)
	time.Sleep(50 * time.Millisecond)
	if _, err := a.Authenticate(context.Background(), Request{
		ClientID: "dev-A", Username: "dev-A", Password: "s", RemoteIP: "10.0.0.1"}); err != nil {
		t.Fatalf("过载恢复后应可认证: %v", err)
	}
}

// TestAuthenticate_失败达阈值触发黑名单 验证 03 §2.1 的失败计数与封禁。
func TestAuthenticate_失败达阈值触发黑名单(t *testing.T) {
	id := mustDevice(t, "dev-A", "s", ModePerDevice)
	a, _ := newTestAuth(t, id)

	policy := a.policy
	for i := 0; i < policy.FailLimitPerDevice; i++ {
		_, err := a.Authenticate(context.Background(), Request{
			ClientID: "dev-A", Username: "dev-A", Password: "wrong", RemoteIP: "10.0.0.1"})
		if reasonOf(t, err) != ReasonBadCredential {
			t.Fatalf("第 %d 次应为 bad_credential，得到 %v", i, err)
		}
	}

	// 达到阈值后，即使凭据正确也应先被黑名单拦下。
	_, err := a.Authenticate(context.Background(), Request{
		ClientID: "dev-A", Username: "dev-A", Password: "s", RemoteIP: "10.0.0.1"})
	if reasonOf(t, err) != ReasonBlacklisted {
		t.Fatalf("期望 blacklisted，得到 %v", err)
	}
}

// TestAuthenticate_失败计数表有上限 防止伪造 clientID 把计数表撑爆。
func TestAuthenticate_失败计数表有上限(t *testing.T) {
	id := mustDevice(t, "dev-A", "s", ModePerDevice)
	a, _ := newTestAuth(t, id)

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_, _ = a.Authenticate(ctx, Request{
			ClientID: "ghost-" + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			Username: "x", Password: "y", RemoteIP: "10.0.0.1"})
	}

	a.mu.Lock()
	size := len(a.failCounts)
	a.mu.Unlock()
	if size > maxFailCounters {
		t.Fatalf("失败计数表超过上限: %d", size)
	}
}

// TestCachedDirectory_负缓存生效 验证伪造 clientID 不会每次都穿透后端。
func TestCachedDirectory_负缓存生效(t *testing.T) {
	var calls int
	var mu sync.Mutex
	dir := DirectoryFunc(func(_ context.Context, _ string) (*Identity, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil, nil
	})

	c := NewCachedDirectory(dir, WithNegativeTTL(time.Minute))
	for i := 0; i < 10; i++ {
		id, err := c.Lookup(context.Background(), "ghost")
		if err != nil || id != nil {
			t.Fatalf("查无此设备应返回 (nil, nil)，得到 (%v, %v)", id, err)
		}
	}

	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("负缓存应把 10 次查询收敛为 1 次回源，实际 %d 次", got)
	}

	st := c.Stats()
	if st.NegativeHits != 9 || st.Misses != 1 {
		t.Fatalf("缓存统计不对: %+v", st)
	}
	// 9 次负缓存命中 / 10 次查询 = 0.9：负缓存也是「命中」，否则指标会误导。
	if r := st.HitRatio(); r < 0.899 || r > 0.901 {
		t.Fatalf("命中率应为 0.9，得到 %v", r)
	}
}

func TestCachedDirectory_LRU淘汰(t *testing.T) {
	dir := DirectoryFunc(func(_ context.Context, clientID string) (*Identity, error) {
		return &Identity{DeviceKey: clientID, Mode: ModePerDevice}, nil
	})
	c := NewCachedDirectory(dir, WithCacheCapacity(2))

	for _, k := range []string{"a", "b", "c"} {
		if _, err := c.Lookup(context.Background(), k); err != nil {
			t.Fatalf("查询 %s 失败: %v", k, err)
		}
	}
	if st := c.Stats(); st.Size != 2 || st.Evictions != 1 {
		t.Fatalf("LRU 行为不对: %+v", st)
	}
}

// TestCachedDirectory_宽限期后不再降级 验证降级有边界。
func TestCachedDirectory_宽限期后不再降级(t *testing.T) {
	now := time.Now()
	dir := &flakyDirectory{ids: map[string]*Identity{
		"dev-A": {DeviceKey: "dev-A", Mode: ModePerDevice},
	}}
	c := NewCachedDirectory(dir,
		WithCacheTTL(time.Second),
		WithStaleGrace(5*time.Second),
		WithClock(func() time.Time { return now }),
	)

	if _, err := c.Lookup(context.Background(), "dev-A"); err != nil {
		t.Fatalf("首次查询失败: %v", err)
	}

	dir.setDown(true)

	// 宽限期内 → 降级放行。
	now = now.Add(3 * time.Second)
	if id, err := c.Lookup(context.Background(), "dev-A"); err != nil || id == nil {
		t.Fatalf("宽限期内应降级放行，得到 (%v, %v)", id, err)
	}

	// 超过宽限期 → fail-closed。
	now = now.Add(10 * time.Second)
	if _, err := c.Lookup(context.Background(), "dev-A"); err == nil {
		t.Fatal("超过宽限期必须拒绝")
	}
}

// ---------- 测试替身 ----------

type flakyDirectory struct {
	mu   sync.Mutex
	ids  map[string]*Identity
	down bool
}

func (f *flakyDirectory) setDown(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = v
}

func (f *flakyDirectory) Lookup(_ context.Context, clientID string) (*Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.down {
		return nil, errors.New("后端不可用")
	}
	return f.ids[clientID], nil
}

func reasonOf(t *testing.T, err error) Reason {
	t.Helper()

	if err == nil {
		t.Fatal("期望认证失败，实际通过")
	}
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("期望 *Failure，得到 %T: %v", err, err)
	}
	return f.Reason
}
