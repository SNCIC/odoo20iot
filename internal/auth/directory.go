package auth

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Identity 是按 clientID 解析出的设备身份，是认证与 ACL 的唯一输入。
//
// 它把「认证需要知道的全部」收在一处：凭据摘要、档位、撤销状态、ACL 归属。
// 网关不接触秘钥原文，也不接触数据库。
type Identity struct {
	ProjectID    int64
	DeviceID     int64
	DeviceTypeID int64
	DeviceKey    string
	Mode         Mode
	// IsGateway 为真时额外放行 v1/gateways/{key}/devices/{sub}/...（03 §2.2）。
	IsGateway bool

	// B 档：设备 secret 的慢哈希摘要。
	Secret Digest

	// A 档：项目级凭据。
	// AccessToken 是高熵随机串 → SHA-256；ProjectKey 参与共享，用 Argon2id。
	ProjectTokenHash []byte
	ProjectKey       Digest

	// CredentialVersion 用于轮换与吊销。
	CredentialVersion int64
	// Revoked 为真表示凭据已吊销（03 §2.1「吊销与轮换」）。
	Revoked bool
}

// Directory 把 clientID 解析为设备身份。
//
// 契约（fail-closed）：返回 error 表示**无法判定**，调用方必须拒绝认证；
// 返回 (nil, nil) 表示查无此设备，同样拒绝。任何情况下都不得「查不到就放行」。
type Directory interface {
	Lookup(ctx context.Context, clientID string) (*Identity, error)
}

// DirectoryFunc 让普通函数满足 Directory（测试与简单部署用）。
type DirectoryFunc func(ctx context.Context, clientID string) (*Identity, error)

// Lookup 实现 Directory。
func (f DirectoryFunc) Lookup(ctx context.Context, clientID string) (*Identity, error) {
	return f(ctx, clientID)
}

// CacheStats 是 L1 缓存的观测数据（对应 06 §4 的 `gw_auth_cache_hit_ratio`）。
type CacheStats struct {
	Hits         int64 // 命中且未过期
	Misses       int64
	NegativeHits int64 // 命中「查无此设备」的负缓存
	Degraded     int64 // 上游不可用，用了宽限期内的旧值
	UpstreamErrs int64
	Evictions    int64
	Size         int
}

// HitRatio 返回有效命中率（命中 / 全部查询）。
func (s CacheStats) HitRatio() float64 {
	total := s.Hits + s.NegativeHits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits+s.NegativeHits) / float64(total)
}

// 03 §2.1 的 L1 默认值。
const (
	DefaultCacheCapacity = 10000
	DefaultCacheTTL      = 60 * time.Second
	// DefaultStaleGrace 是上游不可用时，过期缓存还能继续放行的时长。
	//
	// 03 §2.1「降级」要求：svc-auth 不可用时，已认证过的设备仍可接入，
	// 新设备一律拒绝（fail-closed）。宽限期实现前半句；
	// 后半句由「缓存里没有 → 回源失败 → 拒绝」天然保证。
	DefaultStaleGrace = 10 * time.Minute
	// DefaultNegativeTTL 是「查无此设备」的缓存时长。
	//
	// 负缓存不是可选优化：重连风暴里混入伪造 clientID 时，没有它就会
	// 每次穿透到后端，把认证后端一起打垮。
	DefaultNegativeTTL = 10 * time.Second
)

type cacheEntry struct {
	clientID  string
	id        *Identity // nil 表示负缓存（查无此设备）
	expire    time.Time // 正常复用截止
	hardStale time.Time // 超过它连降级也不复用
}

// CachedDirectory 在真实目录之前加一层本地 LRU + TTL 缓存。
//
// 它是 03 §2.1 三级缓存里的 L1；L2（Redis）/ L3（svc-auth）由 upstream 承担。
type CachedDirectory struct {
	upstream Directory
	capacity int
	ttl      time.Duration
	negTTL   time.Duration
	staleFor time.Duration
	now      func() time.Time

	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List // 前端 = 最近使用

	hits         atomic.Int64
	misses       atomic.Int64
	negativeHits atomic.Int64
	degraded     atomic.Int64
	upstreamErrs atomic.Int64
	evictions    atomic.Int64
}

// CacheOption 用于覆盖默认参数。
type CacheOption func(*CachedDirectory)

// WithCacheCapacity 设置 L1 容量。
func WithCacheCapacity(n int) CacheOption {
	return func(c *CachedDirectory) {
		if n > 0 {
			c.capacity = n
		}
	}
}

// WithCacheTTL 设置正缓存 TTL。
func WithCacheTTL(d time.Duration) CacheOption {
	return func(c *CachedDirectory) {
		if d > 0 {
			c.ttl = d
		}
	}
}

// WithStaleGrace 设置降级宽限期。
func WithStaleGrace(d time.Duration) CacheOption {
	return func(c *CachedDirectory) {
		if d >= 0 {
			c.staleFor = d
		}
	}
}

// WithNegativeTTL 设置负缓存 TTL。
func WithNegativeTTL(d time.Duration) CacheOption {
	return func(c *CachedDirectory) {
		if d > 0 {
			c.negTTL = d
		}
	}
}

// WithClock 注入时钟（测试用）。
func WithClock(f func() time.Time) CacheOption {
	return func(c *CachedDirectory) {
		if f != nil {
			c.now = f
		}
	}
}

// NewCachedDirectory 包装一个上游目录。
func NewCachedDirectory(upstream Directory, opts ...CacheOption) *CachedDirectory {
	c := &CachedDirectory{
		upstream: upstream,
		capacity: DefaultCacheCapacity,
		ttl:      DefaultCacheTTL,
		negTTL:   DefaultNegativeTTL,
		staleFor: DefaultStaleGrace,
		now:      time.Now,
		entries:  make(map[string]*list.Element, DefaultCacheCapacity),
		lru:      list.New(),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Lookup 实现 Directory。
//
// 三种结果，语义各不相同，调用方必须区分：
//   - (id, nil)    命中设备；
//   - (nil, nil)   确认「查无此设备」→ 拒绝；
//   - (nil, err)   **无法判定** → 拒绝（fail-closed），且不得计入凭据失败。
func (c *CachedDirectory) Lookup(ctx context.Context, clientID string) (*Identity, error) {
	now := c.now()

	if id, _, fresh := c.probe(clientID, now); fresh {
		if id == nil {
			c.negativeHits.Add(1)
		} else {
			c.hits.Add(1)
		}
		return id, nil
	}

	c.misses.Add(1)

	id, err := c.upstream.Lookup(ctx, clientID)
	if err != nil {
		c.upstreamErrs.Add(1)

		// 降级：只复用**已认证过**的设备身份，绝不在这里放行未知设备。
		if stale, found, _ := c.probe(clientID, now); found && stale != nil {
			c.degraded.Add(1)
			return stale, nil
		}
		return nil, err
	}

	if id == nil {
		c.store(clientID, nil, now.Add(c.negTTL), now)
		return nil, nil
	}
	c.store(clientID, id, now.Add(c.ttl), now)
	return id, nil
}

// Stats 返回缓存观测数据。
func (c *CachedDirectory) Stats() CacheStats {
	c.mu.Lock()
	size := c.lru.Len()
	c.mu.Unlock()

	return CacheStats{
		Hits:         c.hits.Load(),
		Misses:       c.misses.Load(),
		NegativeHits: c.negativeHits.Load(),
		Degraded:     c.degraded.Load(),
		UpstreamErrs: c.upstreamErrs.Load(),
		Evictions:    c.evictions.Load(),
		Size:         size,
	}
}

// Invalidate 主动失效一条缓存（吊销/轮换时由控制面调用）。
func (c *CachedDirectory) Invalidate(clientID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.entries[clientID]; ok {
		c.lru.Remove(el)
		delete(c.entries, clientID)
	}
}

// probe 返回 (身份, 是否存在且未过宽限期, 是否新鲜)。
func (c *CachedDirectory) probe(clientID string, now time.Time) (*Identity, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.entries[clientID]
	if !ok {
		return nil, false, false
	}
	e := el.Value.(*cacheEntry)
	if now.After(e.hardStale) {
		// 连降级都不该再用，顺手清掉，避免长期占容量。
		c.lru.Remove(el)
		delete(c.entries, clientID)
		return nil, false, false
	}
	c.lru.MoveToFront(el)
	return e.id, true, !now.After(e.expire)
}

// store 写入缓存。hardStale 由「新鲜窗口 + 宽限期」推导，
// 这样过期后仍能在上游故障时按 §2.1 的降级语义复用一段时间。
func (c *CachedDirectory) store(clientID string, id *Identity, expire, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.entries[clientID]; ok {
		e := el.Value.(*cacheEntry)
		e.id, e.expire = id, expire
		e.hardStale = expire.Add(c.staleFor)
		c.lru.MoveToFront(el)
		return
	}

	el := c.lru.PushFront(&cacheEntry{
		clientID:  clientID,
		id:        id,
		expire:    expire,
		hardStale: expire.Add(c.staleFor),
	})
	c.entries[clientID] = el

	for c.lru.Len() > c.capacity {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.lru.Remove(back)
		delete(c.entries, back.Value.(*cacheEntry).clientID)
		c.evictions.Add(1)
	}
}
