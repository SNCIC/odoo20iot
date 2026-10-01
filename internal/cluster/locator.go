package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Locator 回答一个问题：**某台设备当前连在哪个网关节点的上？**
//
// # 为什么是「客户端位置」而不是文档里的「过滤器反向索引」
//
// 03 §1.4 原本用 `gw:route:{filter_hash}` 做「由 topic 反查哪些节点有订阅」。
// 但订阅是**通配符过滤器**（`v1/devices/+/cmd/+`），按 filter 的哈希建的索引
// 只能精确匹配 —— 给定一条具体 topic，不枚举全部 filter 就无法找出匹配项，
// 而设备级订阅数量与设备数同阶（10 万级），枚举不可行。
//
// 好在文档自己的约束把这个问题化掉了（§2.2）：
//   - 设备**只能**订阅自己的命名空间（防横向越权）；
//   - 平台侧订阅不走 MQTT。
//
// 于是设备命名空间下「谁可能订阅了 v1/devices/{key}/...」只剩一个答案：
// **{key} 这台设备本身**。跨节点投递因此退化成一次 **O(1) 的位置查询**，
// 既不需要过滤器索引，也不需要广播。
//
// 非设备命名空间（应用侧订阅，数量少）仍走广播兜底 —— 见 Node.Route。
type Locator interface {
	// Bind 记录设备当前所在节点。
	Bind(ctx context.Context, deviceKey, nodeID string) error
	// Unbind 在设备断开时清除记录（仅当记录仍指向该节点）。
	Unbind(ctx context.Context, deviceKey, nodeID string) error
	// NodeOf 返回设备所在节点；第二个返回值为 false 表示设备不在线。
	NodeOf(ctx context.Context, deviceKey string) (string, bool, error)
}

// RedisLocator 是 Locator 的 Redis 实现（03 §1.3 的注册表）。
//
// 记录带 TTL：网关节点被 kill 时来不及 Unbind，TTL 是最后一道防线，
// 避免「僵尸位置」让消息永远路由到一个已死的节点。
type RedisLocator struct {
	rdb    redis.UniversalClient
	prefix string
	ttl    time.Duration
}

// DefaultLocatorTTL 是位置记录的有效期。
//
// 取 5 分钟：远大于心跳周期（设备侧 KeepAlive 通常 ≤ 5 分钟），
// 又短到足以在节点被 kill 后自动收敛。
const DefaultLocatorTTL = 5 * time.Minute

// NewRedisLocator 构造 Redis 位置注册表。
func NewRedisLocator(rdb redis.UniversalClient, prefix string, ttl time.Duration) *RedisLocator {
	if ttl <= 0 {
		ttl = DefaultLocatorTTL
	}
	if prefix == "" {
		prefix = "gw:client"
	}
	return &RedisLocator{rdb: rdb, prefix: prefix, ttl: ttl}
}

func (l *RedisLocator) key(deviceKey string) string { return l.prefix + ":" + deviceKey }

// Bind 实现 Locator。
func (l *RedisLocator) Bind(ctx context.Context, deviceKey, nodeID string) error {
	if err := l.rdb.Set(ctx, l.key(deviceKey), nodeID, l.ttl).Err(); err != nil {
		return fmt.Errorf("登记设备位置 %s→%s: %w", deviceKey, nodeID, err)
	}
	return nil
}

// Unbind 实现 Locator。用 Lua 保证「只删自己那条」，避免竞态下
// 把设备在新节点上的登记删掉（设备刚重连到别的节点时会发生）。
func (l *RedisLocator) Unbind(ctx context.Context, deviceKey, nodeID string) error {
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`

	if err := l.rdb.Eval(ctx, script, []string{l.key(deviceKey)}, nodeID).Err(); err != nil {
		return fmt.Errorf("清除设备位置 %s: %w", deviceKey, err)
	}
	return nil
}

// NodeOf 实现 Locator。
func (l *RedisLocator) NodeOf(ctx context.Context, deviceKey string) (string, bool, error) {
	v, err := l.rdb.Get(ctx, l.key(deviceKey)).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return "", false, nil // 不在线，不是错误
	case err != nil:
		return "", false, fmt.Errorf("查询设备位置 %s: %w", deviceKey, err)
	}
	return v, true, nil
}

// MemLocator 是 Locator 的内存实现（单节点部署与测试用）。
type MemLocator struct {
	mu    sync.RWMutex
	nodes map[string]string
}

// NewMemLocator 构造内存位置注册表。
func NewMemLocator() *MemLocator {
	return &MemLocator{nodes: make(map[string]string, 1024)}
}

// Bind 实现 Locator。
func (l *MemLocator) Bind(_ context.Context, deviceKey, nodeID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nodes[deviceKey] = nodeID
	return nil
}

// Unbind 实现 Locator。
func (l *MemLocator) Unbind(_ context.Context, deviceKey, nodeID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.nodes[deviceKey] == nodeID {
		delete(l.nodes, deviceKey)
	}
	return nil
}

// NodeOf 实现 Locator。
func (l *MemLocator) NodeOf(_ context.Context, deviceKey string) (string, bool, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	n, ok := l.nodes[deviceKey]
	return n, ok, nil
}

// Size 返回登记条数（观测与测试用）。
func (l *MemLocator) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.nodes)
}

// RedisCursor 是 CursorStore 的 Redis 实现。
//
// ⚠️ 游标必须**跨节点共享**：设备重连可能落到任意节点（03 §1.5）。
// 只在单节点部署下才可以用 MemCursor。
type RedisCursor struct {
	rdb    redis.UniversalClient
	prefix string
}

// NewRedisCursor 构造 Redis 游标存储。
func NewRedisCursor(rdb redis.UniversalClient, prefix string) *RedisCursor {
	if prefix == "" {
		prefix = "gw:offline:cursor"
	}
	return &RedisCursor{rdb: rdb, prefix: prefix}
}

// DefaultCursorTTL 是游标的保留时长，与离线流 TTL 对齐 ——
// 游标过期不会造成丢失，只会让一台设备重复回放一次离线消息（设备侧按 msg_id 去重）。
const DefaultCursorTTL = DefaultOfflineTTL + time.Hour

// Get 实现 CursorStore。
func (c *RedisCursor) Get(ctx context.Context, deviceKey string) (uint64, error) {
	v, err := c.rdb.Get(ctx, c.prefix+":"+deviceKey).Uint64()
	switch {
	case errors.Is(err, redis.Nil):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("读取离线游标 %s: %w", deviceKey, err)
	}
	return v, nil
}

// Set 实现 CursorStore。
func (c *RedisCursor) Set(ctx context.Context, deviceKey string, seq uint64) error {
	if err := c.rdb.Set(ctx, c.prefix+":"+deviceKey, seq, DefaultCursorTTL).Err(); err != nil {
		return fmt.Errorf("写入离线游标 %s: %w", deviceKey, err)
	}
	return nil
}
