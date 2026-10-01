package cluster

import (
	"strconv"
	"sync"
	"time"
)

// dedupCache 是接收端按 (Origin, Seq) 的去重缓存。
//
// 它服务于一个很窄的场景：NATS JetStream 的 redelivery ——
// 注入成功后若 Ack 未在 AckWait 内被服务端确认（网络抖动、Ack 报文丢失），
// 同一条路由消息会再次投给本节点。若不拦截，跨节点命令会被重复注入
// 本地 broker，导致设备重复执行同一条命令。
//
// 边界（诚实声明，不做 exactly-once 的假象）：
//   - 进程内、有界、带 TTL，只做 best-effort 去重；
//   - 节点**进程崩溃**后缓存随之消失，重启后的重投仍会重复注入 ——
//     那个场景由设备侧 msg_id 幂等兜底（03 §1.4 的「至少一次」语义）。
type dedupCache struct {
	mu      sync.Mutex
	entries map[string]time.Time
	ttl     time.Duration
	max     int
}

// defaultDedupTTL 覆盖绝大多数 redelivery：AckWait 默认 5s，
// 重投通常在同一窗口内发生，1 分钟是几倍的余量。
const defaultDedupTTL = time.Minute

// defaultDedupMax 是缓存条目上限。去重是 best-effort：超限时整体清空，
// 宁可放行重复，也不能让缓存无界增长拖垮网关。
const defaultDedupMax = 1 << 17 // 131072

func newDedupCache() *dedupCache {
	return &dedupCache{
		entries: make(map[string]time.Time, 4096),
		ttl:     defaultDedupTTL,
		max:     defaultDedupMax,
	}
}

func dedupKey(origin string, seq uint64) string {
	return origin + ":" + strconv.FormatUint(seq, 10)
}

// seen 返回该 (Origin, Seq) 是否已经成功处理过。调用会顺带清理过期条目。
func (d *dedupCache) seen(origin string, seq uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := dedupKey(origin, seq)
	at, ok := d.entries[key]
	if !ok {
		return false
	}
	if time.Since(at) > d.ttl {
		delete(d.entries, key)
		return false
	}
	return true
}

// mark 记录该 (Origin, Seq) 已被成功处理。
func (d *dedupCache) mark(origin string, seq uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.entries) >= d.max {
		// 先清过期项；仍超限则整体清空（best-effort，见类型注释）。
		now := time.Now()
		for k, at := range d.entries {
			if now.Sub(at) > d.ttl {
				delete(d.entries, k)
			}
		}
		if len(d.entries) >= d.max {
			d.entries = make(map[string]time.Time, 4096)
		}
	}
	d.entries[dedupKey(origin, seq)] = time.Now()
}
