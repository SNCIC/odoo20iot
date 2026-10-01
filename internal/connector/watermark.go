package connector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// WatermarkPrefix 是 C-2 水位在 Redis 里的键前缀（每模型一个键）。
const WatermarkPrefix = "reconcile:c2:wm:"

// Watermark 是 C-2 对账的水位：`(write_date, id)` 二元组。
//
// 为什么不能只用 `write_date`：同一秒内可能有多条记录被写入，
// 单用时间戳做 `write_date > 水位` 会**漏掉与水位同期的那几条** ——
// 而对账的全部价值就在于「不遗漏」。带上 id 做次级排序后边界才是精确的。
type Watermark struct {
	WriteDate time.Time
	ID        int64
}

// After 判定 w 是否严格晚于 other。
func (w Watermark) After(other Watermark) bool {
	if w.WriteDate.After(other.WriteDate) {
		return true
	}
	return w.WriteDate.Equal(other.WriteDate) && w.ID > other.ID
}

// Watermarks 存取「C-2 已处理到哪个位置」（07 §4.4 对账的水位）。
type Watermarks interface {
	// Get 读取水位；第二个返回值为 false 表示尚未建立水位。
	Get(ctx context.Context, model string) (Watermark, bool, error)
	// Advance 把水位推进到 w；**只进不退**（落后会导致重复处理，重复由下游去重兜底）。
	Advance(ctx context.Context, model string, w Watermark) error
}

// MemWatermarks 是内存实现（单实例与测试）。
type MemWatermarks struct {
	mu   sync.Mutex
	data map[string]Watermark
}

var _ Watermarks = (*MemWatermarks)(nil)

// NewMemWatermarks 构造内存水位存储。
func NewMemWatermarks() *MemWatermarks {
	return &MemWatermarks{data: make(map[string]Watermark, 8)}
}

// Get 实现 Watermarks。
func (m *MemWatermarks) Get(_ context.Context, model string) (Watermark, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.data[model]
	return w, ok, nil
}

// Advance 实现 Watermarks。
func (m *MemWatermarks) Advance(_ context.Context, model string, w Watermark) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.data[model]; ok && !w.After(cur) {
		return nil
	}
	m.data[model] = w
	return nil
}

// RedisWatermarks 是 Redis 实现（多副本部署必须用它）。
//
// 存储形式为 `<unix_micros>:<id>`，便于在 Lua 里做「只进不退」的原子比较。
type RedisWatermarks struct {
	rdb redis.UniversalClient
}

var _ Watermarks = (*RedisWatermarks)(nil)

// NewRedisWatermarks 构造 Redis 水位存储。
func NewRedisWatermarks(rdb redis.UniversalClient) *RedisWatermarks {
	return &RedisWatermarks{rdb: rdb}
}

func watermarkKey(model string) string { return WatermarkPrefix + model }

// Get 实现 Watermarks。
func (r *RedisWatermarks) Get(ctx context.Context, model string) (Watermark, bool, error) {
	raw, err := r.rdb.Get(ctx, watermarkKey(model)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) { // 尚未建立水位
			return Watermark{}, false, nil
		}
		return Watermark{}, false, err
	}
	return parseWatermark(raw)
}

// advanceScript 原子地「只在更新时才写」。
//
// 必须原子：多副本同时推进时，非原子的读—比较—写可能让**较小**的水位后写入，
// 把水位拉回去，下一轮就会重复处理一整段记录。
const advanceScript = `
local cur = redis.call("GET", KEYS[1])
local newTs, newID = tonumber(ARGV[1]), tonumber(ARGV[2])
if cur then
  local curTs, curID = string.match(cur, "^(%d+):(%d+)$")
  if curTs then
    curTs, curID = tonumber(curTs), tonumber(curID)
    if curTs > newTs or (curTs == newTs and curID >= newID) then
      return 0
    end
  end
end
redis.call("SET", KEYS[1], ARGV[1] .. ":" .. ARGV[2])
return 1`

// Advance 实现 Watermarks。
func (r *RedisWatermarks) Advance(ctx context.Context, model string, w Watermark) error {
	_, err := r.rdb.Eval(ctx, advanceScript,
		[]string{watermarkKey(model)},
		w.WriteDate.UnixMicro(), w.ID,
	).Int()
	if err != nil {
		return fmt.Errorf("推进水位 %s: %w", model, err)
	}
	return nil
}

func parseWatermark(raw string) (Watermark, bool, error) {
	ts, idRaw, ok := strings.Cut(raw, ":")
	if !ok {
		return Watermark{}, false, fmt.Errorf("水位格式非法: %q", raw)
	}
	micros, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return Watermark{}, false, fmt.Errorf("水位时间戳非法: %q", raw)
	}
	id, err := strconv.ParseInt(idRaw, 10, 64)
	if err != nil {
		return Watermark{}, false, fmt.Errorf("水位 id 非法: %q", raw)
	}
	return Watermark{WriteDate: time.UnixMicro(micros).UTC(), ID: id}, true, nil
}
