// Package latest 提供设备遥测最新值的 Redis Write-Through 缓存。
package latest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

type Snapshot struct {
	ProjectID int64
	DeviceID  int64
	TS        time.Time
	Seq       int64
	Values    map[string]any
}

type Store interface {
	Put(ctx context.Context, row tsdb.Row, metrics []tsdb.Metric) error
	Get(ctx context.Context, projectID, deviceID int64) (Snapshot, error)
}

type RedisStore struct{ rdb redis.UniversalClient }

var _ Store = (*RedisStore)(nil)

func NewRedisStore(rdb redis.UniversalClient) *RedisStore { return &RedisStore{rdb: rdb} }

func key(projectID, deviceID int64) string {
	return fmt.Sprintf("iot:latest:%d:%d", projectID, deviceID)
}

func (s *RedisStore) Put(ctx context.Context, row tsdb.Row, metrics []tsdb.Metric) error {
	if len(row.Values) != len(metrics) {
		return fmt.Errorf("latest: values/metrics 长度不一致: %d/%d", len(row.Values), len(metrics))
	}
	values := make(map[string]any, len(metrics))
	for i, metric := range metrics {
		value := row.Values[i]
		if value.Null {
			continue
		}
		if metric.Kind == tsdb.KindBool {
			values[metric.Key] = value.Bool
		} else {
			values[metric.Key] = value.Number
		}
	}
	payload, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("latest: 编码值: %w", err)
	}
	const script = `
local old_ts = redis.call("HGET", KEYS[1], "ts_unix_nano")
local old_seq = redis.call("HGET", KEYS[1], "seq")
if old_ts and (tonumber(old_ts) > tonumber(ARGV[1]) or (tonumber(old_ts) == tonumber(ARGV[1]) and old_seq and tonumber(old_seq) >= tonumber(ARGV[2]))) then return 0 end
local merged = {}
local old_values = redis.call("HGET", KEYS[1], "values")
if old_values then merged = cjson.decode(old_values) end
local new_values = cjson.decode(ARGV[3])
for k, v in pairs(new_values) do merged[k] = v end
redis.call("HSET", KEYS[1], "ts_unix_nano", ARGV[1], "seq", ARGV[2], "ts", ARGV[3], "values", cjson.encode(merged))
redis.call("EXPIRE", KEYS[1], ARGV[5])
return 1`
	if err := s.rdb.Eval(ctx, script, []string{key(row.ProjectID, row.DeviceID)},
		strconv.FormatInt(row.TS.UnixNano(), 10), strconv.FormatInt(row.Seq, 10), row.TS.UTC().Format(time.RFC3339Nano), string(payload), "604800").Err(); err != nil {
		return fmt.Errorf("latest: 写入 Redis: %w", err)
	}
	return nil
}

func (s *RedisStore) Get(ctx context.Context, projectID, deviceID int64) (Snapshot, error) {
	values, err := s.rdb.HGetAll(ctx, key(projectID, deviceID)).Result()
	if err != nil {
		return Snapshot{}, fmt.Errorf("latest: 读取 Redis: %w", err)
	}
	if len(values) == 0 {
		return Snapshot{}, redis.Nil
	}
	ts, err := time.Parse(time.RFC3339Nano, values["ts"])
	if err != nil {
		return Snapshot{}, fmt.Errorf("latest: 解析时间: %w", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(values["values"]), &decoded); err != nil {
		return Snapshot{}, fmt.Errorf("latest: 解析值: %w", err)
	}
	seq, _ := strconv.ParseInt(values["seq"], 10, 64)
	return Snapshot{ProjectID: projectID, DeviceID: deviceID, TS: ts, Seq: seq, Values: decoded}, nil
}

// MemStore 用于单元测试和无 Redis 的本地冒烟。
type MemStore struct {
	mu    sync.RWMutex
	items map[string]Snapshot
}

func NewMemStore() *MemStore { return &MemStore{items: make(map[string]Snapshot)} }

func (s *MemStore) Put(_ context.Context, row tsdb.Row, metrics []tsdb.Metric) error {
	if len(row.Values) != len(metrics) {
		return fmt.Errorf("latest: values/metrics 长度不一致: %d/%d", len(row.Values), len(metrics))
	}
	values := make(map[string]any, len(metrics))
	for i, metric := range metrics {
		if row.Values[i].Null {
			continue
		}
		if metric.Kind == tsdb.KindBool {
			values[metric.Key] = row.Values[i].Bool
		} else {
			values[metric.Key] = row.Values[i].Number
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(row.ProjectID, row.DeviceID)
	if old, ok := s.items[k]; ok && (row.TS.Before(old.TS) || (row.TS.Equal(old.TS) && row.Seq <= old.Seq)) {
		return nil
	}
	if old, ok := s.items[k]; ok {
		for name, value := range old.Values {
			if _, exists := values[name]; !exists {
				values[name] = value
			}
		}
	}
	s.items[k] = Snapshot{ProjectID: row.ProjectID, DeviceID: row.DeviceID, TS: row.TS, Seq: row.Seq, Values: values}
	return nil
}

func (s *MemStore) Get(_ context.Context, projectID, deviceID int64) (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.items[key(projectID, deviceID)]
	if !ok {
		return Snapshot{}, redis.Nil
	}
	return snapshot, nil
}
