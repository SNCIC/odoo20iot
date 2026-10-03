package rules

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/latest"
)

var prevTiebreakDegradedTotal atomic.Int64

// PrevTiebreakDegradedTotal reports history stores that cannot compare seq.
func PrevTiebreakDegradedTotal() int64 { return prevTiebreakDegradedTotal.Load() }

// PrevResolver supplies the previous snapshot for a device. Production
// implementations may use Redis first and a time-series store as fallback.
type PrevResolver interface {
	ResolvePrev(context.Context, string, time.Time) (map[string]any, error)
}

type RuntimeInput struct {
	DeviceKey string
	Message   map[string]any
	Meta      map[string]any
	Window    map[string]any
	State     map[string]any
	At        time.Time
}

type Runtime struct{ Prev PrevResolver }

// PrevSnapshotStore 是规则运行时的历史快照来源。实现应返回指定设备、指定时刻之前
// 最近的一条已落库快照；没有历史数据时返回 redis.Nil。
type PrevSnapshotStore interface {
	ResolvePrevSnapshot(context.Context, int64, int64, time.Time) (map[string]any, error)
}

// DeviceLocator 将设备侧 device_key 映射为控制面中的项目/设备 ID。
type DeviceLocator interface {
	LocateDevice(context.Context, string) (projectID, deviceID int64, err error)
}

// CachedPrevResolver 先读取最新值 Write-Through 缓存，缓存缺失、过期或不可用时
// 回落到时序库。缓存时间戳不早于当前消息时，不能作为 prev 使用。
type CachedPrevResolver struct {
	Locator DeviceLocator
	Cache   latest.Store
	History PrevSnapshotStore
}

func (r CachedPrevResolver) ResolvePrev(ctx context.Context, deviceKey string, at time.Time) (map[string]any, error) {
	return r.resolvePrev(ctx, deviceKey, at, int64(^uint64(0)>>1))
}

// ResolvePrevWithSeq applies the full (timestamp, sequence) predecessor order.
func (r CachedPrevResolver) ResolvePrevWithSeq(ctx context.Context, deviceKey string, at time.Time, seq int64) (map[string]any, error) {
	return r.resolvePrev(ctx, deviceKey, at, seq)
}

func (r CachedPrevResolver) resolvePrev(ctx context.Context, deviceKey string, at time.Time, seq int64) (map[string]any, error) {
	if r.Locator == nil {
		return nil, fmt.Errorf("rules: prev 缺少设备定位器")
	}
	projectID, deviceID, err := r.Locator.LocateDevice(ctx, deviceKey)
	if err != nil {
		return nil, fmt.Errorf("定位设备 %q: %w", deviceKey, err)
	}
	if r.Cache != nil {
		snapshot, cacheErr := r.Cache.Get(ctx, projectID, deviceID)
		if cacheErr == nil && (snapshot.TS.Before(at) || (seq != int64(^uint64(0)>>1) && snapshot.TS.Equal(at) && snapshot.Seq < seq)) {
			return snapshot.Values, nil
		}
	}
	if r.History == nil {
		return nil, nil
	}
	var values map[string]any
	var historyErr error
	if exact, ok := r.History.(interface {
		ResolvePrevSnapshotBefore(context.Context, int64, int64, time.Time, int64) (map[string]any, error)
	}); ok {
		values, historyErr = exact.ResolvePrevSnapshotBefore(ctx, projectID, deviceID, at, seq)
	} else {
		prevTiebreakDegradedTotal.Add(1)
		values, historyErr = r.History.ResolvePrevSnapshot(ctx, projectID, deviceID, at)
	}
	if historyErr != nil {
		if historyErr == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("查询历史快照: %w", historyErr)
	}
	return values, nil
}

func (r Runtime) Eval(ctx context.Context, program *Program, in RuntimeInput) (bool, error) {
	if program == nil {
		return false, fmt.Errorf("rules: program 不能为空")
	}
	if in.At.IsZero() {
		in.At = time.Now()
	}
	var prev map[string]any
	if r.Prev != nil {
		var err error
		prev, err = r.Prev.ResolvePrev(ctx, in.DeviceKey, in.At)
		if err != nil {
			return false, fmt.Errorf("读取 prev 快照: %w", err)
		}
	}
	return program.Eval(NewEnv().Bind(in.Message, in.Meta, prev, in.Window, in.State))
}
