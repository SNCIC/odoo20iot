package rules

import (
	"context"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/latest"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
	"github.com/redis/go-redis/v9"
)

type prevResolverFunc func(context.Context, string, time.Time) (map[string]any, error)

func (f prevResolverFunc) ResolvePrev(ctx context.Context, key string, at time.Time) (map[string]any, error) {
	return f(ctx, key, at)
}

func TestRuntimeResolvesPrev(t *testing.T) {
	p, err := Compile("r", "prev != nil && msg.temperature > prev.temperature", &DeviceSchema{Metrics: map[string]Kind{"temperature": KindNumber}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := Runtime{Prev: prevResolverFunc(func(_ context.Context, key string, _ time.Time) (map[string]any, error) {
		if key != "dev-1" {
			t.Fatalf("device key = %q", key)
		}
		return map[string]any{"temperature": 20.0}, nil
	})}
	got, err := r.Eval(context.Background(), p, RuntimeInput{DeviceKey: "dev-1", Message: map[string]any{"temperature": 21.0}})
	if err != nil || !got {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

type locatorFunc func(context.Context, string) (int64, int64, error)

func (f locatorFunc) LocateDevice(ctx context.Context, key string) (int64, int64, error) {
	return f(ctx, key)
}

type historyFunc func(context.Context, int64, int64, time.Time) (map[string]any, error)

func (f historyFunc) ResolvePrevSnapshot(ctx context.Context, projectID, deviceID int64, at time.Time) (map[string]any, error) {
	return f(ctx, projectID, deviceID, at)
}

func TestCachedPrevResolverFallsBackOnMiss(t *testing.T) {
	called := false
	resolver := CachedPrevResolver{
		Locator: locatorFunc(func(_ context.Context, key string) (int64, int64, error) {
			if key != "dev-1" {
				t.Fatalf("key = %q", key)
			}
			return 7, 9, nil
		}),
		Cache: latest.NewMemStore(),
		History: historyFunc(func(_ context.Context, projectID, deviceID int64, _ time.Time) (map[string]any, error) {
			called = true
			if projectID != 7 || deviceID != 9 {
				t.Fatalf("device = %d/%d", projectID, deviceID)
			}
			return map[string]any{"temperature": 20.0}, nil
		}),
	}
	got, err := resolver.ResolvePrev(context.Background(), "dev-1", time.Now())
	if err != nil || !called || got["temperature"] != 20.0 {
		t.Fatalf("got=%v called=%v err=%v", got, called, err)
	}
}

func TestCachedPrevResolverRejectsCurrentCacheSnapshot(t *testing.T) {
	now := time.Now().UTC()
	cache := latest.NewMemStore()
	if err := cache.Put(context.Background(), tsdb.Row{
		ProjectID: 7, DeviceID: 9, TS: now,
		Values: []tsdb.Value{tsdb.Number(30), tsdb.Null(), tsdb.Null(), tsdb.Null(), tsdb.Null()},
	}, tsdb.BenchMetrics); err != nil {
		t.Fatal(err)
	}
	resolver := CachedPrevResolver{
		Locator: locatorFunc(func(context.Context, string) (int64, int64, error) { return 7, 9, nil }),
		Cache:   cache,
		History: historyFunc(func(context.Context, int64, int64, time.Time) (map[string]any, error) {
			return map[string]any{"temperature": 20.0}, nil
		}),
	}
	got, err := resolver.ResolvePrev(context.Background(), "dev-1", now)
	if err != nil || got["temperature"] != 20.0 {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestCachedPrevResolverReturnsEmptyOnNoHistory(t *testing.T) {
	resolver := CachedPrevResolver{
		Locator: locatorFunc(func(context.Context, string) (int64, int64, error) { return 1, 2, nil }),
		History: historyFunc(func(context.Context, int64, int64, time.Time) (map[string]any, error) {
			return nil, redis.Nil
		}),
	}
	got, err := resolver.ResolvePrev(context.Background(), "dev-1", time.Now())
	if err != nil || got != nil {
		t.Fatalf("got=%v err=%v", got, err)
	}
}
