package latest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

func TestMemStoreKeepsNewestSnapshot(t *testing.T) {
	store := NewMemStore()
	metrics := tsdb.BenchMetrics
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	newRow := func(ts time.Time, temperature float64, running bool) tsdb.Row {
		return tsdb.Row{
			ProjectID: 1, DeviceID: 1001, TS: ts,
			Values: []tsdb.Value{tsdb.Number(temperature), tsdb.Null(), tsdb.Null(), tsdb.Null(), tsdb.Bool(running)},
		}
	}
	if err := store.Put(context.Background(), newRow(base.Add(time.Minute), 22, true), metrics); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), newRow(base, 11, false), metrics); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Get(context.Background(), 1, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.TS.Equal(base.Add(time.Minute)) || snapshot.Values["temperature"] != float64(22) || snapshot.Values["running"] != true {
		t.Fatalf("快照未保持最新值: %+v", snapshot)
	}
	if err := store.Put(context.Background(), tsdb.Row{
		ProjectID: 1, DeviceID: 1001, TS: base.Add(2 * time.Minute),
		Values: []tsdb.Value{tsdb.Null(), tsdb.Number(60), tsdb.Null(), tsdb.Null(), tsdb.Null()},
	}, metrics); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Get(context.Background(), 1, 1001)
	if err != nil || snapshot.Values["temperature"] != float64(22) || snapshot.Values["humidity"] != float64(60) {
		t.Fatalf("部分指标更新不应抹掉旧值: %+v / %v", snapshot, err)
	}
}

func TestMemStoreMissing(t *testing.T) {
	_, err := NewMemStore().Get(context.Background(), 1, 1001)
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("缺失值应返回 redis.Nil，得到 %v", err)
	}
}

func TestMemStoreRejectsShapeMismatch(t *testing.T) {
	err := NewMemStore().Put(context.Background(), tsdb.Row{Values: []tsdb.Value{tsdb.Number(1)}}, tsdb.BenchMetrics)
	if err == nil {
		t.Fatal("值与指标长度不一致必须报错")
	}
}
