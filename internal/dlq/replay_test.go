package dlq

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeReplayStore struct {
	rows  []Record
	marks []struct {
		id     int64
		ok     bool
		reason string
	}
}

func (f *fakeReplayStore) ListPending(context.Context, string, string, int) ([]Record, error) {
	return append([]Record(nil), f.rows...), nil
}
func (f *fakeReplayStore) MarkReplayed(_ context.Context, id int64, _ time.Time, ok bool, reason string) error {
	f.marks = append(f.marks, struct {
		id     int64
		ok     bool
		reason string
	}{id, ok, reason})
	return nil
}

func TestReplayerDispatchesAndMarks(t *testing.T) {
	store := &fakeReplayStore{rows: []Record{{ID: 1, CreatedAt: time.Now(), Entry: Entry{EntityType: "event", IdempotencyKey: "idem-1"}}, {ID: 2, CreatedAt: time.Now(), Entry: Entry{EntityType: "unknown"}}}}
	r, err := NewReplayer(store, 3)
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := r.Register("event", func(_ context.Context, row Record) error { got = row.IdempotencyKey; return nil }); err != nil {
		t.Fatal(err)
	}
	result, err := r.Replay(context.Background(), "svc", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 1 || result.Skipped != 1 || got != "idem-1" {
		t.Fatalf("result=%+v key=%q", result, got)
	}
	if len(store.marks) != 2 || !store.marks[0].ok || store.marks[1].ok {
		t.Fatalf("marks=%+v", store.marks)
	}
}

func TestReplayerFailureAndAttemptLimit(t *testing.T) {
	store := &fakeReplayStore{rows: []Record{{ID: 1, CreatedAt: time.Now(), ReplayCount: 0, Entry: Entry{EntityType: "event"}}, {ID: 2, CreatedAt: time.Now(), ReplayCount: 3, Entry: Entry{EntityType: "event"}}}}
	r, _ := NewReplayer(store, 3)
	_ = r.Register("event", func(context.Context, Record) error { return errors.New("downstream unavailable") })
	result, err := r.Replay(context.Background(), "svc", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 || result.Skipped != 1 {
		t.Fatalf("result=%+v", result)
	}
	if len(store.marks) != 1 || store.marks[0].ok {
		t.Fatalf("marks=%+v", store.marks)
	}
}
