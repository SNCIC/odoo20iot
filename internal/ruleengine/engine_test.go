package ruleengine

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/dag"
	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/latest"
	"github.com/SNCIC/odoo20iot/internal/ruleconfig"
	"github.com/SNCIC/odoo20iot/internal/rules"
)

type loaderFunc func(context.Context, string, *rules.DeviceSchema, *dag.Registry) ([]ruleconfig.Rule, error)

func (f loaderFunc) LoadEnabled(ctx context.Context, project string, schema *rules.DeviceSchema, registry *dag.Registry) ([]ruleconfig.Rule, error) {
	return f(ctx, project, schema, registry)
}

type publisherFunc func(context.Context, string, []byte) error

func (f publisherFunc) Publish(ctx context.Context, subject string, data []byte) error {
	return f(ctx, subject, data)
}

type historyFunc func(context.Context, int64, int64, time.Time) (map[string]any, error)

func (f historyFunc) ResolvePrevSnapshot(ctx context.Context, projectID, deviceID int64, at time.Time) (map[string]any, error) {
	return f(ctx, projectID, deviceID, at)
}

func TestProcessEvaluatesPrevAndPublishesAlarm(t *testing.T) {
	compiler := rules.NewCompiler(8)
	program, err := compiler.Compile("r:1", "prev != nil && msg.temperature > prev.temperature", &rules.DeviceSchema{Metrics: map[string]rules.Kind{"temperature": rules.KindNumber}})
	if err != nil {
		t.Fatal(err)
	}
	var subject string
	var got alarm.TriggerPayload
	engine, err := New(loaderFunc(func(context.Context, string, *rules.DeviceSchema, *dag.Registry) ([]ruleconfig.Rule, error) {
		return []ruleconfig.Rule{{RuleID: "r", Name: "hot", Level: "critical", Match: ruleconfig.Match{}, Program: program}}, nil
	}), nil, historyFunc(func(context.Context, int64, int64, time.Time) (map[string]any, error) {
		return map[string]any{"temperature": 50.0}, nil
	}), publisherFunc(func(_ context.Context, s string, data []byte) error { subject = s; return json.Unmarshal(data, &got) }), Config{ReloadEvery: time.Minute}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"ts":"2026-10-03T10:00:00Z","seq":4,"data":{"temperature":61}}`)
	if err := engine.Process(context.Background(), envelope.Envelope{ProjectID: 7, DeviceID: 9, DeviceTypeID: 3, DeviceKey: "d-1", ReceivedAt: time.Date(2026, 10, 3, 10, 0, 1, 0, time.UTC), Payload: raw}); err != nil {
		t.Fatal(err)
	}
	if subject != alarm.TriggerSubject || got.ProjectID != "7" || got.DeviceID != "9" || got.RuleID != "r" || got.Level != "critical" {
		t.Fatalf("unexpected alarm publish subject=%q event=%+v", subject, got)
	}
}

func TestProcessDoesNotPublishWhenRuleDoesNotMatch(t *testing.T) {
	program, err := rules.NewCompiler(8).Compile("r:1", "msg.temperature > 100", &rules.DeviceSchema{Metrics: map[string]rules.Kind{"temperature": rules.KindNumber}})
	if err != nil {
		t.Fatal(err)
	}
	published := false
	engine, err := New(loaderFunc(func(context.Context, string, *rules.DeviceSchema, *dag.Registry) ([]ruleconfig.Rule, error) {
		return []ruleconfig.Rule{{RuleID: "r", Program: program}}, nil
	}), latest.NewMemStore(), nil, publisherFunc(func(context.Context, string, []byte) error { published = true; return nil }), Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Process(context.Background(), envelope.Envelope{ProjectID: 1, DeviceID: 2, DeviceKey: "d", Payload: []byte(`{"data":{"temperature":20}}`)}); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("non-matching rule must not publish")
	}
}
