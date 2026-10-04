package ota

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/cluster"
)

type rolloutTestStore struct {
	task     Task
	firmware Firmware
	devices  []TaskDevice
	lease    bool
}

func (s *rolloutTestStore) ClaimRunningTasks(_ context.Context, projectID int64, _ int, _ time.Duration) ([]Task, error) {
	if s.lease || s.task.Status != TaskRunning {
		return nil, nil
	}
	s.lease = true
	return []Task{s.task}, nil
}
func (s *rolloutTestStore) ListTaskDevices(context.Context, int64, string) ([]TaskDevice, error) {
	return s.devices, nil
}
func (s *rolloutTestStore) GetFirmware(context.Context, int64, int64) (Firmware, error) {
	return s.firmware, nil
}
func (s *rolloutTestStore) BeginDispatch(_ context.Context, _ int64, _ string, key string) error {
	for i := range s.devices {
		if s.devices[i].DeviceKey == key && (s.devices[i].Status == DevicePending || s.devices[i].Status == DeviceDispatching) {
			s.devices[i].Status = DeviceDispatching
			return nil
		}
	}
	return ErrDeviceNotFound
}
func (s *rolloutTestStore) MarkNotified(_ context.Context, _ int64, _ string, key string) error {
	for i := range s.devices {
		if s.devices[i].DeviceKey == key {
			s.devices[i].Status = DeviceNotified
			return nil
		}
	}
	return ErrDeviceNotFound
}
func (s *rolloutTestStore) ResetDispatch(_ context.Context, _ int64, _ string, key string) error {
	for i := range s.devices {
		if s.devices[i].DeviceKey == key && s.devices[i].Status == DeviceDispatching {
			s.devices[i].Status = DevicePending
			return nil
		}
	}
	return nil
}
func (s *rolloutTestStore) SetBatchState(_ context.Context, _ int64, _ string, expected, next int, status TaskStatus) error {
	if s.task.BatchIndex != expected {
		return ErrTaskNotFound
	}
	s.task.BatchIndex, s.task.Status, s.lease = next, status, false
	return nil
}
func (s *rolloutTestStore) ReleaseDispatchLease(context.Context, int64, string) error {
	s.lease = false
	return nil
}

type rolloutTestRouter struct{ topics []string }

func (r *rolloutTestRouter) RouteExternal(_ context.Context, env cluster.Envelope) error {
	r.topics = append(r.topics, env.Topic)
	return nil
}

type rolloutTestSigner struct{}

func (rolloutTestSigner) SignManifest(m Manifest, now time.Time, ttl time.Duration) (Manifest, error) {
	m.SigningKeyID, m.Signature, m.ExpiresAt = "test", "signature", now.Add(ttl).Format(time.RFC3339)
	return m, nil
}

func TestRolloutWorkerAdvancesOnlyAfterCohortSuccess(t *testing.T) {
	store := &rolloutTestStore{task: Task{ID: "550e8400-e29b-41d4-a716-446655440000", ProjectID: 1, FirmwareID: 1, Status: TaskRunning, Rollout: DefaultRollout()}, firmware: Firmware{Version: "v1", Filename: "fw.bin", ObjectKey: "1/abc", SizeBytes: 1, SHA256: Digest([]byte("x"))}}
	for i := 0; i < 100; i++ {
		store.devices = append(store.devices, TaskDevice{DeviceKey: fmt.Sprintf("dev-%03d", i), Status: DevicePending})
	}
	router := new(rolloutTestRouter)
	worker, err := NewRolloutWorker(store, router, rolloutTestSigner{}, "secret", "https://ota.invalid/api/v1/ota/download")
	if err != nil {
		t.Fatal(err)
	}
	worker.now = func() time.Time { return time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC) }
	if err := worker.RunProject(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(router.topics) != 1 || store.devices[0].Status != DeviceNotified {
		t.Fatalf("首批通知不符: topics=%d status=%s", len(router.topics), store.devices[0].Status)
	}
	if store.task.BatchIndex != 0 {
		t.Fatalf("当前批次未结束不应推进: %d", store.task.BatchIndex)
	}
	store.devices[0].Status = DeviceSucceeded
	if err := worker.RunProject(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if store.task.BatchIndex != 1 {
		t.Fatalf("首批成功后应推进到第二批: %d", store.task.BatchIndex)
	}
	if err := worker.RunProject(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(router.topics) != 10 {
		t.Fatalf("第二批应再下发 9 台，总数为 10，得到 %d", len(router.topics))
	}
}

func TestRolloutWorkerPausesBelowThreshold(t *testing.T) {
	store := &rolloutTestStore{task: Task{ID: "550e8400-e29b-41d4-a716-446655440000", ProjectID: 1, FirmwareID: 1, Status: TaskRunning, Rollout: DefaultRollout()}, firmware: Firmware{Version: "v1", Filename: "fw.bin", ObjectKey: "1/abc", SizeBytes: 1, SHA256: Digest([]byte("x"))}}
	for i := 0; i < 100; i++ {
		status := DevicePending
		if i == 0 {
			status = DeviceFailed
		}
		store.devices = append(store.devices, TaskDevice{DeviceKey: fmt.Sprintf("dev-%03d", i), Status: status})
	}
	router := new(rolloutTestRouter)
	worker, err := NewRolloutWorker(store, router, rolloutTestSigner{}, "secret", "https://ota.invalid/download")
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunProject(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if store.task.Status != TaskPaused || len(router.topics) != 0 {
		t.Fatalf("首批失败需暂停，不得发送后续批次: status=%s sent=%d", store.task.Status, len(router.topics))
	}
}
