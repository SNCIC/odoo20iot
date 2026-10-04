package ota

import (
	"crypto/ed25519"
	"testing"
	"time"
)

func TestManifestSignAndVerify(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	manifest, err := (Manifest{
		Version: "v2.1.3", Filename: "edge.bin", URL: "https://store.invalid/edge.bin",
		SizeBytes: 123, SHA256: Digest([]byte("firmware")), SigningKeyID: "dev-key",
		ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
	}).Sign(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.Verify(publicKey, now); err != nil {
		t.Fatalf("签名清单应通过校验: %v", err)
	}
	manifest.Version = "v2.1.4"
	if err := manifest.Verify(publicKey, now); err == nil {
		t.Fatal("篡改后的清单不应通过校验")
	}
}

func TestRolloutBatchSizeAndValidation(t *testing.T) {
	r := DefaultRollout()
	if got, err := r.BatchSize(100, 0); err != nil || got != 1 {
		t.Fatalf("首批应为 1 台，得到 %d/%v", got, err)
	}
	if got, err := r.BatchSize(100, 1); err != nil || got != 9 {
		t.Fatalf("第二批应新增 9 台，得到 %d/%v", got, err)
	}
	if got, err := r.BatchSize(5, 1); err != nil || got != 0 {
		t.Fatalf("小规模目标的第二批可为空，得到 %d/%v", got, err)
	}
	if _, err := (Rollout{Batches: []int{10, 5, 100}, SuccessThreshold: .98}).BatchSize(10, 0); err == nil {
		t.Fatal("非递增批次应被拒绝")
	}
}

func TestDeviceTransition(t *testing.T) {
	if err := Transition(DevicePending, DeviceNotified); err != nil {
		t.Fatal(err)
	}
	if err := Transition(DevicePending, DeviceSucceeded); err == nil {
		t.Fatal("pending 不应直接变为 succeeded")
	}
	if err := Transition(DeviceFailed, DevicePending); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationAndProgressValidation(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	manifest := Manifest{Version: "v1", Filename: "fw.bin", URL: "https://store.invalid/fw.bin", SizeBytes: 1, SHA256: Digest([]byte("x")), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339), Signature: "signed"}
	notify, err := manifest.Notification("task-1")
	if err != nil || notify.TaskID != "task-1" || !notify.Resume {
		t.Fatalf("通知构造失败: %+v/%v", notify, err)
	}
	if err := (Progress{TaskID: "task-1", Status: "downloading", Progress: 101}).Validate(); err == nil {
		t.Fatal("超范围 progress 必须拒绝")
	}
	if err := (Progress{TaskID: "task-1", Status: "succeeded", Progress: 100, BytesDownloaded: 1}).Validate(); err != nil {
		t.Fatal(err)
	}
}
