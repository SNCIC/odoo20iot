package ota

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestArtifactStorePutOpenAndTenantIsolation(t *testing.T) {
	store, err := NewArtifactStore(t.TempDir(), 32)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := store.Put(7, "firmware.bin", strings.NewReader("firmware-image"))
	if err != nil {
		t.Fatal(err)
	}
	if artifact.SHA256 != Digest([]byte("firmware-image")) || artifact.SizeBytes != int64(len("firmware-image")) {
		t.Fatalf("制品摘要或大小不正确: %+v", artifact)
	}
	file, err := store.Open(7, artifact.Key)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil || string(data) != "firmware-image" {
		t.Fatalf("读取制品失败: %q/%v", data, err)
	}
	if _, err := store.Open(8, artifact.Key); err == nil {
		t.Fatal("其他租户不能打开该制品")
	}
	if _, err := store.Open(7, "7/../8/"+artifact.SHA256); err == nil {
		t.Fatal("路径穿越对象键必须拒绝")
	}
}

func TestArtifactStoreRejectsOversizeAndEmpty(t *testing.T) {
	store, err := NewArtifactStore(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(1, "large.bin", strings.NewReader("four")); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("大文件应拒绝，得到 %v", err)
	}
	if _, err := store.Put(1, "empty.bin", strings.NewReader("")); err == nil {
		t.Fatal("空固件应拒绝")
	}
}
