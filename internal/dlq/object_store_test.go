package dlq

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFileObjectStorePutIsContentAddressedAndAtomic(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), "svc/entity", []byte(`{"id":1}`))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(ref)))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"id":1}` {
		t.Fatalf("payload=%q", data)
	}
	if matches, _ := filepath.Glob(filepath.Join(root, "svc", "entity", ".dlq-*")); len(matches) != 0 {
		t.Fatalf("temporary files remain: %v", matches)
	}
}

func TestFileObjectStoreRejectsPathTraversal(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../escape", `/tmp/escape`, "svc/../escape", "svc\\..\\escape"} {
		if _, err := store.Put(context.Background(), name, []byte("x")); err == nil {
			t.Fatalf("name %q should be rejected", name)
		}
	}
}
