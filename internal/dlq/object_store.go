package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

type ObjectStore interface {
	Put(context.Context, string, []byte) (string, error)
}

type FileObjectStore struct{ root string }

func NewFileObjectStore(root string) (*FileObjectStore, error) {
	if root == "" {
		return nil, fmt.Errorf("dlq: 对象存储目录不能为空")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	return &FileObjectStore{root: root}, nil
}

func (s *FileObjectStore) Put(_ context.Context, name string, data []byte) (string, error) {
	sum := sha256.Sum256(append([]byte(name), data...))
	rel := filepath.Join(name, hex.EncodeToString(sum[:]))
	path := filepath.Join(s.root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return "", err
	}
	return rel, nil
}
