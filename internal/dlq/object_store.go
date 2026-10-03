package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	if s == nil || s.root == "" {
		return "", fmt.Errorf("dlq: 对象存储未初始化")
	}
	if err := validateObjectName(name); err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte(name), data...))
	rel := filepath.Join(name, hex.EncodeToString(sum[:]))
	path := filepath.Join(s.root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".dlq-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return "", err
	}
	return rel, nil
}

func validateObjectName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" || filepath.IsAbs(name) {
		return fmt.Errorf("dlq: 对象名称非法")
	}
	for _, part := range strings.FieldsFunc(name, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == "" || part == "." || part == ".." || strings.ContainsRune(part, 0) {
			return fmt.Errorf("dlq: 对象名称包含非法路径段")
		}
	}
	if strings.ContainsRune(name, 0) {
		return fmt.Errorf("dlq: 对象名称包含非法字符")
	}
	return nil
}
