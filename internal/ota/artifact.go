package ota

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const DefaultMaxArtifactBytes int64 = 512 << 20

var ErrArtifactTooLarge = errors.New("ota: 固件超过大小限制")

type Artifact struct {
	Key       string `json:"key"`
	Filename  string `json:"filename"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type ArtifactStore struct {
	root    string
	maxSize int64
}

func (s *ArtifactStore) PutContext(_ context.Context, projectID int64, filename string, source io.Reader) (Artifact, error) {
	return s.Put(projectID, filename, source)
}

func NewArtifactStore(root string, maxSize int64) (*ArtifactStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("ota: 固件存储目录不能为空")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxArtifactBytes
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("ota: 创建固件存储目录: %w", err)
	}
	return &ArtifactStore{root: root, maxSize: maxSize}, nil
}

func (s *ArtifactStore) Put(projectID int64, filename string, source io.Reader) (Artifact, error) {
	if projectID <= 0 || source == nil {
		return Artifact{}, fmt.Errorf("ota: project_id 和固件内容必填")
	}
	filename = filepath.Base(strings.TrimSpace(filename))
	if filename == "." || filename == "" || strings.ContainsAny(filename, "\\/\x00") {
		return Artifact{}, fmt.Errorf("ota: 固件文件名非法")
	}
	dir := filepath.Join(s.root, strconv.FormatInt(projectID, 10))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return Artifact{}, fmt.Errorf("ota: 创建租户固件目录: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".upload-*")
	if err != nil {
		return Artifact{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return Artifact{}, err
	}
	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, hasher), io.LimitReader(source, s.maxSize+1))
	if err != nil {
		tmp.Close()
		return Artifact{}, fmt.Errorf("ota: 保存固件: %w", err)
	}
	if written == 0 {
		tmp.Close()
		return Artifact{}, fmt.Errorf("ota: 固件不能为空")
	}
	if written > s.maxSize {
		tmp.Close()
		return Artifact{}, ErrArtifactTooLarge
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return Artifact{}, err
	}
	if err := tmp.Close(); err != nil {
		return Artifact{}, err
	}
	digest := fmt.Sprintf("%x", hasher.Sum(nil))
	key := filepath.ToSlash(filepath.Join(strconv.FormatInt(projectID, 10), digest))
	target, err := s.path(projectID, key)
	if err != nil {
		return Artifact{}, err
	}
	if err := os.Rename(tmpName, target); err != nil {
		return Artifact{}, fmt.Errorf("ota: 提交固件文件: %w", err)
	}
	directory, err := os.Open(dir)
	if err == nil {
		err = directory.Sync()
		_ = directory.Close()
	}
	if err != nil {
		return Artifact{}, fmt.Errorf("ota: 同步固件目录: %w", err)
	}
	return Artifact{Key: key, Filename: filename, SizeBytes: written, SHA256: digest}, nil
}

func (s *ArtifactStore) Open(projectID int64, key string) (*os.File, error) {
	path, err := s.path(projectID, key)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func (s *ArtifactStore) path(projectID int64, key string) (string, error) {
	if projectID <= 0 || filepath.IsAbs(key) || strings.Contains(key, "\\") {
		return "", fmt.Errorf("ota: 固件对象键非法")
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	expected := filepath.Join(strconv.FormatInt(projectID, 10), filepath.Base(clean))
	if clean != expected || filepath.Base(clean) != filepath.Base(key) || len(filepath.Base(clean)) != sha256.Size*2 {
		return "", fmt.Errorf("ota: 固件对象键非法")
	}
	if _, err := hex.DecodeString(filepath.Base(clean)); err != nil {
		return "", fmt.Errorf("ota: 固件对象键非法")
	}
	return filepath.Join(s.root, clean), nil
}
