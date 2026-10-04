// Package ota 定义 OTA 固件清单、签名和升级状态机。
package ota

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultOfflineTTL = 24 * time.Hour
	MinProgress       = 0
	MaxProgress       = 100
)

var ErrInvalidSignature = errors.New("ota: 固件签名无效")

type Firmware struct {
	ID           int64          `json:"id"`
	ProjectID    int64          `json:"project_id"`
	Version      string         `json:"version"`
	Filename     string         `json:"filename"`
	ObjectKey    string         `json:"object_key"`
	SizeBytes    int64          `json:"size_bytes"`
	SHA256       string         `json:"sha256"`
	Signature    string         `json:"signature"`
	SigningKeyID string         `json:"signing_key_id"`
	Metadata     map[string]any `json:"metadata"`
	CreatedAt    time.Time      `json:"created_at"`
}

type Manifest struct {
	Version      string `json:"version"`
	Filename     string `json:"filename"`
	URL          string `json:"url"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
	Signature    string `json:"signature"`
	SigningKeyID string `json:"signing_key_id"`
	ExpiresAt    string `json:"expires_at"`
}

type Notify struct {
	TaskID       string   `json:"task_id"`
	Version      string   `json:"version"`
	DownloadURL  string   `json:"url"`
	SizeBytes    int64    `json:"size_bytes"`
	SHA256       string   `json:"sha256"`
	Signature    string   `json:"signature"`
	SigningKeyID string   `json:"signing_key_id"`
	ExpiresAt    string   `json:"expires_at"`
	Resume       bool     `json:"resume"`
	Actions      []string `json:"actions,omitempty"`
}

type Progress struct {
	TaskID          string `json:"task_id"`
	Status          string `json:"status"`
	Progress        int    `json:"progress"`
	BytesDownloaded int64  `json:"bytes_downloaded"`
	FirmwareVersion string `json:"firmware_version"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
}

func (p Progress) Validate() error {
	if !ValidTaskID(p.TaskID) {
		return fmt.Errorf("ota: progress task_id 非法")
	}
	if p.Progress < MinProgress || p.Progress > MaxProgress {
		return fmt.Errorf("ota: progress 超出 0..100")
	}
	if p.BytesDownloaded < 0 {
		return fmt.Errorf("ota: bytes_downloaded 不能为负数")
	}
	if !validDeviceStatus(DeviceStatus(p.Status)) || DeviceStatus(p.Status) == DevicePending {
		return fmt.Errorf("ota: progress status 非法")
	}
	return nil
}

func (m Manifest) Notification(taskID string) (Notify, error) {
	if strings.TrimSpace(taskID) == "" {
		return Notify{}, fmt.Errorf("ota: task_id 不能为空")
	}
	if err := m.VerifyURL(); err != nil {
		return Notify{}, err
	}
	return Notify{TaskID: taskID, Version: m.Version, DownloadURL: m.URL, SizeBytes: m.SizeBytes,
		SHA256: m.SHA256, Signature: m.Signature, SigningKeyID: m.SigningKeyID,
		ExpiresAt: m.ExpiresAt, Resume: true, Actions: []string{"download", "verify", "install"}}, nil
}

type TaskStatus string

const (
	TaskDraft     TaskStatus = "draft"
	TaskRunning   TaskStatus = "running"
	TaskPaused    TaskStatus = "paused"
	TaskCompleted TaskStatus = "completed"
	TaskCancelled TaskStatus = "cancelled"
)

type DeviceStatus string

const (
	DevicePending     DeviceStatus = "pending"
	DeviceNotified    DeviceStatus = "notified"
	DeviceDownloading DeviceStatus = "downloading"
	DeviceVerifying   DeviceStatus = "verifying"
	DeviceInstalling  DeviceStatus = "installing"
	DeviceSucceeded   DeviceStatus = "succeeded"
	DeviceFailed      DeviceStatus = "failed"
	DeviceExpired     DeviceStatus = "expired"
	DeviceRolledBack  DeviceStatus = "rolled_back"
)

type Rollout struct {
	Batches          []int   `json:"batches"`
	SuccessThreshold float64 `json:"success_threshold"`
}

func NextBatchDevices(devices []TaskDevice, rollout Rollout) ([]TaskDevice, error) {
	if err := rollout.Validate(); err != nil {
		return nil, err
	}
	if len(devices) == 0 {
		return nil, nil
	}
	active := 0
	for _, device := range devices {
		if device.Status != DevicePending {
			active++
		}
	}
	batchIndex := -1
	for index, percent := range rollout.Batches {
		target := (len(devices)*percent + 99) / 100
		if target > active {
			batchIndex = index
			break
		}
	}
	if batchIndex < 0 {
		return nil, nil
	}
	count, err := rollout.BatchSize(len(devices), batchIndex)
	if err != nil {
		return nil, err
	}
	selected := make([]TaskDevice, 0, count)
	for _, device := range devices {
		if device.Status == DevicePending {
			selected = append(selected, device)
			if len(selected) == count {
				break
			}
		}
	}
	return selected, nil
}

func DefaultRollout() Rollout {
	return Rollout{Batches: []int{1, 10, 50, 100}, SuccessThreshold: 0.98}
}

func (r Rollout) Validate() error {
	if len(r.Batches) == 0 || r.Batches[len(r.Batches)-1] != 100 {
		return fmt.Errorf("ota: 灰度批次必须以 100%% 结束")
	}
	previous := 0
	for _, batch := range r.Batches {
		if batch <= previous || batch < 1 || batch > 100 {
			return fmt.Errorf("ota: 灰度批次必须递增且在 1..100")
		}
		previous = batch
	}
	if r.SuccessThreshold <= 0 || r.SuccessThreshold > 1 {
		return fmt.Errorf("ota: 成功率阈值必须在 (0,1]")
	}
	return nil
}

func (r Rollout) BatchSize(total, index int) (int, error) {
	if err := r.Validate(); err != nil {
		return 0, err
	}
	if total <= 0 || index < 0 || index >= len(r.Batches) {
		return 0, fmt.Errorf("ota: 批次参数非法")
	}
	previous := 0
	if index > 0 {
		previous = r.Batches[index-1]
	}
	size := (total*r.Batches[index] + 99) / 100
	prior := (total*previous + 99) / 100
	return size - prior, nil
}

func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (m Manifest) signingBytes() ([]byte, error) {
	copy := m
	copy.Signature = ""
	return json.Marshal(copy)
}

func (m Manifest) Sign(privateKey ed25519.PrivateKey) (Manifest, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return Manifest{}, fmt.Errorf("ota: Ed25519 私钥长度非法")
	}
	data, err := m.signingBytes()
	if err != nil {
		return Manifest{}, err
	}
	m.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, data))
	return m, nil
}

func (m Manifest) Verify(publicKey ed25519.PublicKey, now time.Time) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("ota: Ed25519 公钥长度非法")
	}
	if strings.TrimSpace(m.Signature) == "" || strings.TrimSpace(m.SHA256) == "" || m.SizeBytes <= 0 || strings.TrimSpace(m.Version) == "" || filepath.Base(m.Filename) != m.Filename {
		return ErrInvalidSignature
	}
	digest, err := hex.DecodeString(m.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return ErrInvalidSignature
	}
	if err := m.VerifyURL(); err != nil {
		return err
	}
	expires, err := time.Parse(time.RFC3339, m.ExpiresAt)
	if err != nil || !expires.After(now) {
		return fmt.Errorf("ota: 固件下载地址已过期")
	}
	signature, err := base64.RawURLEncoding.DecodeString(m.Signature)
	if err != nil {
		return ErrInvalidSignature
	}
	data, err := m.signingBytes()
	if err != nil || !ed25519.Verify(publicKey, data, signature) {
		return ErrInvalidSignature
	}
	return nil
}

func (m Manifest) VerifyURL() error {
	downloadURL, err := url.Parse(m.URL)
	if err != nil || downloadURL.Scheme != "https" || downloadURL.Host == "" || downloadURL.User != nil {
		return fmt.Errorf("ota: 固件下载地址必须是有效 HTTPS URL")
	}
	return nil
}

func Transition(from, to DeviceStatus) error {
	if !validDeviceStatus(from) || !validDeviceStatus(to) {
		return fmt.Errorf("ota: 设备状态非法")
	}
	allowed := map[DeviceStatus][]DeviceStatus{
		DevicePending:     {DeviceNotified, DeviceExpired, DeviceFailed},
		DeviceNotified:    {DeviceDownloading, DeviceExpired, DeviceFailed},
		DeviceDownloading: {DeviceVerifying, DeviceFailed, DeviceExpired},
		DeviceVerifying:   {DeviceInstalling, DeviceFailed},
		DeviceInstalling:  {DeviceSucceeded, DeviceFailed, DeviceRolledBack},
		DeviceSucceeded:   {DeviceRolledBack},
		DeviceFailed:      {DevicePending},
		DeviceExpired:     {DevicePending},
	}
	for _, candidate := range allowed[from] {
		if candidate == to {
			return nil
		}
	}
	return fmt.Errorf("ota: 设备状态不能从 %q 转为 %q", from, to)
}

func validDeviceStatus(status DeviceStatus) bool {
	switch status {
	case DevicePending, DeviceNotified, DeviceDownloading, DeviceVerifying,
		DeviceInstalling, DeviceSucceeded, DeviceFailed, DeviceExpired, DeviceRolledBack:
		return true
	default:
		return false
	}
}
