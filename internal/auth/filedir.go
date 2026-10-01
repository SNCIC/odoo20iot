package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// FileDirectory 从本地 JSON 文件读取设备凭据。
//
// ⚠️ **它是开发 / 私有化 PoC 适配器，不是生产形态。**
// 生产必须走 L3（`svc-auth` gRPC）以保证凭据可轮换、可吊销、多副本一致；
// 本实现只在单机（Lite）与本地冒烟场景下有意义。
//
// 它的存在是为了让「三档认证」有一个**可运行、可端到端验证**的实现，
// 而不是把生产依赖缺失伪装成「已完成」。
type FileDirectory struct {
	mu     sync.RWMutex
	byKey  map[string]*Identity
	path   string
	loaded int
}

// fileFormat 是凭据文件的结构。摘要与盐一律 base64 —— 不要明文 secret：
// 文件里存的是**校验材料**，不是凭据本身。
type fileFormat struct {
	Devices []fileDevice `json:"devices"`
}

type fileDevice struct {
	DeviceKey    string `json:"device_key"`
	ProjectID    int64  `json:"project_id"`
	DeviceTypeID int64  `json:"device_type_id"`
	Mode         Mode   `json:"auth_mode"`
	IsGateway    bool   `json:"is_gateway,omitempty"`

	// B 档：设备 secret 的 Argon2id 摘要
	SecretSalt string  `json:"secret_salt,omitempty"`
	SecretHash string  `json:"secret_hash,omitempty"`
	Params     *params `json:"argon2,omitempty"`

	// A 档：项目级凭据
	ProjectTokenHash string `json:"project_token_hash,omitempty"` // SHA-256
	ProjectKeySalt   string `json:"project_key_salt,omitempty"`
	ProjectKeyHash   string `json:"project_key_hash,omitempty"`

	CredentialVersion int64 `json:"credential_version,omitempty"`
	Revoked           bool  `json:"revoked,omitempty"`
}

type params struct {
	Time    uint32 `json:"time"`
	Memory  uint32 `json:"memory"`
	Threads uint8  `json:"threads"`
	KeyLen  uint32 `json:"key_len"`
}

// LoadFile 读取凭据文件并返回一个 Directory。
//
// 文件缺失或格式错误一律返回 error —— 认证配置不可用时**必须让服务起不来**，
// 而不是退化成「无认证运行」。
func LoadFile(path string) (*FileDirectory, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取凭据文件 %s: %w", path, err)
	}

	var ff fileFormat
	if err := json.Unmarshal(raw, &ff); err != nil {
		return nil, fmt.Errorf("解析凭据文件 %s: %w", path, err)
	}
	if len(ff.Devices) == 0 {
		return nil, fmt.Errorf("凭据文件 %s 里没有任何设备", path)
	}

	d := &FileDirectory{byKey: make(map[string]*Identity, len(ff.Devices)), path: path}
	for i, fd := range ff.Devices {
		id, err := fd.toIdentity()
		if err != nil {
			return nil, fmt.Errorf("凭据文件 %s 第 %d 条: %w", path, i+1, err)
		}
		d.byKey[id.DeviceKey] = id
	}
	d.loaded = len(d.byKey)
	return d, nil
}

// Lookup 实现 Directory。
func (d *FileDirectory) Lookup(_ context.Context, clientID string) (*Identity, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.byKey[clientID], nil
}

// Size 返回已加载的设备数。
func (d *FileDirectory) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.loaded
}

// Reload 重新加载文件（控制面轮换凭据后调用）。
func (d *FileDirectory) Reload() error {
	next, err := LoadFile(d.path)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.byKey, d.loaded = next.byKey, next.loaded
	d.mu.Unlock()
	return nil
}

func (fd fileDevice) toIdentity() (*Identity, error) {
	if fd.DeviceKey == "" {
		return nil, fmt.Errorf("device_key 为空")
	}
	if !fd.Mode.Valid() {
		return nil, fmt.Errorf("设备 %s 的 auth_mode %q 非法", fd.DeviceKey, fd.Mode)
	}

	id := &Identity{
		ProjectID:         fd.ProjectID,
		DeviceTypeID:      fd.DeviceTypeID,
		DeviceKey:         fd.DeviceKey,
		Mode:              fd.Mode,
		IsGateway:         fd.IsGateway,
		CredentialVersion: fd.CredentialVersion,
		Revoked:           fd.Revoked,
	}

	if fd.SecretHash != "" {
		hash, err := b64(fd.SecretHash)
		if err != nil {
			return nil, fmt.Errorf("设备 %s 的 secret_hash: %w", fd.DeviceKey, err)
		}
		salt, err := b64(fd.SecretSalt)
		if err != nil {
			return nil, fmt.Errorf("设备 %s 的 secret_salt: %w", fd.DeviceKey, err)
		}
		p := DefaultParams
		if fd.Params != nil {
			p = Params{Time: fd.Params.Time, Memory: fd.Params.Memory, Threads: fd.Params.Threads, KeyLen: fd.Params.KeyLen}
		}
		id.Secret = Digest{Params: p, Salt: salt, Hash: hash}
	}

	if fd.ProjectTokenHash != "" {
		h, err := b64(fd.ProjectTokenHash)
		if err != nil {
			return nil, fmt.Errorf("设备 %s 的 project_token_hash: %w", fd.DeviceKey, err)
		}
		id.ProjectTokenHash = h
	}
	if fd.ProjectKeyHash != "" {
		hash, err := b64(fd.ProjectKeyHash)
		if err != nil {
			return nil, fmt.Errorf("设备 %s 的 project_key_hash: %w", fd.DeviceKey, err)
		}
		salt, err := b64(fd.ProjectKeySalt)
		if err != nil {
			return nil, fmt.Errorf("设备 %s 的 project_key_salt: %w", fd.DeviceKey, err)
		}
		id.ProjectKey = Digest{Params: DefaultParams, Salt: salt, Hash: hash}
	}

	return id, nil
}

func b64(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(s)
}

// DeviceCredential 是创建凭据时的输入（含明文 secret，仅此一次）。
type DeviceCredential struct {
	DeviceKey    string
	ProjectID    int64
	DeviceTypeID int64
	Mode         Mode
	IsGateway    bool

	// Secret 是明文，写入文件时会被摘要替换。
	Secret string
	// ProjectToken / ProjectKey 仅 A 档使用。
	ProjectToken string
	ProjectKey   string
}

// WriteCredentials 把凭据写入文件（摘要化后）。
//
// 明文 secret **只在返回值里回显一次**，与 03 §2.1「仅创建时展示一次」一致；
// 文件里不落明文。
func WriteCredentials(path string, creds []DeviceCredential, p Params) (map[string]string, error) {
	if p.Time == 0 {
		p = DefaultParams
	}

	ff := fileFormat{Devices: make([]fileDevice, 0, len(creds))}
	plaintext := make(map[string]string, len(creds))

	for _, c := range creds {
		secret := c.Secret
		if secret == "" && c.Mode == ModePerDevice {
			var err error
			secret, err = GenerateSecret()
			if err != nil {
				return nil, err
			}
			plaintext[c.DeviceKey] = secret
		}

		salt, err := NewSalt()
		if err != nil {
			return nil, err
		}

		fd := fileDevice{
			DeviceKey:    c.DeviceKey,
			ProjectID:    c.ProjectID,
			DeviceTypeID: c.DeviceTypeID,
			Mode:         c.Mode,
			IsGateway:    c.IsGateway,
			Params:       &params{Time: p.Time, Memory: p.Memory, Threads: p.Threads, KeyLen: p.KeyLen},
		}

		if c.Mode == ModePerDevice {
			d := HashSecret(secret, p, salt)
			fd.SecretSalt = base64.StdEncoding.EncodeToString(d.Salt)
			fd.SecretHash = base64.StdEncoding.EncodeToString(d.Hash)
		}
		if c.Mode == ModeProject {
			token := c.ProjectToken
			if token == "" {
				var err error
				token, err = GenerateSecret()
				if err != nil {
					return nil, err
				}
				plaintext[c.DeviceKey+"#access_token"] = token
			}
			key := c.ProjectKey
			if key == "" {
				var err error
				key, err = GenerateSecret()
				if err != nil {
					return nil, err
				}
				plaintext[c.DeviceKey+"#project_key"] = key
			}
			kSalt, err := NewSalt()
			if err != nil {
				return nil, err
			}
			kd := HashSecret(key, p, kSalt)

			fd.ProjectTokenHash = base64.StdEncoding.EncodeToString(HashToken(token))
			fd.ProjectKeySalt = base64.StdEncoding.EncodeToString(kd.Salt)
			fd.ProjectKeyHash = base64.StdEncoding.EncodeToString(kd.Hash)
		}

		ff.Devices = append(ff.Devices, fd)
	}

	// 稳定顺序，便于 diff 与人工核对。
	sort.Slice(ff.Devices, func(i, j int) bool { return ff.Devices[i].DeviceKey < ff.Devices[j].DeviceKey })

	out, err := json.MarshalIndent(ff, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("序列化凭据: %w", err)
	}
	if err := os.WriteFile(path, out, 0o600); err != nil {
		return nil, fmt.Errorf("写入凭据文件 %s: %w", path, err)
	}
	return plaintext, nil
}
