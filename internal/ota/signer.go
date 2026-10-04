package ota

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"time"
)

type Signer struct {
	privateKey ed25519.PrivateKey
	KeyID      string
}

func NewSigner(privateKey ed25519.PrivateKey, keyID string) (*Signer, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("ota: Ed25519 私钥长度非法")
	}
	if strings.TrimSpace(keyID) == "" {
		return nil, fmt.Errorf("ota: signing key id 不能为空")
	}
	return &Signer{privateKey: append(ed25519.PrivateKey(nil), privateKey...), KeyID: keyID}, nil
}

func LoadSigner(path, keyID string) (*Signer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取 OTA 签名私钥: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("OTA 签名私钥不是 PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("解析 OTA 签名私钥: %w", err)
	}
	privateKey, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("OTA 签名私钥不是 Ed25519")
	}
	return NewSigner(privateKey, keyID)
}

func (s *Signer) SignManifest(manifest Manifest, now time.Time, ttl time.Duration) (Manifest, error) {
	if s == nil {
		return Manifest{}, fmt.Errorf("ota: 签名器未配置")
	}
	if ttl <= 0 || ttl > 24*time.Hour {
		return Manifest{}, fmt.Errorf("ota: 清单有效期必须在 0..24h")
	}
	manifest.SigningKeyID = s.KeyID
	manifest.ExpiresAt = now.Add(ttl).UTC().Format(time.RFC3339)
	return manifest.Sign(s.privateKey)
}
