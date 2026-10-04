package ota

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func DownloadSignature(secret string, projectID int64, objectKey string, expires time.Time) string {
	payload := fmt.Sprintf("%d\n%s\n%d", projectID, objectKey, expires.Unix())
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func VerifyDownloadSignature(secret string, projectID int64, objectKey, rawExpires, signature string, now time.Time) error {
	if strings.TrimSpace(secret) == "" || projectID <= 0 || strings.TrimSpace(objectKey) == "" {
		return fmt.Errorf("ota: 下载签名参数缺失")
	}
	expiresUnix, err := strconv.ParseInt(rawExpires, 10, 64)
	if err != nil || expiresUnix <= now.Unix() || expiresUnix > now.Add(24*time.Hour).Unix() {
		return fmt.Errorf("ota: 下载地址已过期或有效期非法")
	}
	expires := time.Unix(expiresUnix, 0).UTC()
	expected := DownloadSignature(secret, projectID, objectKey, expires)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("ota: 下载签名无效")
	}
	return nil
}
