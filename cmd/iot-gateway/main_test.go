package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crypto/tls"
)

func TestLoadMQTTTLSConfig(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey := makeCertificate(t, true, nil, nil)
	serverCert, serverKey := makeCertificate(t, false, caCert, caKey)

	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	caFile := filepath.Join(dir, "client-ca.crt")
	writeFile(t, certFile, serverCert)
	writeFile(t, keyFile, serverKey)
	writeFile(t, caFile, caCert)

	config, err := loadMQTTTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("加载 mTLS 配置失败: %v", err)
	}
	if config.MinVersion != tls.VersionTLS13 {
		t.Fatalf("TLS 最低版本应为 1.3，得到 %d", config.MinVersion)
	}
	if config.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("配置客户端 CA 后应强制双向证书，得到 %v", config.ClientAuth)
	}
	if len(config.Certificates) != 1 || len(config.ClientCAs.Subjects()) != 1 {
		t.Fatalf("证书池加载不完整: certificates=%d ca=%d", len(config.Certificates), len(config.ClientCAs.Subjects()))
	}
}

func TestLoadMQTTTLSConfigRejectsPartialConfig(t *testing.T) {
	if _, err := loadMQTTTLSConfig("server.crt", "", ""); err == nil {
		t.Fatal("只提供证书而没有私钥时应拒绝启动")
	}
	if _, err := loadMQTTTLSConfig("", "", "client-ca.crt"); err == nil {
		t.Fatal("只提供客户端 CA 而没有服务端证书时应拒绝启动")
	}
}

func makeCertificate(t *testing.T, ca bool, issuerCert, issuerKey []byte) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成测试密钥失败: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("生成测试序列号失败: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "odoo20iot-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	if ca {
		template.IsCA = true
		template.BasicConstraintsValid = true
		template.KeyUsage |= x509.KeyUsageCertSign
	}
	var parent *x509.Certificate
	issuerPrivateKey := key
	if len(issuerCert) > 0 {
		block, _ := pem.Decode(issuerCert)
		parent, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("解析测试 CA 失败: %v", err)
		}
		keyBlock, _ := pem.Decode(issuerKey)
		issuerPrivateKey, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
		if err != nil {
			t.Fatalf("解析测试 CA 私钥失败: %v", err)
		}
	} else {
		parent = template
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, issuerPrivateKey)
	if err != nil {
		t.Fatalf("创建测试证书失败: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("写入测试证书失败: %v", err)
	}
}
