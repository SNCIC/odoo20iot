package gateway

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/SNCIC/odoo20iot/internal/auth"
)

// 端到端认证用例：走**真实 broker + 真实 MQTT 报文**，验证 A1 的两件事 ——
// 凭据判定与物模型 ACL。这是 A1 最关键的证据：单元测试只能证明函数正确，
// 这里证明的是「接到 broker 上以后行为正确」。
//
// 参数取廉价值：本用例验证的是认证**路径**（凭据读取、分派、ACL 判定），
// 不是 KDF 成本 —— KDF 成本由 capacity_test.go 单独测。

var cheapParams = auth.Params{Time: 1, Memory: 8 * 1024, Threads: 1, KeyLen: 32}

const (
	devASecret = "secret-of-dev-A"
	devBSecret = "secret-of-dev-B"
)

// newAuthBroker 起一个**带认证**的 broker，并返回认证器与指标。
func newAuthBroker(t *testing.T) (*Broker, *auth.Authenticator, *Metrics) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "creds.json")
	if _, err := auth.WriteCredentials(path, []auth.DeviceCredential{
		{DeviceKey: "dev-A", ProjectID: 10231, DeviceTypeID: 55, Mode: auth.ModePerDevice, Secret: devASecret},
		{DeviceKey: "dev-B", ProjectID: 10231, DeviceTypeID: 55, Mode: auth.ModePerDevice, Secret: devBSecret},
		{DeviceKey: "gw-1", ProjectID: 10231, DeviceTypeID: 60, Mode: auth.ModePerDevice, Secret: devASecret, IsGateway: true},
	}, cheapParams); err != nil {
		t.Fatalf("写入凭据文件失败: %v", err)
	}

	dir, err := auth.LoadFile(path)
	if err != nil {
		t.Fatalf("加载凭据文件失败: %v", err)
	}

	policy := auth.DefaultPolicy()
	policy.MaxConcurrentVerify = 4
	authMetrics := new(auth.Metrics)
	authenticator := auth.NewAuthenticator(dir, policy, authMetrics)

	metrics := new(Metrics)
	broker, err := New(context.Background(), Options{
		MQTTAddr:                 "127.0.0.1:0",
		Publisher:                &fakePublisher{},
		DeviceLifecyclePublisher: discardLifecyclePublisher{},
		Router:                   ContractRouter{Project: "spike"},
		PubackTimeout:            5 * time.Second,
		Metrics:                  metrics,
		Log:                      testLogger(),
		Authenticator:            authenticator,
	})
	if err != nil {
		t.Fatalf("构造带认证的 broker 失败: %v", err)
	}

	broker.Serve()
	t.Cleanup(func() { _ = broker.Close() })
	return broker, authenticator, metrics
}

// TestAuthE2E_凭据判定 覆盖三档认证里 B 档的各项拒绝路径。
func TestAuthE2E_凭据判定(t *testing.T) {
	broker, _, metrics := newAuthBroker(t)

	cases := []struct {
		name     string
		clientID string
		username string
		password string
		wantOK   bool
	}{
		{"正确凭据", "dev-A", "dev-A", devASecret, true},
		{"密码错误", "dev-A", "dev-A", "wrong-secret", false},
		{"设备不存在", "ghost", "ghost", devASecret, false},
		{"clientID 与 username 不一致", "dev-A", "dev-B", devBSecret, false},
		{"冒用他人 clientID", "dev-A", "dev-A", devBSecret, false},
		{"不提供用户名密码", "dev-A", "", "", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dev := dialTestDevice(t, broker.Addr(), c.clientID)
			code := dev.connectWith(c.clientID, c.username, c.password)

			if c.wantOK && code != packets.CodeSuccess.Code {
				t.Fatalf("应认证通过，CONNACK reason=%d", code)
			}
			if !c.wantOK && code == packets.CodeSuccess.Code {
				t.Fatal("应被拒绝，却收到了成功的 CONNACK")
			}
		})
	}

	if got := metrics.AuthSuccessTotal.Load(); got != 1 {
		t.Fatalf("认证成功计数应为 1，得到 %d", got)
	}
	if got := metrics.ConnectFailTotal.Load(); got != int64(len(cases)-1) {
		t.Fatalf("认证失败计数应为 %d，得到 %d", len(cases)-1, got)
	}
	// 失败必须能按原因分类，否则线上无法定位。
	reasons := metrics.connectFailReasons()
	if reasons[string(auth.ReasonUnknownDevice)] == 0 {
		t.Errorf("应记录 unknown_device 原因，实际 %+v", reasons)
	}
	if reasons[string(auth.ReasonBadCredential)] == 0 {
		t.Errorf("应记录 bad_credential 原因，实际 %+v", reasons)
	}
}

// TestAuthE2E_ACL订阅 验证 03 §2.2 的横向越权防护。
func TestAuthE2E_ACL订阅(t *testing.T) {
	broker, _, metrics := newAuthBroker(t)

	dev := dialTestDevice(t, broker.Addr(), "dev-A")
	if code := dev.connectWith("dev-A", "dev-A", devASecret); code != packets.CodeSuccess.Code {
		t.Fatalf("应认证通过，reason=%d", code)
	}

	allowed := []string{
		"v1/devices/dev-A/cmd/+",
		"v1/devices/dev-A/shadow/desired",
		"v1/devices/dev-A/cfg/+",
		"v1/devices/dev-A/ota/+",
	}
	for i, f := range allowed {
		if code := dev.subscribe(f, uint16(10+i)); code >= 0x80 {
			t.Errorf("应允许订阅 %s，SUBACK=%d", f, code)
		}
	}

	denied := []string{
		"v1/devices/dev-B/cmd/+", // 别的设备
		"v1/devices/dev-A/#",     // 覆盖到不该订阅的层级
		"v1/devices/#",
		"#",
		"$SYS/#",
	}
	for i, f := range denied {
		if code := dev.subscribe(f, uint16(50+i)); code < 0x80 {
			t.Errorf("不应允许订阅 %s（SUBACK=%d）", f, code)
		}
	}

	if got := metrics.ACLDeniedTotal.Load(); got < int64(len(denied)) {
		t.Fatalf("ACL 拒绝计数应 ≥ %d，得到 %d", len(denied), got)
	}
}

// TestAuthE2E_ACL发布 验证越权发布会**立刻断连**（mochi 对 v3.1.1 QoS1 的行为）。
func TestAuthE2E_ACL发布(t *testing.T) {
	broker, _, _ := newAuthBroker(t)

	dev := dialTestDevice(t, broker.Addr(), "dev-A")
	if code := dev.connectWith("dev-A", "dev-A", devASecret); code != packets.CodeSuccess.Code {
		t.Fatalf("应认证通过，reason=%d", code)
	}

	// 自己的命名空间：正常回 PUBACK。
	id := dev.publish("v1/devices/dev-A/telemetry", []byte(`{"temperature":25.3}`), false)
	if err := dev.awaitPuback(id, 3*time.Second); err != nil {
		t.Fatalf("自己的 topic 应收到 PUBACK: %v", err)
	}

	// 别人的命名空间：必须被断开，而不是静默丢弃。
	id = dev.publish("v1/devices/dev-B/telemetry", []byte(`{"temperature":25.3}`), false)
	_ = id
	dev.expectClosed(3 * time.Second)
}

// TestAuthE2E_网关型设备的子设备命名空间 覆盖 03 §2.2 的网关例外。
func TestAuthE2E_网关型设备的子设备命名空间(t *testing.T) {
	broker, _, _ := newAuthBroker(t)

	dev := dialTestDevice(t, broker.Addr(), "gw-1")
	if code := dev.connectWith("gw-1", "gw-1", devASecret); code != packets.CodeSuccess.Code {
		t.Fatalf("应认证通过，reason=%d", code)
	}

	if code := dev.subscribe("v1/gateways/gw-1/devices/+/cmd/+", 1); code >= 0x80 {
		t.Errorf("网关应可订阅自己名下子设备的命令，SUBACK=%d", code)
	}
	if code := dev.subscribe("v1/gateways/gw-2/devices/+/cmd/+", 2); code < 0x80 {
		t.Error("网关不应可订阅别人名下子设备的命令")
	}
}

// TestAuthE2E_断开后授权记录被回收 验证不会随连接数增长而泄漏内存。
func TestAuthE2E_断开后授权记录被回收(t *testing.T) {
	broker, _, _ := newAuthBroker(t)

	for i := 0; i < 20; i++ {
		dev := dialTestDevice(t, broker.Addr(), "dev-A")
		if code := dev.connectWith("dev-A", "dev-A", devASecret); code != packets.CodeSuccess.Code {
			t.Fatalf("第 %d 次认证失败，reason=%d", i, code)
		}
		dev.close()
	}

	// 断开是异步的（服务端要读到自己发的 DISCONNECT/EOF），给一点时间。
	hook := broker.AuthHook()
	if hook == nil {
		t.Fatal("broker 未启用认证")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hook.grantCount() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("断开后授权记录未回收，残留 %d 条", hook.grantCount())
}
