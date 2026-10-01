package catalog

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/auth"
)

// TestSecretHashRoundTrip 校验摘要编码/解码往返，且解码后仍能验签。
func TestSecretHashRoundTrip(t *testing.T) {
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatalf("生成盐失败: %v", err)
	}
	const secret = "s3cr3t-from-device"
	d := auth.HashSecret(secret, auth.DefaultParams, salt)

	enc := EncodeSecretHash(d)
	got, err := DecodeSecretHash(enc)
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	if got.Params != d.Params {
		t.Errorf("参数往返不一致：期望 %+v，得到 %+v", d.Params, got.Params)
	}
	if !bytes.Equal(got.Salt, d.Salt) || !bytes.Equal(got.Hash, d.Hash) {
		t.Error("盐或摘要往返不一致")
	}
	if !got.Verify(secret) {
		t.Error("解码后的摘要应能验签原 secret")
	}
	if got.Verify("wrong") {
		t.Error("错误 secret 不应通过")
	}
}

// TestDecodeSecretHashErrors 校验任何格式问题都报错 —— 绝不「解析失败当空摘要」。
func TestDecodeSecretHashErrors(t *testing.T) {
	valid := EncodeSecretHash(auth.HashSecret("x", auth.DefaultParams, []byte("0123456789abcdef")))
	cases := map[string]string{
		"空串":         "",
		"段数不足":       "argon2id$t=3",
		"算法不对":       "bcrypt$t=3,m=32768,p=2$AAAA$BBBB",
		"参数缺等号":      "argon2id$t3$AAAA$BBBB",
		"参数非整数":      "argon2id$t=x,m=32768,p=2$AAAA$BBBB",
		"参数为零":       "argon2id$t=0,m=32768,p=2$AAAA$BBBB",
		"未知参数":       "argon2id$t=3,m=32768,p=2,z=1$AAAA$BBBB",
		"盐非 base64":  "argon2id$t=3,m=32768,p=2$!!!!$BBBB",
		"摘要非 base64": "argon2id$t=3,m=32768,p=2$AAAA$!!!!",
		"盐为空":        "argon2id$t=3,m=32768,p=2$$BBBB",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeSecretHash(in); err == nil {
				t.Fatalf("非法输入 %q 必须报错", in)
			}
		})
	}
	if _, err := DecodeSecretHash(valid); err != nil {
		t.Fatalf("合法编码不应报错: %v", err)
	}
}

func dev(id, project, dtype int64, key, name, status string) Device {
	return Device{
		ID: id, ProjectID: project, DeviceTypeID: dtype, DeviceKey: key, Name: name,
		Status: status, AuthMode: "per_device",
		CreatedAt: time.Unix(0, 0).UTC(), UpdatedAt: time.Unix(0, 0).UTC(),
	}
}

// TestMemStoreListDevices 校验过滤与 keyset 分页。
func TestMemStoreListDevices(t *testing.T) {
	m := NewMemStore()
	m.Add(
		dev(1, 1, 10, "a-1", "泵 A", "active"),
		dev(2, 1, 10, "a-2", "泵 B", "inactive"),
		dev(3, 1, 11, "b-1", "阀 C", "active"),
		dev(4, 2, 10, "x-1", "他租户", "active"),
	)
	ctx := context.Background()

	t.Run("租户隔离", func(t *testing.T) {
		page, err := m.ListDevices(ctx, DeviceFilter{ProjectID: 1})
		if err != nil {
			t.Fatalf("列出失败: %v", err)
		}
		if len(page.Devices) != 3 {
			t.Fatalf("期望 3 台（project 1），得到 %d", len(page.Devices))
		}
	})

	t.Run("按类型与状态过滤", func(t *testing.T) {
		page, err := m.ListDevices(ctx, DeviceFilter{ProjectID: 1, DeviceTypeID: 10})
		if err != nil || len(page.Devices) != 2 {
			t.Fatalf("类型过滤期望 2 台，得到 %d（err=%v）", len(page.Devices), err)
		}
		page, err = m.ListDevices(ctx, DeviceFilter{ProjectID: 1, Status: "active"})
		if err != nil || len(page.Devices) != 2 {
			t.Fatalf("状态过滤期望 2 台，得到 %d（err=%v）", len(page.Devices), err)
		}
	})

	t.Run("子串匹配 name 与 device_key", func(t *testing.T) {
		page, _ := m.ListDevices(ctx, DeviceFilter{ProjectID: 1, Query: "泵"})
		if len(page.Devices) != 2 {
			t.Fatalf("按名称匹配期望 2 台，得到 %d", len(page.Devices))
		}
		page, _ = m.ListDevices(ctx, DeviceFilter{ProjectID: 1, Query: "b-1"})
		if len(page.Devices) != 1 {
			t.Fatalf("按 device_key 匹配期望 1 台，得到 %d", len(page.Devices))
		}
	})

	t.Run("keyset 分页", func(t *testing.T) {
		page, err := m.ListDevices(ctx, DeviceFilter{ProjectID: 1, Limit: 2})
		if err != nil {
			t.Fatalf("首页失败: %v", err)
		}
		if len(page.Devices) != 2 || page.NextAfterID != 2 {
			t.Fatalf("首页期望 2 台且游标=2，得到 %d 台 / 游标 %d", len(page.Devices), page.NextAfterID)
		}
		page2, err := m.ListDevices(ctx, DeviceFilter{ProjectID: 1, Limit: 2, AfterID: page.NextAfterID})
		if err != nil {
			t.Fatalf("次页失败: %v", err)
		}
		if len(page2.Devices) != 1 || page2.NextAfterID != 0 {
			t.Fatalf("次页期望 1 台且无更多，得到 %d 台 / 游标 %d", len(page2.Devices), page2.NextAfterID)
		}
		if page2.Devices[0].ID != 3 {
			t.Fatalf("次页应接着 id=3，得到 %d", page2.Devices[0].ID)
		}
	})

	t.Run("必须提供租户", func(t *testing.T) {
		if _, err := m.ListDevices(ctx, DeviceFilter{}); err == nil {
			t.Fatal("project_id<=0 必须报错")
		}
	})
}

// TestMemStoreDeviceIDsOwned 校验归属判定：他租户与软删都不算「拥有」。
func TestMemStoreDeviceIDsOwned(t *testing.T) {
	deleted := time.Now()
	m := NewMemStore()
	d := dev(5, 1, 10, "gone", "已删", "active")
	d.DeletedAt = &deleted
	m.Add(dev(1, 1, 10, "a", "A", "active"), dev(4, 2, 10, "x", "他租户", "active"), d)

	got, err := m.DeviceIDsOwned(context.Background(), 1, []int64{1, 4, 5, 999})
	if err != nil {
		t.Fatalf("归属查询失败: %v", err)
	}
	if !got[1] {
		t.Error("id=1 属于本租户，应在结果里")
	}
	for _, id := range []int64{4, 5, 999} {
		if got[id] {
			t.Errorf("id=%d 不该算本租户（他租户/软删/不存在）", id)
		}
	}
}
