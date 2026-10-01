package catalog_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/auth"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/pg/pgtest"
)

// 打**真 PG**（表来自 internal/pg/migrations，含 0006 控制面主数据）。
// 未设 IOT_PG_DSN 时 pgtest.DB 会跳过。

type harness struct {
	store *catalog.PGStore
	pool  *pgxpool.Pool
	ctx   context.Context
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	pool := pgtest.DB(t)
	store, err := catalog.NewPGStore(pool)
	if err != nil {
		t.Fatalf("构造 PGStore 失败: %v", err)
	}
	return &harness{store: store, pool: pool, ctx: context.Background()}
}

// seedTenant 建一个租户 + 一个设备类型（类型 id = 租户 id × 10）。
func (h *harness) seedTenant(t *testing.T, pid int64, key string) {
	t.Helper()
	if _, err := h.store.UpsertProject(h.ctx, catalog.Project{ID: pid, ProjectKey: key, Name: "租户 " + key}); err != nil {
		t.Fatalf("建租户 %d 失败: %v", pid, err)
	}
	if _, err := h.store.UpsertDeviceType(h.ctx, catalog.DeviceType{
		ID: pid * 10, ProjectID: pid, TypeKey: key + "-dt", Name: "类型",
	}); err != nil {
		t.Fatalf("建设备类型失败: %v", err)
	}
}

func (h *harness) mkDevice(t *testing.T, d catalog.DeviceUpsert) int64 {
	t.Helper()
	if d.AuthMode == "" {
		d.AuthMode = "per_device"
	}
	if d.Status == "" {
		d.Status = "active"
	}
	id, err := h.store.UpsertDevice(h.ctx, d)
	if err != nil {
		t.Fatalf("建设备 %s 失败: %v", d.DeviceKey, err)
	}
	return id
}

// TestPGStore_PartitionedSchema 校验迁移 0006 落下的结构与分区。
func TestPGStore_PartitionedSchema(t *testing.T) {
	h := newHarness(t)

	var partitions int
	if err := h.pool.QueryRow(h.ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 't_device_p%' AND relkind = 'r'`).Scan(&partitions); err != nil {
		t.Fatalf("统计分区失败: %v", err)
	}
	if partitions != 16 {
		t.Fatalf("t_device 期望 16 个分区，得到 %d", partitions)
	}

	// 主键必须包含分区键（02 §3.4 的硬规则）。
	var pkDef string
	if err := h.pool.QueryRow(h.ctx,
		`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'pk_device'`).Scan(&pkDef); err != nil {
		t.Fatalf("读取主键定义失败: %v", err)
	}
	for _, want := range []string{"id", "project_id"} {
		if !strings.Contains(pkDef, want) {
			t.Errorf("pk_device 应包含 %q，实际 %s", want, pkDef)
		}
	}

	// 本批刻意**不启用** RLS（见 0006 注释）—— 若被误开，读路径会返回空行。
	var rls bool
	if err := h.pool.QueryRow(h.ctx,
		`SELECT relrowsecurity FROM pg_class WHERE relname = 't_device'`).Scan(&rls); err != nil {
		t.Fatalf("读取 RLS 状态失败: %v", err)
	}
	if rls {
		t.Error("本批不应启用 t_device 的 RLS（策略未建时会默认拒绝所有行）")
	}
}

// TestPGStore_UpsertIdempotent 校验 upsert 幂等：重跑不新增行，版本递增；默认不动凭据。
func TestPGStore_UpsertIdempotent(t *testing.T) {
	h := newHarness(t)
	h.seedTenant(t, 1, "dev")

	secret1 := catalog.EncodeSecretHash(auth.HashSecret("s1", auth.DefaultParams, []byte("0123456789abcdef")))
	up := catalog.DeviceUpsert{
		ID: 1001, ProjectID: 1, DeviceTypeID: 10, DeviceKey: "dev-1", Name: "设备 1",
		SecretHash: secret1, AuthMode: "per_device", Status: "active",
	}
	id1 := h.mkDevice(t, up)
	if id1 != 1001 {
		t.Fatalf("首次 upsert 期望 id=1001，得到 %d", id1)
	}

	// 第二次带不同凭据但 rotate=false：冲突命中同一行、且保留原凭据。
	secret2 := catalog.EncodeSecretHash(auth.HashSecret("s2", auth.DefaultParams, []byte("fedcba9876543210")))
	up.SecretHash = secret2
	id2 := h.mkDevice(t, up)
	if id2 != id1 {
		t.Fatalf("冲突键 (project_id, device_key) 应命中同一行，得到 %d / %d", id1, id2)
	}

	page, err := h.store.ListDevices(h.ctx, catalog.DeviceFilter{ProjectID: 1})
	if err != nil {
		t.Fatalf("列出失败: %v", err)
	}
	if len(page.Devices) != 1 {
		t.Fatalf("重跑不应新增行，得到 %d", len(page.Devices))
	}
	if page.Devices[0].Version < 2 {
		t.Errorf("更新后 version 应递增，得到 %d", page.Devices[0].Version)
	}

	stored := h.secretHash(t, 1001)
	if d, err := catalog.DecodeSecretHash(stored); err != nil || !d.Verify("s1") {
		t.Fatalf("rotate=false 时不应覆盖已有凭据（err=%v）", err)
	}

	// rotate=true 才覆盖。
	up.RotateSecret = true
	h.mkDevice(t, up)
	stored = h.secretHash(t, 1001)
	if d, err := catalog.DecodeSecretHash(stored); err != nil || !d.Verify("s2") {
		t.Fatalf("rotate=true 应覆盖凭据（err=%v）", err)
	}
}

func (h *harness) secretHash(t *testing.T, id int64) string {
	t.Helper()
	var out string
	if err := h.pool.QueryRow(h.ctx, `SELECT secret_hash FROM t_device WHERE id = $1`, id).Scan(&out); err != nil {
		t.Fatalf("读取摘要失败: %v", err)
	}
	return out
}

// TestPGStore_ListDevicesFiltersAndPagination 校验过滤与 keyset 分页。
func TestPGStore_ListDevicesFiltersAndPagination(t *testing.T) {
	h := newHarness(t)
	h.seedTenant(t, 1, "dev")
	h.seedTenant(t, 2, "other")

	h.mkDevice(t, catalog.DeviceUpsert{ID: 1, ProjectID: 1, DeviceTypeID: 10, DeviceKey: "a-1", Name: "泵 A", Status: "active"})
	h.mkDevice(t, catalog.DeviceUpsert{ID: 2, ProjectID: 1, DeviceTypeID: 10, DeviceKey: "a-2", Name: "泵 B", Status: "inactive"})
	h.mkDevice(t, catalog.DeviceUpsert{ID: 3, ProjectID: 1, DeviceTypeID: 10, DeviceKey: "b-1", Name: "阀 C", Status: "active"})
	h.mkDevice(t, catalog.DeviceUpsert{ID: 4, ProjectID: 2, DeviceTypeID: 20, DeviceKey: "x-1", Name: "他租户", Status: "active"})

	page, err := h.store.ListDevices(h.ctx, catalog.DeviceFilter{ProjectID: 1})
	if err != nil {
		t.Fatalf("列出失败: %v", err)
	}
	if len(page.Devices) != 3 {
		t.Fatalf("租户隔离失败：期望 3 台，得到 %d", len(page.Devices))
	}

	if page, _ = h.store.ListDevices(h.ctx, catalog.DeviceFilter{ProjectID: 1, Status: "active"}); len(page.Devices) != 2 {
		t.Fatalf("状态过滤期望 2 台，得到 %d", len(page.Devices))
	}
	if page, _ = h.store.ListDevices(h.ctx, catalog.DeviceFilter{ProjectID: 1, Query: "泵"}); len(page.Devices) != 2 {
		t.Fatalf("子串过滤期望 2 台，得到 %d", len(page.Devices))
	}
	if page, _ = h.store.ListDevices(h.ctx, catalog.DeviceFilter{ProjectID: 1, Query: "b-1"}); len(page.Devices) != 1 {
		t.Fatalf("按 device_key 过滤期望 1 台，得到 %d", len(page.Devices))
	}

	// keyset 分页：每页 2 条。
	page1, err := h.store.ListDevices(h.ctx, catalog.DeviceFilter{ProjectID: 1, Limit: 2})
	if err != nil {
		t.Fatalf("首页失败: %v", err)
	}
	if len(page1.Devices) != 2 || page1.NextAfterID != 2 {
		t.Fatalf("首页期望 2 台/游标 2，得到 %d/%d", len(page1.Devices), page1.NextAfterID)
	}
	page2, err := h.store.ListDevices(h.ctx, catalog.DeviceFilter{ProjectID: 1, Limit: 2, AfterID: page1.NextAfterID})
	if err != nil {
		t.Fatalf("次页失败: %v", err)
	}
	if len(page2.Devices) != 1 || page2.NextAfterID != 0 || page2.Devices[0].ID != 3 {
		t.Fatalf("次页期望 1 台(id=3)/无更多，得到 %d 台/游标 %d", len(page2.Devices), page2.NextAfterID)
	}
}

// TestPGStore_DeviceIDsOwned 校验归属：他租户与软删都不算「拥有」。
func TestPGStore_DeviceIDsOwned(t *testing.T) {
	h := newHarness(t)
	h.seedTenant(t, 1, "dev")
	h.seedTenant(t, 2, "other")

	h.mkDevice(t, catalog.DeviceUpsert{ID: 1, ProjectID: 1, DeviceTypeID: 10, DeviceKey: "a", Name: "A"})
	h.mkDevice(t, catalog.DeviceUpsert{ID: 2, ProjectID: 2, DeviceTypeID: 20, DeviceKey: "x", Name: "X"})
	h.mkDevice(t, catalog.DeviceUpsert{ID: 3, ProjectID: 1, DeviceTypeID: 10, DeviceKey: "gone", Name: "已删"})

	if _, err := h.pool.Exec(h.ctx, `UPDATE t_device SET deleted_at = now() WHERE id = 3`); err != nil {
		t.Fatalf("软删设备失败: %v", err)
	}

	got, err := h.store.DeviceIDsOwned(h.ctx, 1, []int64{1, 2, 3, 999})
	if err != nil {
		t.Fatalf("归属查询失败: %v", err)
	}
	if !got[1] {
		t.Error("id=1 属于本租户，应在结果里")
	}
	for _, id := range []int64{2, 3, 999} {
		if got[id] {
			t.Errorf("id=%d 不该算本租户（他租户/软删/不存在）", id)
		}
	}
}

// TestPGStore_CrossTenantDeviceTypeRejected 校验复合外键挡住「设备挂到别的租户的类型上」。
func TestPGStore_CrossTenantDeviceTypeRejected(t *testing.T) {
	h := newHarness(t)
	h.seedTenant(t, 1, "dev")
	h.seedTenant(t, 2, "other")

	// 设备类型 20 属于租户 2；设备却声明属于租户 1 ⇒ 必须被 DB 拒绝。
	_, err := h.store.UpsertDevice(h.ctx, catalog.DeviceUpsert{
		ID: 1, ProjectID: 1, DeviceTypeID: 20, DeviceKey: "bad", Name: "跨界",
		AuthMode: "per_device", Status: "active",
	})
	if err == nil {
		t.Fatal("跨租户挂设备类型必须被外键拒绝")
	}
}

// TestPGStore_ProjectKeyUnique 校验租户标识唯一。
func TestPGStore_ProjectKeyUnique(t *testing.T) {
	h := newHarness(t)
	h.seedTenant(t, 1, "dev")
	if _, err := h.store.UpsertProject(h.ctx, catalog.Project{ID: 99, ProjectKey: "dev", Name: "重复"}); err == nil {
		t.Fatal("重复 project_key 必须被拒绝")
	}
}

// TestPGStore_RequiresProject 校验读路径不接受「无租户」的查询。
func TestPGStore_RequiresProject(t *testing.T) {
	h := newHarness(t)
	if _, err := h.store.ListDevices(h.ctx, catalog.DeviceFilter{ProjectID: 0}); err == nil {
		t.Fatal("project_id<=0 必须报错，不能全表扫描")
	}
	if _, err := h.store.DeviceIDsOwned(h.ctx, 0, []int64{1}); err == nil {
		t.Fatal("project_id<=0 必须报错")
	}
}
