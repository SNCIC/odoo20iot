package querysvc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/apiauth"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/pg/pgtest"
	"github.com/SNCIC/odoo20iot/internal/querysvc"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
	"github.com/SNCIC/odoo20iot/internal/tsdb/greptimedb"
)

// 端到端：真实 PG（控制面）+ 真实 GreptimeDB（时序）→ 真实 handler。
//
//	IOT_PG_DSN=... IOT_GREPTIMEDB_DSN=... go test ./internal/querysvc -run TestE2E -count=1 -v
//
// ⚠️ 会 DROP/重建 `telemetry`（与其他 tsdb 集成测试一致），只在可重建的开发栈上跑。
const e2eToken = "e2e-token"

func TestE2E_SeriesAndDevices(t *testing.T) {
	gresDSN := greptimeDSN(t) // 未设 IOT_GREPTIMEDB_DSN 即跳过

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// ---- 真实 PG：迁移 + 种子（pgtest.DB 未设 IOT_PG_DSN 时会跳过）----
	pool := pgtest.DB(t)
	store, err := catalog.NewPGStore(pool)
	if err != nil {
		t.Fatalf("构造 catalog 失败: %v", err)
	}
	if _, err := store.UpsertProject(ctx, catalog.Project{ID: 1, ProjectKey: "e2e", Name: "E2E 租户"}); err != nil {
		t.Fatalf("建租户失败: %v", err)
	}
	if _, err := store.UpsertDeviceType(ctx, catalog.DeviceType{ID: 10, ProjectID: 1, TypeKey: "e2e-dt", Name: "E2E 类型"}); err != nil {
		t.Fatalf("建设备类型失败: %v", err)
	}
	for i, key := range []string{"e2e-1", "e2e-2"} {
		if _, err := store.UpsertDevice(ctx, catalog.DeviceUpsert{
			ID: int64(1001 + i), ProjectID: 1, DeviceTypeID: 10, DeviceKey: key,
			Name: "E2E 设备 " + key, AuthMode: "per_device", Status: "active",
		}); err != nil {
			t.Fatalf("建设备失败: %v", err)
		}
	}

	// ---- 真实 GreptimeDB：写入几行遥测 ----
	// ⚠️ 注册顺序要紧：t.Cleanup 是 LIFO，先注册 Close、后注册 Drop，
	// 才能保证删表时连接池还活着（用 defer Close 会让删表打在已关闭的池上、静默失败）。
	tsStore, err := greptimedb.Open(ctx, gresDSN, 4)
	if err != nil {
		t.Fatalf("连接 GreptimeDB 失败: %v", err)
	}
	t.Cleanup(tsStore.Close)
	if err := tsStore.DropTable(ctx, tsdb.PlanJSON); err != nil {
		t.Fatalf("删表失败: %v", err)
	}
	if err := tsStore.CreateTable(ctx, tsdb.PlanJSON); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() { _ = tsStore.DropTable(context.Background(), tsdb.PlanJSON) })

	base := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)
	var rows []tsdb.Row
	for _, dev := range []int64{1001, 1002} {
		for i := 0; i < 120; i++ {
			rows = append(rows, tsdb.Row{
				TS: base.Add(time.Duration(i) * time.Second), ProjectID: 1, DeviceID: dev, DeviceTypeID: 10,
				Values: []tsdb.Value{
					tsdb.Number(20 + float64(i%10)), tsdb.Number(50), tsdb.Number(1013), tsdb.Number(3.7), tsdb.Bool(i%2 == 0),
				},
			})
		}
	}
	if err := tsStore.InsertRows(ctx, tsdb.PlanJSON, rows); err != nil {
		t.Fatalf("写遥测失败: %v", err)
	}

	// ---- 装配服务 ----
	verifier, err := apiauth.NewStaticTokenVerifier(e2eToken, 1)
	if err != nil {
		t.Fatalf("构造校验器失败: %v", err)
	}
	svc, err := querysvc.New(querysvc.DefaultConfig(), querysvc.Deps{
		Reader:   tsStore,
		Catalog:  store,
		Verifier: verifier,
		Health:   querysvc.Health{PingPG: pool.Ping, PingTSDB: tsStore.Ping},
	})
	if err != nil {
		t.Fatalf("构造服务失败: %v", err)
	}
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// ---- /api/v1/devices ----
	var devices struct {
		Devices []struct {
			ID        int64  `json:"id"`
			DeviceKey string `json:"device_key"`
		} `json:"devices"`
	}
	code := getJSON(t, srv.URL+"/api/v1/devices", &devices)
	if code != http.StatusOK || len(devices.Devices) != 2 {
		t.Fatalf("设备列表期望 200 且 2 台，得到 %d / %d 台", code, len(devices.Devices))
	}
	t.Logf("✓ /api/v1/devices 返回 %d 台", len(devices.Devices))

	// ---- /api/v1/series（原始） ----
	since := base.Add(-time.Second).Format(time.RFC3339)
	until := base.Add(2 * time.Minute).Format(time.RFC3339)
	url := fmt.Sprintf("%s/api/v1/series?device_ids=1001,1002&metric=temperature&since=%s&until=%s", srv.URL, since, until)

	var series struct {
		Granularity string `json:"granularity"`
		Source      string `json:"source"`
		Points      []struct {
			TS       time.Time `json:"ts"`
			DeviceID int64     `json:"device_id"`
			Value    *float64  `json:"value"`
		} `json:"points"`
	}
	if code := getJSON(t, url, &series); code != http.StatusOK {
		t.Fatalf("曲线查询期望 200，得到 %d", code)
	}
	if series.Granularity != "raw" || series.Source != "raw_json" {
		t.Fatalf("元数据不符: %+v", series)
	}
	if len(series.Points) != 240 {
		t.Fatalf("期望 240 个点（2 设备 × 120s），得到 %d", len(series.Points))
	}
	t.Logf("✓ /api/v1/series 原始：%d 点，source=%s", len(series.Points), series.Source)

	// ---- /api/v1/series（显式分桶） ----
	var agg struct {
		Granularity string `json:"granularity"`
		Bucket      string `json:"bucket"`
		Buckets     []struct {
			DeviceID int64    `json:"device_id"`
			Avg      *float64 `json:"avg"`
			Count    int64    `json:"count"`
		} `json:"buckets"`
	}
	if code := getJSON(t, url+"&bucket=1m", &agg); code != http.StatusOK {
		t.Fatalf("分桶查询期望 200，得到 %d", code)
	}
	if agg.Granularity != "aggregated" || agg.Bucket != "1m0s" {
		t.Fatalf("分桶元数据不符: %+v", agg)
	}
	if len(agg.Buckets) == 0 {
		t.Fatal("分桶结果不应为空")
	}
	t.Logf("✓ /api/v1/series 分桶：粒度=%s 桶宽=%s %d 行", agg.Granularity, agg.Bucket, len(agg.Buckets))

	// ---- 租户不变量：query 里出现 project_id 一律 400 ----
	if code := getJSON(t, srv.URL+"/api/v1/series?project_id=1&device_ids=1001&metric=temperature", nil); code != http.StatusBadRequest {
		t.Fatalf("project_id 参数期望 400，得到 %d", code)
	}
	// ---- 跨租户设备一律 403 ----
	if code := getJSON(t, srv.URL+"/api/v1/series?device_ids=9999&metric=temperature", nil); code != http.StatusForbidden {
		t.Fatalf("未知设备期望 403，得到 %d", code)
	}
	t.Log("✓ 租户不变量：project_id 参数 400、跨租户设备 403")
}

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+e2eToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("请求 %s 失败: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("读取响应失败: %v", err)
	}
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, out); err != nil {
			t.Fatalf("解析响应失败: %v（%s）", err, string(body))
		}
	}
	return resp.StatusCode
}

func greptimeDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("IOT_GREPTIMEDB_DSN")
	if dsn == "" {
		t.Skip("未设置 IOT_GREPTIMEDB_DSN，跳过端到端用例")
	}
	return dsn
}
