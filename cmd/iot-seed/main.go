// Command iot-seed 往控制面写入**开发用**的租户 / 设备类型 / 设备种子数据。
//
// 为什么是独立命令而不是迁移：
//  1. 迁移文件按**校验和不可变**（internal/pg/migrate.go），而种子含随机 Argon2 盐 ——
//     文件不可复现，改一次就触发「校验和漂移」的硬错误；
//  2. 迁移在**每个环境**（含生产）都会执行，开发数据/凭据会跟着进生产；
//  3. DDL 与 DML 应分离；种子要能参数化、可重跑。
//
// 真实来源是 Odoo（README 铁律 1），但 Odoo 侧的设备同步尚未实现；
// 本命令是该缺口下**诚实的替身**（09 §5.1 遗留）。
//
// 幂等：默认重跑不改任何已有设备的凭据，只更新元数据；`-rotate` 才重置凭据。
//
// 用法（devbox 内）：
//
//	go run ./cmd/iot-seed -pg-dsn "$IOT_PG_DSN" -devices 3 -out tmp/iot-seed-creds.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/auth"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/pg"
)

type config struct {
	pgDSN         string
	projectID     int64
	projectKey    string
	projectName   string
	deviceTypeID  int64
	deviceTypeKey string
	deviceTypeNam string
	devices       int
	devicePrefix  string
	firstDeviceID int64
	rotate        bool
	out           string
	timeout       time.Duration
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\niot-seed 退出: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.pgDSN, "pg-dsn", pg.DefaultDSN, "业务库 DSN")
	flag.Int64Var(&cfg.projectID, "project-id", 1, "租户 id（显式指定，便于 curl/测试引用）")
	flag.StringVar(&cfg.projectKey, "project-key", "dev", "租户标识")
	flag.StringVar(&cfg.projectName, "project-name", "开发租户", "租户名")
	flag.Int64Var(&cfg.deviceTypeID, "device-type-id", 1, "设备类型 id")
	flag.StringVar(&cfg.deviceTypeKey, "device-type-key", "dt-dev", "设备类型标识")
	flag.StringVar(&cfg.deviceTypeNam, "device-type-name", "开发设备类型", "设备类型名")
	flag.IntVar(&cfg.devices, "devices", 10, "设备台数")
	flag.StringVar(&cfg.devicePrefix, "device-prefix", "dev-", "device_key 前缀")
	flag.Int64Var(&cfg.firstDeviceID, "first-device-id", 1001, "首台设备 id（后续递增）")
	flag.BoolVar(&cfg.rotate, "rotate", false, "重置已有设备的凭据（默认幂等保留）")
	flag.StringVar(&cfg.out, "out", "tmp/iot-seed-creds.json", "新凭据写出路径（含明文 secret，勿提交）")
	flag.DurationVar(&cfg.timeout, "timeout", 60*time.Second, "整体超时")
	flag.Parse()
	return cfg
}

// credFile 是写出的凭据文件（含明文 secret，仅开发）。
type credFile struct {
	ProjectID int64     `json:"project_id"`
	Note      string    `json:"note"`
	Devices   []credRow `json:"devices"`
}

type credRow struct {
	ID        int64  `json:"id"`
	DeviceKey string `json:"device_key"`
	Secret    string `json:"secret"`
}

func run(cfg config) error {
	if cfg.devices <= 0 {
		return fmt.Errorf("-devices 必须为正")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	pool, err := pg.Open(ctx, pg.Config{DSN: cfg.pgDSN})
	if err != nil {
		return err
	}
	defer pool.Close()

	store, err := catalog.NewPGStore(pool)
	if err != nil {
		return err
	}

	if _, err := store.UpsertProject(ctx, catalog.Project{
		ID: cfg.projectID, ProjectKey: cfg.projectKey, Name: cfg.projectName, Status: "active",
	}); err != nil {
		return err
	}
	if _, err := store.UpsertDeviceType(ctx, catalog.DeviceType{
		ID: cfg.deviceTypeID, ProjectID: cfg.projectID, TypeKey: cfg.deviceTypeKey,
		Name: cfg.deviceTypeNam, ThingModel: json.RawMessage(`{"metrics":["temperature","humidity","pressure","voltage","running"]}`),
	}); err != nil {
		return err
	}

	keys := make([]string, 0, cfg.devices)
	for i := 0; i < cfg.devices; i++ {
		keys = append(keys, fmt.Sprintf("%s%d", cfg.devicePrefix, i+1))
	}
	existing, err := store.DeviceKeysExisting(ctx, cfg.projectID, keys)
	if err != nil {
		return err
	}

	var (
		created int
		rotated int
		creds   []credRow
	)
	for i, key := range keys {
		id := cfg.firstDeviceID + int64(i)
		needSecret := cfg.rotate || !existing[key]

		var secretHash string
		var secret string
		if needSecret {
			secret, err = auth.GenerateSecret()
			if err != nil {
				return err
			}
			salt, err := auth.NewSalt()
			if err != nil {
				return err
			}
			secretHash = catalog.EncodeSecretHash(auth.HashSecret(secret, auth.DefaultParams, salt))
		}

		gotID, err := store.UpsertDevice(ctx, catalog.DeviceUpsert{
			ID: id, ProjectID: cfg.projectID, DeviceTypeID: cfg.deviceTypeID,
			DeviceKey: key, Name: "种子设备 " + key,
			AuthMode: "per_device", Status: "active",
			SecretHash: secretHash, RotateSecret: cfg.rotate,
			ThingModelVersion: 1,
			Tags:              map[string]any{"seed": true},
		})
		if err != nil {
			return err
		}
		switch {
		case !existing[key]:
			created++
		case cfg.rotate:
			rotated++
		}
		if needSecret {
			creds = append(creds, credRow{ID: gotID, DeviceKey: key, Secret: secret})
		}
		fmt.Printf("设备 %-10s id=%-6d %s\n", key, gotID, map[bool]string{true: "凭据已生成", false: "沿用已有凭据"}[needSecret])
	}

	fmt.Printf("\n租户 id=%d（%s）｜ 设备类型 id=%d（%s）｜ 新增 %d 台，重置 %d 台，复用 %d 台\n",
		cfg.projectID, cfg.projectKey, cfg.deviceTypeID, cfg.deviceTypeKey,
		created, rotated, len(keys)-created-rotated)

	if len(creds) > 0 && strings.TrimSpace(cfg.out) != "" {
		body, err := json.MarshalIndent(credFile{
			ProjectID: cfg.projectID,
			Note:      "开发种子凭据：含明文 secret，仅用于本地验证；已被 .gitignore 的 tmp/ 覆盖",
			Devices:   creds,
		}, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(cfg.out, body, 0o600); err != nil {
			return fmt.Errorf("写凭据文件 %s: %w", cfg.out, err)
		}
		fmt.Printf("新凭据已写入 %s（0600，含明文，勿提交）\n", cfg.out)
	}
	return nil
}
