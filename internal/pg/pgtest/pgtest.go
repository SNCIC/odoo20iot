// Package pgtest 提供连**真实 PG** 的测试基建。
//
// 单独成包（而不是写成 _test.go）是因为多个包的测试都要用它：
// internal/pg 的迁移测试与 internal/alarm 的 PGStore 测试共用同一套
// 「建临时库 → 跑迁移 → 用完删库」。各自实现一遍就会各自错一遍 ——
// 事实上这套基建曾经因为一个安静的 DSN 陷阱让整套用例打在基准库上还「通过」。
package pgtest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/pg"
)

// EnvDSN 是基准库 DSN 的环境变量名。
const EnvDSN = "IOT_PG_DSN"

// DSN 返回基准 DSN；未配置则**跳过**用例。
//
// 打真 PG 的用例必须显式跳过而不是静默通过：静默通过会让人以为
// 「这条路径测过了」，实际一次都没跑。
func DSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(EnvDSN)
	if dsn == "" {
		t.Skipf("未设置 %s，跳过真实 PG 测试", EnvDSN)
	}
	return dsn
}

// DB 建临时库并**跑完迁移**，返回可用的连接池；测试结束自动删库。
//
// 每个用例一个库（而不是共用一个库再清表）：迁移器本身要改 schema，
// 共库时用例会互相看到对方的表与 schema_migrations 行。
// t.Cleanup 在测试函数的 defer 之后执行，故调用方的连接池已先关闭。
func DB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return newTempDB(t, true)
}

// EmptyDB 建临时库但**不跑迁移**，供「测试迁移器本身」的用例使用 ——
// 已经跑过迁移的库测不出「首次应用」这件事。
func EmptyDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return newTempDB(t, false)
}

func newTempDB(t *testing.T, migrate bool) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	base := DSN(t)

	admin, err := pg.Open(ctx, pg.Config{DSN: base})
	if err != nil {
		t.Fatalf("连接基准库: %v", err)
	}

	// 名字带 pid 与纳秒，避免并发或残留导致撞名。
	name := fmt.Sprintf("iot_t_%d_%d", os.Getpid(), time.Now().UnixNano()%1000000)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatalf("建临时库: %v", err)
	}
	admin.Close()

	t.Cleanup(func() {
		a, err := pg.Open(context.Background(), pg.Config{DSN: base})
		if err != nil {
			return
		}
		defer a.Close()
		_, _ = a.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	pool := OpenDB(t, base, name)
	if migrate {
		if _, err := pg.Migrate(ctx, pool); err != nil {
			t.Fatalf("迁移临时库: %v", err)
		}
	}
	return pool
}

// OpenDB 连到指定名字的库。
//
// ⚠️ 必须传 dbName 而不是「改完 ConnConfig.Database 再取 ConnString()」：
// 后者会**原样吐回最初传入的字符串**，于是「换库」悄悄变成「连回原库」，
// 用例还会通过 —— 这正是本包存在的原因。
func OpenDB(t *testing.T, baseDSN, dbName string) *pgxpool.Pool {
	t.Helper()
	poolCfg, err := pg.Parse(pg.Config{DSN: baseDSN})
	if err != nil {
		t.Fatalf("解析 DSN: %v", err)
	}
	poolCfg.ConnConfig.Database = dbName
	pool, err := pg.OpenWithConfig(context.Background(), poolCfg, 5*time.Second)
	if err != nil {
		t.Fatalf("连接 %s: %v", dbName, err)
	}
	t.Cleanup(pool.Close)

	// 隔离性自检：连错库必须当场炸，而不是让用例悄悄改坏基准库。
	var got string
	if err := pool.QueryRow(context.Background(), "SELECT current_database()").Scan(&got); err != nil {
		t.Fatalf("核对当前库: %v", err)
	}
	if got != dbName {
		t.Fatalf("测试没连到临时库（current_database=%s，期望 %s）—— 隔离失效，拒绝继续",
			got, dbName)
	}
	return pool
}
