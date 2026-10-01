package greptimedb

import (
	"context"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestDiag_JSONParamEncoding 是一次性的诊断用例：确定 GreptimeDB 的 JSON 语义
// 在 PostgreSQL wire protocol 下接受哪种参数编码，以及能否绕开 JSON 类型本身。
//
// 结论已固化进 store.go 并记录在 02 §4.1.1；本用例保留作为「换版本后回归检查」。
func TestDiag_JSONParamEncoding(t *testing.T) {
	dsn := os.Getenv("IOT_GREPTIMEDB_DSN")
	if dsn == "" {
		t.Skip("未设置 IOT_GREPTIMEDB_DSN，跳过")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := Open(ctx, dsn, 2)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	defer store.Close()
	pool := store.Pool()

	for _, ddl := range []string{
		`DROP TABLE IF EXISTS diag_json`,
		`DROP TABLE IF EXISTS diag_text`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("清理失败: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS diag_json`)
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS diag_text`)
	})

	if _, err := pool.Exec(ctx, `CREATE TABLE diag_json (
		ts TIMESTAMP TIME INDEX, device_id BIGINT, "metrics" JSON, PRIMARY KEY (device_id)
	) WITH ('ttl'='1d')`); err != nil {
		t.Fatalf("建表 diag_json 失败: %v", err)
	}
	// 备选：用 TEXT 承载 JSON 文档，看能否绕开 JSON 类型的 bytea 参数映射。
	if _, err := pool.Exec(ctx, `CREATE TABLE diag_text (
		ts TIMESTAMP TIME INDEX, device_id BIGINT, "metrics" TEXT, PRIMARY KEY (device_id)
	) WITH ('ttl'='1d')`); err != nil {
		t.Fatalf("建表 diag_text 失败: %v", err)
	}

	doc := `{"temperature":25.3,"running":true}`
	hexDoc := `\x` + hex.EncodeToString([]byte(doc))

	cases := []struct {
		name string
		run  func() error
	}{
		{"JSON 列 · 内联字面量", func() error {
			_, err := pool.Exec(ctx, `INSERT INTO diag_json (ts, device_id, "metrics") VALUES (NOW(), 1, '`+doc+`')`)
			return err
		}},
		{"JSON 列 · 普通字符串参数", func() error {
			_, err := pool.Exec(ctx, `INSERT INTO diag_json (ts, device_id, "metrics") VALUES (NOW(), 2, $1)`, doc)
			return err
		}},
		{"JSON 列 · \\x 十六进制参数", func() error {
			_, err := pool.Exec(ctx, `INSERT INTO diag_json (ts, device_id, "metrics") VALUES (NOW(), 3, $1)`, hexDoc)
			return err
		}},
		{"JSON 列 · CAST($1 AS JSON)", func() error {
			_, err := pool.Exec(ctx, `INSERT INTO diag_json (ts, device_id, "metrics") VALUES (NOW(), 4, CAST($1 AS JSON))`, doc)
			return err
		}},
		{"TEXT 列 · 普通字符串参数", func() error {
			_, err := pool.Exec(ctx, `INSERT INTO diag_text (ts, device_id, "metrics") VALUES (NOW(), 1, $1)`, doc)
			return err
		}},
	}

	for _, c := range cases {
		if err := c.run(); err != nil {
			t.Logf("✗ %-28s %v", c.name, err)
		} else {
			t.Logf("✓ %-28s 成功", c.name)
		}
	}

	t.Log("── 回读（json_get 对 JSON 列）")
	dump(t, ctx, pool, `SELECT device_id, json_get("metrics", 'temperature', 0.0) FROM diag_json ORDER BY device_id`)

	t.Log("── TEXT 列原始内容")
	dumpText(t, ctx, pool, `SELECT device_id, "metrics" FROM diag_text ORDER BY device_id`)

	// 关键交叉验证：确认「json_get 对 TEXT 返回空」不是「表里没数据」造成的。
	t.Log("── TEXT 列行数 / JSON 列行数")
	dumpCount(t, ctx, pool, `SELECT count(1) FROM diag_text`)
	dumpCount(t, ctx, pool, `SELECT count(1) FROM diag_json`)
	dumpCount(t, ctx, pool, `SELECT count(1) FROM diag_text WHERE json_get("metrics", 'temperature', 0.0) > 0`)

	t.Log("── 回读（json_get 对 TEXT 列）")
	dump(t, ctx, pool, `SELECT device_id, json_get("metrics", 'temperature', 0.0) FROM diag_text ORDER BY device_id`)
}

func dumpCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, q string) {
	t.Helper()

	var n int64
	if err := pool.QueryRow(ctx, q).Scan(&n); err != nil {
		t.Logf("  %s → 查询失败: %v", q, err)
		return
	}
	t.Logf("  %s → %d", q, n)
}

func dumpText(t *testing.T, ctx context.Context, pool *pgxpool.Pool, q string) {
	t.Helper()

	rows, err := pool.Query(ctx, q)
	if err != nil {
		t.Logf("  查询失败: %v", err)
		return
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var id int64
		var v *string
		if err := rows.Scan(&id, &v); err != nil {
			t.Logf("  扫描失败: %v", err)
			return
		}
		n++
		if v == nil {
			t.Logf("  device_id=%d metrics=<null>", id)
		} else {
			t.Logf("  device_id=%d metrics=%s", id, *v)
		}
	}
	t.Logf("  共 %d 行", n)
	if err := rows.Err(); err != nil {
		t.Logf("  迭代出错: %v", err)
	}
}

func dump(t *testing.T, ctx context.Context, pool *pgxpool.Pool, q string) {
	t.Helper()

	rows, err := pool.Query(ctx, q)
	if err != nil {
		t.Logf("  查询失败: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var v *float64
		if err := rows.Scan(&id, &v); err != nil {
			t.Logf("  扫描失败: %v", err)
			return
		}
		if v == nil {
			t.Logf("  device_id=%d temperature=<null>", id)
		} else {
			t.Logf("  device_id=%d temperature=%v", id, *v)
		}
	}
	// 必须检查 rows.Err()：pgx 的查询错误不一定从 Query() 返回，
	// 漏检会表现为「静默返回空结果集」——本次探测就踩过这个坑。
	if err := rows.Err(); err != nil {
		t.Logf("  迭代出错: %v", err)
	}
}
