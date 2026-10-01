package pg

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationsDir = "migrations"

// migrationLockID 是 pg_advisory_lock 的键（任意常量，只要全库唯一）。
const migrationLockID int64 = 0x10AD_0D0C_2026

const schemaMigrationsDDL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT        PRIMARY KEY,
    checksum   TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Migration 是一个已加载的迁移。
type Migration struct {
	// Version 是文件名去掉 .sql（按字典序排序，故必须零填充：0001、0002…）。
	Version string
	// Checksum 是内容 sha256，用于发现「旧迁移被改过」。
	Checksum string
	SQL      string
}

// LoadMigrations 按版本升序加载内嵌迁移。
func LoadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("pg: 读取内嵌迁移目录: %w", err)
	}
	out := make([]Migration, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		b, err := fs.ReadFile(migrationsFS, path.Join(migrationsDir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("pg: 读取迁移 %s: %w", e.Name(), err)
		}
		sum := sha256.Sum256(b)
		out = append(out, Migration{
			Version:  strings.TrimSuffix(e.Name(), ".sql"),
			Checksum: hex.EncodeToString(sum[:]),
			SQL:      string(b),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Migrate 应用尚未执行的迁移，返回本次实际应用的版本号。
//
// 三条规矩，每条都对应一种真实故障：
//  1. **每个迁移单独一个事务**：整批一个事务时，第 5 个失败会把前 4 个一起回滚，
//     而大表 DDL 可能已经跑了几分钟 —— 白等，还得重来。
//  2. **已应用的迁移必须校验和一致**：改了旧迁移文件而没人发现，
//     是「本地好好的、线上炸了」最经典的成因，必须在启动时立刻报错。
//  3. **用 pg_advisory_lock 串行化**：多副本同时启动会并发建表。
//
// 多语句迁移走 simple 协议（`PgConn().Exec`）：扩展协议一次只允许一条命令，
// 用默认路径会得到「cannot insert multiple commands into a prepared statement」。
// 它在同一连接的已开启事务里执行，故仍然受事务保护。
func Migrate(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	migs, err := LoadMigrations()
	if err != nil {
		return nil, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("pg: 取连接: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return nil, fmt.Errorf("pg: 取迁移锁: %w", err)
	}
	defer func() {
		// 会话结束会自动释放；显式解锁是为了尽早放掉，且用 WithoutCancel
		// 保证 ctx 被取消时仍然解锁。
		_, _ = conn.Exec(context.WithoutCancel(ctx),
			"SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	if _, err := conn.Exec(ctx, schemaMigrationsDDL); err != nil {
		return nil, fmt.Errorf("pg: 建版本表: %w", err)
	}

	applied, err := loadApplied(ctx, conn.Conn())
	if err != nil {
		return nil, err
	}

	var done []string
	for _, m := range migs {
		if prev, ok := applied[m.Version]; ok {
			if prev != m.Checksum {
				return done, fmt.Errorf(
					"pg: 迁移 %s 的校验和已变（库中 %s，当前文件 %s）："+
						"已应用的迁移不可修改，请新增一个迁移",
					m.Version, short(prev), short(m.Checksum))
			}
			continue
		}
		if err := applyOne(ctx, conn.Conn(), m); err != nil {
			return done, err
		}
		done = append(done, m.Version)
	}
	return done, nil
}

func applyOne(ctx context.Context, conn *pgx.Conn, m Migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg: 迁移 %s 开启事务: %w", m.Version, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// simple 协议：允许一个字符串里有多条语句。
	results, err := tx.Conn().PgConn().Exec(ctx, m.SQL).ReadAll()
	if err != nil {
		return fmt.Errorf("pg: 迁移 %s 执行失败: %w", m.Version, err)
	}
	for _, r := range results {
		if r.Err != nil {
			return fmt.Errorf("pg: 迁移 %s 执行失败: %w", m.Version, r.Err)
		}
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)`,
		m.Version, m.Checksum); err != nil {
		return fmt.Errorf("pg: 迁移 %s 记录版本: %w", m.Version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg: 迁移 %s 提交: %w", m.Version, err)
	}
	return nil
}

func loadApplied(ctx context.Context, conn *pgx.Conn) (map[string]string, error) {
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("pg: 读取已应用迁移: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var v, c string
		if err := rows.Scan(&v, &c); err != nil {
			return nil, fmt.Errorf("pg: 扫描迁移行: %w", err)
		}
		out[v] = c
	}
	// 只看 rows.Err() 漏掉它，会把「查询中途失败」当成「表里就这些」。
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg: 遍历迁移行: %w", err)
	}
	return out, nil
}

// short 截断摘要，便于在错误信息里比对（完整值太长，人眼反而看不出差别）。
func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// AppliedVersions 返回库中已应用的迁移 → 校验和，供 `-check` 这类只读路径使用。
//
// 单独暴露它而不是让调用方自己查表：版本表的表名与列名是本包的知识，
// 让外部拼 SQL 等于把 schema 细节漏出去。
func AppliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[string]string, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("pg: 取连接: %w", err)
	}
	defer conn.Release()

	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return nil, fmt.Errorf("pg: 检查版本表: %w", err)
	}
	if !exists {
		// 一个迁移都没跑过：不是错误，只是空集合。
		return map[string]string{}, nil
	}
	return loadApplied(ctx, conn.Conn())
}
