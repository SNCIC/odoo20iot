package quota

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var telemetryTableName = regexp.MustCompile(`^telemetry(?:_[a-zA-Z0-9]+)?$`)

// GreptimeSnapshotSource 按租户行数占比估算遥测表的落盘容量。
// Greptime 当前提供表级 region 统计，不提供按 tenant 的字节统计，因此这是
// 目前可自动化得到的近似值；表级统计包含 disk_size，不把 memtable 重复计入。
type GreptimeSnapshotSource struct{ pool *pgxpool.Pool }

func NewGreptimeSnapshotSource(ctx context.Context, dsn string) (*GreptimeSnapshotSource, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("quota: GreptimeDB DSN 不能为空")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析 GreptimeDB DSN: %w", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建 GreptimeDB 连接池: %w", err)
	}
	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		pool.Close()
		return nil, fmt.Errorf("连接 GreptimeDB: %w", err)
	}
	return &GreptimeSnapshotSource{pool: pool}, nil
}

func (s *GreptimeSnapshotSource) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *GreptimeSnapshotSource) Snapshot(ctx context.Context, projectID int64) (int64, error) {
	if projectID <= 0 {
		return 0, fmt.Errorf("quota: project_id 必须为正数")
	}
	rows, err := s.pool.Query(ctx, `SELECT t.table_name, SUM(r.region_rows), SUM(r.disk_size)
FROM information_schema.region_statistics r
JOIN information_schema.tables t ON r.table_id=t.table_id
GROUP BY t.table_name`)
	if err != nil {
		return 0, fmt.Errorf("查询 GreptimeDB 表统计: %w", err)
	}
	defer rows.Close()
	var totalBytes int64
	for rows.Next() {
		var table string
		var totalRows, diskSize *int64
		if err := rows.Scan(&table, &totalRows, &diskSize); err != nil {
			return 0, fmt.Errorf("扫描 GreptimeDB 表统计: %w", err)
		}
		if !telemetryTableName.MatchString(table) || totalRows == nil || *totalRows <= 0 || diskSize == nil || *diskSize <= 0 {
			continue
		}
		projectRows, err := s.projectRows(ctx, table, projectID)
		if err != nil {
			return 0, err
		}
		if projectRows == 0 {
			continue
		}
		totalBytes += (*diskSize * projectRows) / *totalRows
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("读取 GreptimeDB 表统计: %w", err)
	}
	return totalBytes, nil
}

func (s *GreptimeSnapshotSource) projectRows(ctx context.Context, table string, projectID int64) (int64, error) {
	if !telemetryTableName.MatchString(table) {
		return 0, fmt.Errorf("quota: 非法 GreptimeDB 表名 %q", table)
	}
	var count int64
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE project_id=$1`, pgx.Identifier{table}.Sanitize())
	if err := s.pool.QueryRow(ctx, query, projectID).Scan(&count); err != nil {
		return 0, fmt.Errorf("统计 GreptimeDB 表 %s 的租户行数: %w", table, err)
	}
	return count, nil
}

type CombinedSnapshotSource struct {
	pg   *PGSnapshotSource
	tsdb *GreptimeSnapshotSource
}

func NewCombinedSnapshotSource(pgSource *PGSnapshotSource, tsdbSource *GreptimeSnapshotSource) *CombinedSnapshotSource {
	return &CombinedSnapshotSource{pg: pgSource, tsdb: tsdbSource}
}

func (s *CombinedSnapshotSource) Snapshot(ctx context.Context, projectID int64) (map[string]int64, error) {
	values, err := s.pg.Snapshot(ctx, projectID)
	if err != nil {
		return nil, err
	}
	if s.tsdb != nil {
		bytes, err := s.tsdb.Snapshot(ctx, projectID)
		if err != nil {
			return nil, err
		}
		values["storage_bytes"] += bytes
	}
	return values, nil
}
