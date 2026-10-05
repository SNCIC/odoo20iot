package quota

import (
	"context"
	"fmt"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SnapshotSource 提供租户当前快照型计量值。快照值由聚合器按窗口取最大值，
// 不应当像消息数一样累加。
type SnapshotSource interface {
	Snapshot(context.Context, int64) (map[string]int64, error)
}

// PGSnapshotSource 从控制面数据库采集设备数和已登记固件占用量。
// telemetry/对象存储容量需要对应存储服务提供精确统计后再扩展此接口。
type PGSnapshotSource struct{ pool *pgxpool.Pool }

func NewPGSnapshotSource(pool *pgxpool.Pool) (*PGSnapshotSource, error) {
	if pool == nil {
		return nil, fmt.Errorf("quota: snapshot source 需要连接池")
	}
	return &PGSnapshotSource{pool: pool}, nil
}

func (s *PGSnapshotSource) Snapshot(ctx context.Context, projectID int64) (map[string]int64, error) {
	if projectID <= 0 {
		return nil, fmt.Errorf("quota: project_id 必须为正数")
	}
	values := make(map[string]int64, 2)
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var devices, storage int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM t_device WHERE project_id=$1 AND deleted_at IS NULL AND status='active'`, projectID).Scan(&devices); err != nil {
			return fmt.Errorf("统计设备数: %w", err)
		}
		if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(size_bytes),0) FROM t_ota_firmware WHERE project_id=$1`, projectID).Scan(&storage); err != nil {
			return fmt.Errorf("统计固件存储量: %w", err)
		}
		values["device_count"] = devices
		values["storage_bytes"] = storage
		return nil
	})
	return values, err
}
