package shadow

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGStore struct{ pool *pgxpool.Pool }

func NewPGStore(pool *pgxpool.Pool) (*PGStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("shadow: PG 连接池不能为空")
	}
	return &PGStore{pool: pool}, nil
}

func (s *PGStore) Get(ctx context.Context, projectID int64, deviceKey string) (Snapshot, error) {
	var snapshot Snapshot
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		if err := ensureDeviceShadow(ctx, tx, projectID, deviceKey); err != nil {
			return err
		}
		var desired, reported []byte
		if err := tx.QueryRow(ctx, `SELECT device_key, desired, reported, version, desired_updated_at, reported_updated_at, updated_at FROM t_device_shadow WHERE project_id=$1 AND device_key=$2`, projectID, deviceKey).Scan(&snapshot.DeviceKey, &desired, &reported, &snapshot.Version, &snapshot.DesiredUpdatedAt, &snapshot.ReportedUpdatedAt, &snapshot.UpdatedAt); err != nil {
			if err == pgx.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		snapshot.ProjectID = projectID
		if err := json.Unmarshal(desired, &snapshot.Desired); err != nil {
			return err
		}
		if err := json.Unmarshal(reported, &snapshot.Reported); err != nil {
			return err
		}
		snapshot.Delta = Delta(snapshot.Desired, snapshot.Reported)
		return nil
	})
	return snapshot, err
}

func (s *PGStore) UpdateDesired(ctx context.Context, projectID int64, deviceKey string, patch map[string]any, expectedVersion *int64) (Snapshot, error) {
	if len(patch) == 0 {
		return s.Get(ctx, projectID, deviceKey)
	}
	patchData, err := json.Marshal(patch)
	if err != nil {
		return Snapshot{}, err
	}
	if len(patchData) > MaxStateBytes {
		return Snapshot{}, fmt.Errorf("shadow: desired patch 超过 %d 字节", MaxStateBytes)
	}
	var snapshot Snapshot
	err = pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var desired, reported []byte
		var currentVersion int64
		err := tx.QueryRow(ctx, `SELECT desired, reported, version FROM t_device_shadow WHERE project_id=$1 AND device_key=$2 FOR UPDATE`, projectID, deviceKey).Scan(&desired, &reported, &currentVersion)
		switch err {
		case nil:
			if expectedVersion != nil && *expectedVersion != currentVersion {
				return ErrVersionConflict
			}
			var currentDesired map[string]any
			if err := json.Unmarshal(desired, &currentDesired); err != nil {
				return err
			}
			merged := Merge(currentDesired, patch)
			mergedData, err := json.Marshal(merged)
			if err != nil {
				return err
			}
			if stateSize(mergedData, reported) > MaxStateBytes {
				return fmt.Errorf("shadow: desired/reported 总大小超过 %d 字节", MaxStateBytes)
			}
			if _, err := tx.Exec(ctx, `UPDATE t_device_shadow SET desired=$1::jsonb, version=version+1, desired_updated_at=now(), updated_at=now() WHERE project_id=$2 AND device_key=$3`, string(mergedData), projectID, deviceKey); err != nil {
				return err
			}
		case pgx.ErrNoRows:
			if expectedVersion != nil && *expectedVersion != 0 {
				return ErrVersionConflict
			}
			if err := ensureDeviceExists(ctx, tx, projectID, deviceKey); err != nil {
				return err
			}
			if stateSize(patchData, []byte(`{}`)) > MaxStateBytes {
				return fmt.Errorf("shadow: desired/reported 总大小超过 %d 字节", MaxStateBytes)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO t_device_shadow(project_id,device_key,desired,version,desired_updated_at,updated_at) VALUES($1,$2,$3::jsonb,1,now(),now())`, projectID, deviceKey, string(patchData)); err != nil {
				return err
			}
		default:
			return err
		}
		var desiredJSON, reportedJSON []byte
		if err := tx.QueryRow(ctx, `SELECT device_key, desired, reported, version, desired_updated_at, reported_updated_at, updated_at FROM t_device_shadow WHERE project_id=$1 AND device_key=$2`, projectID, deviceKey).Scan(&snapshot.DeviceKey, &desiredJSON, &reportedJSON, &snapshot.Version, &snapshot.DesiredUpdatedAt, &snapshot.ReportedUpdatedAt, &snapshot.UpdatedAt); err != nil {
			return err
		}
		snapshot.ProjectID = projectID
		if err := json.Unmarshal(desiredJSON, &snapshot.Desired); err != nil {
			return err
		}
		if err := json.Unmarshal(reportedJSON, &snapshot.Reported); err != nil {
			return err
		}
		snapshot.Delta = Delta(snapshot.Desired, snapshot.Reported)
		return nil
	})
	return snapshot, err
}

func (s *PGStore) ApplyReported(ctx context.Context, projectID int64, deviceKey string, patch map[string]any) (Snapshot, error) {
	data, err := json.Marshal(patch)
	if err != nil {
		return Snapshot{}, err
	}
	if len(data) > MaxStateBytes {
		return Snapshot{}, fmt.Errorf("shadow: reported patch 超过 %d 字节", MaxStateBytes)
	}
	var snapshot Snapshot
	err = pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var currentDesired, currentReported []byte
		err := tx.QueryRow(ctx, `SELECT desired, reported FROM t_device_shadow WHERE project_id=$1 AND device_key=$2 FOR UPDATE`, projectID, deviceKey).Scan(&currentDesired, &currentReported)
		if err == pgx.ErrNoRows {
			if err := ensureDeviceExists(ctx, tx, projectID, deviceKey); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO t_device_shadow(project_id,device_key,reported,version,reported_updated_at,updated_at) VALUES($1,$2,$3::jsonb,1,now(),now())`, projectID, deviceKey, string(data)); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			var current map[string]any
			if err := json.Unmarshal(currentReported, &current); err != nil {
				return err
			}
			mergedData, err := json.Marshal(Merge(current, patch))
			if err != nil {
				return err
			}
			if stateSize(mergedData, currentDesired) > MaxStateBytes {
				return fmt.Errorf("shadow: desired/reported 总大小超过 %d 字节", MaxStateBytes)
			}
			if _, err := tx.Exec(ctx, `UPDATE t_device_shadow SET reported=$1::jsonb,version=version+1,reported_updated_at=now(),updated_at=now() WHERE project_id=$2 AND device_key=$3`, string(mergedData), projectID, deviceKey); err != nil {
				return err
			}
		}
		var desired, reported []byte
		if err := tx.QueryRow(ctx, `SELECT device_key, desired, reported, version, desired_updated_at, reported_updated_at, updated_at FROM t_device_shadow WHERE project_id=$1 AND device_key=$2`, projectID, deviceKey).Scan(&snapshot.DeviceKey, &desired, &reported, &snapshot.Version, &snapshot.DesiredUpdatedAt, &snapshot.ReportedUpdatedAt, &snapshot.UpdatedAt); err != nil {
			return err
		}
		snapshot.ProjectID = projectID
		if err := json.Unmarshal(desired, &snapshot.Desired); err != nil {
			return err
		}
		if err := json.Unmarshal(reported, &snapshot.Reported); err != nil {
			return err
		}
		snapshot.Delta = Delta(snapshot.Desired, snapshot.Reported)
		return nil
	})
	return snapshot, err
}

func ensureDeviceShadow(ctx context.Context, tx pgx.Tx, projectID int64, deviceKey string) error {
	if err := ensureDeviceExists(ctx, tx, projectID, deviceKey); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO t_device_shadow(project_id,device_key) VALUES($1,$2) ON CONFLICT(project_id,device_key) DO NOTHING`, projectID, deviceKey)
	return err
}

func ensureDeviceExists(ctx context.Context, tx pgx.Tx, projectID int64, deviceKey string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM t_device WHERE project_id=$1 AND device_key=$2 AND deleted_at IS NULL)`, projectID, deviceKey).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}
	return nil
}

func stateSize(desired, reported []byte) int {
	return len(desired) + len(reported)
}

var _ Store = (*PGStore)(nil)
