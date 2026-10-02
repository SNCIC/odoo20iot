package auth

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PGDirectory 从 t_device 读取设备凭据摘要。只读取认证所需字段，绝不返回明文。
type PGDirectory struct{ pool *pgxpool.Pool }

func NewPGDirectory(pool *pgxpool.Pool) (*PGDirectory, error) {
	if pool == nil {
		return nil, fmt.Errorf("auth: PGDirectory 需要连接池")
	}
	return &PGDirectory{pool: pool}, nil
}

func (d *PGDirectory) Lookup(ctx context.Context, clientID string) (*Identity, error) {
	var id Identity
	var mode, status, hash string
	var projectID, typeID, version int64
	var revoked bool
	err := d.pool.QueryRow(ctx, `SELECT project_id, device_type_id, device_key, auth_mode, status, secret_hash, secret_version, (status='disabled') FROM t_device WHERE device_key=$1 AND deleted_at IS NULL`, clientID).
		Scan(&projectID, &typeID, &id.DeviceKey, &mode, &status, &hash, &version, &revoked)
	if err != nil {
		return nil, err
	}
	id.ProjectID, id.DeviceTypeID, id.CredentialVersion = projectID, typeID, version
	id.Mode = Mode(mode)
	id.Revoked = revoked || status != "active"
	if hash != "" {
		digest, err := ParseEncodedDigest(hash)
		if err != nil {
			return nil, fmt.Errorf("解析设备 %s 凭据: %w", clientID, err)
		}
		id.Secret = digest
	}
	return &id, nil
}
