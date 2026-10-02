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
	var projectID, deviceID, typeID, version int64
	var revoked bool
	err := d.pool.QueryRow(ctx, `SELECT d.project_id, d.id, d.device_type_id, d.device_key, d.auth_mode, d.status, d.secret_hash, d.secret_version, (d.status='disabled'), COALESCE(t.is_gateway, false)
FROM t_device AS d
LEFT JOIN t_device_type AS t ON t.project_id=d.project_id AND t.id=d.device_type_id
WHERE d.device_key=$1 AND d.deleted_at IS NULL`, clientID).
		Scan(&projectID, &deviceID, &typeID, &id.DeviceKey, &mode, &status, &hash, &version, &revoked, &id.IsGateway)
	if err != nil {
		return nil, err
	}
	id.ProjectID, id.DeviceID, id.DeviceTypeID, id.CredentialVersion = projectID, deviceID, typeID, version
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
