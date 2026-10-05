package notifyconfig

import (
	"context"
	"fmt"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/secureconfig"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type Endpoint struct {
	ID, ProjectID         int64
	Name, Channel, Target string
	Enabled               bool
	CreatedAt, UpdatedAt  time.Time
}
type Store struct {
	pool *pgxpool.Pool
	key  []byte
}

func New(pool *pgxpool.Pool, key []byte) (*Store, error) {
	if pool == nil || len(key) != 32 {
		return nil, fmt.Errorf("通知配置需要 PG 和 32 字节密钥")
	}
	return &Store{pool: pool, key: key}, nil
}
func (s *Store) List(ctx context.Context, projectID int64) ([]Endpoint, error) {
	var out []Endpoint
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id,project_id,name,channel,enabled,created_at,updated_at FROM t_notification_endpoint WHERE project_id=$1 AND enabled ORDER BY id`, projectID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e Endpoint
			if err := rows.Scan(&e.ID, &e.ProjectID, &e.Name, &e.Channel, &e.Enabled, &e.CreatedAt, &e.UpdatedAt); err != nil {
				return err
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	return out, err
}
func (s *Store) Create(ctx context.Context, projectID int64, name, channel, target string) (Endpoint, error) {
	if name == "" || target == "" {
		return Endpoint{}, fmt.Errorf("通知端点名称和目标不能为空")
	}
	if channel != "webhook" && channel != "email" && channel != "sms" && channel != "voice" {
		return Endpoint{}, fmt.Errorf("通知通道非法")
	}
	ct, nonce, err := secureconfig.Encrypt(s.key, target)
	if err != nil {
		return Endpoint{}, err
	}
	var e Endpoint
	err = pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO t_notification_endpoint(project_id,name,channel,target_ciphertext,target_nonce) VALUES($1,$2,$3,$4,$5) RETURNING id,project_id,name,channel,enabled,created_at,updated_at`, projectID, name, channel, ct, nonce).Scan(&e.ID, &e.ProjectID, &e.Name, &e.Channel, &e.Enabled, &e.CreatedAt, &e.UpdatedAt)
	})
	return e, err
}
func (s *Store) Delete(ctx context.Context, projectID, id int64) error {
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE t_notification_endpoint SET enabled=false,updated_at=now() WHERE project_id=$1 AND id=$2`, projectID, id)
		return err
	})
}
func (s *Store) Targets(ctx context.Context, projectID int64, channel string) ([]string, error) {
	var out []string
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT target_ciphertext,target_nonce FROM t_notification_endpoint WHERE project_id=$1 AND channel=$2 AND enabled`, projectID, channel)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var ct, nonce []byte
			if err := rows.Scan(&ct, &nonce); err != nil {
				return err
			}
			v, err := secureconfig.Decrypt(s.key, ct, nonce)
			if err != nil {
				return err
			}
			out = append(out, v)
		}
		return rows.Err()
	})
	return out, err
}
