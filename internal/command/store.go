package command

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Record struct {
	ProjectID     int64           `json:"project_id"`
	CommandID     string          `json:"command_id"`
	DeviceKey     string          `json:"device_key"`
	CommandKey    string          `json:"command_key"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload"`
	Status        string          `json:"status"`
	Attempts      int             `json:"attempts"`
	LastError     string          `json:"last_error,omitempty"`
	IssuedAt      time.Time       `json:"issued_at"`
	AckedAt       *time.Time      `json:"acked_at,omitempty"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("command: PG 连接池不能为空")
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Create(ctx context.Context, record Record) (Record, error) {
	if record.ProjectID <= 0 || record.DeviceKey == "" || record.CommandKey == "" {
		return Record{}, fmt.Errorf("command: 账本字段不完整")
	}
	if len(record.Payload) == 0 {
		record.Payload = json.RawMessage(`{}`)
	}
	if record.CommandID == "" || record.CorrelationID == "" {
		id, err := newID()
		if err != nil {
			return Record{}, err
		}
		if record.CommandID == "" {
			record.CommandID = id
		}
		if record.CorrelationID == "" {
			record.CorrelationID = record.CommandID
		}
	}
	err := pg.WithProjectTx(ctx, s.pool, record.ProjectID, func(ctx context.Context, tx pgx.Tx) error {
		var thingModel []byte
		if err := tx.QueryRow(ctx, `SELECT t.thing_model FROM t_device d JOIN t_device_type t ON t.id=d.device_type_id AND t.project_id=d.project_id WHERE d.project_id=$1 AND d.device_key=$2 AND d.deleted_at IS NULL AND t.deleted_at IS NULL`, record.ProjectID, record.DeviceKey).Scan(&thingModel); err != nil {
			return fmt.Errorf("command: 设备不存在或不属于当前租户: %w", err)
		}
		var model struct {
			Commands []struct {
				Key string `json:"key"`
			} `json:"commands"`
		}
		if err := json.Unmarshal(thingModel, &model); err != nil {
			return fmt.Errorf("command: 设备物模型解析失败: %w", err)
		}
		allowed := false
		for _, command := range model.Commands {
			if command.Key == record.CommandKey {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("command: 指令 %q 不在设备物模型白名单中", record.CommandKey)
		}
		return tx.QueryRow(ctx, `
INSERT INTO t_command(project_id,command_id,device_key,command_key,correlation_id,payload,status,attempts)
VALUES($1,$2,$3,$4,$5,$6::jsonb,'pending',1)
ON CONFLICT(project_id,correlation_id) DO UPDATE SET updated_at=now()
RETURNING project_id,command_id,device_key,command_key,correlation_id,payload,status,attempts,last_error,issued_at,acked_at,updated_at`,
			record.ProjectID, record.CommandID, record.DeviceKey, record.CommandKey, record.CorrelationID, string(record.Payload)).
			Scan(&record.ProjectID, &record.CommandID, &record.DeviceKey, &record.CommandKey, &record.CorrelationID, &record.Payload, &record.Status, &record.Attempts, &record.LastError, &record.IssuedAt, &record.AckedAt, &record.UpdatedAt)
	})
	return record, err
}

type Service struct {
	Store  *Store
	Sender Sender
}

func (s *Service) Get(ctx context.Context, projectID int64, commandID string) (Record, error) {
	return s.Store.Get(ctx, projectID, commandID)
}

func (s *Service) Issue(ctx context.Context, record Record) (Record, error) {
	if s == nil || s.Store == nil || s.Sender.Router == nil {
		return Record{}, fmt.Errorf("command: 下发服务未完整配置")
	}
	created, err := s.Store.Create(ctx, record)
	if err != nil {
		return Record{}, err
	}
	if err := s.Sender.Send(ctx, Request{ProjectID: created.ProjectID, DeviceKey: created.DeviceKey, CommandKey: created.CommandKey, Payload: created.Payload, CorrelationID: created.CorrelationID}); err != nil {
		_ = s.markFailed(ctx, created.ProjectID, created.CommandID, err.Error())
		return created, err
	}
	return created, nil
}

func (s *Service) markFailed(ctx context.Context, projectID int64, commandID, reason string) error {
	return pg.WithProjectTx(ctx, s.Store.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE t_command SET status='failed',last_error=$1,updated_at=now() WHERE project_id=$2 AND command_id=$3`, reason, projectID, commandID)
		return err
	})
}

func (s *Store) Get(ctx context.Context, projectID int64, commandID string) (Record, error) {
	var record Record
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT project_id,command_id,device_key,command_key,correlation_id,payload,status,attempts,last_error,issued_at,acked_at,updated_at FROM t_command WHERE project_id=$1 AND command_id=$2`, projectID, commandID).
			Scan(&record.ProjectID, &record.CommandID, &record.DeviceKey, &record.CommandKey, &record.CorrelationID, &record.Payload, &record.Status, &record.Attempts, &record.LastError, &record.IssuedAt, &record.AckedAt, &record.UpdatedAt)
	})
	return record, err
}

func (s *Store) ApplyReply(ctx context.Context, projectID int64, reply Reply) error {
	if projectID <= 0 {
		return fmt.Errorf("command: project_id 非法")
	}
	status := strings.ToLower(strings.TrimSpace(reply.Status))
	switch status {
	case "acked", "ok", "success":
		status = "acked"
	case "failed", "error", "rejected":
		status = "failed"
	default:
		return fmt.Errorf("command: 不支持的回执状态 %q", reply.Status)
	}
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		if reply.CommandID == "" && reply.CorrelationID == "" {
			return fmt.Errorf("command: 回执缺少关联 ID")
		}
		var updated int64
		err := tx.QueryRow(ctx, `WITH updated AS (
			UPDATE t_command SET status=$1,last_error=$2,acked_at=CASE WHEN $1='acked' THEN now() ELSE acked_at END,updated_at=now()
			WHERE project_id=$3 AND ($4='' OR correlation_id=$4) AND ($5='' OR command_id=$5) AND ($6='' OR device_key=$6)
			RETURNING 1
		) SELECT count(*) FROM updated`, status, reply.Error, projectID, reply.CorrelationID, reply.CommandID, reply.DeviceKey).Scan(&updated)
		if err != nil {
			return err
		}
		if updated == 0 {
			return fmt.Errorf("command: 未找到匹配的命令回执")
		}
		return nil
	})
}
