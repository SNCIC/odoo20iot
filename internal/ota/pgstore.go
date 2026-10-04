package ota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrFirmwareNotFound = errors.New("ota: firmware not found")
	ErrTaskNotFound     = errors.New("ota: task not found")
	ErrDeviceNotFound   = errors.New("ota: target device not found in project")
)

type Task struct {
	ID         string        `json:"id"`
	ProjectID  int64         `json:"project_id"`
	FirmwareID int64         `json:"firmware_id"`
	Status     TaskStatus    `json:"status"`
	BatchIndex int           `json:"batch_index"`
	Rollout    Rollout       `json:"rollout"`
	OfflineTTL time.Duration `json:"offline_ttl"`
	CreatedBy  string        `json:"created_by"`
	CreatedAt  time.Time     `json:"created_at"`
	StartedAt  *time.Time    `json:"started_at,omitempty"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
}

type TaskDevice struct {
	TaskID          string       `json:"task_id"`
	ProjectID       int64        `json:"project_id"`
	DeviceKey       string       `json:"device_key"`
	Status          DeviceStatus `json:"status"`
	Progress        int          `json:"progress"`
	BytesDownloaded int64        `json:"bytes_downloaded"`
	ErrorCode       string       `json:"error_code,omitempty"`
	ErrorMessage    string       `json:"error_message,omitempty"`
	FirmwareVersion string       `json:"firmware_version,omitempty"`
	UpdatedAt       time.Time    `json:"updated_at"`
}

type PGStore struct{ pool *pgxpool.Pool }

func NewPGStore(pool *pgxpool.Pool) (*PGStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("ota: PG 连接池不能为空")
	}
	return &PGStore{pool: pool}, nil
}

func (s *PGStore) RegisterFirmware(ctx context.Context, firmware Firmware, createdBy string) (Firmware, error) {
	if firmware.ProjectID <= 0 || strings.TrimSpace(firmware.Version) == "" || firmware.SizeBytes <= 0 || len(firmware.SHA256) != 64 || strings.TrimSpace(firmware.Signature) == "" || strings.TrimSpace(firmware.ObjectKey) == "" {
		return Firmware{}, fmt.Errorf("ota: 固件元数据不完整")
	}
	metadata, err := json.Marshal(firmware.Metadata)
	if err != nil {
		return Firmware{}, fmt.Errorf("ota: 编码固件 metadata: %w", err)
	}
	err = pg.WithProjectTx(ctx, s.pool, firmware.ProjectID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
INSERT INTO t_ota_firmware(project_id, version, filename, object_key, size_bytes, sha256, signature, signing_key_id, metadata, created_by)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
RETURNING id, created_at`, firmware.ProjectID, firmware.Version, firmware.Filename, firmware.ObjectKey, firmware.SizeBytes, firmware.SHA256, firmware.Signature, firmware.SigningKeyID, string(metadata), createdBy).Scan(&firmware.ID, &firmware.CreatedAt)
	})
	return firmware, err
}

func (s *PGStore) ListFirmwares(ctx context.Context, projectID int64) ([]Firmware, error) {
	var firmwares []Firmware
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id,project_id,version,filename,object_key,size_bytes,sha256,signature,signing_key_id,metadata,created_at FROM t_ota_firmware WHERE project_id=$1 ORDER BY created_at DESC,id DESC`, projectID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var firmware Firmware
			var metadata []byte
			if err := rows.Scan(&firmware.ID, &firmware.ProjectID, &firmware.Version, &firmware.Filename, &firmware.ObjectKey, &firmware.SizeBytes, &firmware.SHA256, &firmware.Signature, &firmware.SigningKeyID, &metadata, &firmware.CreatedAt); err != nil {
				return err
			}
			if err := json.Unmarshal(metadata, &firmware.Metadata); err != nil {
				return err
			}
			firmwares = append(firmwares, firmware)
		}
		return rows.Err()
	})
	return firmwares, err
}

func (s *PGStore) GetFirmware(ctx context.Context, projectID, firmwareID int64) (Firmware, error) {
	var firmware Firmware
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var metadata []byte
		err := tx.QueryRow(ctx, `SELECT id,project_id,version,filename,object_key,size_bytes,sha256,signature,signing_key_id,metadata,created_at FROM t_ota_firmware WHERE project_id=$1 AND id=$2`, projectID, firmwareID).Scan(&firmware.ID, &firmware.ProjectID, &firmware.Version, &firmware.Filename, &firmware.ObjectKey, &firmware.SizeBytes, &firmware.SHA256, &firmware.Signature, &firmware.SigningKeyID, &metadata, &firmware.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrFirmwareNotFound
		}
		if err != nil {
			return err
		}
		return json.Unmarshal(metadata, &firmware.Metadata)
	})
	return firmware, err
}

func (s *PGStore) CreateTask(ctx context.Context, projectID, firmwareID int64, deviceKeys []string, rollout Rollout, offlineTTL time.Duration, createdBy string) (Task, error) {
	if projectID <= 0 || firmwareID <= 0 || len(deviceKeys) == 0 {
		return Task{}, fmt.Errorf("ota: project_id、firmware_id 和目标设备必填")
	}
	if err := rollout.Validate(); err != nil {
		return Task{}, err
	}
	if offlineTTL <= 0 || offlineTTL > 7*24*time.Hour {
		return Task{}, fmt.Errorf("ota: offline_ttl 必须在 0..7d")
	}
	deviceKeys, err := normalizeDeviceKeys(deviceKeys)
	if err != nil {
		return Task{}, err
	}
	rolloutJSON, err := json.Marshal(rollout)
	if err != nil {
		return Task{}, err
	}
	taskID, err := newTaskID()
	if err != nil {
		return Task{}, err
	}
	task := Task{ID: taskID, ProjectID: projectID, FirmwareID: firmwareID, Status: TaskDraft, Rollout: rollout, OfflineTTL: offlineTTL, CreatedBy: createdBy}
	err = pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM t_ota_firmware WHERE project_id=$1 AND id=$2)`, projectID, firmwareID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrFirmwareNotFound
		}
		if err := tx.QueryRow(ctx, `INSERT INTO t_ota_task(id,project_id,firmware_id,rollout,offline_ttl,created_by) VALUES($1,$2,$3,$4::jsonb,$5,$6) RETURNING created_at`, taskID, projectID, firmwareID, string(rolloutJSON), offlineTTL, createdBy).Scan(&task.CreatedAt); err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `INSERT INTO t_ota_task_device(task_id,project_id,device_key) SELECT $1,$2,d.device_key FROM t_device d WHERE d.project_id=$2 AND d.device_key=ANY($3::text[]) AND d.status='active' AND d.deleted_at IS NULL`, taskID, projectID, deviceKeys)
		if err != nil {
			return err
		}
		if result.RowsAffected() != int64(len(deviceKeys)) {
			return ErrDeviceNotFound
		}
		return nil
	})
	return task, err
}

func (s *PGStore) GetTask(ctx context.Context, projectID int64, taskID string) (Task, error) {
	var task Task
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var rolloutJSON []byte
		err := tx.QueryRow(ctx, `SELECT id::text,project_id,firmware_id,status,rollout,extract(epoch from offline_ttl)::bigint,created_by,created_at,started_at,finished_at,rollout_batch_index FROM t_ota_task WHERE project_id=$1 AND id=$2`, projectID, taskID).Scan(&task.ID, &task.ProjectID, &task.FirmwareID, &task.Status, &rolloutJSON, &task.OfflineTTL, &task.CreatedBy, &task.CreatedAt, &task.StartedAt, &task.FinishedAt, &task.BatchIndex)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		if err != nil {
			return err
		}
		task.OfflineTTL *= time.Second
		return json.Unmarshal(rolloutJSON, &task.Rollout)
	})
	return task, err
}

func (s *PGStore) ClaimRunningTasks(ctx context.Context, projectID int64, limit int, lease time.Duration) ([]Task, error) {
	if projectID <= 0 || limit <= 0 || lease <= 0 {
		return nil, fmt.Errorf("ota: claim 参数非法")
	}
	var tasks []Task
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `WITH picked AS (
			SELECT id FROM t_ota_task WHERE project_id=$1 AND status='running' AND (dispatch_lease_until IS NULL OR dispatch_lease_until < now())
			ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $2
		) UPDATE t_ota_task t SET dispatch_lease_until=now()+$3::interval FROM picked p
		WHERE t.project_id=$1 AND t.id=p.id
		RETURNING t.id::text,t.project_id,t.firmware_id,t.status,t.rollout,extract(epoch from t.offline_ttl)::bigint,t.created_by,t.created_at,t.started_at,t.finished_at,t.rollout_batch_index`, projectID, limit, fmt.Sprintf("%d seconds", int64(lease/time.Second)))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var task Task
			var rolloutJSON []byte
			if err := rows.Scan(&task.ID, &task.ProjectID, &task.FirmwareID, &task.Status, &rolloutJSON, &task.OfflineTTL, &task.CreatedBy, &task.CreatedAt, &task.StartedAt, &task.FinishedAt, &task.BatchIndex); err != nil {
				return err
			}
			task.OfflineTTL *= time.Second
			if err := json.Unmarshal(rolloutJSON, &task.Rollout); err != nil {
				return err
			}
			tasks = append(tasks, task)
		}
		return rows.Err()
	})
	return tasks, err
}

func (s *PGStore) SetBatchState(ctx context.Context, projectID int64, taskID string, expectedIndex, nextIndex int, status TaskStatus) error {
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var result pgconn.CommandTag
		var err error
		if status == TaskPaused || status == TaskCompleted {
			result, err = tx.Exec(ctx, `UPDATE t_ota_task SET status=$1,rollout_batch_index=$2,dispatch_lease_until=NULL,finished_at=now() WHERE project_id=$3 AND id=$4 AND status='running' AND rollout_batch_index=$5`, status, nextIndex, projectID, taskID, expectedIndex)
		} else {
			result, err = tx.Exec(ctx, `UPDATE t_ota_task SET rollout_batch_index=$1,dispatch_lease_until=NULL WHERE project_id=$2 AND id=$3 AND status='running' AND rollout_batch_index=$4`, nextIndex, projectID, taskID, expectedIndex)
		}
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			return fmt.Errorf("ota: 灰度批次状态已被并发修改")
		}
		return nil
	})
}

func (s *PGStore) ReleaseDispatchLease(ctx context.Context, projectID int64, taskID string) error {
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE t_ota_task SET dispatch_lease_until=NULL WHERE project_id=$1 AND id=$2 AND status='running'`, projectID, taskID)
		return err
	})
}

func (s *PGStore) StartTask(ctx context.Context, projectID int64, taskID string) (Task, error) {
	var task Task
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var rolloutJSON []byte
		err := tx.QueryRow(ctx, `SELECT id::text,project_id,firmware_id,status,rollout,extract(epoch from offline_ttl)::bigint,created_by,created_at,started_at,finished_at FROM t_ota_task WHERE project_id=$1 AND id=$2 FOR UPDATE`, projectID, taskID).Scan(&task.ID, &task.ProjectID, &task.FirmwareID, &task.Status, &rolloutJSON, &task.OfflineTTL, &task.CreatedBy, &task.CreatedAt, &task.StartedAt, &task.FinishedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		if err != nil {
			return err
		}
		if task.Status == TaskRunning {
			if err := json.Unmarshal(rolloutJSON, &task.Rollout); err != nil {
				return err
			}
			task.OfflineTTL *= time.Second
			return nil
		}
		if task.Status != TaskDraft {
			return fmt.Errorf("ota: 任务当前状态 %q，不可启动", task.Status)
		}
		if err := json.Unmarshal(rolloutJSON, &task.Rollout); err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `UPDATE t_ota_task SET status='running',started_at=now() WHERE project_id=$1 AND id=$2 RETURNING status,started_at`, projectID, taskID).Scan(&task.Status, &task.StartedAt)
		return err
	})
	return task, err
}

func (s *PGStore) MarkNotified(ctx context.Context, projectID int64, taskID, deviceKey string) error {
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		result, err := tx.Exec(ctx, `UPDATE t_ota_task_device SET status=CASE WHEN status IN ('pending','dispatching') THEN 'notified' ELSE status END,notified_at=COALESCE(notified_at,now()),updated_at=now() WHERE project_id=$1 AND task_id=$2 AND device_key=$3 AND status IN ('pending','dispatching','notified','downloading','verifying','installing','succeeded','failed','expired','rolled_back')`, projectID, taskID, deviceKey)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			return fmt.Errorf("ota: 设备任务不存在或已处理")
		}
		return nil
	})
}

func (s *PGStore) BeginDispatch(ctx context.Context, projectID int64, taskID, deviceKey string) error {
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		result, err := tx.Exec(ctx, `UPDATE t_ota_task_device SET status='dispatching',notified_at=COALESCE(notified_at,now()),updated_at=now() WHERE project_id=$1 AND task_id=$2 AND device_key=$3 AND status IN ('pending','dispatching')`, projectID, taskID, deviceKey)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			return fmt.Errorf("ota: 设备不在待下发状态")
		}
		return nil
	})
}

func (s *PGStore) ResetDispatch(ctx context.Context, projectID int64, taskID, deviceKey string) error {
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE t_ota_task_device SET status='pending',updated_at=now() WHERE project_id=$1 AND task_id=$2 AND device_key=$3 AND status='dispatching'`, projectID, taskID, deviceKey)
		return err
	})
}

func (s *PGStore) ListTaskDevices(ctx context.Context, projectID int64, taskID string) ([]TaskDevice, error) {
	var devices []TaskDevice
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT task_id::text,project_id,device_key,status,progress,bytes_downloaded,error_code,error_message,last_seen_version,updated_at FROM t_ota_task_device WHERE project_id=$1 AND task_id=$2 ORDER BY device_key`, projectID, taskID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var device TaskDevice
			if err := rows.Scan(&device.TaskID, &device.ProjectID, &device.DeviceKey, &device.Status, &device.Progress, &device.BytesDownloaded, &device.ErrorCode, &device.ErrorMessage, &device.FirmwareVersion, &device.UpdatedAt); err != nil {
				return err
			}
			devices = append(devices, device)
		}
		return rows.Err()
	})
	return devices, err
}

func (s *PGStore) ReportProgress(ctx context.Context, projectID int64, deviceKey string, progress Progress) error {
	if err := progress.Validate(); err != nil {
		return err
	}
	if projectID <= 0 || strings.TrimSpace(deviceKey) == "" {
		return fmt.Errorf("ota: project_id 和 device_key 必填")
	}
	to := DeviceStatus(progress.Status)
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		var from DeviceStatus
		err := tx.QueryRow(ctx, `SELECT status FROM t_ota_task_device WHERE project_id=$1 AND task_id=$2 AND device_key=$3 FOR UPDATE`, projectID, progress.TaskID, deviceKey).Scan(&from)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		if err != nil {
			return err
		}
		if from != to {
			if err := Transition(from, to); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE t_ota_task_device SET status=$1,progress=GREATEST(progress,$2),bytes_downloaded=GREATEST(bytes_downloaded,$3),error_code=$4,error_message=$5,last_seen_version=$6,progress_at=now(),completed_at=CASE WHEN $1 IN ('succeeded','failed','expired','rolled_back') THEN COALESCE(completed_at,now()) ELSE completed_at END,updated_at=now() WHERE project_id=$7 AND task_id=$8 AND device_key=$9`, to, progress.Progress, progress.BytesDownloaded, progress.ErrorCode, progress.ErrorMessage, progress.FirmwareVersion, projectID, progress.TaskID, deviceKey)
		if err != nil {
			return err
		}
		return reconcileTaskTx(ctx, tx, projectID, progress.TaskID)
	})
}

func (s *PGStore) ReconcileTask(ctx context.Context, projectID int64, taskID string) error {
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		return reconcileTaskTx(ctx, tx, projectID, taskID)
	})
}

func (s *PGStore) ReconcileStale(ctx context.Context, projectID int64) error {
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text FROM t_ota_task WHERE project_id=$1 AND status='running' ORDER BY created_at FOR UPDATE`, projectID)
		if err != nil {
			return err
		}
		defer rows.Close()
		var taskIDs []string
		for rows.Next() {
			var taskID string
			if err := rows.Scan(&taskID); err != nil {
				return err
			}
			taskIDs = append(taskIDs, taskID)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		for _, taskID := range taskIDs {
			if err := reconcileTaskTx(ctx, tx, projectID, taskID); err != nil {
				return err
			}
		}
		return nil
	})
}

func reconcileTaskTx(ctx context.Context, tx pgx.Tx, projectID int64, taskID string) error {
	var status TaskStatus
	var threshold float64
	var offlineTTL time.Duration
	var rolloutJSON []byte
	if err := tx.QueryRow(ctx, `SELECT status,rollout,extract(epoch from offline_ttl)::bigint FROM t_ota_task WHERE project_id=$1 AND id=$2 FOR UPDATE`, projectID, taskID).Scan(&status, &rolloutJSON, &offlineTTL); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		return err
	}
	var rollout Rollout
	if err := json.Unmarshal(rolloutJSON, &rollout); err != nil {
		return err
	}
	threshold = rollout.SuccessThreshold
	var total, succeeded, failed, expired int
	if err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE status='succeeded'),count(*) FILTER (WHERE status='failed'),count(*) FILTER (WHERE status='expired') FROM t_ota_task_device WHERE project_id=$1 AND task_id=$2`, projectID, taskID).Scan(&total, &succeeded, &failed, &expired); err != nil {
		return err
	}
	if total == 0 || status != TaskRunning {
		return nil
	}
	if offlineTTL > 0 {
		_, err := tx.Exec(ctx, `UPDATE t_ota_task_device SET status='expired',error_code='offline_timeout',error_message='设备在离线窗口内未完成 OTA',completed_at=COALESCE(completed_at,now()),updated_at=now() WHERE project_id=$1 AND task_id=$2 AND status IN ('dispatching','notified','downloading','verifying','installing') AND COALESCE(progress_at,notified_at) < now() - $3::interval`, projectID, taskID, fmt.Sprintf("%d seconds", int64(offlineTTL/time.Second)))
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='succeeded'),count(*) FILTER (WHERE status='failed'),count(*) FILTER (WHERE status IN ('expired','rolled_back')) FROM t_ota_task_device WHERE project_id=$1 AND task_id=$2`, projectID, taskID).Scan(&succeeded, &failed, &expired); err != nil {
			return err
		}
	}
	terminal := succeeded + failed + expired
	if terminal < total {
		return nil
	}
	rate := float64(succeeded) / float64(total)
	newStatus := TaskCompleted
	if rate < threshold {
		newStatus = TaskPaused
	}
	_, err := tx.Exec(ctx, `UPDATE t_ota_task SET status=$1,finished_at=now() WHERE project_id=$2 AND id=$3 AND status='running'`, newStatus, projectID, taskID)
	return err
}

func normalizeDeviceKeys(deviceKeys []string) ([]string, error) {
	set := make(map[string]struct{}, len(deviceKeys))
	for _, deviceKey := range deviceKeys {
		deviceKey = strings.TrimSpace(deviceKey)
		if deviceKey == "" || strings.ContainsAny(deviceKey, "/+#") {
			return nil, fmt.Errorf("ota: 目标 device_key 非法")
		}
		set[deviceKey] = struct{}{}
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func newTaskID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(id[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func ValidTaskID(taskID string) bool {
	if len(taskID) != 36 || taskID[8] != '-' || taskID[13] != '-' || taskID[18] != '-' || taskID[23] != '-' {
		return false
	}
	for index, value := range taskID {
		if value == '-' {
			continue
		}
		if !((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')) {
			return false
		}
		if index == 14 && value != '4' {
			return false
		}
	}
	return true
}

var _ = (*PGStore)(nil)
