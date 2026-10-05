package modbusgw

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("modbus 配置需要 PG 连接池")
	}
	return &Store{pool: pool}, nil
}

func (s *Store) List(ctx context.Context, projectID int64) ([]Config, error) {
	return s.list(ctx, projectID, true)
}

// ListAll returns both enabled and disabled configurations for the control plane.
// The polling worker uses List so a disabled row never starts a poller.
func (s *Store) ListAll(ctx context.Context, projectID int64) ([]Config, error) {
	return s.list(ctx, projectID, false)
}

func (s *Store) list(ctx context.Context, projectID int64, enabledOnly bool) ([]Config, error) {
	if projectID <= 0 {
		return nil, fmt.Errorf("modbus project_id 必须为正数")
	}
	var out []Config
	err := pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		query := `SELECT id,project_id,device_key,device_id,device_type_id,transport,endpoint,serial_path,baud_rate,data_bits,stop_bits,parity,unit_id,interval_ms,timeout_ms,points,enabled FROM t_modbus_poll_config WHERE project_id=$1 ORDER BY id`
		if enabledOnly {
			query = `SELECT id,project_id,device_key,device_id,device_type_id,transport,endpoint,serial_path,baud_rate,data_bits,stop_bits,parity,unit_id,interval_ms,timeout_ms,points,enabled FROM t_modbus_poll_config WHERE project_id=$1 AND enabled ORDER BY id`
		}
		rows, err := tx.Query(ctx, query, projectID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var cfg Config
			var intervalMS, timeoutMS int
			var raw []byte
			if err := rows.Scan(&cfg.ID, &cfg.ProjectID, &cfg.DeviceKey, &cfg.DeviceID, &cfg.DeviceTypeID, &cfg.Transport, &cfg.Endpoint, &cfg.SerialPath, &cfg.BaudRate, &cfg.DataBits, &cfg.StopBits, &cfg.Parity, &cfg.UnitID, &intervalMS, &timeoutMS, &raw, &cfg.Enabled); err != nil {
				return err
			}
			if err := json.Unmarshal(raw, &cfg.Points); err != nil {
				return fmt.Errorf("解析 Modbus 点位 %s: %w", cfg.DeviceKey, err)
			}
			cfg.Interval, cfg.Timeout = time.Duration(intervalMS)*time.Millisecond, time.Duration(timeoutMS)*time.Millisecond
			if _, err := NormalizeConfig(cfg); err != nil {
				return err
			}
			out = append(out, cfg)
		}
		return rows.Err()
	})
	return out, err
}

// Upsert creates or replaces the configuration identified by project and device key.
// The project id is always taken from the authenticated tenant by the caller.
func (s *Store) Upsert(ctx context.Context, cfg Config) (Config, error) {
	normalized, err := NormalizeConfig(cfg)
	if err != nil {
		return Config{}, err
	}
	points, err := json.Marshal(normalized.Points)
	if err != nil {
		return Config{}, fmt.Errorf("编码 Modbus 点位: %w", err)
	}
	var out Config
	err = pg.WithProjectTx(ctx, s.pool, normalized.ProjectID, func(ctx context.Context, tx pgx.Tx) error {
		var intervalMS, timeoutMS int
		var raw []byte
		err := tx.QueryRow(ctx, `
			INSERT INTO t_modbus_poll_config
				(project_id,device_key,device_id,device_type_id,transport,endpoint,serial_path,baud_rate,data_bits,stop_bits,parity,unit_id,interval_ms,timeout_ms,points,enabled)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16)
			ON CONFLICT (project_id,device_key) DO UPDATE SET
				device_id=EXCLUDED.device_id,
				device_type_id=EXCLUDED.device_type_id,
				transport=EXCLUDED.transport,
				endpoint=EXCLUDED.endpoint,
				serial_path=EXCLUDED.serial_path,
				baud_rate=EXCLUDED.baud_rate,
				data_bits=EXCLUDED.data_bits,
				stop_bits=EXCLUDED.stop_bits,
				parity=EXCLUDED.parity,
				unit_id=EXCLUDED.unit_id,
				interval_ms=EXCLUDED.interval_ms,
				timeout_ms=EXCLUDED.timeout_ms,
				points=EXCLUDED.points,
				enabled=EXCLUDED.enabled,
				updated_at=now()
			RETURNING id,project_id,device_key,device_id,device_type_id,transport,endpoint,serial_path,baud_rate,data_bits,stop_bits,parity,unit_id,interval_ms,timeout_ms,points,enabled`,
			normalized.ProjectID, normalized.DeviceKey, normalized.DeviceID, normalized.DeviceTypeID,
			normalized.Transport, normalized.Endpoint, normalized.SerialPath, normalized.BaudRate,
			normalized.DataBits, normalized.StopBits, normalized.Parity, normalized.UnitID,
			int(normalized.Interval/time.Millisecond), int(normalized.Timeout/time.Millisecond), points, normalized.Enabled,
		).Scan(&out.ID, &out.ProjectID, &out.DeviceKey, &out.DeviceID, &out.DeviceTypeID, &out.Transport, &out.Endpoint, &out.SerialPath, &out.BaudRate, &out.DataBits, &out.StopBits, &out.Parity, &out.UnitID, &intervalMS, &timeoutMS, &raw, &out.Enabled)
		if err != nil {
			return err
		}
		out.Interval, out.Timeout = time.Duration(intervalMS)*time.Millisecond, time.Duration(timeoutMS)*time.Millisecond
		return json.Unmarshal(raw, &out.Points)
	})
	return out, err
}

// Delete disables a configuration without losing its last known settings.
// Recreating the same device key through Upsert re-enables it atomically.
func (s *Store) Delete(ctx context.Context, projectID int64, deviceKey string) error {
	if projectID <= 0 || strings.TrimSpace(deviceKey) == "" {
		return fmt.Errorf("modbus project_id/device_key 非法")
	}
	return pg.WithProjectTx(ctx, s.pool, projectID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE t_modbus_poll_config SET enabled=false,updated_at=now() WHERE project_id=$1 AND device_key=$2`, projectID, strings.TrimSpace(deviceKey))
		return err
	})
}

// NormalizeConfig fills safe defaults before validation and persistence.
func NormalizeConfig(cfg Config) (Config, error) {
	cfg.DeviceKey = strings.TrimSpace(cfg.DeviceKey)
	cfg.Transport = strings.ToLower(strings.TrimSpace(cfg.Transport))
	if cfg.Transport == "" {
		cfg.Transport = "tcp"
	}
	if cfg.UnitID == 0 {
		cfg.UnitID = 1
	}
	if cfg.Interval == 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.BaudRate == 0 {
		cfg.BaudRate = 9600
	}
	if cfg.DataBits == 0 {
		cfg.DataBits = 8
	}
	if cfg.StopBits == 0 {
		cfg.StopBits = 1
	}
	if cfg.Parity == "" {
		cfg.Parity = "none"
	}
	cfg.Parity = strings.ToLower(strings.TrimSpace(cfg.Parity))
	if err := ValidateConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func ValidateConfig(cfg Config) error {
	if cfg.ProjectID <= 0 || cfg.DeviceKey == "" || len(cfg.Points) == 0 {
		return fmt.Errorf("Modbus 配置缺少 project_id/device_key/points")
	}
	if len(cfg.DeviceKey) > 128 || strings.ContainsAny(cfg.DeviceKey, " /#+") {
		return fmt.Errorf("Modbus device_key 非法")
	}
	if cfg.Transport != "tcp" && cfg.Transport != "rtu" {
		return fmt.Errorf("Modbus transport 非法: %q", cfg.Transport)
	}
	if cfg.Transport == "tcp" {
		if cfg.Endpoint == "" {
			return fmt.Errorf("Modbus TCP endpoint 为空: %s", cfg.DeviceKey)
		}
		host, port, err := net.SplitHostPort(cfg.Endpoint)
		if err != nil || strings.TrimSpace(host) == "" || port == "" {
			return fmt.Errorf("Modbus TCP endpoint 必须为 host:port: %s", cfg.Endpoint)
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return fmt.Errorf("Modbus TCP endpoint 端口非法: %s", cfg.Endpoint)
		}
	}
	if cfg.Transport == "rtu" && cfg.SerialPath == "" {
		return fmt.Errorf("Modbus RTU serial_path 为空: %s", cfg.DeviceKey)
	}
	if cfg.Transport == "rtu" && !filepath.IsAbs(cfg.SerialPath) {
		return fmt.Errorf("Modbus RTU serial_path 必须为绝对路径: %s", cfg.SerialPath)
	}
	if cfg.UnitID == 0 || cfg.UnitID > 247 {
		return fmt.Errorf("Modbus unit_id 非法: %d", cfg.UnitID)
	}
	if cfg.BaudRate != 1200 && cfg.BaudRate != 2400 && cfg.BaudRate != 4800 && cfg.BaudRate != 9600 && cfg.BaudRate != 19200 && cfg.BaudRate != 38400 && cfg.BaudRate != 57600 && cfg.BaudRate != 115200 {
		return fmt.Errorf("Modbus 波特率不支持: %d", cfg.BaudRate)
	}
	if cfg.DataBits < 5 || cfg.DataBits > 8 || (cfg.StopBits != 1 && cfg.StopBits != 2) {
		return fmt.Errorf("Modbus 串口数据位/停止位非法")
	}
	if cfg.Parity != "none" && cfg.Parity != "even" && cfg.Parity != "odd" {
		return fmt.Errorf("Modbus 串口校验位非法: %q", cfg.Parity)
	}
	if cfg.Interval < 100*time.Millisecond || cfg.Interval > 24*time.Hour {
		return fmt.Errorf("Modbus interval 必须在 100ms 到 24h 之间")
	}
	if cfg.Timeout < 100*time.Millisecond || cfg.Timeout > 5*time.Minute {
		return fmt.Errorf("Modbus timeout 必须在 100ms 到 5m 之间")
	}
	if len(cfg.Points) > 128 {
		return fmt.Errorf("Modbus 点位最多 128 个")
	}
	used := make(map[uint16]string)
	minAddress, maxAddress := uint32(65536), uint32(0)
	for _, point := range cfg.Points {
		if strings.TrimSpace(point.Name) == "" || len(point.Name) > 128 || strings.ContainsAny(point.Name, " \t\r\n") {
			return fmt.Errorf("Modbus 点位名称非法: %q", point.Name)
		}
		width := uint32(1)
		switch point.Type {
		case "uint16", "int16":
		case "uint32", "int32", "float32":
			width = 2
		default:
			return fmt.Errorf("不支持的 Modbus 类型 %q", point.Type)
		}
		end := uint32(point.Address) + width
		if end > 65536 {
			return fmt.Errorf("Modbus 点位 %s 超出寄存器范围", point.Name)
		}
		if end-uint32(point.Address) > 0 {
			if uint32(point.Address) < minAddress {
				minAddress = uint32(point.Address)
			}
			if end-1 > maxAddress {
				maxAddress = end - 1
			}
		}
		for address := uint32(point.Address); address < end; address++ {
			if previous, exists := used[uint16(address)]; exists {
				return fmt.Errorf("Modbus 点位地址重叠: %s 与 %s", point.Name, previous)
			}
			used[uint16(address)] = point.Name
		}
		if math.IsNaN(point.Scale) || math.IsInf(point.Scale, 0) || math.IsNaN(point.Offset) || math.IsInf(point.Offset, 0) {
			return fmt.Errorf("Modbus 点位 %s scale/offset 必须为有限数", point.Name)
		}
	}
	if maxAddress >= minAddress && maxAddress-minAddress+1 > 125 {
		return fmt.Errorf("Modbus 点位跨度不能超过 125 个寄存器")
	}
	return nil
}
