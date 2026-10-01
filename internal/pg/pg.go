// Package pg 是 IoT 业务库（PostgreSQL）的连接与迁移地基。
//
// 与 internal/tsdb/greptimedb 的区别：那个借 PG wire 协议访问 GreptimeDB，
// 受 §6 坑 18/19 的限制（Ping 不可用、simple protocol 不可用、JSON 列是 bytea）；
// 这个连的是**真 PostgreSQL**，分区表、advisory lock、多语句 simple 查询都能用，
// 不要把那两条坑的规避手段搬过来。
package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultDSN 走 Unix socket 的 peer 认证连本机 PG，**不需要密码** ——
// 开发环境不因此引入新凭据（TCP 口是 scram-sha-256，得配密码才行）。
// 生产用 DSN 覆盖。
const DefaultDSN = "postgres:///iot?host=/var/run/postgresql"

// Config 是连接池参数。
type Config struct {
	DSN string
	// MaxConns 默认 16：devbox 限 16C，池开得比核数大只会让请求在 PG 端排队，
	// 不提升吞吐，反而放大锁等待。
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	ConnectTimeout  time.Duration
}

// DefaultConfig 返回开发默认值。
func DefaultConfig() Config {
	return Config{
		DSN:             DefaultDSN,
		MaxConns:        16,
		MinConns:        2,
		MaxConnLifetime: time.Hour,
		MaxConnIdleTime: 5 * time.Minute,
		ConnectTimeout:  5 * time.Second,
	}
}

// Parse 解析 DSN 并套用池参数，返回可继续改字段的池配置。
//
// ⚠️ 不要在改完字段后用 `poolCfg.ConnString()` 取 DSN：它**原样返回最初传入的
// 字符串**，`ConnConfig.Database` 的改动不会体现出来。这个陷阱很安静 ——
// 测试里「换一个临时库」会悄悄变成「连回原库」，而且还会通过。
// 需要换库就改 `poolCfg.ConnConfig.Database` 后走 OpenWithConfig。
func Parse(cfg Config) (*pgxpool.Config, error) {
	if cfg.DSN == "" {
		cfg.DSN = DefaultDSN
	}
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("pg: 解析 DSN: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.ConnectTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	return poolCfg, nil
}

// Open 建池并**确认连得通**。
//
// 刻意在这里 Ping：真 PG 上它可用（§6 坑 19「Ping 发空语句」是 GreptimeDB 的
// 限制）。建池不校验连通性的话，DSN 写错要拖到第一次业务查询才暴露 ——
// 那时错误信息离根因已经很远了。
func Open(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := Parse(cfg)
	if err != nil {
		return nil, err
	}
	return OpenWithConfig(ctx, poolCfg, cfg.ConnectTimeout)
}

// OpenWithConfig 用已解析（并可能已被调用方改过字段）的池配置建池。
func OpenWithConfig(ctx context.Context, poolCfg *pgxpool.Config, connectTimeout time.Duration) (*pgxpool.Pool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pg: 建连接池: %w", err)
	}
	pingCtx := ctx
	if connectTimeout > 0 {
		var cancel context.CancelFunc
		pingCtx, cancel = context.WithTimeout(ctx, connectTimeout)
		defer cancel()
	}
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: 连通性检查失败: %w", err)
	}
	return pool, nil
}
