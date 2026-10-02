// Package dlq 是死信的 PostgreSQL 落地（04 §3.3 的 t_dlq）。
//
// 独立成包而不是塞进 notify：死信是**跨服务**的设施 ——
// odoo-connector、svc-pipeline 都会往同一张表写（靠 service 列区分），
// 把条目类型绑在通知包上，会让另外两个服务为了写一条死信而依赖整个通知模块。
package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Entry 是一条死信（04 §3.3：原始消息、失败原因、重试历史、trace_id）。
type Entry struct {
	// Service 是写入方（`svc-notify` / `odoo-connector` / …）。
	// 它是「按 service 聚合告警」的分组键，不能为空。
	Service string
	// Subject 是二级分类：失败的通道、主题名、实体类型。
	Subject string
	// EntityType 是业务实体类型，用于按实体聚合和选择重放处理器。
	EntityType string
	// IdempotencyKey 是原请求的幂等键，重放必须沿用它。
	IdempotencyKey string
	// Reason 是失败原因（最后一条错误）。
	Reason string
	// Attempts 是总尝试次数。
	Attempts int
	// History 是重试历史（逐条人可读）。
	History []string
	// TraceID 贯通链路（04 §3.3 明确要求）。
	TraceID string
	// Payload 是原文。对象存储接入前先落库当兜底；
	// 接入后可只留引用，避免 PG 被大报文撑大。
	Payload    string
	PayloadRef string
	// CreatedAt 零值取当前时间。
	CreatedAt time.Time
}

type Record struct {
	ID        int64
	CreatedAt time.Time
	Entry
	ReplayCount    int
	LastReplayedAt *time.Time
	ResolvedAt     *time.Time
}

type Aggregate struct {
	Service    string
	Subject    string
	EntityType string
	Count      int64
}

// Store 把死信写入 t_dlq。
type Store struct {
	pool    *pgxpool.Pool
	objects ObjectStore

	mu sync.Mutex
	// ensuredMonth 缓存「本月分区已确认」，避免每条死信都多一次函数调用。
	// 死信是低频路径，但跨月那一刻的单条写入恰恰最不能失败。
	ensuredMonth string
}

// New 构造。
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func NewWithObjectStore(pool *pgxpool.Pool, objects ObjectStore) *Store {
	return &Store{pool: pool, objects: objects}
}

// Put 写入一条死信。
func (s *Store) Put(ctx context.Context, e Entry) error {
	if e.Service == "" {
		// service 是聚合告警的分组键（04 §3.3「按 service 聚合告警」）。
		// 空的 service 会让所有服务的事件混成一个桶，聚合出来的告警
		// 只剩「有一堆死信」这一句，等于没有信息。
		return fmt.Errorf("dlq: 死信必须标注 service")
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	if err := s.ensurePartition(ctx, e.CreatedAt); err != nil {
		return err
	}

	history, err := json.Marshal(e.History)
	if err != nil {
		return fmt.Errorf("dlq: 序列化重试历史: %w", err)
	}
	if s.objects != nil && e.Payload != "" {
		ref, err := s.objects.Put(ctx, e.Service+"/"+e.EntityType, []byte(e.Payload))
		if err != nil {
			return fmt.Errorf("dlq: 保存原文对象: %w", err)
		}
		e.PayloadRef = ref
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO t_dlq (service, subject, entity_type, idempotency_key, reason, attempts, history, trace_id, payload, payload_ref, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		e.Service, e.Subject, e.EntityType, e.IdempotencyKey, e.Reason, e.Attempts, string(history),
		e.TraceID, e.Payload, e.PayloadRef, e.CreatedAt,
	); err != nil {
		return fmt.Errorf("dlq: 写入死信: %w", err)
	}
	return nil
}

func (s *Store) ListPending(ctx context.Context, service, subject string, limit int) ([]Record, error) {
	if service == "" {
		return nil, fmt.Errorf("dlq: service 不能为空")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	q := `SELECT id, service, subject, entity_type, idempotency_key, reason, attempts, history, trace_id, payload, created_at, replay_count, last_replayed_at, resolved_at FROM t_dlq WHERE service=$1 AND resolved_at IS NULL`
	args := []any{service}
	if subject != "" {
		q += ` AND subject=$2`
		args = append(args, subject)
	}
	q += ` ORDER BY created_at, id LIMIT $` + fmt.Sprint(len(args)+1)
	args = append(args, limit)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("dlq: 查询待重放: %w", err)
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		var history []byte
		if err := rows.Scan(&r.ID, &r.Service, &r.Subject, &r.EntityType, &r.IdempotencyKey, &r.Reason, &r.Attempts, &history, &r.TraceID, &r.Payload, &r.CreatedAt, &r.ReplayCount, &r.LastReplayedAt, &r.ResolvedAt); err != nil {
			return nil, fmt.Errorf("dlq: 扫描待重放: %w", err)
		}
		if err := json.Unmarshal(history, &r.History); err != nil {
			return nil, fmt.Errorf("dlq: 解析重试历史: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dlq: 遍历待重放: %w", err)
	}
	return out, nil
}

func (s *Store) MarkReplayed(ctx context.Context, id int64, createdAt time.Time, success bool, reason string) error {
	if id <= 0 || createdAt.IsZero() {
		return fmt.Errorf("dlq: id 和 created_at 必填")
	}
	_, err := s.pool.Exec(ctx, `UPDATE t_dlq SET replay_count=replay_count+1, last_replayed_at=now(), resolved_at=CASE WHEN $3 THEN now() ELSE resolved_at END, reason=CASE WHEN $4<>'' THEN $4 ELSE reason END WHERE id=$1 AND created_at=$2`, id, createdAt, success, reason)
	if err != nil {
		return fmt.Errorf("dlq: 更新重放状态: %w", err)
	}
	return nil
}

func (s *Store) AggregateSince(ctx context.Context, since time.Time) ([]Aggregate, error) {
	rows, err := s.pool.Query(ctx, `SELECT service, subject, entity_type, count(*) FROM t_dlq WHERE created_at >= $1 GROUP BY service, subject, entity_type ORDER BY count(*) DESC`, since)
	if err != nil {
		return nil, fmt.Errorf("dlq: 聚合统计: %w", err)
	}
	defer rows.Close()
	var out []Aggregate
	for rows.Next() {
		var a Aggregate
		if err := rows.Scan(&a.Service, &a.Subject, &a.EntityType, &a.Count); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ensurePartition 保证目标月份的分区存在（幂等）。
//
// 每次调用都是一次「先查再建」的往返，但只在跨月后第一次写时才真的执行 ——
// 平时的开销是一次廉价 SELECT。这个代价换的是：
// 定时建分区的任务挂了、或者服务正好在跨月那一刻写死信，插入也不会失败。
func (s *Store) ensurePartition(ctx context.Context, at time.Time) error {
	month := at.UTC().Format("2006-01")
	s.mu.Lock()
	if s.ensuredMonth == month {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	var part string
	if err := s.pool.QueryRow(ctx,
		`SELECT ensure_month_partition('t_dlq', $1)`, at).Scan(&part); err != nil {
		return fmt.Errorf("dlq: 确保分区存在: %w", err)
	}

	s.mu.Lock()
	s.ensuredMonth = month
	s.mu.Unlock()
	return nil
}

// CountSince 返回某服务在 since 之后的死信条数。
//
// 供「DLQ 增长」这类告警使用（06 §：DLQ 增长属 P2）。
// 没有这个读数，「死信在涨」就只能靠人偶尔去翻表 —— 而没人会翻。
func (s *Store) CountSince(ctx context.Context, service string, since time.Time) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM t_dlq WHERE service = $1 AND created_at >= $2`,
		service, since).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("dlq: 统计死信: %w", err)
	}
	return n, nil
}
