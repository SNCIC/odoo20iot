package alarm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore 是 Store 的 PostgreSQL 实现（04 §2.5：状态持久化在 PG）。
//
// 表是 t_alarm_active（见 internal/pg/migrations/0001_alarm_active.sql）：
// **非分区表**，靠 PRIMARY KEY (dedup_key) 提供**跨时间的全局唯一** ——
// 这正是 t_alarm（按 first_ts 分区）做不到的，而 04 §2.2 要求
// 「同一设备同一规则只允许一个活跃告警」。
//
// CAS 落在 `WHERE dedup_key = $1 AND state = $2` + RowsAffected 上，
// 与 04 §2.5 的 `UPDATE ... WHERE id=? AND state=?` 是同一件事。
type PGStore struct {
	pool *pgxpool.Pool
}

var _ Store = (*PGStore)(nil)

// NewPGStore 构造。
func NewPGStore(pool *pgxpool.Pool) (*PGStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("alarm: 需要非空连接池")
	}
	return &PGStore{pool: pool}, nil
}

// alarmColumns 与 scanAlarm 严格一一对应；顺序写死在一处，避免两边错位 ——
// 错位的表现是「字段串了」而不是报错，最难查。
const alarmColumns = `dedup_key, id, project_id, device_id, device_type_id,
	rule_id, rule_name, level, state, parent_id, timing,
	first_ts, last_ts, state_ts, confirmed_ts, notified_ts, resolved_ts, closed_ts,
	notify_count, flap_count, suppressed, suppress_reason, batch_id`

// timingJSON 是 timing 列的线上格式，用 04 §2.4 的字段名与秒为单位 ——
// 运维会直接 `psql` 看这张表，存成 base64/二进制对排障毫无帮助。
type timingJSON struct {
	DetectWindowS float64 `json:"detect_window_s"`
	SuppressS     float64 `json:"suppress_s"`
	AutoCloseS    float64 `json:"auto_close_s"`
}

func encodeTiming(t Timing) (string, error) {
	b, err := json.Marshal(timingJSON{
		DetectWindowS: t.DetectWindow.Seconds(),
		SuppressS:     t.Suppress.Seconds(),
		AutoCloseS:    t.AutoClose.Seconds(),
	})
	if err != nil {
		return "", fmt.Errorf("alarm: 序列化 timing: %w", err)
	}
	return string(b), nil
}

// Get 实现 Store。不存在时返回 (nil, nil) —— 「没有」不是错误。
func (s *PGStore) Get(ctx context.Context, dedupKey string) (*Alarm, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+alarmColumns+` FROM t_alarm_active WHERE dedup_key = $1`, dedupKey)
	a, err := scanAlarm(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("alarm: 读取告警 %s: %w", dedupKey, err)
	}
	return a, nil
}

// GetByID 实现 Store（根因抑制要按父告警 id 反查，04 §2.2）。
func (s *PGStore) GetByID(ctx context.Context, id string) (*Alarm, error) {
	if id == "" {
		return nil, nil
	}
	row := s.pool.QueryRow(ctx,
		`SELECT `+alarmColumns+` FROM t_alarm_active WHERE id = $1`, id)
	a, err := scanAlarm(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("alarm: 按 id 读取告警 %s: %w", id, err)
	}
	return a, nil
}

// Active 实现 Store。
//
// 刻意带上 `ORDER BY state_ts`：Store 是无序容器时，扫描顺序会随实现而变，
// 「同一轮里父告警先于子告警被求值」这种事就变得不确定（本包测试踩过）。
// 定死顺序后，抑制判定的结果可复现，也天然做到「拖得最久的先处理」。
func (s *PGStore) Active(ctx context.Context, states ...State) ([]*Alarm, error) {
	q := `SELECT ` + alarmColumns + ` FROM t_alarm_active`
	var args []any
	if len(states) > 0 {
		ss := make([]string, len(states))
		for i, st := range states {
			ss[i] = string(st)
		}
		q += ` WHERE state = ANY($1)`
		args = append(args, ss)
	}
	q += ` ORDER BY state_ts, dedup_key`

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("alarm: 列举活跃告警: %w", err)
	}
	defer rows.Close()

	var out []*Alarm
	for rows.Next() {
		a, err := scanAlarm(rows)
		if err != nil {
			return nil, fmt.Errorf("alarm: 扫描活跃告警: %w", err)
		}
		out = append(out, a)
	}
	// 只看 rows.Err() 之外还要看它：漏检会把「中途失败」当成「就这些」，
	// 表现为扫描悄悄少处理一批告警。
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("alarm: 遍历活跃告警: %w", err)
	}
	return out, nil
}

// scanRow 让 scanAlarm 同时接受 QueryRow 与 Rows。
type scanRow interface {
	Scan(dest ...any) error
}

func scanAlarm(row scanRow) (*Alarm, error) {
	var (
		a           Alarm
		parentID    *string
		timingRaw   []byte
		confirmedTS *time.Time
		notifiedTS  *time.Time
		resolvedTS  *time.Time
		closedTS    *time.Time
	)
	if err := row.Scan(
		&a.DedupKey, &a.ID, &a.ProjectID, &a.DeviceID, &a.DeviceTypeID,
		&a.RuleID, &a.RuleName, &a.Level, &a.State, &parentID, &timingRaw,
		&a.FirstTS, &a.LastTS, &a.StateTS, &confirmedTS, &notifiedTS, &resolvedTS, &closedTS,
		&a.NotifyCount, &a.FlapCount, &a.Suppressed, &a.SuppressReason, &a.BatchID,
	); err != nil {
		return nil, err
	}
	if parentID != nil {
		a.ParentID = *parentID
	}
	a.ConfirmedTS = derefTime(confirmedTS)
	a.NotifiedTS = derefTime(notifiedTS)
	a.ResolvedTS = derefTime(resolvedTS)
	a.ClosedTS = derefTime(closedTS)
	a.Timing = decodeTiming(timingRaw)
	return &a, nil
}

func decodeTiming(raw []byte) Timing {
	if len(raw) == 0 {
		return Timing{}.Normalize()
	}
	var t timingJSON
	if err := json.Unmarshal(raw, &t); err != nil {
		// 列被写坏时退回默认窗口而不是崩掉：读不出来时用默认值继续跑，
		// 比让整个扫描循环停摆要好。
		return Timing{}.Normalize()
	}
	return Timing{
		DetectWindow: time.Duration(t.DetectWindowS * float64(time.Second)),
		Suppress:     time.Duration(t.SuppressS * float64(time.Second)),
		AutoClose:    time.Duration(t.AutoCloseS * float64(time.Second)),
	}.Normalize()
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func nullText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// pgUniqueViolation 是 PG 的唯一约束冲突码（SQLSTATE 23505）。
const pgUniqueViolation = "23505"

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

// Update 实现 Store（CAS 语义）。
//
// 三种情形分得很清楚，因为它们的冲突含义不同：
//   - 目标为 idle → 删除该行（idle 由「行不存在」表达），仍带 CAS；
//   - 期望不存在 → 插入；主键冲突说明别的实例抢先建了同一个 dedup_key；
//   - 其余 → 带 `AND state = expected` 的 UPDATE，RowsAffected=0 即冲突。
func (s *PGStore) Update(ctx context.Context, a *Alarm, expected State) error {
	if a == nil {
		return fmt.Errorf("alarm: 不能写回 nil")
	}
	if expected == StateIdle && a.State == StateIdle {
		// 从「无」到「无」：没有任何东西需要落库（调用方不该走到这，但不该炸）。
		return nil
	}
	if a.State == StateIdle {
		return s.delete(ctx, a, expected)
	}
	if expected == StateIdle {
		return s.insert(ctx, a)
	}
	return s.casUpdate(ctx, a, expected)
}

const insertAlarmSQL = `
INSERT INTO t_alarm_active (
    dedup_key, project_id, device_id, device_type_id, rule_id, rule_name, level, state,
    parent_id, timing, first_ts, last_ts, state_ts, confirmed_ts, notified_ts, resolved_ts,
    closed_ts, notify_count, flap_count, suppressed, suppress_reason, batch_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
RETURNING id`

func (s *PGStore) insert(ctx context.Context, a *Alarm) error {
	timing, err := encodeTiming(a.Timing)
	if err != nil {
		return err
	}
	// id 让数据库生成并回传（DEFAULT nextval），而不是在 Go 侧造 ——
	// 两个实例各自造 id 的冲突只能靠唯一约束事后发现。
	var id string
	err = s.pool.QueryRow(ctx, insertAlarmSQL,
		a.DedupKey, a.ProjectID, a.DeviceID, a.DeviceTypeID, a.RuleID, a.RuleName, a.Level, string(a.State),
		nullText(a.ParentID), timing, a.FirstTS, a.LastTS, a.StateTS,
		nullTime(a.ConfirmedTS), nullTime(a.NotifiedTS), nullTime(a.ResolvedTS), nullTime(a.ClosedTS),
		a.NotifyCount, a.FlapCount, a.Suppressed, a.SuppressReason, a.BatchID,
	).Scan(&id)
	if err == nil {
		a.ID = id
		return nil
	}
	if isUniqueViolation(err) {
		return fmt.Errorf("%w: %s 已被并发创建", ErrStateConflict, a.DedupKey)
	}
	return fmt.Errorf("alarm: 新建告警 %s: %w", a.DedupKey, err)
}

const casUpdateAlarmSQL = `
UPDATE t_alarm_active SET
    state = $3, level = $4, parent_id = $5, rule_name = $6,
    last_ts = $7, state_ts = $8, confirmed_ts = $9, notified_ts = $10,
    resolved_ts = $11, closed_ts = $12, notify_count = $13, flap_count = $14,
    suppressed = $15, suppress_reason = $16, batch_id = $17, updated_at = now()
WHERE dedup_key = $1 AND state = $2`

// casUpdate 只更新**会变**的字段。
//
// 刻意不碰 first_ts / project_id / device_id / rule_id：这些是身份字段，
// 一旦被写脏，告警会在无人察觉的情况下「改挂到另一台设备上」，
// 而那比状态推进失败要难查得多。
func (s *PGStore) casUpdate(ctx context.Context, a *Alarm, expected State) error {
	tag, err := s.pool.Exec(ctx, casUpdateAlarmSQL,
		a.DedupKey, string(expected), string(a.State), a.Level, nullText(a.ParentID), a.RuleName,
		a.LastTS, a.StateTS, nullTime(a.ConfirmedTS), nullTime(a.NotifiedTS),
		nullTime(a.ResolvedTS), nullTime(a.ClosedTS), a.NotifyCount, a.FlapCount,
		a.Suppressed, a.SuppressReason, a.BatchID,
	)
	if err != nil {
		return fmt.Errorf("alarm: 更新告警 %s: %w", a.DedupKey, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s 期望 %s，与实际不符", ErrStateConflict, a.DedupKey, expected)
	}
	return nil
}

func (s *PGStore) delete(ctx context.Context, a *Alarm, expected State) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM t_alarm_active WHERE dedup_key = $1 AND state = $2`,
		a.DedupKey, string(expected))
	if err != nil {
		return fmt.Errorf("alarm: 关闭告警 %s: %w", a.DedupKey, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s 期望 %s，但不存在或状态已变",
			ErrStateConflict, a.DedupKey, expected)
	}
	return nil
}
