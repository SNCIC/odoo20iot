package alarm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrStateConflict 表示乐观锁冲突：写回时库里的状态与预期不符。
//
// 04 §2.5 要求状态迁移用 UPDATE ... WHERE id=? AND state=?，冲突则**重读重试
// （最多 3 次）**。把它做成哨兵错误而不是字符串，是因为调用方必须能区分
// 「并发冲突，重试即可」与「真故障，重试无用」。
var ErrStateConflict = errors.New("alarm: 告警状态已被并发修改")

// Store 存取告警状态。
//
// 04 §2.5：状态持久化在 PG（t_alarm），内存维护活跃告警索引
// （map[dedup_key]→state），启动时从 PG 重建。故这里的接口是**持久化语义**，
// 由 svc-alarm 用 PG 实现；本包提供内存实现供测试与单机运行。
type Store interface {
	// Get 按去重键取告警；不存在时返回 (nil, nil) ——
	// 「没有」不是错误，用 error 表达会逼调用方做错误串比对。
	Get(ctx context.Context, dedupKey string) (*Alarm, error)
	// GetByID 按告警 ID 取（根因抑制要用它查父告警，04 §2.2）。
	GetByID(ctx context.Context, id string) (*Alarm, error)
	// Update 以 CAS 语义写回：expected 与库中当前状态不符时返回 ErrStateConflict。
	Update(ctx context.Context, a *Alarm, expected State) error
	// Active 列出指定状态的告警，供定时扫描使用（不传状态表示全部活跃告警）。
	Active(ctx context.Context, states ...State) ([]*Alarm, error)
}

// MemStore 是内存实现（测试与单机运行）。
type MemStore struct {
	mu    sync.Mutex
	byKey map[string]*Alarm
	byID  map[string]string // id → dedupKey
	seq   int64
	// CASConflicts 记录冲突次数，供测试断言「并发写入确实被乐观锁挡住」。
	CASConflicts int64
}

var _ Store = (*MemStore)(nil)

// NewMemStore 构造内存存储。
func NewMemStore() *MemStore {
	return &MemStore{byKey: make(map[string]*Alarm), byID: make(map[string]string)}
}

// Get 实现 Store。
func (m *MemStore) Get(_ context.Context, dedupKey string) (*Alarm, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.byKey[dedupKey]
	if !ok {
		return nil, nil
	}
	return a.clone(), nil
}

// GetByID 实现 Store。
func (m *MemStore) GetByID(_ context.Context, id string) (*Alarm, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, ok := m.byID[id]
	if !ok {
		return nil, nil
	}
	return m.byKey[key].clone(), nil
}

// Update 实现 Store。
func (m *MemStore) Update(_ context.Context, a *Alarm, expected State) error {
	if a == nil {
		return fmt.Errorf("alarm: 不能写回 nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.byKey[a.DedupKey]
	if !ok {
		// 期望「不存在」时是新建，否则是并发删除。
		if expected != StateIdle {
			m.CASConflicts++
			return fmt.Errorf("%w: 期望 %s，但记录已不存在", ErrStateConflict, expected)
		}
		if a.ID == "" {
			m.seq++
			a.ID = fmt.Sprintf("alarm-%d", m.seq)
		}
		m.byKey[a.DedupKey] = a.clone()
		m.byID[a.ID] = a.DedupKey
		return nil
	}

	if cur.State != expected {
		m.CASConflicts++
		return fmt.Errorf("%w: 期望 %s，实际 %s", ErrStateConflict, expected, cur.State)
	}

	if a.ID == "" {
		a.ID = cur.ID
	}
	if a.State == StateIdle {
		// 关闭即释放去重键（04 §2.2：只允许一个**活跃**告警）。
		delete(m.byKey, a.DedupKey)
		delete(m.byID, a.ID)
		return nil
	}
	m.byKey[a.DedupKey] = a.clone()
	return nil
}

// Active 实现 Store。
func (m *MemStore) Active(_ context.Context, states ...State) ([]*Alarm, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	want := make(map[State]bool, len(states))
	for _, s := range states {
		want[s] = true
	}
	out := make([]*Alarm, 0, len(m.byKey))
	for _, a := range m.byKey {
		if len(want) > 0 && !want[a.State] {
			continue
		}
		out = append(out, a.clone())
	}
	return out, nil
}

// Age 返回处于当前状态的时长（at 时刻）。
func (a *Alarm) Age(at time.Time) time.Duration { return at.Sub(a.StateTS) }

// SinceNotify 返回距上次通知的时长。
func (a *Alarm) SinceNotify(at time.Time) time.Duration {
	if a.NotifiedTS.IsZero() {
		return 0
	}
	return at.Sub(a.NotifiedTS)
}
