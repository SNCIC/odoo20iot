package dlq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Handler 重放一条死信。处理器必须沿用 Record.IdempotencyKey，
// 由下游幂等账本保证重放不会重复写入业务数据。
type Handler func(context.Context, Record) error

type replayStore interface {
	ListPending(context.Context, string, string, int) ([]Record, error)
	MarkReplayed(context.Context, int64, time.Time, bool, string) error
}

type projectReplayStore interface {
	ListPendingForProject(context.Context, int64, string, string, int) ([]Record, error)
	MarkReplayedForProject(context.Context, int64, int64, time.Time, bool, string) error
}

// Replayer 按 entity_type 分发死信重放。
type Replayer struct {
	store       replayStore
	mu          sync.RWMutex
	handlers    map[string]Handler
	maxAttempts int
}

type ReplayResult struct {
	Scanned   int
	Succeeded int
	Failed    int
	Skipped   int
}

func NewReplayer(store replayStore, maxAttempts int) (*Replayer, error) {
	if store == nil {
		return nil, fmt.Errorf("dlq: 重放器需要存储")
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	return &Replayer{store: store, handlers: make(map[string]Handler), maxAttempts: maxAttempts}, nil
}

func (r *Replayer) Register(entityType string, handler Handler) error {
	if entityType == "" {
		return fmt.Errorf("dlq: entity_type 不能为空")
	}
	if handler == nil {
		return fmt.Errorf("dlq: handler 不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[entityType]; exists {
		return fmt.Errorf("dlq: entity_type 已注册: %s", entityType)
	}
	r.handlers[entityType] = handler
	return nil
}

func (r *Replayer) Replay(ctx context.Context, service, subject string, limit int) (ReplayResult, error) {
	rows, err := r.store.ListPending(ctx, service, subject, limit)
	if err != nil {
		return ReplayResult{}, err
	}
	result := ReplayResult{Scanned: len(rows)}
	for _, row := range rows {
		r.mu.RLock()
		h := r.handlers[row.EntityType]
		r.mu.RUnlock()
		if h == nil {
			result.Skipped++
			if err := r.store.MarkReplayed(ctx, row.ID, row.CreatedAt, false, "没有匹配的 DLQ 重放处理器"); err != nil {
				return result, err
			}
			continue
		}
		if row.ReplayCount >= r.maxAttempts {
			result.Skipped++
			continue
		}
		if err := h(ctx, row); err != nil {
			result.Failed++
			if markErr := r.store.MarkReplayed(ctx, row.ID, row.CreatedAt, false, err.Error()); markErr != nil {
				return result, markErr
			}
			continue
		}
		result.Succeeded++
		if err := r.store.MarkReplayed(ctx, row.ID, row.CreatedAt, true, ""); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (r *Replayer) ReplayForProject(ctx context.Context, projectID int64, service, subject string, limit int) (ReplayResult, error) {
	store, ok := r.store.(projectReplayStore)
	if !ok {
		return ReplayResult{}, fmt.Errorf("dlq: 存储不支持租户重放")
	}
	rows, err := store.ListPendingForProject(ctx, projectID, service, subject, limit)
	if err != nil {
		return ReplayResult{}, err
	}
	result := ReplayResult{Scanned: len(rows)}
	mark := func(row Record, success bool, reason string) error {
		return store.MarkReplayedForProject(ctx, projectID, row.ID, row.CreatedAt, success, reason)
	}
	for _, row := range rows {
		r.mu.RLock()
		h := r.handlers[row.EntityType]
		r.mu.RUnlock()
		if h == nil {
			result.Skipped++
			if err := mark(row, false, "没有匹配的 DLQ 重放处理器"); err != nil {
				return result, err
			}
			continue
		}
		if row.ReplayCount >= r.maxAttempts {
			result.Skipped++
			continue
		}
		if err := h(ctx, row); err != nil {
			result.Failed++
			if markErr := mark(row, false, err.Error()); markErr != nil {
				return result, markErr
			}
			continue
		}
		result.Succeeded++
		if err := mark(row, true, ""); err != nil {
			return result, err
		}
	}
	return result, nil
}
