-- DLQ 运维闭环：记录实体类型、重放次数与最终处理状态。
ALTER TABLE t_dlq ADD COLUMN IF NOT EXISTS entity_type TEXT NOT NULL DEFAULT '';
ALTER TABLE t_dlq ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT '';
ALTER TABLE t_dlq ADD COLUMN IF NOT EXISTS replay_count INT NOT NULL DEFAULT 0;
ALTER TABLE t_dlq ADD COLUMN IF NOT EXISTS last_replayed_at TIMESTAMPTZ;
ALTER TABLE t_dlq ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_dlq_pending_replay
    ON t_dlq(service, created_at DESC) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_dlq_entity_group
    ON t_dlq(service, entity_type, subject, created_at DESC);
