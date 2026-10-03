ALTER TABLE t_dlq
    ADD COLUMN IF NOT EXISTS project_id BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_dlq_project_service_time
    ON t_dlq(project_id, service, created_at DESC);
