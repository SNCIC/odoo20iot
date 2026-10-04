ALTER TABLE t_ota_task
    ADD COLUMN IF NOT EXISTS rollout_batch_index SMALLINT NOT NULL DEFAULT 0 CHECK (rollout_batch_index >= 0),
    ADD COLUMN IF NOT EXISTS dispatch_lease_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_ota_task_dispatch
    ON t_ota_task(project_id, status, dispatch_lease_until, created_at);
