ALTER TABLE t_ota_task
    ADD COLUMN IF NOT EXISTS rollback_of UUID,
    ADD COLUMN IF NOT EXISTS rollback_reason TEXT NOT NULL DEFAULT '';

ALTER TABLE t_ota_task
    DROP CONSTRAINT IF EXISTS fk_ota_task_rollback_of;
ALTER TABLE t_ota_task
    ADD CONSTRAINT fk_ota_task_rollback_of
    FOREIGN KEY (project_id, rollback_of)
    REFERENCES t_ota_task(project_id, id);

CREATE INDEX IF NOT EXISTS idx_ota_task_rollback_of
    ON t_ota_task(project_id, rollback_of);
