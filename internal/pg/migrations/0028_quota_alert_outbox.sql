ALTER TABLE t_quota_alert
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_quota_alert_pending
    ON t_quota_alert(project_id, created_at)
    WHERE published_at IS NULL;
