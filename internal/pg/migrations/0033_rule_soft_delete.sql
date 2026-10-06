-- 规则删除使用软删除，保留审计与恢复所需的历史数据。
ALTER TABLE t_alarm_rule
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_alarm_rule_project_active
    ON t_alarm_rule(project_id, priority, rule_id)
    WHERE deleted_at IS NULL;
