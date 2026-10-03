-- 为 svc-rule 补齐可版本化规则运行时契约。
-- 旧字段保持兼容；规则仍由 enabled/version/effective_from 控制发布。
ALTER TABLE t_alarm_rule
    ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 100,
    ADD COLUMN IF NOT EXISTS "match" JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS "window" JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS dag JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS error_policy TEXT NOT NULL DEFAULT 'drop',
    ADD COLUMN IF NOT EXISTS effective_from TIMESTAMPTZ;

ALTER TABLE t_alarm_rule
    DROP CONSTRAINT IF EXISTS ck_alarm_rule_error_policy;
ALTER TABLE t_alarm_rule
    ADD CONSTRAINT ck_alarm_rule_error_policy
    CHECK (error_policy IN ('drop', 'retry', 'dlq'));

CREATE INDEX IF NOT EXISTS idx_alarm_rule_runtime_active
    ON t_alarm_rule(project_id, enabled, effective_from, priority, rule_id);
