ALTER TABLE t_quota_policy
    ADD COLUMN IF NOT EXISTS warning_limit BIGINT NOT NULL DEFAULT 0;

UPDATE t_quota_policy
SET warning_limit = (hard_limit / 100) * 90 + ((hard_limit % 100) * 90 + 99) / 100
WHERE hard_limit > 0 AND warning_limit = 0;

ALTER TABLE t_quota_policy
    DROP CONSTRAINT IF EXISTS ck_quota_policy_limits;
ALTER TABLE t_quota_policy
    ADD CONSTRAINT ck_quota_policy_limits
    CHECK (soft_limit >= 0 AND warning_limit >= soft_limit AND hard_limit >= warning_limit);

ALTER TABLE t_quota_alert
    DROP CONSTRAINT IF EXISTS ck_quota_alert_level;
ALTER TABLE t_quota_alert
    ADD CONSTRAINT ck_quota_alert_level CHECK (level IN ('info', 'warning', 'critical'));
