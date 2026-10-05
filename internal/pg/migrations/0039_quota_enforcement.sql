ALTER TABLE t_quota_policy
    ADD COLUMN IF NOT EXISTS enforcement_mode TEXT NOT NULL DEFAULT 'reject';

ALTER TABLE t_quota_policy
    DROP CONSTRAINT IF EXISTS ck_quota_policy_enforcement;
ALTER TABLE t_quota_policy
    ADD CONSTRAINT ck_quota_policy_enforcement
    CHECK (enforcement_mode IN ('reject', 'throttle'));
