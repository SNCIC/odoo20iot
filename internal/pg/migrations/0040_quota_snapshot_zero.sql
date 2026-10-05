ALTER TABLE t_quota_usage
    DROP CONSTRAINT IF EXISTS ck_quota_delta;

ALTER TABLE t_quota_usage
    ADD CONSTRAINT ck_quota_delta CHECK (delta >= 0);
