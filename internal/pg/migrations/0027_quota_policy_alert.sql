CREATE TABLE t_quota_policy (
    project_id BIGINT NOT NULL REFERENCES t_project(id) ON DELETE CASCADE,
    metric TEXT NOT NULL,
    soft_limit BIGINT NOT NULL DEFAULT 0,
    hard_limit BIGINT NOT NULL DEFAULT 0,
    window_kind TEXT NOT NULL DEFAULT 'day',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, metric),
    CONSTRAINT ck_quota_policy_metric CHECK (metric <> ''),
    CONSTRAINT ck_quota_policy_limits CHECK (soft_limit >= 0 AND hard_limit >= soft_limit),
    CONSTRAINT ck_quota_policy_window CHECK (window_kind IN ('day', 'month'))
);

CREATE TABLE t_quota_alert (
    project_id BIGINT NOT NULL REFERENCES t_project(id) ON DELETE CASCADE,
    metric TEXT NOT NULL,
    level TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    usage BIGINT NOT NULL,
    limit_value BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, metric, level, window_start),
    CONSTRAINT ck_quota_alert_level CHECK (level IN ('warning', 'critical'))
);

ALTER TABLE t_quota_policy ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_quota_policy FORCE ROW LEVEL SECURITY;
ALTER TABLE t_quota_alert ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_quota_alert FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_quota_policy ON t_quota_policy
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
CREATE POLICY tenant_quota_alert ON t_quota_alert
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
