CREATE TABLE t_quota_usage (
    project_id   BIGINT NOT NULL REFERENCES t_project(id) ON DELETE CASCADE,
    metric       TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    report_id    TEXT NOT NULL,
    delta        BIGINT NOT NULL,
    node_id       TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, metric, report_id),
    CONSTRAINT ck_quota_metric CHECK (metric <> ''),
    CONSTRAINT ck_quota_delta CHECK (delta > 0)
);

CREATE INDEX idx_quota_usage_project_window
    ON t_quota_usage(project_id, metric, window_start DESC);

ALTER TABLE t_quota_usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_quota_usage FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_quota_usage ON t_quota_usage
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
