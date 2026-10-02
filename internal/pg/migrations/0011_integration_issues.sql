CREATE TABLE IF NOT EXISTS t_integration_issue (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT,
    ext_system TEXT NOT NULL,
    ext_model TEXT NOT NULL,
    ext_id BIGINT NOT NULL,
    issue_type TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    CONSTRAINT uk_integration_issue UNIQUE (ext_system, ext_model, ext_id, issue_type)
);
CREATE INDEX IF NOT EXISTS idx_integration_issue_open
    ON t_integration_issue(status, last_seen_at DESC) WHERE status = 'open';
