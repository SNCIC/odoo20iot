CREATE TABLE IF NOT EXISTS t_command (
    project_id BIGINT NOT NULL REFERENCES t_project(id) ON DELETE CASCADE,
    command_id TEXT NOT NULL,
    device_key TEXT NOT NULL,
    command_key TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    acked_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, command_id),
    UNIQUE (project_id, correlation_id),
    CONSTRAINT ck_command_status CHECK (status IN ('pending','queued','sent','acked','timeout','failed'))
);
CREATE INDEX IF NOT EXISTS idx_command_device_status ON t_command(project_id, device_key, status, updated_at);
ALTER TABLE t_command ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_command FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_command ON t_command;
CREATE POLICY tenant_command ON t_command
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
GRANT SELECT, INSERT, UPDATE ON t_command TO iot_app;
