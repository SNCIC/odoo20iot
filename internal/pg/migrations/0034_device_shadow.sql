CREATE TABLE IF NOT EXISTS t_device_shadow (
    project_id BIGINT NOT NULL REFERENCES t_project(id) ON DELETE CASCADE,
    device_key TEXT NOT NULL,
    desired JSONB NOT NULL DEFAULT '{}'::jsonb,
    reported JSONB NOT NULL DEFAULT '{}'::jsonb,
    version BIGINT NOT NULL DEFAULT 0,
    desired_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reported_updated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, device_key),
    CONSTRAINT ck_device_shadow_desired_object CHECK (jsonb_typeof(desired) = 'object'),
    CONSTRAINT ck_device_shadow_reported_object CHECK (jsonb_typeof(reported) = 'object'),
    CONSTRAINT ck_device_shadow_size CHECK (octet_length(desired::text) + octet_length(reported::text) <= 8192)
);
ALTER TABLE t_device_shadow ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_device_shadow FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_device_shadow ON t_device_shadow;
CREATE POLICY tenant_device_shadow ON t_device_shadow
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
GRANT SELECT, INSERT, UPDATE ON t_device_shadow TO iot_app;
