CREATE TABLE IF NOT EXISTS t_ota_firmware (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL REFERENCES t_project(id) ON DELETE CASCADE,
    version TEXT NOT NULL,
    filename TEXT NOT NULL,
    object_key TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    signature TEXT NOT NULL,
    signing_key_id TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, version),
    UNIQUE (project_id, id),
    CONSTRAINT ck_ota_firmware_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);
ALTER TABLE t_ota_firmware ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_ota_firmware FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_ota_firmware ON t_ota_firmware;
CREATE POLICY tenant_ota_firmware ON t_ota_firmware
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
GRANT SELECT, INSERT, UPDATE ON t_ota_firmware TO iot_app;
GRANT USAGE, SELECT ON SEQUENCE t_ota_firmware_id_seq TO iot_app;

CREATE TABLE IF NOT EXISTS t_ota_task (
    id UUID PRIMARY KEY,
    project_id BIGINT NOT NULL REFERENCES t_project(id) ON DELETE CASCADE,
    firmware_id BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'running', 'paused', 'completed', 'cancelled')),
    rollout JSONB NOT NULL DEFAULT '{"batches":[1,10,50,100],"success_threshold":0.98}'::jsonb,
    offline_ttl INTERVAL NOT NULL DEFAULT '24 hours',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT fk_ota_task_firmware FOREIGN KEY (project_id, firmware_id)
        REFERENCES t_ota_firmware(project_id, id),
    UNIQUE (project_id, id),
    CONSTRAINT ck_ota_task_rollout_object CHECK (jsonb_typeof(rollout) = 'object')
);
ALTER TABLE t_ota_task ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_ota_task FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_ota_task ON t_ota_task;
CREATE POLICY tenant_ota_task ON t_ota_task
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
GRANT SELECT, INSERT, UPDATE ON t_ota_task TO iot_app;

CREATE TABLE IF NOT EXISTS t_ota_task_device (
    task_id UUID NOT NULL,
    project_id BIGINT NOT NULL REFERENCES t_project(id) ON DELETE CASCADE,
    device_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'notified', 'downloading', 'verifying', 'installing', 'succeeded', 'failed', 'expired', 'rolled_back')),
    progress SMALLINT NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
    bytes_downloaded BIGINT NOT NULL DEFAULT 0 CHECK (bytes_downloaded >= 0),
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    last_seen_version TEXT NOT NULL DEFAULT '',
    notified_at TIMESTAMPTZ,
    progress_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, device_key),
    CONSTRAINT fk_ota_task_device_task FOREIGN KEY (project_id, task_id)
        REFERENCES t_ota_task(project_id, id) ON DELETE CASCADE,
    CONSTRAINT fk_ota_task_device_device FOREIGN KEY (project_id, device_key)
        REFERENCES t_device(project_id, device_key) ON DELETE CASCADE
);
ALTER TABLE t_ota_task_device ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_ota_task_device FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_ota_task_device ON t_ota_task_device;
CREATE POLICY tenant_ota_task_device ON t_ota_task_device
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
GRANT SELECT, INSERT, UPDATE ON t_ota_task_device TO iot_app;

CREATE INDEX IF NOT EXISTS idx_ota_task_device_pending ON t_ota_task_device(project_id, status, updated_at);
