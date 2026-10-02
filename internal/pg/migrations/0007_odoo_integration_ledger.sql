-- Odoo 集成基础账本：外部引用、集成日志、IoT 侧幂等注册表。
-- 本迁移暂不启用 RLS；连接池 SET LOCAL app.project_id 管线完成后再启用，避免默认拒绝现有读路径。
CREATE SEQUENCE IF NOT EXISTS t_external_ref_id_seq AS BIGINT START 1;
CREATE SEQUENCE IF NOT EXISTS t_integration_log_id_seq AS BIGINT START 1;

CREATE TABLE IF NOT EXISTS t_external_ref (
    id BIGINT PRIMARY KEY DEFAULT nextval('t_external_ref_id_seq'),
    project_id BIGINT NOT NULL,
    odoo_company_id BIGINT,
    ext_system TEXT NOT NULL,
    ext_model TEXT NOT NULL,
    ext_id BIGINT NOT NULL,
    ext_version TEXT,
    local_entity TEXT NOT NULL,
    local_id BIGINT NOT NULL,
    bind_source TEXT NOT NULL,
    bind_confidence SMALLINT NOT NULL DEFAULT 100 CHECK (bind_confidence BETWEEN 0 AND 100),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'conflict', 'orphan')),
    synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uk_external_ref_remote UNIQUE (project_id, ext_system, ext_model, ext_id),
    CONSTRAINT uk_external_ref_local UNIQUE (project_id, ext_system, local_entity, local_id)
);
CREATE INDEX IF NOT EXISTS idx_external_ref_status
    ON t_external_ref(project_id, status) WHERE status <> 'active';

CREATE TABLE IF NOT EXISTS t_integration_log (
    id BIGINT NOT NULL DEFAULT nextval('t_integration_log_id_seq'),
    project_id BIGINT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('odoo_to_iot', 'iot_to_odoo')),
    channel TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id BIGINT,
    ext_model TEXT,
    ext_id BIGINT,
    idem_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ABSENT', 'PROCESSING', 'SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL')),
    request JSONB,
    response JSONB,
    error TEXT,
    trace_id TEXT,
    duration_ms INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS idx_integration_log_key ON t_integration_log(project_id, idem_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_integration_log_entity ON t_integration_log(project_id, entity_type, entity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS t_idem_registry (
    project_id BIGINT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('command', 'event', 'alarm_notify', 'integration')),
    idem_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ABSENT', 'PROCESSING', 'SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL')),
    result JSONB,
    error TEXT,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, scope, idem_key)
);
CREATE INDEX IF NOT EXISTS idx_idem_registry_expiry
    ON t_idem_registry(expires_at) WHERE expires_at IS NOT NULL;

CREATE OR REPLACE FUNCTION ensure_month_partition_integration(at_ts TIMESTAMPTZ)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    start_ts TIMESTAMPTZ := date_trunc('month', at_ts);
    end_ts TIMESTAMPTZ := start_ts + INTERVAL '1 month';
    part TEXT := 't_integration_log_' || to_char(start_ts, 'YYYYMM');
BEGIN
    IF to_regclass(part) IS NULL THEN
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF t_integration_log FOR VALUES FROM (%L) TO (%L)', part, start_ts, end_ts);
    END IF;
    RETURN part;
END;
$$;

SELECT ensure_month_partition_integration(now() - INTERVAL '1 month');
SELECT ensure_month_partition_integration(now());
SELECT ensure_month_partition_integration(now() + INTERVAL '1 month');
