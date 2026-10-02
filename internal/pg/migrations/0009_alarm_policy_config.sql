-- 告警规则/静默窗口配置源。
-- svc-alarm 以服务身份读取全租户配置；事件写入仍通过 project_id 做业务隔离。
CREATE TABLE IF NOT EXISTS t_alarm_rule (
    project_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    rule_name TEXT NOT NULL DEFAULT '',
    level TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    notify JSONB NOT NULL DEFAULT '{}'::jsonb,
    escalation JSONB NOT NULL DEFAULT '{"notify_after_s":1800,"p1_after_s":7200}'::jsonb,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, rule_id)
);

CREATE TABLE IF NOT EXISTS t_alarm_silence (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL DEFAULT '',
    device_type_id BIGINT NOT NULL DEFAULT 0,
    device_id TEXT NOT NULL DEFAULT '',
    rule_id TEXT NOT NULL DEFAULT '',
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT true,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (end_at > start_at)
);
CREATE INDEX IF NOT EXISTS idx_alarm_silence_active ON t_alarm_silence(start_at, end_at) WHERE enabled;
