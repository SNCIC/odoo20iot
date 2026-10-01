-- 死信表（04 §3.3 / 02 §3.3）。
--
-- 为什么必须落库而不是只记日志：日志会被轮转、会被采样、不会有人按
-- 「service=svc-notify 且 最近 7 天」去查。而没有这张表，
-- 「通知没送到人手上」这件事在系统里就**没有痕迹** —— 那正是最坏的情形：
-- 一条告警既没被处理，也没有留下「它没被处理」的记录。
--
-- 分区：按月 RANGE（02 §3.3），与 t_alarm / t_command 同一套约定。
CREATE SEQUENCE t_dlq_id_seq AS BIGINT START 1;

CREATE TABLE t_dlq (
    id         BIGINT      NOT NULL DEFAULT nextval('t_dlq_id_seq'),
    -- service 用于「按 service 聚合告警」（04 §3.3）。
    service    TEXT        NOT NULL,
    -- subject 是失败的通道 / 主题 / 实体类型，构成二级分类。
    subject    TEXT        NOT NULL DEFAULT '',
    reason     TEXT        NOT NULL,
    attempts   INT         NOT NULL DEFAULT 0,
    -- history 是重试历史（04 §3.3：DLQ 条目含重试历史）。
    history    JSONB       NOT NULL DEFAULT '[]'::jsonb,
    trace_id   TEXT        NOT NULL DEFAULT '',
    -- payload 是原文（对象存储未接入时的兜底；接上之后可只留引用）。
    payload    TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- ⚠️ 分区表的唯一约束必须包含分区键，故主键是 (id, created_at)
    -- （02 §3.4 的三条规则之一，写错会建表即报错）。
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- 按 service + 时间查（DLQ 运维界面与聚合告警都走这条）。
CREATE INDEX idx_dlq_service_time ON t_dlq(service, created_at DESC);
CREATE INDEX idx_dlq_trace ON t_dlq(trace_id) WHERE trace_id <> '';

-- 分区自动创建。02 §3.4 要求「提前 3 个月自动创建分区（定时任务 + 失败告警）」。
--
-- 这里做成**幂等函数**并在写入前调用（而不是只靠定时任务）：
-- 定时任务挂了、或者服务跨月那一刻正好在写死信，插入会直接失败 ——
-- 而死信写入失败意味着「既没送到人、也没留下记录」，
-- 是整条链路上最不能失败的一步。
CREATE OR REPLACE FUNCTION ensure_month_partition(parent TEXT, at TIMESTAMPTZ)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    start_ts TIMESTAMPTZ := date_trunc('month', at);
    end_ts   TIMESTAMPTZ := start_ts + INTERVAL '1 month';
    part     TEXT        := parent || '_' || to_char(start_ts, 'YYYYMM');
BEGIN
    IF to_regclass(part) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
            part, parent, start_ts, end_ts);
    END IF;
    RETURN part;
END;
$$;

-- 建好当月与前后各 3 个月；后续月份由上面的函数按需补。
SELECT ensure_month_partition('t_dlq', now() - INTERVAL '3 months');
SELECT ensure_month_partition('t_dlq', now() - INTERVAL '2 months');
SELECT ensure_month_partition('t_dlq', now() - INTERVAL '1 month');
SELECT ensure_month_partition('t_dlq', now());
SELECT ensure_month_partition('t_dlq', now() + INTERVAL '1 month');
SELECT ensure_month_partition('t_dlq', now() + INTERVAL '2 months');
SELECT ensure_month_partition('t_dlq', now() + INTERVAL '3 months');
