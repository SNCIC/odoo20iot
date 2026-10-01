-- 补齐触发取值快照（02 §3.3 的 t_alarm.trigger_value）。
--
-- 为什么是**新迁移**而不是改 0001：0001 已应用到开发库 `iot`，
-- 而迁移器的校验和检查会直接拒绝改动已应用的迁移 —— 那是有意的，
-- 「本地改了旧迁移、线上还是老 schema」正是最难查的一类分叉。
--
-- 为什么需要它：07 §6 S3 的告警→Odoo 工单契约要求携带 iot_metric_snapshot，
-- 工单上必须能看到「当时是什么值触发的」；只留时间戳的话，运维到现场
-- 已经看不到触发时的现场了。
ALTER TABLE t_alarm_active
    ADD COLUMN trigger_value JSONB NOT NULL DEFAULT '{}'::jsonb;
