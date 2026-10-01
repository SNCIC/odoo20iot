-- 告警事件的发布标记（可空 = 尚未发布）。
--
-- 为什么需要它：扫描把告警推进到 active 之后，若 NATS 发布失败，这条告警
-- **永远不会再发** —— 状态已是 active，之后的重复触发都被抑制（04 §2.2），
-- 而 Tick 只推进与时间有关的迁移。结果就是工单静默丢失，
-- 只在日志里留一行，没人会去看。
--
-- 有了这个标记，发布就变成「至少一次」：发布成功才写 published_at，
-- 没写上的下一轮扫描会补发。重复投递由下游兜住 ——
-- 07 §6 S3 本来就为这件事定义了幂等键
-- `idem:{tenant}:maintenance_req:eq-{eq}-{alarm_dedup_key}`，
-- 且「同 device + 同 rule 在 10 min 内合并为一单」。
--
-- 进入 active 时由引擎重置为 NULL（含抖动后重新 active 的情况），
-- 否则第二次告警会因为没有新工单而静默丢失。
ALTER TABLE t_alarm_active
    ADD COLUMN published_at TIMESTAMPTZ;

-- 补发扫描按「活跃但未发布」取行，故用部分索引 —— 正常状态下它几乎全为空。
CREATE INDEX idx_alarm_active_unpublished ON t_alarm_active(state, state_ts)
    WHERE state = 'active' AND published_at IS NULL;
