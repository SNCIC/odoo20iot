-- 预聚合水位账本（B1 补充项（2）· 02 §7）。
--
-- 为什么必须落库：rollup 是**定时增量**任务，必须记住「已经物化到哪了」。
-- 水位不能只放进程内存（重启即忘，会从有界回看点重扫）；也不该放 Redis ——
-- 按 02 §5.3 的原则「Redis 是加速层，不是事实层」，水位丢了就得靠「写入幂等」
-- 兜底重算。放 PG 与调度用的咨询锁同库，便于「写成功后推进」的清晰时序。
--
-- 语义：window_start 是**已物化窗口的开区间下界** —— 严格小于它的原始数据已入聚合，
-- 下一轮从它开始。窗口半开 [window_start, closed_end)，相邻窗口平铺不重不漏。
CREATE TABLE t_rollup_watermark (
    granularity  TEXT        PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- last_error 是最近一次物化失败的原因（成功时清空）。只用于排障与 /metrics，
    -- **不参与调度**；且 RecordError 只 UPDATE 不 INSERT，避免用假水位污染账本。
    last_error   TEXT        NOT NULL DEFAULT '',
    CONSTRAINT ck_rollup_granularity CHECK (granularity IN ('1m', '1h'))
);
