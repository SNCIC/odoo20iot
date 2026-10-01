-- 告警活跃索引表（04 §2.2 / §2.5）。
--
-- ⚠️ 为什么不是直接写文档里的 t_alarm：t_alarm 按 first_ts RANGE 分区，而 02 §3.4
-- 自己写明「分区表上的唯一索引只保证**分区内**唯一」「跨分区/跨时间的全局唯一必须用
-- 独立非分区表」。可 04 §2.2 要求「同一设备同一规则只允许一个活跃告警」——
-- 落在分区表上，跨月重叠就会放过第二条。故活跃索引单独成表：
-- 它同时是唯一约束的载体、CAS 的载体，以及 04 §2.5 说的「启动时从 PG 重建」的来源。
--
-- 分工：本表是 FSM 的**权威当前状态**（hot）；t_alarm 是记录/查询/Odoo 回链（warm）。
-- 两者必须同事务写入，否则会出现「索引说 active、记录说 detected」这种最难查的分叉。
--
-- ⚠️ 标识口径：文档 t_alarm 的 id/project_id/device_id/rule_id 是 BIGINT 代理键，
-- 本表一律用 TEXT。因为 04 §2.4 自己就把 rule_id 建模成字符串（"ar_001"），
-- 且引擎把标识当**不透明字符串**用；用 BIGINT 会逼引擎在推进状态前先去 join 出
-- 代理键，把告警状态机绑死在表布局上。id 仍由序列生成（单调、唯一），只是格式化为文本。

-- 告警 id 序列。用独立序列而不是 IDENTITY：t_alarm（分区记录表）要与本表共用同一批 id。
CREATE SEQUENCE t_alarm_id_seq AS BIGINT START 1;

CREATE TABLE t_alarm_active (
    dedup_key       TEXT        PRIMARY KEY,
    -- id 由数据库生成并回传（RETURNING）：比在 Go 侧生成再回写更省一轮，
    -- 也避免了「两个实例生成同一个 id」这种只能靠唯一约束才发现的问题。
    id              TEXT        NOT NULL DEFAULT ('alarm-' || nextval('t_alarm_id_seq')),
    project_id      TEXT        NOT NULL,
    device_id       TEXT        NOT NULL,
    device_type_id  BIGINT      NOT NULL DEFAULT 0,
    rule_id         TEXT        NOT NULL,
    rule_name       TEXT        NOT NULL DEFAULT '',
    level           TEXT        NOT NULL,
    state           TEXT        NOT NULL,
    -- 根因抑制（04 §2.2）要按父告警 id 反查，故与 id 同为 TEXT。
    parent_id       TEXT,
    timing          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    first_ts        TIMESTAMPTZ NOT NULL,
    last_ts         TIMESTAMPTZ NOT NULL,
    state_ts        TIMESTAMPTZ NOT NULL,
    confirmed_ts    TIMESTAMPTZ,
    notified_ts     TIMESTAMPTZ,
    resolved_ts     TIMESTAMPTZ,
    closed_ts       TIMESTAMPTZ,
    notify_count    INT         NOT NULL DEFAULT 0,
    flap_count      INT         NOT NULL DEFAULT 0,
    suppressed      BOOLEAN     NOT NULL DEFAULT false,
    suppress_reason TEXT        NOT NULL DEFAULT '',
    batch_id        TEXT        NOT NULL DEFAULT '',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 定时扫描（04 §2.1，每 5s）按状态取候选，故这是扫描路径的主索引：
-- 没有它，每轮扫描都是全表顺扫，活跃告警一多就把 PG 打成瓶颈。
CREATE INDEX idx_alarm_active_state ON t_alarm_active(state, state_ts);

-- 根因抑制要按父告警 id 反查（04 §2.2）。父告警是少数，故用部分索引。
CREATE INDEX idx_alarm_active_parent ON t_alarm_active(parent_id) WHERE parent_id IS NOT NULL;

-- 控制台按租户 + 状态 + 级别筛。
CREATE INDEX idx_alarm_active_project ON t_alarm_active(project_id, state, level);

-- ⚠️ state 白名单用 CHECK 表达，而不是靠应用层自觉：
-- 04 §2.1 的五态是状态机的定义域，写进一个拼错的 state 会让扫描**永远看不见**这条
-- 告警（它既不在 detected 也不在 active），属于「告警安静地消失」的故障。
-- 刻意不含 idle：idle 表示「无告警」，由**行不存在**表达，不是一种可持久化的状态。
ALTER TABLE t_alarm_active ADD CONSTRAINT ck_alarm_state
    CHECK (state IN ('detected', 'confirmed', 'active', 'resolved'));

-- ⚠️ 时间字段的先后关系也进约束：last_ts 早于 first_ts 说明写入路径搞混了变量，
-- 这类错误在报表里表现为「确认时长为负」，很难倒查到写入点。
ALTER TABLE t_alarm_active ADD CONSTRAINT ck_alarm_ts_order
    CHECK (last_ts >= first_ts AND state_ts >= first_ts);
