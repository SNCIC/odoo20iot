-- 控制面主数据**最小集**（02 §3.1/§3.2/§3.3）。
--
-- 本批只建读侧真正需要的三张表：租户、设备类型、设备台账。
-- 其余（t_user/t_role/t_api_token/t_thing_model/t_alarm_rule/t_external_ref/
-- t_idem_registry …）仍缺，见 09 §5.1。
--
-- ⚠️ 两处与 02 §3.3 的**有意偏离**，都在同一批里回写了文档：
--
-- 1) `t_device` 的主键改成 `(id, project_id)`。02 §3.3 原写的是
--    `id BIGINT PRIMARY KEY` + `PARTITION BY HASH (project_id)` —— 这在 PostgreSQL 里
--    **是非法 DDL**：分区表上的唯一/PK 必须包含全部分区键列（§3.4 自己写了这条规则）。
--    之所以现在就分区而不是以后再说：PG 没有 `ALTER TABLE … PARTITION BY`，
--    等表长到 10⁷ 行再改要整表重写。
--
-- 2) **不启用 Row Level Security**。§3.1 要求租户表开 RLS（策略依赖
--    `current_setting('app.project_id')`），但仓库里没有任何 `SET LOCAL app.project_id`
--    的连接池管线；只 `ENABLE` 而不建策略 = **默认拒绝** = 所有查询返回空行，
--    会直接打死这次要建的读路径。故本批隔离改由应用层强制：catalog 每个查询首参
--    `project_id`，`tsdb.SeriesQuery.Normalize` 拒绝 `ProjectID<=0`。
--    这是**遗留**（09 §5.1），不是「已完成」。

CREATE SEQUENCE t_project_id_seq AS BIGINT START 1;
CREATE SEQUENCE t_device_type_id_seq AS BIGINT START 1;
CREATE SEQUENCE t_device_id_seq AS BIGINT START 1;

-- 租户 / 项目。tenant 维度即 project_id（02 §5.2 的 tenant）。
CREATE TABLE t_project (
    id              BIGINT      PRIMARY KEY DEFAULT nextval('t_project_id_seq'),
    project_key     TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'active',
    -- odoo_company_id 权威来源是 t_external_ref（未建）；0 = 未绑定 Odoo，
    -- 与缓存键约定 cache:{tenant}:{company}:… 里 company=0 一致（02 §5.2）。
    odoo_company_id BIGINT      NOT NULL DEFAULT 0,
    timezone        TEXT        NOT NULL DEFAULT 'UTC',
    config          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    version         BIGINT      NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ,
    CONSTRAINT uk_project_key UNIQUE (project_key),
    CONSTRAINT ck_project_status CHECK (status IN ('active', 'suspended', 'archived'))
);

-- 设备类型。物模型直接内联在 JSONB 上：**不建 t_thing_model 表** ——
-- 目前没有任何按版本消费物模型的读者，建一张没人读的表是假系统（09 §5.1 遗留）。
CREATE TABLE t_device_type (
    id                  BIGINT      PRIMARY KEY DEFAULT nextval('t_device_type_id_seq'),
    project_id          BIGINT      NOT NULL REFERENCES t_project(id) ON DELETE RESTRICT,
    type_key            TEXT        NOT NULL,
    name                TEXT        NOT NULL,
    category            TEXT        NOT NULL DEFAULT '',
    thing_model         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    thing_model_version INT         NOT NULL DEFAULT 1,
    version             BIGINT      NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at          TIMESTAMPTZ,
    CONSTRAINT uk_device_type_key UNIQUE (project_id, type_key),
    -- 供 t_device 的**复合外键**使用：让「设备挂到别的租户的类型上」在 DB 层就不可能。
    CONSTRAINT uk_device_type_project_id UNIQUE (project_id, id)
);

-- 设备台账（02 §3.3）。字段与文档一致，仅主键按上面第 1) 条修正。
CREATE TABLE t_device (
    id                  BIGINT      NOT NULL DEFAULT nextval('t_device_id_seq'),
    project_id          BIGINT      NOT NULL,
    device_type_id      BIGINT      NOT NULL,
    -- device_key 是端侧唯一标识；租户内唯一（全局唯一由 (project_id, device_key) 表达）。
    device_key          TEXT        NOT NULL,
    name                TEXT        NOT NULL,
    -- Argon2id 摘要，编码形如 argon2id$t=3,m=32768,p=2$<b64salt>$<b64hash>；
    -- 由 internal/catalog.EncodeSecretHash 生成，**禁止明文**。
    secret_hash         TEXT        NOT NULL DEFAULT '',
    secret_version      INT         NOT NULL DEFAULT 1,
    auth_mode           TEXT        NOT NULL DEFAULT 'per_device',
    status              TEXT        NOT NULL DEFAULT 'inactive',
    online              BOOLEAN     NOT NULL DEFAULT false,
    last_seen_at        TIMESTAMPTZ,
    thing_model_version INT         NOT NULL DEFAULT 1,
    config_template_id  BIGINT,
    tags                JSONB       NOT NULL DEFAULT '{}'::jsonb,
    version             BIGINT      NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at          TIMESTAMPTZ,
    -- 02 §3.4：分区表上的唯一约束必须包含全部分区键列。
    CONSTRAINT pk_device PRIMARY KEY (id, project_id),
    CONSTRAINT uk_device_key UNIQUE (project_id, device_key),
    CONSTRAINT ck_device_auth CHECK (auth_mode IN ('project', 'per_device', 'mtls')),
    CONSTRAINT ck_device_status CHECK (status IN ('inactive', 'active', 'disabled')),
    -- 复合外键：设备与设备类型必须同租户（单列 FK 挡不住跨租户挂载）。
    CONSTRAINT fk_device_type FOREIGN KEY (project_id, device_type_id)
        REFERENCES t_device_type(project_id, id) ON DELETE RESTRICT
) PARTITION BY HASH (project_id);

-- 显式 16 个分区（不用 DO 块：显式更易 diff，且与 02 §3.4 的「16 路 HASH」对齐）。
CREATE TABLE t_device_p0  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE t_device_p1  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE t_device_p2  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE t_device_p3  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE t_device_p4  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE t_device_p5  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE t_device_p6  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE t_device_p7  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE t_device_p8  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE t_device_p9  PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE t_device_p10 PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE t_device_p11 PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE t_device_p12 PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE t_device_p13 PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE t_device_p14 PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE t_device_p15 PARTITION OF t_device FOR VALUES WITH (MODULUS 16, REMAINDER 15);

CREATE INDEX idx_device_project_type ON t_device(project_id, device_type_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_device_project_status ON t_device(project_id, status) WHERE deleted_at IS NULL;
CREATE INDEX idx_device_tags ON t_device USING GIN (tags jsonb_path_ops);
