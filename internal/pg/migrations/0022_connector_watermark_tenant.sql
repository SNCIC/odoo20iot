-- 将已存在的旧水位表迁移为按租户 + 模型隔离。
ALTER TABLE t_connector_watermark
    ADD COLUMN IF NOT EXISTS project_id BIGINT;

UPDATE t_connector_watermark
SET project_id = 0
WHERE project_id IS NULL;

ALTER TABLE t_connector_watermark
    ALTER COLUMN project_id SET DEFAULT 0,
    ALTER COLUMN project_id SET NOT NULL;

ALTER TABLE t_connector_watermark
    DROP CONSTRAINT IF EXISTS t_connector_watermark_pkey;

ALTER TABLE t_connector_watermark
    ADD CONSTRAINT t_connector_watermark_pkey PRIMARY KEY (project_id, model);
