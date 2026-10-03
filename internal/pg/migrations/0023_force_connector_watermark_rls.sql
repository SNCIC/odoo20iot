ALTER TABLE t_connector_watermark ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_connector_watermark FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_connector_watermark ON t_connector_watermark;
CREATE POLICY tenant_connector_watermark ON t_connector_watermark
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
