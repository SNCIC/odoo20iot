-- t_dlq 的所有访问必须显式绑定 project_id。
-- project_id=0 是租户传播改造前的兼容作用域，不代表任一真实租户。
ALTER TABLE t_dlq ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_dlq FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_dlq ON t_dlq;
CREATE POLICY tenant_dlq ON t_dlq
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
