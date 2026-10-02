-- 第一步只保护已具备租户事务管线的新集成账本。
-- catalog 旧读写路径仍待逐个迁移到 pg.WithProjectTx 后再纳入 RLS。
ALTER TABLE t_external_ref ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_external_ref FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_external_ref ON t_external_ref;
CREATE POLICY tenant_external_ref ON t_external_ref
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);

ALTER TABLE t_integration_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_integration_log FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_integration_log ON t_integration_log;
CREATE POLICY tenant_integration_log ON t_integration_log
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);

ALTER TABLE t_idem_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_idem_registry FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_idem_registry ON t_idem_registry;
CREATE POLICY tenant_idem_registry ON t_idem_registry
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
