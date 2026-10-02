-- 仅对已完成 pg.WithProjectTx 访问改造的租户表启用 FORCE RLS。
-- 目录与告警表仍保持 ENABLE RLS，待其全量读写路径迁入租户事务后再单独迁移。

ALTER TABLE t_notification_endpoint FORCE ROW LEVEL SECURITY;
ALTER TABLE t_audit_log FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_notification_endpoint ON t_notification_endpoint;
CREATE POLICY tenant_notification_endpoint ON t_notification_endpoint
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);

DROP POLICY IF EXISTS tenant_audit_log ON t_audit_log;
CREATE POLICY tenant_audit_log ON t_audit_log
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
