-- First close the unprotected integration-issue table. The remaining control
-- plane tables are intentionally not FORCE'd here: alarm and reconciliation
-- stores still need tenant-aware transaction plumbing before owner bypass can
-- be disabled without breaking existing reads.
ALTER TABLE t_integration_issue ENABLE ROW LEVEL SECURITY;
ALTER TABLE t_integration_issue FORCE ROW LEVEL SECURITY;

ALTER TABLE t_integration_issue DROP CONSTRAINT IF EXISTS uk_integration_issue;
ALTER TABLE t_integration_issue ALTER COLUMN project_id SET NOT NULL;
ALTER TABLE t_integration_issue ADD CONSTRAINT uk_integration_issue
    UNIQUE (project_id, ext_system, ext_model, ext_id, issue_type);

DROP POLICY IF EXISTS tenant_integration_issue ON t_integration_issue;
CREATE POLICY tenant_integration_issue ON t_integration_issue
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::BIGINT);
