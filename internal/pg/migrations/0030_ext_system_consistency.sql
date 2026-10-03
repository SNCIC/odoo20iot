UPDATE t_external_ref SET ext_system = 'odoo20tbb' WHERE ext_system = 'odoo';
UPDATE t_integration_issue SET ext_system = 'odoo20tbb' WHERE ext_system = 'odoo';

ALTER TABLE t_external_ref
    DROP CONSTRAINT IF EXISTS ck_external_ref_ext_system;
ALTER TABLE t_external_ref
    ADD CONSTRAINT ck_external_ref_ext_system CHECK (ext_system = 'odoo20tbb');

ALTER TABLE t_integration_issue
    DROP CONSTRAINT IF EXISTS ck_integration_issue_ext_system;
ALTER TABLE t_integration_issue
    ADD CONSTRAINT ck_integration_issue_ext_system CHECK (ext_system = 'odoo20tbb');
