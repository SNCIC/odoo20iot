-- Development defaults only. Production must override credentials through a
-- secret-managed init script and keep migration/app roles separate.
CREATE ROLE iot_app LOGIN PASSWORD 'iot_app_dev_only_change_me' NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT CONNECT ON DATABASE odoo20iot TO iot_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO iot_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO iot_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO iot_app;
