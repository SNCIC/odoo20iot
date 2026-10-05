-- Modbus 控制面 API 需要写配置；0043 已应用的数据库通过本迁移补齐权限。
GRANT SELECT, INSERT, UPDATE, DELETE ON t_modbus_poll_config TO iot_app;
GRANT USAGE, SELECT ON SEQUENCE t_modbus_poll_config_id_seq TO iot_app;
