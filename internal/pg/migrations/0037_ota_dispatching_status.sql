ALTER TABLE t_ota_task_device DROP CONSTRAINT IF EXISTS t_ota_task_device_status_check;
ALTER TABLE t_ota_task_device DROP CONSTRAINT IF EXISTS ck_ota_task_device_status;
ALTER TABLE t_ota_task_device ADD CONSTRAINT ck_ota_task_device_status
    CHECK (status IN ('pending', 'dispatching', 'notified', 'downloading', 'verifying', 'installing', 'succeeded', 'failed', 'expired', 'rolled_back'));
