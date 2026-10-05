-- 语音是可选的 HTTP 语音网关通道；号码仍由应用层加密保存。
ALTER TABLE t_notification_endpoint
    DROP CONSTRAINT IF EXISTS ck_notification_channel;
ALTER TABLE t_notification_endpoint
    ADD CONSTRAINT ck_notification_channel
    CHECK (channel IN ('webhook','email','sms','voice'));
