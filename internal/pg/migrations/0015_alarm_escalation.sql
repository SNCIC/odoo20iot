-- Persist notification escalation progress so restarts and replicas do not
-- resend the same unacknowledged-alarm escalation.
ALTER TABLE t_alarm_active
    ADD COLUMN IF NOT EXISTS escalation_stage INT NOT NULL DEFAULT 0;
ALTER TABLE t_alarm_active
    ADD CONSTRAINT ck_alarm_escalation_stage CHECK (escalation_stage >= 0 AND escalation_stage <= 2);
CREATE INDEX IF NOT EXISTS idx_alarm_active_escalation
    ON t_alarm_active(state, notified_ts, escalation_stage)
    WHERE state = 'active';
