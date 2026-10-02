-- 告警活跃表的所有访问均已改为显式租户事务后，强制执行租户策略。
ALTER TABLE t_alarm_active FORCE ROW LEVEL SECURITY;
