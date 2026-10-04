#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then
  set -a
  . /home/xfusion/etc/odoo20iot.env
  set +a
fi
exec /home/xfusion/projects/odoo20iot/bin/svc-rule \
  -pg-dsn "${IOT_PG_DSN:-postgres://iot_app:iot_app_dev_only_change_me@100.64.0.3:28543/odoo20iot}" \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -redis-url "${IOT_REDIS_URL:-redis://100.64.0.3:28637/0}" \
  -history-dsn "${IOT_GREPTIME_DSN:-postgres://greptime:greptime@100.64.0.3:28403/public}" \
  -script-enabled="${IOT_RULE_SCRIPT_ENABLED:-false}" \
  -command-origin-id "${IOT_COMMAND_ORIGIN_ID:-}" \
  -log-format "${IOT_LOG_FORMAT:-json}"
