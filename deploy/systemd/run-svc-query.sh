#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then
  set -a
  . /home/xfusion/etc/odoo20iot.env
  set +a
fi
exec /home/xfusion/projects/odoo20iot/bin/svc-query \
  -dsn "${IOT_GREPTIME_DSN:-postgres://greptime:greptime@100.64.0.3:28403/public}" \
  -pg-dsn "${IOT_PG_DSN:-postgres://iot_app:iot_app_dev_only_change_me@100.64.0.3:28543/odoo20iot}" \
  -http-addr "${IOT_QUERY_HTTP_ADDR:-127.0.0.1:18094}" \
  -auth-mode "${IOT_QUERY_AUTH_MODE:-dev}" \
  -dev-token "${IOT_QUERY_DEV_TOKEN:-}" \
  -dev-project-id "${IOT_QUERY_DEV_PROJECT_ID:-1}" \
  -allow-dev-lan="${IOT_QUERY_ALLOW_DEV_LAN:-true}" \
  -latest-redis-url "${IOT_LATEST_REDIS_URL:-redis://100.64.0.3:28637/0}" \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -command-origin-id "${IOT_COMMAND_ORIGIN_ID:-}" \
  -log-format "${IOT_LOG_FORMAT:-json}"
