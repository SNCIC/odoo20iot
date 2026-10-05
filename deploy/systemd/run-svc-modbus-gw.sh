#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then set -a; . /home/xfusion/etc/odoo20iot.env; set +a; fi
exec /home/xfusion/projects/odoo20iot/bin/svc-modbus-gw \
  -config "${IOT_MODBUS_CONFIG:-/home/xfusion/projects/odoo20iot/deploy/modbus/example.json}" \
  -config-source "${IOT_MODBUS_CONFIG_SOURCE:-file}" \
  -pg-dsn "${IOT_PG_DSN:-postgres://iot_app:iot_app_dev_only_change_me@100.64.0.3:28543/odoo20iot}" \
  -project-id "${IOT_MODBUS_PROJECT_ID:-0}" \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -nats-stream "${IOT_NATS_STREAM:-IOT_TELEMETRY}" \
  -reload-interval "${IOT_MODBUS_RELOAD_INTERVAL:-30s}"
