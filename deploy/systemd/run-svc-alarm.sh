#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then
  set -a
  . /home/xfusion/etc/odoo20iot.env
  set +a
fi

exec /home/xfusion/projects/odoo20iot/bin/svc-alarm \
  -dsn "${IOT_PG_DSN:-postgres://iot:iot_dev_only_change_me@100.64.0.3:28543/odoo20iot}" \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -http-addr "${IOT_ALARM_HTTP_ADDR:-127.0.0.1:18092}" \
  -scan-interval "${IOT_ALARM_SCAN_INTERVAL:-5s}" \
  -log-format "${IOT_LOG_FORMAT:-json}"
