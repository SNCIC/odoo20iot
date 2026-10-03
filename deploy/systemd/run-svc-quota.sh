#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then
  set -a
  . /home/xfusion/etc/odoo20iot.env
  set +a
fi
exec /home/xfusion/projects/odoo20iot/bin/svc-quota \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -redis-url "${IOT_REDIS_URL:-redis://100.64.0.3:28637/0}" \
  -log-json "${IOT_LOG_JSON:-true}"
