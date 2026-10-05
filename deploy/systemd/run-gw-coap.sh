#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then set -a; . /home/xfusion/etc/odoo20iot.env; set +a; fi
exec /home/xfusion/projects/odoo20iot/bin/gw-coap \
  -addr "${IOT_COAP_ADDR:-127.0.0.1:5683}" \
  -auth-file "${IOT_PROTOCOL_AUTH_FILE:-tmp/dev-credentials.json}" \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -nats-stream "${IOT_NATS_STREAM:-IOT_TELEMETRY}" \
  -project "${IOT_PROJECT:-spike}"
