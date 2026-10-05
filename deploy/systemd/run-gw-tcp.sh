#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then set -a; . /home/xfusion/etc/odoo20iot.env; set +a; fi
args=(
  -addr "${IOT_TCP_ADDR:-127.0.0.1:9001}"
  -auth-file "${IOT_PROTOCOL_AUTH_FILE:-tmp/dev-credentials.json}"
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}"
  -nats-stream "${IOT_NATS_STREAM:-IOT_TELEMETRY}"
  -project "${IOT_PROJECT:-spike}"
)
if [[ "${IOT_TCP_ALLOW_PLAINTEXT:-false}" == "true" ]]; then
  args+=( -allow-plaintext )
else
  args+=( -tls-cert "${IOT_TCP_TLS_CERT:?IOT_TCP_TLS_CERT is required}" -tls-key "${IOT_TCP_TLS_KEY:?IOT_TCP_TLS_KEY is required}" )
fi
exec /home/xfusion/projects/odoo20iot/bin/gw-tcp "${args[@]}"
