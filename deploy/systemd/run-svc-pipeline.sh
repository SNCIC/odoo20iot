#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then
  set -a
  . /home/xfusion/etc/odoo20iot.env
  set +a
fi
exec /home/xfusion/projects/odoo20iot/bin/svc-pipeline \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -redis-url "${IOT_REDIS_URL:-redis://100.64.0.3:28637/0}" \
  -stream "${IOT_TELEMETRY_STREAM:-IOT_TELEMETRY}" \
  -subject "${IOT_TELEMETRY_SUBJECT:-iot.telemetry.>}" \
  -durable "${IOT_PIPELINE_DURABLE:-svc-pipeline}" \
  -dsn "${IOT_GREPTIME_DSN:-postgres://greptime:greptime@100.64.0.3:28403/public}" \
  -batch-size "${IOT_PIPELINE_BATCH_SIZE:-1000}" \
  -batch-wait "${IOT_PIPELINE_BATCH_WAIT:-200ms}" \
  -concurrency "${IOT_PIPELINE_CONCURRENCY:-512}" \
  -max-ack-pending "${IOT_PIPELINE_MAX_ACK_PENDING:-2000}" \
  -ack-wait "${IOT_PIPELINE_ACK_WAIT:-30s}" \
  -log-json "${IOT_PIPELINE_LOG_JSON:-true}"
