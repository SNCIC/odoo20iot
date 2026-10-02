#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then
  set -a
  . /home/xfusion/etc/odoo20iot.env
  set +a
fi
if [[ -z "${IOT_NOTIFY_WEBHOOK_URL:-}" ]]; then
  echo 'IOT_NOTIFY_WEBHOOK_URL is required for Feishu notifications' >&2
  exit 78
fi
if [[ -z "${IOT_NOTIFY_EGRESS_ALLOW:-}" ]]; then
  echo 'IOT_NOTIFY_EGRESS_ALLOW is required for webhook SSRF protection' >&2
  exit 78
fi

exec /home/xfusion/projects/odoo20iot/bin/svc-notify \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -egress-allow "${IOT_NOTIFY_EGRESS_ALLOW}" \
  -log-format "${IOT_LOG_FORMAT:-json}"
