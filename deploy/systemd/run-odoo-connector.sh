#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot
if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then
  set -a
  . /home/xfusion/etc/odoo20iot.env
  set +a
fi
if [[ -z "${ODOO_API_KEY:-}" ]]; then
  echo 'ODOO_API_KEY is required for odoo-connector' >&2
  exit 78
fi

exec /home/xfusion/projects/odoo20iot/bin/odoo-connector \
  -odoo-url "${ODOO_URL:-http://100.64.0.3:9070}" \
  -odoo-db "${ODOO_DB:-odoo20}" \
  -http-addr "${ODOO_CONNECTOR_HTTP_ADDR:-127.0.0.1:18091}" \
  -reconcile-models "${ODOO_RECONCILE_MODELS:-}" \
  -masterdata-model "${ODOO_MASTERDATA_MODEL:-}" \
  -alarm-to-odoo="${ODOO_ALARM_TO_ODOO:-true}" \
  -log-format "${IOT_LOG_FORMAT:-json}"
