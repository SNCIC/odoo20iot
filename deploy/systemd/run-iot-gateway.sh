#!/usr/bin/env bash
set -euo pipefail
cd /home/xfusion/projects/odoo20iot

if [[ -f /home/xfusion/etc/odoo20iot.env ]]; then
  set -a
  . /home/xfusion/etc/odoo20iot.env
  set +a
fi

tls_args=()
if [[ -n "${IOT_MQTT_TLS_CERT:-}" || -n "${IOT_MQTT_TLS_KEY:-}" ]]; then
  if [[ -z "${IOT_MQTT_TLS_CERT:-}" || -z "${IOT_MQTT_TLS_KEY:-}" ]]; then
    echo 'IOT_MQTT_TLS_CERT and IOT_MQTT_TLS_KEY must be set together' >&2
    exit 78
  fi
  tls_args+=("-mqtt-tls-cert" "$IOT_MQTT_TLS_CERT" "-mqtt-tls-key" "$IOT_MQTT_TLS_KEY")
  if [[ -n "${IOT_MQTT_TLS_CLIENT_CA:-}" ]]; then
    tls_args+=("-mqtt-tls-client-ca" "$IOT_MQTT_TLS_CLIENT_CA")
  fi
elif [[ "${IOT_MQTT_ALLOW_PLAINTEXT:-false}" != true ]]; then
  echo 'MQTT TLS is required; set IOT_MQTT_TLS_CERT/KEY or explicitly enable IOT_MQTT_ALLOW_PLAINTEXT=true for local testing' >&2
  exit 78
fi

exec /home/xfusion/projects/odoo20iot/bin/iot-gateway \
  -http-addr "${IOT_GATEWAY_HTTP_ADDR:-127.0.0.1:18080}" \
  -mqtt-addr "${IOT_MQTT_ADDR:-100.64.0.3:1883}" \
  -nats-url "${IOT_NATS_URL:-nats://100.64.0.3:28222}" \
  -redis-url "${IOT_REDIS_URL:-redis://100.64.0.3:28637/0}" \
  -pg-dsn "${IOT_PG_DSN:-postgres://iot:iot_dev_only_change_me@100.64.0.3:28543/odoo20iot}" \
  -auth-source "${IOT_AUTH_SOURCE:-pg}" \
  -auth-file "${IOT_AUTH_FILE:-tmp/dev-credentials.json}" \
  -project "${IOT_GATEWAY_PROJECT:-spike}" \
  -cluster-node-id "${IOT_GATEWAY_CLUSTER_NODE_ID:-}" \
  -cluster-peers "${IOT_GATEWAY_CLUSTER_PEERS:-}" \
  -log-json="${IOT_LOG_JSON:-true}" \
  "${tls_args[@]}"
