# 服务编排模板

服务统一通过宿主机用户级 systemd 启动；二进制由 `devbox` 内的 Go 环境构建，
通过共享目录运行。连接器和查询服务都使用 Tailscale 地址访问 IoT 中间件。

## 配置

创建 `/home/xfusion/etc/odoo20iot.env`，权限设为 `0600`，至少包含：

```dotenv
IOT_QUERY_DEV_TOKEN=replace-with-a-random-token
IOT_QUERY_AUTH_MODE=dev
IOT_QUERY_DEV_PROJECT_ID=1
IOT_QUERY_ALLOW_DEV_LAN=true
IOT_CONFIG_KEY=replace-with-the-existing-key
ODOO_API_KEY=replace-at-deploy-time
ODOO_URL=http://100.64.0.3:9070
ODOO_DB=odoo20
ODOO_ALARM_TO_ODOO=true
```

生产环境应切换 `IOT_QUERY_AUTH_MODE=jwt`，并配置 JWKS、issuer、audience 和 Redis 吊销表。

## 安装

```bash
mkdir -p /home/xfusion/etc
chmod 700 /home/xfusion/etc
chmod 600 /home/xfusion/etc/odoo20iot.env
systemctl --user daemon-reload
systemctl --user enable --now odoo20iot-svc-query.service
systemctl --user enable --now odoo20iot-svc-alarm.service
systemctl --user enable --now odoo20iot-odoo-connector.service
systemctl --user enable --now odoo20iot-svc-notify.service
systemctl --user enable --now odoo20iot-iot-gateway.service
systemctl --user enable --now odoo20iot-svc-quota.service
```

网关 HTTP 设备接入：`POST /ingest/v1/devices/{device_key}/{stream}`。设备使用
`X-Device-Secret` 认证并提交 JSON；该入口只在设备认证器启用时注册，匿名开发模式不会开放，避免无法确认租户归属。

真实密钥禁止写入 unit、命令行、Git 或日志。

构建并启动连接器：

```bash
make build
curl -fsS http://127.0.0.1:18091/healthz
curl -fsS http://127.0.0.1:18091/readyz
```

飞书告警由 `svc-notify` 发送。将机器人 Webhook 放入受保护的
`IOT_NOTIFY_WEBHOOK_URL`，并把 `open.feishu.cn` 配入
`IOT_NOTIFY_EGRESS_ALLOW`；不要通过命令行参数传递 Webhook。

网关启动前必须在环境文件配置 `IOT_MQTT_TLS_CERT` 和
`IOT_MQTT_TLS_KEY`；设置 `IOT_MQTT_TLS_CLIENT_CA` 后启用 mTLS。
未配置证书时启动脚本默认拒绝明文 MQTT，只有本地临时测试才允许显式设置
`IOT_MQTT_ALLOW_PLAINTEXT=true`。
